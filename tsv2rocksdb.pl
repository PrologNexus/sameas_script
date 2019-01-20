:- module(tsv2rocksdb, [run/0]).

/** <module> TSV-2-RocksDB

Example invocation:

```sh
$ swipl -s tsv2rocksdb.pl -g run -t halt -- <explicit-sorted.tsv.gz> <rocksdb-directory>
```

---

@author Wouter Beek
@version 2018-2019
*/

:- use_module(library(apply)).
:- use_module(library(ordsets)).
:- use_module(library(readutil)).
:- use_module(library(rlimit)).
:- use_module(library(yall)).

:- use_module(library(conf_ext)).
:- use_module(library(debug_ext)).
:- use_module(library(file_ext)).
:- use_module(library(rocksdb)).

:- debug(tsv2rocksdb).

:- initialization
   rlimit(nofile, _, 250 000).

run :-
  cli_arguments([TsvFile,RocksdbDir]),
  flag(number_of_sets, _, 0),
  debug_time(tsv2rocksdb, "Start time: ~w"),
  maplist(directory_file_path(RocksdbDir), [term2id,id2terms], [Term2idDir,Id2termsDir]),
  maplist(rocksdb_clear, [Term2idDir,Id2termsDir]),
  setup_call_cleanup(
    (
      rocksdb_open(Term2idDir, Term2idDb, [key(atom),value(int64)]),
      rocksdb_open(Id2termsDir, Id2termsDb, [key(int64),merge(rocksdb_merge_set),value(term)])
    ),
    forall(
      file_line(TsvFile, Line),
      (
        split_string(Line, "\t", "", [X0,Y0]),
        maplist(atom_string, [X,Y], [X0,Y0]),
        id_add(Term2idDb, Id2termsDb, X, Y)
      )
    ),
    maplist(rocksdb_close, [Term2idDb,Id2termsDb])
  ),
  debug_time(tsv2rocksdb, "End time: ~w").



%! id_add(+Term2idDb:blob, +Id2termsDb:blob, +X:atom, +Y:atom) is det.
%
% Adds the given identity pair to the identity index.  Assumes that X
% @< Y.

id_add(Term2idDb, Id2termsDb, X, Y) :-
  % This task is split into three cases: (1) both X and Y already
  % denote an identity set, (2) only X or Y denotes an identity set,
  % (3) neither X nor Y denotes an identity set.
  (   rocksdb_key_value(Term2idDb, X, XId)
  ->  (   rocksdb_key_value(Term2idDb, Y, YId)
      ->  both(Term2idDb, Id2termsDb, XId, YId)
      ;   only_one(Term2idDb, Id2termsDb, XId, Y)
      )
  ;   (   rocksdb_key_value(Term2idDb, Y, YId)
      ->  only_one(Term2idDb, Id2termsDb, YId, X)
      ;   neither(Term2idDb, Id2termsDb, X, Y)
      )
  ).



%! both(+Term2idDb:blob, +Id2termsDb:blob, +XId:atom, +YId:atom) is det.
%
% The case in which both X and Y are already mapped to an identity set
% ID.

% X and Y are already in the same identity set.
both(_, _, Id, Id) :- !.
% X and Y are in different identity sets.  These must now be merged.
both(Term2idDb, Id2termsDb, XId, YId) :-
  rocksdb_key_value(Id2termsDb, XId, Xs),
  rocksdb_key_value(Id2termsDb, YId, Ys),
  length(Xs, NumXs),
  length(Ys, NumYs),
  % For efficiency, we want to change the /smallest/ identity set.
  (   NumXs >= NumYs
  ->  both_(Term2idDb, Id2termsDb, XId, YId, Ys)
  ;   both_(Term2idDb, Id2termsDb, YId, XId, Xs)
  ).

both_(Term2idDb, Id2termsDb, XId, YId, Ys) :-
  % Merge Y's idenity set into X's.
  rocksdb_merge_set_(Id2termsDb, XId, Ys),
  % Now that the two identity sets are merged, let all members of Y's
  % original identity set point to X's identity set.
  maplist({XId}/[Y,Action]>>(Action = put(Y,XId)), Ys, Actions),
  rocksdb_batch(Term2idDb, Actions),
  % Remove Y's original identity set.
  rocksdb_delete(Id2termsDb, YId).

rocksdb_merge_set_(Id2termsDb, Id, Ys) :-
  rocksdb_key_value(Id2termsDb, Id, Xs),
  ord_union(Xs, Ys, Zs),
  rocksdb_put(Id2termsDb, Id, Zs).



%! only_one(+Term2idDb:blob, +Id2termsDb:blob, +XId:atom, +Y:atom) is det.
%
% The case in which X is already mapped on an identity set, and Y is
% added to it.

only_one(Term2idDb, Id2termsDb, XId, Y) :-
  % Merge Y into X's identity set.
  rocksdb_merge_element_(Id2termsDb, XId, Y),
  % Let Y point to this identity set.
  rocksdb_put(Term2idDb, Y, XId).

rocksdb_merge_element_(Id2termsDb, Id, Y) :-
  rocksdb_key_value(Id2termsDb, Id, Xs),
  ord_add_element(Xs, Y, Zs),
  rocksdb_put(Id2termsDb, Id, Zs).



%! neither(+Term2idDb:blob, +Id2termsDb:blob, +X:atom, +Y:atom) is det.
%
% The case in which neither X nor Y are mapped to an identity set.

neither(Term2idDb, Id2termsDb, X, Y) :-
  % Create one new identity set to which X and Y belong.
  flag(number_of_sets, Id, Id+1),
  rocksdb_put(Id2termsDb, Id, [X,Y]),
  % Let X and Y point to this new identity set.
  rocksdb_batch(Term2idDb, [put(X,Id),put(Y,Id)]).
