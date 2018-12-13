:- module(tsv2rocks, [run/0]).

/** <module> TSV-2-RocksDB

The location of the input HDT file can be specified with the
`--input=FILE' command-line argument.

The directory of the output RocksDB subdirectories can be specified
with the `--output=DIRECTORY' command-line argument.

Example invocation:

```sh
$ swipl -s tsv2rocks.pl -g run -t halt --intput=/abc/xyz/data.tsv.gz --output=/abc/xyz/
```

---

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(filesex)).
:- use_module(library(ordsets)).
:- use_module(library(readutil)).
:- use_module(library(rlimit)).
:- use_module(library(yall)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(rocksdb)).

:- debug(id).

:- initialization
   rlimit(nofile, _, 10 000).

run :-
  cli_argument(input, 'id-sorted.tsv.gz', File),
  flag(number_of_sets, _, 0),
  (   debugging(id)
  ->  get_time(Start),
      debug(id, "Start time: ~w", [Start]),
      flag(number_of_pairs, _, 0)
  ;   true
  ),
  read_from_file(File, run),
  (   debugging(id)
  ->  get_time(End),
      debug(id, "End time: ~w", [End])
  ;   true
  ).

run(In) :-
  cli_argument(output, ., Dir),
  maplist(directory_file_path(Dir), [term_id,id_terms], [Dir1,Dir2]),
  maplist(rocksdb_clear, [Dir1,Dir2]),
  setup_call_cleanup(
    (
      rocksdb_open(Dir1, Term2id, [key(atom),value(int64)]),
      rocksdb_open(Dir2, Id2terms, [key(int64),merge(rocksdb_merge_set),value(term)])
    ),
    (
      repeat,
      read_line_to_string(In, Line),
      (   Line == end_of_file
      ->  !
      ;   split_string(Line, "\t", "", [X0,Y0]),
          maplist(atom_string, [X,Y], [X0,Y0]),
          id_add(Term2id, Id2terms, X, Y),
          (   debugging(id)
          ->  flag(number_of_pairs, N, N+1),
              (N mod 10 000 =:= 0 -> debug(id, "~D", [N]) ; true)
          ;   true
          ),
          fail
      )
    ),
    maplist(rocksdb_close, [Term2id,Id2terms])
  ).



%! id_add(+Term2id:blob, +Id2terms:blob, +X:atom, +Y:atom) is det.
%
% Adds the given identity pair to the identity index.  Assumes that X
% @< Y.

id_add(Term2id, Id2terms, X, Y) :-
  % This task is split into three cases: (1) both X and Y already
  % denote an identity set, (2) only X or Y denotes an identity set,
  % (3) neither X nor Y denotes an identity set.
  (   rocksdb_key_value(Term2id, X, XId)
  ->  (   rocksdb_key_value(Term2id, Y, YId)
      ->  both(Term2id, Id2terms, XId, YId)
      ;   only_one(Term2id, Id2terms, XId, Y)
      )
  ;   (   rocksdb_key_value(Term2id, Y, YId)
      ->  only_one(Term2id, Id2terms, YId, X)
      ;   neither(Term2id, Id2terms, X, Y)
      )
  ).



%! both(+Term2id:blob, +Id2terms:blob, +XId:atom, +YId:atom) is det.
%
% The case in which both X and Y are already mapped to an identity set
% ID.

% X and Y are already in the same identity set.
both(_, _, Id, Id) :- !.
% X and Y are in different identity sets.  These must now be merged.
both(Term2id, Id2terms, XId, YId) :-
  rocksdb_key_value(Id2terms, XId, Xs),
  rocksdb_key_value(Id2terms, YId, Ys),
  length(Xs, NumXs),
  length(Ys, NumYs),
  % For efficiency, we want to change the /smallest/ identity set.
  (   NumXs >= NumYs
  ->  both_(Term2id, Id2terms, XId, YId, Ys)
  ;   both_(Term2id, Id2terms, YId, XId, Xs)
  ).

both_(Term2id, Id2terms, XId, YId, Ys) :-
  % Merge Y's idenity set into X's.
  rocksdb_merge_set_(Id2terms, XId, Ys),
  % Now that the two identity sets are merged, let all members of Y's
  % original identity set point to X's identity set.
  maplist({XId}/[Y,Action]>>(Action = put(Y,XId)), Ys, Actions),
  rocksdb_batch(Term2id, Actions),
  % Remove Y's original identity set.
  rocksdb_delete(Id2terms, YId).

rocksdb_merge_set_(Id2terms, Id, Ys) :-
  rocksdb_key_value(Id2terms, Id, Xs),
  ord_union(Xs, Ys, Zs),
  rocksdb_put(Id2terms, Id, Zs).



%! only_one(+Term2id:blob, +Id2terms:blob, +XId:atom, +Y:atom) is det.
%
% The case in which X is already mapped on an identity set, and Y is
% added to it.

only_one(Term2id, Id2terms, XId, Y) :-
  % Merge Y into X's identity set.
  rocksdb_merge_element_(Id2terms, XId, Y),
  % Let Y point to this identity set.
  rocksdb_put(Term2id, Y, XId).

rocksdb_merge_element_(Id2terms, Id, Y) :-
  rocksdb_key_value(Id2terms, Id, Xs),
  ord_add_element(Xs, Y, Zs),
  rocksdb_put(Id2terms, Id, Zs).



%! neither(+Term2id:blob, +Id2terms:blob, +X:atom, +Y:atom) is det.
%
% The case in which neither X nor Y are mapped to an identity set.

neither(Term2id, Id2terms, X, Y) :-
  % Create one new identity set to which X and Y belong.
  flag(number_of_sets, Id, Id+1),
  rocksdb_put(Id2terms, Id, [X,Y]),
  % Let X and Y point to this new identity set.
  rocksdb_batch(Term2id, [put(X,Id),put(Y,Id)]).
