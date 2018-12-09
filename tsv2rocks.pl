:- module(tsv2rocks, [run/0]).

/** <module> TSV-2-RocksDB

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

run :-
  maplist(rocks_clear, [term_id,id_terms]),
  flag(number_of_sets, _, 0),
  (   debugging(id)
  ->  % Show the maximum number of open files.
      rlimit(nofile, NumFiles, NumFiles),
      debug(id, "Number of files: ~D", [NumFiles]),
      % Show the starting time.
      get_time(Start),
      debug(id, "Start time: ~w", [Start]),
      % Keep track of the number of processed identity pairs.
      flag(number_of_pairs, _, 0)
  ;   true
  ),
  cli_argument(dir, ., Dir),
  directory_file_path(Dir, 'id-sorted.tsv.gz', File),
  read_from_file(File, run_).
run_(In) :-
  setup_call_cleanup(
    (
      rocks_init(term_id, [key(atom),value(int64)]),
      rocks_init(id_terms, [key(int64),merge(rocks_merge_set),value(term)])
    ),
    (
      repeat,
      read_line_to_string(In, Line),
      (   Line == end_of_file
      ->  !
      ;   split_string(Line, "\t", "", [X0,Y0]),
          maplist(atom_string, [X,Y], [X0,Y0]),
          id_add(X, Y),
          (   debugging(id)
          ->  flag(number_of_pairs, N, N+1),
              (N mod 10 000 =:= 0 -> debug(id, "~D", [N]) ; true)
          ;   true
          ),
          fail
      )
    ),
    maplist(rocks_close, [term_id,id_terms])
  ),
  % Show the end time.
  (   debugging(id)
  ->  get_time(End),
      debug(id, "End time: ~w", [End])
  ;   true
  ).



%! id_add(+X:atom, +Y:atom) is det.
%
% Adds the given identity pair to the identity index.  Assumes that X
% @< Y.

id_add(X, Y) :-
  % This task is split into three cases: (1) both X and Y already
  % denote an identity set, (2) only X or Y denotes an identity set,
  % (3) neither X nor Y denotes an identity set.
  (   rocks_get(term_id, X, XId)
  ->  (   rocks_get(term_id, Y, YId)
      ->  both(XId, YId)
      ;   only_one(XId, Y)
      )
  ;   (   rocks_get(term_id, Y, YId)
      ->  only_one(YId, X)
      ;   neither(X, Y)
      )
  ).

%! both(+XId:atom, +YId:atom) is det.
%
% The case in which both X and Y are already mapped to an identity set
% ID.

% X and Y are already in the same identity set.
both(Id, Id) :- !.
% X and Y are in different identity sets.  These must now be merged.
both(XId, YId) :-
  rocks_get(id_terms, XId, Xs),
  rocks_get(id_terms, YId, Ys),
  length(Xs, NumXs),
  length(Ys, NumYs),
  % For efficiency, we want to change the /smallest/ identity set.
  (   NumXs >= NumYs
  ->  both0(XId, YId, Ys)
  ;   both0(YId, XId, Xs)
  ).

both0(XId, YId, Ys) :-
  % Merge Y's idenity set into X's.
  rocks_merge_set(XId, Ys),
  % Now that the two identity sets are merged, let all members of Y's
  % original identity set point to X's identity set.
  maplist({XId}/[Y,Action]>>(Action = put(Y,XId)), Ys, Actions),
  rocks_batch(term_id, Actions),
  % Remove Y's original identity set.
  rocks_delete(id_terms, YId).



%! only_one(+XId:atom, +Y:atom) is det.
%
% The case in which X is already mapped on an identity set, and Y is
% added to it.

only_one(XId, Y) :-
  % Merge Y into X's identity set.
  rocks_merge_element(XId, Y),
  % Let Y point to this identity set.
  rocks_put(term_id, Y, XId).



%! neither(+X:atom, +Y:atom) is det.
%
% The case in which neither X nor Y are mapped to an identity set.

neither(X, Y) :-
  % Create one new identity set to which X and Y belong.
  flag(number_of_sets, Id, Id+1),
  rocks_put(id_terms, Id, [X,Y]),
  % Let X and Y point to this new identity set.
  rocks_batch(term_id, [put(X,Id),put(Y,Id)]).





% HELPERS %

rocks_merge_element(Id, Y) :-
  rocks_get(id_terms, Id, Xs),
  ord_add_element(Xs, Y, Zs),
  rocks_put(id_terms, Id, Zs).

rocks_merge_set(Id, Ys) :-
  rocks_get(id_terms, Id, Xs),
  ord_union(Xs, Ys, Zs),
  rocks_put(id_terms, Id, Zs).
