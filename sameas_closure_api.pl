:- module(
  sameas_closure_api,
  [
    id/1,             % ?Id
    sameas_closure/2, % ?Term1, ?Term2
    term/1,           % ?Term
    term_id/2         % ?Term, ?Id
  ]
).

/** <module> owl:sameAs closure API

Invocation:

```sh
swipl -s sameas_closure_api.pl -- <rocksdb-directory>
```

---

@author Wouter Beek
@version 2017-2019
*/

:- use_module(library(apply)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(rocksdb)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).

:- at_halt(deinit_rocksdb).

:- initialization
   init_rocksdb.

:- rdf_meta
   sameas_closure(o, o),
   term(o),
   term_id(o, ?).





%! id(+Id:nonneg) is semidet.
%! id(-Id:nonneg) is nondet.

id(Id) :-
  rocksdb_key(id2terms, Id).



%! sameas_closure(+Term1:rdf_atom, +Term2:rdf_atom) is semidet.
%! sameas_closure(+Term1:rdf_atom, -Term2:rdf_atom) is nondet.
%! sameas_closure(-Term1:rdf_atom, +Term2:rdf_atom) is nondet.
%! sameas_closure(-Term1:rdf_atom, -Term2:rdf_atom) is nondet.
%
% Read from the identity store.

% reverse order
sameas_closure(Term1, Term2) :-
  var(Term1),
  ground(Term2), !,
  sameas_closure(Term2, Term1).
% present in closure
sameas_closure(Term1, Term2) :-
  term_id(Term1, Id), !,
  term_id(Term2, Id).
% not present in closure (singleton set)
sameas_closure(Term, Term).



%! term(+Term:rdf_term) is semidet.
%! term(-Term:rdf_term) is nondet.

term(Term) :-
  rocksdb_key(term2id, Term).



%! term_id(+Term:rdf_term, +Id:nonneg) is semidet.
%! term_id(+Term:rdf_term, -Id:nonneg) is semidet.
%! term_id(-Term:rdf_term, +Id:nonneg) is nondet.
%! term_id(-Term:rdf_term, -Id:nonneg) is nondet.

term_id(Term, Id) :-
  ground(Term), !,
  rdf_atom_term(Atom, Term),
  rocksdb_key_value(term2id, Atom, Id).
term_id(Term, Id) :-
  ground(Id), !,
  rocksdb_key_value(id2terms, Id, Atoms),
  member(Atom, Atoms),
  rdf_atom_term(Atom, Term).
term_id(Term, Id) :-
  rocksdb_key_value(term2id, Atom, Id),
  rdf_atom_term(Atom, Term).





% (DE)INITIALIZATION %

deinit_rocksdb :-
  maplist(rocksdb_close, [id2terms,term2id]).



init_rocksdb :-
  gtrace,
  cli_arguments([Dir0]),
  absolute_file_name(Dir0, Dir, [access(read),file_type(directory)]),
  maplist(directory_file_path(Dir), [id2terms,term2id], [Id2termsDir,Term2idDir]),
  rocksdb_open(Id2termsDir, _, [alias(id2terms),key(int64),merge(rocks_merge_set),mode(read_only),value(term)]),
  rocksdb_open(Term2idDir, _, [alias(term2id),key(atom),mode(read_only),value(int64)]).
