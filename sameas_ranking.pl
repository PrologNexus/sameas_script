:- module(sameas_ranking, [run/0]).

/** <module> owl:sameAs ranking

@author Wouter Beek
@version 2018/12
*/

:- use_module(library(apply)).
:- use_module(library(settings)).

:- use_module(library(conf_ext)).
:- use_module(library(csv_ext)).
:- use_module(library(file_ext)).
:- use_module(library(rocksdb)).
:- use_module(library(semweb/rdf_api)).
:- use_module(library(semweb/rdf_mem), []).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).

:- initialization
   rlimit(nofile, _, 10 000).

:- maplist(rdf_register_prefix, [
     owl,
     sameas-'https://sameas.cc/def/',
     set-'https://sameas.cc/id/set/'
   ]).

:- set_setting(rdf_term:bnode_prefix_scheme, https).
:- set_setting(rdf_term:bnode_prefix_authority, 'sameas.cc').

run :-
  cli_argument(input, FromFile),
  read_from_file(FromFile, run).

run(In) :-
  cli_argument(output, ToDir),
  maplist(
    directory_file_path(ToDir),
    [id_terms,pair_rank,term_id],
    [Dir1,Dir2,Dir3]
  ),
  setup_call_cleanup(
    (
      rocksdb_open(Dir1, Id2terms, [key(int64),merge(rocksdb_merge_set),value(term)]),
      rocksdb_open(Dir2, Pair2rank, [key(atom),value(double)]),
      rocksdb_open(Dir3, Term2id, [key(atom),value(int64)])
    ),
    forall(
      csv_read_stream_row(In, row(X,Y,Err,Weight,Id,Size,_), [separator(0'\t)]),
      run(Id2terms, Pair2rank, Term2id, X, Y, Err, Weight, Id, Size)
    ),
    maplist(rocksdb_close, [Id2terms,Pair2rank,Term2id])
  ).

run(Id2terms, Pair2rank, Term2id, X0, Y0, Err, Weight, Id, Size) :-
  sort([X0,Y0], [X,Y]),
  rocksdb_merge(Id2terms, Id, [X,Y]),
  atomic_list_concat([X,Y], -, Key),
  rocksdb_put(Pair2rank, Key, Err),
  rocksdb_batch(Term2id, [put(X,Id),put(Y,Id)]),
  assert_link(X, Y, Err, Id, Size),
  (Weight =:= 2 -> assert_link(Y, X, Err, Id, Size) ; true).

assert_link(X, Y, Err, Id, Size) :-
  rdf_default_graph(G),
  assert_reification(mem(G), X, owl:sameAs, Y, Stmt),
  assert_instance(mem(G), Stmt, sameas:'IdentityLink'),
  assert_triple(mem(G), Stmt, sameas:errorDegree, Err),
  atom_number(Local, Id),
  rdf_prefix_iri(set, Local, Set),
  assert_triple(mem(G), Stmt, sameas:identitySet, Set),
  assert_instance(mem(G), Set, sameas:'IdentitySet'),
  assert_triple(mem(G), Set, sameas:identity, Id),
  assert_triple(mem(G), Set, sameas:size, nonneg(Size)).
