:- module(reflexive, [run/0]).

/** <module> Script for adding reflexive links
*/

run :-
  hdt_add_named_graph('explicit.hdt', G1),
  hdt_add_named_graph('nonreflexive.hdt', G2),
  read_write_files('sameas-ranking.tsv.gz', 'reflexive.ttl.gz', run(G1, G2)).

run(G1, G2, Out) :-
  format_prefixes(Out),
  flag(link_id, _, 1 000 000 000),
  forall(
    hdt_tp(G1, X, owl:sameAs, X),
    run(G2, Out, X)
  ).

run(G2, Out, X) :-
  hdt_tp(G2, X, def:equivalenceSet, Eq),
  %Links
  %Comm
  format_link(Out, LinkLocal, X-X, 0.0, _, Comm-Comm)
