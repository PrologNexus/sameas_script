:- module(nonreflexive, [run/0]).

/** <module> Script for processing the file `sameas-ranking.tsv.gz'

[3]0.5, [4]1, [5]0, [6]1, [7]0

3 - the Error Degree (which is a value between 0 and 1; 0 means probably correct, 1 means probably erroneous),
4 - the Weight of the link (if duplicate symmetric the weight is 2, if not the weight is 1),
5 - the equality set ID in which this link belongs,
6 - the number of terms in this equality set,
7 - the community ID (if it's a number (e.g. 23) then it's an intra-community link relating two terms that belong to the community 23, if the ID is two numbers related by a dash (e.g. 3-5) it means that it is an inter-community link relating terms from the community 3 and the community 5.

0.99 undirected different-namespaces
0.4 undirected different-namespaces
*/

:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(settings)).

:- use_module(library(file_ext)).
:- use_module(library(semweb/hdt_dataset)).
:- use_module(library(semweb/rdf_export)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(stream_ext)).

:- maplist(rdf_register_prefix, [
     comm-'https://krr.triply.cc/krr/sameas-meta/id/comm/',
     def-'https://krr.triply.cc/krr/sameas-meta/def/',
     eq-'https://krr.triply.cc/krr/sameas-meta/id/eq/',
     link-'https://krr.triply.cc/krr/sameas-meta/id/link/'
   ]).

run :-
  hdt_set_default_graph('explicit.hdt'),
  read_write_files('sameas-ranking.tsv.gz', 'nonreflexive.ttl.gz', run).

run(In, Out) :-
  rdf_write_triple(Out, def:'SymmetricIdentityStatement', rdfs:subClassOf, def:'IdentityStatement'),
  flag(link_id, _, 0),
  forall(
    stream_line(In, Line),
    run_line(Out, Line)
  ).

run_line(Out, Line) :-
  flag(link_id, LinkN, LinkN+1),
  atom_number(LinkLocal, LinkN),
  split_string(Line, "	", "", [S1_|T1]),
  atom_string(S1, S1_),
  once(append(T2, [Err_,Dir,EqName_,Terms_,CommName_], T1)),
  maplist(number_string, [Err,Terms], [Err_,Terms_]),
  atomic_list_concat(T2, '	', O1),
  maplist(atom_string, [CommName,EqName], [CommName_,EqName_]),
  from_to_community(CommName, FromName1-ToName1),
  (   Dir == "1"
  ->  from_to(S1-O1, S2-O2, FromName1-ToName1, FromName2-ToName2),
      format_link(Out, LinkLocal, S2-O2, Err, EqName, Terms, FromName2-ToName2, Link),
      rdf_write_triple(Out, Link, rdf:type, def:'IdentityStatement')
  ;   format_link(Out, LinkLocal, S1-O1, Err, EqName, Terms, FromName1-ToName1, Link1),
      format_link(Out, LinkLocal, O1-S1, Err, EqName, Terms, ToName1-FromName1, Link2),
      rdf_write_triple(Out, Link1, rdf:type, def:'SymmetricIdentityStatement'),
      rdf_write_triple(Out, Link2, rdf:type, def:'SymmetricIdentityStatement')
  ).

format_link(Out, LinkLocal, S-O, Err, EqName, Terms, FromName-ToName, Link) :-
  rdf_prefix_iri(link, LinkLocal, Link),
  rdf_write_triple(Out, Link, rdf:subject, S),
  rdf_write_triple(Out, Link, rdf:predicate, owl:sameAs),
  rdf_write_triple(Out, Link, rdf:object, O),
  rdf_write_triple(Out, Link, def:error, double(Err)),
  format(string(LinkLabel), "Identity statement ~a", [LinkLocal]),
  rdf_write_triple(Out, Link, rdfs:label, LinkLabel-[en,us]),
  community(Out, Link, EqName, FromName, ToName),
  rdf_prefix_iri(eq, EqName, Eq),
  rdf_write_triple(Out, Eq, rdf:type, def:'EquivalenceSet'),
  rdf_write_triple(Out, Eq, def:cardinality, nonneg(Terms)),
  format(string(EqLabel), "Equivalence set ~a", [EqName]),
  rdf_write_triple(Out, Eq, rdfs:label, EqLabel-[en,us]).

% link within a community
community(Out, Link, EqName, CommName, CommName) :- !,
  rdf_prefix_iri(eq, EqName, Eq),
  community_iri(EqName, CommName, Comm),
  community_label(EqName, CommName, CommLabel),
  rdf_write_triple(Out, Link, def:community, Comm),
  rdf_write_triple(Out, Comm, rdf:type, def:'Community'),
  rdf_write_triple(Out, Comm, def:equivalenceSet, Eq),
  format(string(CommLabel), "Community ~a in equivalence set ~a", [CommName,EqName]),
  rdf_write_triple(Out, Comm, rdfs:label, CommLabel-[en,us]).
% link between two communities
community(Out, Link, EqName, FromName, ToName) :-
  rdf_prefix_iri(eq, EqName, Eq),
  maplist(community_iri(EqName), [FromName,ToName], [From,To]),
  maplist(community_label(EqName), [FromName,ToName], [FromLabel,ToLabel]),
  % from community
  rdf_write_triple(Out, Link, def:fromCommunity, From),
  rdf_write_triple(Out, From, rdf:type, def:'Community'),
  rdf_write_triple(Out, From, def:equivalenceSet, Eq),
  rdf_write_triple(Out, From, rdfs:label, FromLabel-[en,us]),
  % to community
  rdf_write_triple(Out, Link, def:toCommunity, To),
  rdf_write_triple(Out, To, rdf:type, def:'Community'),
  rdf_write_triple(Out, To, def:equivalenceSet, Eq),
  rdf_write_triple(Out, To, rdfs:label, ToLabel-[en,us]).

community_iri(EqName, CommName, Comm) :-
  atomic_list_concat([EqName,CommName], -, CommLocal),
  rdf_prefix_iri(comm, CommLocal, Comm).

community_label(EqName, CommName, CommLabel) :-
  format(string(CommLabel), "Community ~a in equivalence set ~a", [CommName,EqName]).

from_to(S-O, S-O, FromName-ToName, FromName-ToName) :-
  hdt_tp(S, owl:sameAs, O), !.
from_to(S-O, O-S, FromName-ToName, ToName-FromName).

from_to_community(Comm, From1-To1) :-
  atomic_list_concat([From1,To1], -, Comm), !.
from_to_community(Comm, Comm-Comm).
