:- module(nonreflexive, [run/0]).

/** <module> Script for processing the file `sameas-ranking.tsv.gz'

[3]0.5, [4]1, [5]0, [6]1, [7]0

3 - the Error Degree (which is a value between 0 and 1; 0 means probably correct, 1 means probably erroneous),
4 - the Weight of the link (if duplicate symmetric the weight is 2, if not the weight is 1),
5 - the equality set ID in which this link belongs,
6 - the number of links in this equality set,
7 - the community ID (if it's a number (e.g. 23) then it's an intra-community link relating two terms that belong to the community 23, if the ID is two numbers related by a dash (e.g. 3-5) it means that it is an inter-community link relating terms from the community 3 and the community 5.

0.99 undirected different-namespaces
0.4 undirected different-namespaces
*/

:- use_module(library(apply)).
:- use_module(library(lists)).
:- use_module(library(settings)).

:- use_module(library(file_ext)).
:- use_module(library(semweb/hdt_dataset)).
:- use_module(library(semweb/rdf_prefix)).
:- use_module(library(semweb/rdf_term)).
:- use_module(library(stream_ext)).

:- maplist(rdf_register_prefix, [
     comm-'https://sameas.cc/id/comm/',
     def-'https://sameas.cc/def/',
     eq-'https://sameas.cc/id/eq/',
     link-'https://sameas.cc/id/link/'
   ]).

run :-
  hdt_set_default_graph('explicit.hdt'),
  read_write_files('sameas-ranking.tsv.gz', 'nonreflexive.ttl.gz', run).

run(In, Out) :-
  format_prefixes(Out),
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
  once(append(T2, [Err,Dir,Eq0,Links,Comm0], T1)),
  atomic_list_concat(T2, '	', O1),
  maplist(atom_string, [Comm,Eq], [Comm0,Eq0]),
  from_to_community(Comm, From1-To1),
  (   Dir=="1"
  ->  from_to(S1-O1, S2-O2, From1-To1, From2-To2),
      format_link(Out, LinkLocal, S2-O2, Err, Eq, Links, From2-To2, Link),
      format(Out, "<~a> a def:IdentityStatement.\n", [Link])
  ;   format_link(Out, LinkLocal, S1-O1, Err, Eq, Links, From1-To1, Link1),
      format_link(Out, LinkLocal, O1-S1, Err, Eq, Links, To1-From1, Link2),
      format(Out, "<~a> a def:SymmetricIdentityStatement.\n", [Link1]),
      format(Out, "<~a> a def:SymmetricIdentityStatement.\n", [Link2])
  ).

format_link(Out, LinkLocal, S-O_, Err, Eq, Links, From-To, Link) :-
  format_term(O_, O),
  rdf_prefix_iri(link, LinkLocal, Link),
  format(Out, "<~a> a def:IdentityStatement.\n", [Link]),
  format(Out, "<~a> rdf:subject <~a>.\n", [Link,S]),
  format(Out, "<~a> rdf:predicate owl:sameAs.\n", [Link]),
  format(Out, "<~a> rdf:object ~a.\n", [Link,O]),
  format(Out, '<~a> def:id "~a".\n', [Link,LinkLocal]),
  format(Out, '<~a> def:error "~s"^^xsd:double.\n', [Link,Err]),
  format(Out, '<~a> rdfs:label "Identity statement ~a"@en-us.\n', [Link,LinkLocal]),
  community(Out, Link, Eq, From, To),
  format(Out, "eq:~a a def:EquivalenceSet.\n", [Eq]),
  format(Out, 'eq:~a def:id "~a".\n', [Eq,Eq]),
  format(Out, 'eq:~a def:numberOfLinks "~s"^^xsd:nonNegativeInteger.\n', [Eq,Links]),
  format(Out, 'eq:~a rdfs:label "Equivalence set ~a."@en-us.\n', [Eq,Eq]).

community(Out, Link, Eq, Comm, Comm) :- !,
  format(Out, "<~a> def:community comm:~a-~a.\n", [Link,Eq,Comm]),
  format(Out, "comm:~a-~a a def:Community.\n", [Eq,Comm]),
  format(Out, "comm:~a-~a def:equivalenceSet eq:~a.\n", [Eq,Comm,Eq]),
  format(Out, 'comm:~a-~a def:id "~a-~a".\n', [Eq,Comm,Eq,Comm]),
  format(Out, 'comm:~a-~a rdfs:label "Community ~a in equivalence set ~a."@en-us.\n', [Eq,Comm,Comm,Eq]).
community(Out, Link, Eq, From, To) :-
  format(Out, "<~a> def:fromCommunity comm:~a-~a.\n", [Link,Eq,From]),
  format(Out, "<~a> def:toCommunity comm:~a-~a.\n", [Link,Eq,To]),
  format(Out, "comm:~a-~a a def:Community.\n", [Eq,From]),
  format(Out, "comm:~a-~a def:equivalenceSet eq:~a.\n", [Eq,From,Eq]),
  format(Out, 'comm:~a-~a def:id "~a-~a".\n', [Eq,From,Eq,From]),
  format(Out, 'comm:~a-~a rdfs:label "Community ~a in equivalence set ~a."@en-us.\n', [Eq,From,From,Eq]),
  format(Out, "comm:~a-~a a def:Community.\n", [Eq,To]),
  format(Out, "comm:~a-~a def:equivalenceSet eq:~a.\n", [Eq,To,Eq]),
  format(Out, 'comm:~a-~a def:id "~a-~a".\n', [Eq,To,Eq,To]),
  format(Out, 'comm:~a-~a rdfs:label "Community ~a in equivalence set ~a."@en-us.\n', [Eq,To,To,Eq]).

format_prefixes(Out) :-
  format(Out, "prefix comm: <https://sameas.cc/id/comm/>\n", []),
  format(Out, "prefix eq: <https://sameas.cc/id/eq/>\n", []),
  format(Out, "prefix owl: <http://www.w3.org/2002/07/owl#>\n", []),
  format(Out, "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n", []),
  format(Out, "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n", []),
  format(Out, "prefix def: <https://sameas.cc/def/>\n", []),
  format(Out, "prefix xsd: <http://www.w3.org/2001/XMLSchema#>\n", []).

format_term(Atom, Atom) :-
  atom_prefix(Atom, '"'), !.
format_term(Atom, Term) :-
  format(atom(Term), "<~a>", [Atom]).

from_to(S-O, S-O, From-To, From-To) :-
  hdt_tp(S, owl:sameAs, O), !.
from_to(S-O, O-S, From-To, To-From).

from_to_community(Comm, From1-To1) :-
  atomic_list_concat([From1,To1], "-", Comm), !.
from_to_community(Comm, Comm-Comm).
