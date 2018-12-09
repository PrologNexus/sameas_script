:- module(hdt2csv, [run/0]).

/** <module> HDT-2-CSV

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).
:- use_module(library(yall)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(semweb/hdt_api)).
:- use_module(library(semweb/hdt_dataset)).
:- use_module(library(semweb/rdf_term)).

run :-
  cli_argument(lod_a_lot, FromFile),
  hdt_call_file(FromFile, run_).
run_(Hdt) :-
  cli_argument(dir, ., ToDir),
  directory_file_path(ToDir, 'id-unsorted.tsv.gz', ToFile),
  write_to_file(ToFile, run_(Hdt)).
run_(Hdt, Out) :-
  forall(
    (
      hdt_tp0(Hdt, S, owl:sameAs, O),
      % This also ensures that S \== O.
      sort([S,O], Terms)
    ),
    (
      maplist(rdf_atom_term, Atoms, Terms),
      format(Out, "~a\t~a\n", Atoms)
    )
  ).
