:- module(hdt2tsv, [run/0]).

/** <module> HDT-2-TSV

Example invocation:

```sh
$ swipl -s hdt2tsv.pl -g run -t halt -- <explicit.hdt> <explicit-unsorted.tsv.gz>
```

---

@author Wouter Beek
@version 2017-2019
*/

:- use_module(library(apply)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(semweb/hdt_file)).
:- use_module(library(semweb/rdf_term)).

run :-
  cli_arguments([HdtFile,TsvFile]),
  hdt_call_file(HdtFile, run(TsvFile)).

run(TsvFile, Hdt) :-
  write_to_file(TsvFile, run_stream(Hdt)).

run_stream(Hdt, Out) :-
  forall(
    (
      hdt_tp0(Hdt, S, owl:sameAs, O),
      sort([S,O], [X,Y])
    ),
    (
      maplist(rdf_atom_term, Atoms, [X,Y]),
      format(Out, "~a\t~a\n", Atoms)
    )
  ).
