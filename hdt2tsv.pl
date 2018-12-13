:- module(hdt2csv, [run/0]).

/** <module> HDT-2-CSV

The location of the input HDT file can be specified with the
`--input=FILE' command-line argument.

The location of the output TSV file can be specified with the
`--output=FILE' command-line argument.  If `FILE' ends with `.gz' then
GNU zip compression is used.

Example invocation:

```sh
$ swipl -s hdt2tsv.pl -g run -t halt --intput=/abc/xyz/data.hdt --output=/abc/xyz/id-unsorted.tsv.gz
```

---

@author Wouter Beek
@version 2017-2018
*/

:- use_module(library(apply)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(semweb/hdt_file)).
:- use_module(library(semweb/rdf_term)).

run :-
  cli_argument(input, FromFile),
  hdt_call_file(FromFile, run).

run(Hdt) :-
  cli_argument(output, 'id-unsorted.tsv.gz', ToFile),
  write_to_file(ToFile, run(Hdt)).

run(Hdt, Out) :-
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
