:- module(script, [run/1]).

/** <module> Script for generating input files for alternative identity closures

0.99 undirected different-namespaces
0.4 undirected different-namespaces
*/

:- use_module(library(file_ext)).

run(MaxErr) :-
  format(atom(File), 'pairs-~f.tsv.gz', [MaxErr]),
  write_to_file(File, run(MaxErr)).

run(MaxErr, Out) :-
  forall(
    file_line('sameas-ranking.tsv.gz', Line),
    run_line(MaxErr, Out, Line)
  ).

run_line(MaxErr, Out, Line) :-
  split_string(Line, "\t", "", [S1|T1]),
  atom_string(S2, S1),
  once(append(T2, [Err_,_,_,_,_], T1)),
  number_string(Err, Err_),
  (   Err =< MaxErr
  ->  atomic_list_concat(T2, '\t', O),
      format(Out, "~a\t~a\n", [S2,O])
  ;   true
  ).
