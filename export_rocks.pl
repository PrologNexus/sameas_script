:- module(
  export_rocks,
  [
    export_index/0
  ]
).

/** <module> Export RocksDB

The directory in which the exported files will be written can be
specified through the `--dir="your/dir"' flag.

---

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(archive)).
:- use_module(library(yall)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(rocksdb)).





%! export_index is det.

export_index :-
  cli_argument(dir, Dir),
  export_index_id_terms(Dir, File1),
  export_index_term_id(Dir, File2),
  directory_file_path(Dir, 'sameAs-implicit.tgz', File),
  archive_create(File, [File1,File2], [filter(gzip),format(gnutar)]),
  maplist(delete_file, [File1,File2]).



%! export_index_id_terms(+Directory:atom, -File:atom) is det.

export_index_id_terms(Dir, File) :-
  directory_file_path(Dir, 'id_terms.dat', File),
  write_to_file(
    File,
    [Out]>>(
      forall(
        rocks(id_terms, Id, Terms),
        (
          format(Out, "~a", [Id]),
          maplist(format(Out, " ~a"), Terms),
          format(Out, "\n", [])
        )
      )
    )
  ).



%! export_index_term_id(+Directory:atom, -File:atom) is det.

export_index_term_id(Dir, File) :-
  directory_file_path(Dir, 'term_id.dat', File),
  write_to_file(
    File,
    [Out]>>(
      forall(
        rocks(term_id, Term, Id),
        format(Out, "~a ~a\n", [Term,Id])
      )
    )
  ).
