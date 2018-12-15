:- module(export_rocksdb, [run/0]).

/** <module> Export RocksDB

The input directory containing the RocksDB subdirectories can be
specified with the `--input="/abc/xyz/"' flag.

The output TSV file can be specified with the `--output="/abc/xyz/"'
flag.  The file is archived using GNU tar and compressed using GNU
zip.

Example run:

```sh
$ swipl -s export_rocksdb.pl -g run -t halt --input=/abc/xyz/ --output=/abc/xyz/
```

---

@author Wouter Beek
@version 2018
*/

:- use_module(library(apply)).
:- use_module(library(archive)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(rocksdb)).

run :-
  % CLI arguments
  cli_argument(input, ., FromDir),
  cli_argument(output, ., ToDir),

  export_id_terms(FromDir, ToDir, ToFile1),
  export_term_id(FromDir, ToDir, ToFile2),

  % archive & compress
  directory_file_path(ToDir, 'sameas-implicit.tgz', ToFile),
  archive_create(ToFile, [ToFile1,ToFile2], [filter(gzip),format(gnutar)]),
  maplist(delete_file, [ToFile1,ToFile2]).

export_id_terms(FromDir0, ToDir, ToFile) :-
  directory_file_path(FromDir0, id_terms, FromDir),
  directory_file_path(ToDir, 'id_terms.dat', ToFile),
  call_rocksdb(
    FromDir,
    export_id_terms_hdt(ToFile),
    [key(int64),merge(rocksdb_merge_set),value(term)]
  ).

export_id_terms_hdt(ToFile, Hdt) :-
  write_to_file(ToFile, export_id_terms_stream(Hdt)).

export_id_terms_stream(Hdt, Out) :-
  forall(
    rocksdb_key_value(Hdt, Id, Terms),
    (
      format(Out, "~a", [Id]),
      maplist(format(Out, " ~a"), Terms),
      format(Out, "\n", [])
    )
  ).

export_term_id(FromDir0, ToDir, ToFile) :-
  directory_file_path(FromDir0, term_id, FromDir),
  directory_file_path(ToDir, 'term_id.dat', ToFile),
  call_rocksdb(FromDir, export_term_id_hdt(ToFile), [key(atom),value(int64)]).

export_term_id_hdt(ToFile, Hdt) :-
  write_to_file(ToFile, export_term_id_stream(Hdt)).

export_term_id_stream(Hdt, Out) :-
  forall(
    rocksdb_key_value(Hdt, Term, Id),
    format(Out, "~a ~a\n", [Term,Id])
  ).
