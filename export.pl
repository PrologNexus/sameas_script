:- module(export, [run/0]).

/** <module> Export owl:sameAs closure

Invocation:

```sh
$ swipl -s export_rocksdb.pl -g run -t halt -- <input-directory> <output-directory>
```

When `<output-directory>' is not specified, it is the same as the
`<input-directory>'.

---

@author Wouter Beek
@version 2018-2019
*/

:- use_module(library(apply)).
:- use_module(library(archive)).

:- use_module(library(conf_ext)).
:- use_module(library(file_ext)).
:- use_module(library(rocksdb)).

run :-
  cli_arguments(Args),
  (Args = [DbDir] -> ExportDir = DbDir ; Args = [DbDir,ExportDir]),
  export_id2terms(DbDir, ExportDir, Id2termsFile),
  export_term2id(DbDir, ExportDir, Term2idFile),
  directory_file_path(ExportDir, 'closure.tgz', ExportFile),
  archive_create(ExportFile, [Id2termsFile,Term2idFile], [filter(gzip),format(gnutar)]),
  maplist(delete_file, [Id2termsFile,Term2idFile]).



% ID-2-TERMS

export_id2terms(DbDir, ExportDir, ExportFile) :-
  directory_file_path(DbDir, id2terms, DbSubdir),
  directory_file_path(ExportDir, 'id2terms.dat', ExportFile),
  call_rocksdb(DbSubdir, export_id2terms_handle(ExportFile), [key(int64),merge(rocksdb_merge_set),value(term)]).

export_id2terms_handle(ExportFile, Db) :-
  write_to_file(ExportFile, export_id2terms_stream(Db)).

export_id2terms_stream(Db, Out) :-
  forall(
    rocksdb_key_value(Db, Id, Terms),
    (
      format(Out, "~a", [Id]),
      maplist(format(Out, " ~a"), Terms),
      format(Out, "\n", [])
    )
  ).



% TERM-2-ID

export_term2id(DbDir, ExportDir, ExportFile) :-
  directory_file_path(DbDir, term2id, DbSubdir),
  directory_file_path(ExportDir, 'term2id.dat', ExportFile),
  call_rocksdb(DbSubdir, export_term2id_hdt(ExportFile), [key(atom),value(int64)]).

export_term2id_hdt(ExportFile, Db) :-
  write_to_file(ExportFile, export_term2id_stream(Db)).

export_term2id_stream(Db, Out) :-
  forall(
    rocksdb_key_value(Db, Term, Id),
    format(Out, "~a ~a\n", [Term,Id])
  ).
