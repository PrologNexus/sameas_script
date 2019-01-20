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
  % command-line arguments
  cli_arguments(Args),
  (Args = [FromDir] -> ToDir = FromDir ; Args = [FromDir,ToDir]),
  % export
  export_id2terms(FromDir, ToDir, ToFile1),
  export_term2id(FromDir, ToDir, ToFile2),
  % archive & compress
  directory_file_path(ToDir, 'closure.tgz', ToFile),
  archive_create(ToFile, [ToFile1,ToFile2], [filter(gzip),format(gnutar)]).
  %maplist(delete_file, [ToFile1,ToFile2]).

export_id2terms(FromDir0, ToDir, ToFile) :-
  directory_file_path(FromDir0, id2terms, FromDir),
  directory_file_path(ToDir, 'id2terms.dat', ToFile),
  call_rocksdb(
    FromDir,
    export_id2terms_handle(ToFile),
    [key(int64),merge(rocksdb_merge_set),value(term)]
  ).

export_id2terms_handle(ToFile, Db) :-
  write_to_file(ToFile, export_id2terms_stream(Db)).

export_id2terms_stream(Db, Out) :-
  forall(
    rocksdb_key_value(Db, Id, Terms),
    (
      format(Out, "~a", [Id]),
      maplist(format(Out, " ~a"), Terms),
      format(Out, "\n", [])
    )
  ).

export_term2id(FromDir0, ToDir, ToFile) :-
  directory_file_path(FromDir0, term2id, FromDir),
  directory_file_path(ToDir, 'term2id.dat', ToFile),
  call_rocksdb(FromDir, export_term2id_hdt(ToFile), [key(atom),value(int64)]).

export_term2id_hdt(ToFile, Db) :-
  write_to_file(ToFile, export_term2id_stream(Db)).

export_term2id_stream(Db, Out) :-
  forall(
    rocksdb_key_value(Db, Term, Id),
    format(Out, "~a ~a\n", [Term,Id])
  ).
