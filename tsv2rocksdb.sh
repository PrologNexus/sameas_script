#!/bin/sh
swipl -s tsv2rocksdb.pl -g run -t halt --input=/scratch/wbeek/tmp/sameas/sameas-sorted.tsv.gz --output=/scratch/wbeek/data/sameas/
