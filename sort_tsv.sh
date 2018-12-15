#!/bin/sh
LC_ALL=C sort --parallel=4 -T /scratch/wbeek/tmp/sort -u <(gunzip -c /scratch/wbeek/tmp/sameas/id-unsorted.tsv.gz) | gzip >/scratch/wbeek/tmp/sameas/id-sorted.tsv.gz
