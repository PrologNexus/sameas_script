#!/bin/bash
LC_ALL=C sort --parallel=4 -T /scratch/wbeek/tmp/sort -u <(gunzip -c /scratch/wbeek/tmp/id-unsorted.tsv.gz) | gzip >/scratch/wbeek/tmp/id-sorted.tsv.gz
