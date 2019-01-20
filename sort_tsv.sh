#!/bin/bash
LC_ALL=C sort --parallel=4 -T <tmp-directory> -u <(gunzip -c <unsorted.gz>) | gzip > <sorted.gz>
