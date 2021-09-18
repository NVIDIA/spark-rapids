#!/bin/bash

# Copyright (c) 2021, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script de-duplicates .class files at the binary level.
# We could also diff classes using scalap / javap outputs.
# However, with observed warnings in the output we have no guarantee that the
# output is complete, and that the complete output would not exhibit diffs.
# Binary diff may generate false positives for difference between classes at the
# JVM / JDK level due to some potentially irrelevant difference in annotations.
# However, most importantly, it guarantees to have no false negatives
# for identical classes.

# We use first shim e.g spark301 as a reference for the following algorithm of identifying
# identical bytecode across all supported Spark shims:
# 1. diff spark301 with all other shims pairwise and store the list of identical files
#    in spark301-spark302.ident
# 2. count the diff files from Step 1 which is (numShimsInBuild - 1)
# 3. Call sort on all the diff files and replace duplicate entries with
#    uniq counts
# 4. all entries that occur (numShimsInBuild - 1) tinmes are identical for all shims
#    and constitute the list for spark3xx-commons

# PWD should be dist/target
set -ex
SHIM_DIRS=$(ls -d parallel-world/spark3* | cut -d/ -f 2)
REF_SHIM=$(<<< "$SHIM_DIRS" head -1)
SHIMS_TO_COMPARE=$(<<< "$SHIM_DIRS" tail --lines=+2)
NUM_DIFFS=$(<<< "$SHIMS_TO_COMPARE" wc -l)
OUTPUTDIR=binary-diffs

mkdir -p $OUTPUTDIR
<<< "$SHIMS_TO_COMPARE" xargs -I% -n 1 bash -c \
  "diff -s -r parallel-world/$REF_SHIM parallel-world/% |
    grep '.class are identical' |
    cut -d' ' -f 2 |
    cut -d/ -f 3- > $OUTPUTDIR/$REF_SHIM-%.identical"

sort binary-diffs/* | uniq -c | grep "^ \+$NUM_DIFFS " | \
  awk '{print $2}' > spark3xx-common.txt

< spark3xx-common.txt xargs
