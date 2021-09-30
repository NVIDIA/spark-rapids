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


# PWD should be dist/target
set -ex

[[ "${SKIP_BINARY_DEDUPE:-0}" == "1" ]] && {
  echo "Skipping binary-dedupe. Unset SKIP_BINARY_DEDUPE to activate binary-dedupe"
  exit 0
}

SPARK3XX_COMMON_TXT=$PWD/spark3xx-common.txt
SPARK3XX_COMMON_DIR=$PWD/spark3xx-common

# This script de-duplicates .class files at the binary level.
# We could also diff classes using scalap / javap outputs.
# However, with observed warnings in the output we have no guarantee that the
# output is complete, and that the complete output would not exhibit diffs.
# We compute and compare checksum signatures of same-named classes

# The following pipeline determines identical classes across shims in this build.
# - checksum all class files
# - move the varying-prefix shim3xy to the left so it can be easily skipped for uniq and sort
# - sort by path, secondary sort by checksum, print one line per group
# - produce uniq count for paths
# - filter the paths with count=1, the class files without diverging checksums
# - put the path starting with /spark3xy back together for the final list
echo "Retrieving class files hashing to a single value"
find . -path './parallel-world/spark*' -type f -name '*class' | \
  xargs -L 1000 sha1sum -b | \
  awk -F/ '$1=$1' | \
  awk '{checksum=$1; shim=$4; $1=shim; $2=$3=""; $4=checksum;  print $0}' | tr -s ' ' | \
  sort -k3 -k2,2 -u | uniq -f 2 -c | grep '^\s\+1 .*' | \
  awk '{$1=""; $3=""; print $0 }' | tr -s ' ' | sed 's/\ /\//g' > "$SPARK3XX_COMMON_TXT"

echo "Deleting duplicates of spark3xx-common classes"
xargs --arg-file="$SPARK3XX_COMMON_TXT" -P 6 -n 1 -I% bash -c "
    shim=\$(echo '%' | cut -d'/' -f 2)
    class_file=\$(echo '%' | cut -d'/' -f 3-)
    class_dir=\$(dirname \$class_file)
    dest_dir=$SPARK3XX_COMMON_DIR/\$class_dir
    mkdir -p \$dest_dir && \
      cp ./parallel-world/\$shim\/\$class_file \$dest_dir/ && \
      find ./parallel-world -path './parallel-world/spark3*/'\$class_file -exec rm {} + || exit 255
  "

mv "$SPARK3XX_COMMON_DIR" parallel-world/

# TODO further dedupe by FEATURE version lines:
#  spark30x-common
#  spark31x-common
#  spark32x-common

# Verify that all class files in the conventional jar location are bitwise
# identical regardless of the Spark-version-specific jar.
#
# At this point the duplicate classes have not been removed from version-specific jar
# locations such as parallel-world/spark312.
# For each unshimmed class file look for all of its copies inside /spark3* and
# and count the number of distinct checksums. There are two representative cases
# 1) The class is contributed to the unshimmed location via the unshimmed-from-each-spark3xx list. These are classes
#    carrying the shim classifier in their package name such as
#    com.nvidia.spark.rapids.spark312.RapidsShuffleManager. They are by unique by construction,
#    and will have zero copies in any non-spark312 shims. Although such classes are currently excluded from
#    being copied to the /spark312 Parallel World we keep the algorithm below general without assuming this.
#
# 2) The class is contributed to the unshimmed location via unshimmed-common. These are classes that
#    that have the same package and class name across all parallel worlds.
#
#  So if the number of distinct class files per class in the unshimmed location is < 2, the jar
#  is content is as expected
#
#  If we find an unshimmed class file occurring > 1  we fail the build and the code must be refactored
#  until bitwise-identity of each unshimmed class is restored.

# Determine the list of unshimmed class files
UNSHIMMED_LIST_TXT=unshimmed-result.txt
find . -name '*.class' -not -path './parallel-world/spark*' | \
  cut -d/ -f 3- | sort > $UNSHIMMED_LIST_TXT

for classFile in $(< $UNSHIMMED_LIST_TXT); do
  DISTINCT_COPIES=$(find './parallel-world' -path "./*/$classFile" -exec sha1sum -b {} + |
    cut -d' ' -f 1 | sort -u | wc -l)
  ((DISTINCT_COPIES == 1)) || {
    echo >&2 "$classFile is not bitwise-identical, found $DISTINCT_COPIES distincts";
    exit 2;
  }
done

# Remove unshimmed classes from parallel worlds
xargs --arg-file="$UNSHIMMED_LIST_TXT" -P 6 -n 100 -I% \
  find . -path './parallel-world/spark*/%' -exec rm {} +