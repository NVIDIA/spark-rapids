#!/usr/bin/env bash
#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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
#

# This script is partially copied from Spark 3.3:
#   https://github.com/apache/spark/blob/v3.3.0/dev/change-scala-version.sh

set -e

VALID_VERSIONS=( 2.12 2.13 )

usage() {
  echo "Usage: $(basename $0) [-h|--help] <version>
where :
  -h| --help Display this help text
  valid version values : ${VALID_VERSIONS[*]}
" 1>&2
  exit 1
}

if [[ ($# -ne 1) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

TO_VERSION=$1

check_scala_version() {
  for i in ${VALID_VERSIONS[*]}; do [ $i = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

check_scala_version "$TO_VERSION"

if [ $TO_VERSION = "2.13" ]; then
  FROM_VERSION="2.12"
else
  FROM_VERSION="2.13"
fi

sed_i() {
  sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
}

BASEDIR=$(dirname $0)/..
for f in $(find "$BASEDIR" -name 'pom.xml' -not -path '*target*'); do
  echo $f
  sed_i 's/\(artifactId.*\)_'$FROM_VERSION'/\1_'$TO_VERSION'/g' $f
  sed_i 's/^\([[:space:]]*<!-- #if scala-'$TO_VERSION' -->\)<!--/\1/' $f
  sed_i 's/^\([[:space:]]*\)-->\(<!-- #endif scala-'$TO_VERSION' -->\)/\1\2/' $f
  sed_i 's/^\([[:space:]]*<!-- #if scala-'$FROM_VERSION' -->\)$/\1<!--/' $f
  sed_i 's/^\([[:space:]]*\)\(<!-- #endif scala-'$FROM_VERSION' -->\)/\1-->\2/' $f
done

# Update <scala.version> in parent POM
# First find the right full version from the profile's build
SCALA_VERSION=`mvn help:evaluate -Pscala-${TO_VERSION} -Dexpression=scala.version -q -DforceStdout`
sed_i '/<spark-rapids-private\.version>/,/<spark\.version>[0-9]*\.[0-9]*\.[0-9]*</s/<scala\.version>[0-9]*\.[0-9]*\.[0-9]*</<scala.version>'$SCALA_VERSION'</' \
  "$BASEDIR/pom.xml"

# Also update <scala.binary.version> in parent POM
# Match any scala binary version to ensure idempotency
sed_i '/<spark-rapids-private\.version>/,/<scala\.binary\.version>[0-9]*\.[0-9]*</s/<scala\.binary\.version>[0-9]*\.[0-9]*</<scala.binary.version>'$TO_VERSION'</' \
  "$BASEDIR/pom.xml"
