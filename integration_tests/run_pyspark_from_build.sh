#!/bin/bash
# Copyright (c) 2020, NVIDIA CORPORATION.
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
set -ex

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "$SCRIPTPATH"

if [[ $( echo ${SKIP_TESTS} | tr [:upper:] [:lower:] ) == "true" ]];
then
    echo "PYTHON INTEGRATION TESTS SKIPPED..."
elif [[ -z "$SPARK_HOME" ]];
then
    >&2 echo "SPARK_HOME IS NOT SET CANNOT RUN PYTHON INTEGRATION TESTS..."
else
    echo "WILL RUN TESTS WITH SPARK_HOME: ${SPARK_HOME}"
    CUDF_JARS=$(echo "$SCRIPTPATH"/target/dependency/cudf-*.jar)
    PLUGIN_JARS=$(echo "$SCRIPTPATH"/../dist/target/rapids-4-spark*.jar)
    TEST_JARS=$(echo "$SCRIPTPATH"/target/rapids-4-spark-integration-tests*.jar)
    ALL_JARS="$CUDF_JARS $PLUGIN_JARS $TEST_JARS"
    echo "AND PLUGIN JARS: $ALL_JARS"
    if [[ "${TEST}" != "" ]];
    then
        TEST_ARGS="-k $TEST"
    fi
    if [[ "${TEST_TAGS}" != "" ]];
    then
        TEST_TAGS="-m $TEST_TAGS"
    fi
    RUN_DIR="$SCRIPTPATH"/target/run_dir
    mkdir -p "$RUN_DIR"
    cd "$RUN_DIR"
    "$SPARK_HOME"/bin/spark-submit --jars "${ALL_JARS// /,}" \
        --conf "spark.driver.extraJavaOptions=-Duser.timezone=GMT $COVERAGE_SUBMIT_FLAGS" \
        --conf 'spark.executor.extraJavaOptions=-Duser.timezone=GMT' \
        --conf 'spark.sql.session.timeZone=UTC' \
        --conf 'spark.sql.shuffle.partitions=12' \
        $SPARK_SUBMIT_FLAGS \
        "$SCRIPTPATH"/runtests.py --rootdir "$SCRIPTPATH" "$SCRIPTPATH"/src/main/python \
          -v -rfExXs "$TEST_TAGS" \
          --std_input_path="$SCRIPTPATH"/src/test/resources/ \
          "$TEST_ARGS" \
          $RUN_TEST_PARAMS \
          "$@"
fi
