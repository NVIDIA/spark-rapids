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

if [[ "${SKIP_TESTS,,}" == "true" ]];
then
    echo "PYTHON INTEGRATION TESTS SKIPPED..."
elif [[ -z "$SPARK_HOME" ]];
then
    >&2 echo "SPARK_HOME IS NOT SET CANNOT RUN PYTHON INTEGRATION TESTS..."
else
    echo "WILL RUN TESTS WITH SPARK_HOME: ${SPARK_HOME}"
    CUDF_JARS=$(echo ./target/dependency/cudf-*.jar)
    PLUGIN_JARS=$(echo ../dist/target/rapids-4-spark*.jar)
    TEST_JARS=$(echo ./target/rapids-4-spark-integration-tests*.jar)
    ALL_JARS="$CUDF_JARS $PLUGIN_JARS $TEST_JARS"
    echo "AND PLUGIN JARS: $ALL_JARS"
    if [[ "${TEST}" != "" ]];
    then
        TEST_ARGS="-k $TEST"
    fi
    "$SPARK_HOME"/bin/spark-submit --jars "${ALL_JARS// /,}" --conf "spark.driver.extraJavaOptions=-Duser.timezone=GMT $COVERAGE_SUBMIT_FLAGS" --conf 'spark.executor.extraJavaOptions=-Duser.timezone=GMT' --conf 'spark.sql.session.timeZone=UTC' --conf 'spark.sql.shuffle.partitions=12' $SPARK_SUBMIT_FLAGS ./runtests.py -v -rfExXs "$TEST_ARGS" $RUN_TEST_PARAMS "$@"
fi
