#!/bin/bash
# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
    # support alternate local jars NOT building from the source code
    if [ -d "$LOCAL_JAR_PATH" ]; then
        CUDF_JARS=$(echo "$LOCAL_JAR_PATH"/cudf-*.jar)
        PLUGIN_JARS=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark_*.jar)
        TEST_JARS=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark-integration-tests*.jar)
        UDF_EXAMPLE_JARS=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark-udf-examples*.jar)
    else
        CUDF_JARS=$(echo "$SCRIPTPATH"/target/dependency/cudf-*.jar)
        PLUGIN_JARS=$(echo "$SCRIPTPATH"/../dist/target/rapids-4-spark_*.jar)
        TEST_JARS=$(echo "$SCRIPTPATH"/target/rapids-4-spark-integration-tests*.jar)
        UDF_EXAMPLE_JARS=$(echo "$SCRIPTPATH"/../udf-examples/target/rapids-4-spark-udf-examples*.jar)
    fi
    ALL_JARS="$CUDF_JARS $PLUGIN_JARS $TEST_JARS $UDF_EXAMPLE_JARS"
    echo "AND PLUGIN JARS: $ALL_JARS"
    if [[ "${TEST}" != "" ]];
    then
        TEST_ARGS="-k $TEST"
    fi
    if [[ "${TEST_TAGS}" != "" ]];
    then
        TEST_TAGS="-m $TEST_TAGS"
    fi
    if [[ "${TEST_PARALLEL}" == "" ]];
    then
        # For now just assume that we are going to use the GPU on the
        # system with the most free memory and then divide it up into chunks.
        # We use free memory to try and avoid issues if the GPU also is working
        # on graphics, which happens some times.
        # We subtract one for the main controlling process that will still
        # launch an application.  It will not run thing on the GPU but it needs
        # to still launch a spark application.
        TEST_PARALLEL=`nvidia-smi --query-gpu=memory.free --format=csv,noheader | awk '{if (MAX < $1){ MAX = $1}} END {print int(MAX / (2.3 * 1024)) - 1}'`
        echo "AUTO DETECTED PARALLELISM OF $TEST_PARALLEL"
    fi
    if python -c 'import findspark';
    then
        echo "FOUND findspark"
    else
        TEST_PARALLEL=0
        echo "findspark not installed cannot run tests in parallel"
    fi
    if python -c 'import xdist.plugin';
    then
        echo "FOUND xdist"
    else
        TEST_PARALLEL=0
        echo "xdist not installed cannot run tests in parallel"
    fi

    TEST_TYPE_PARAM=""
    if [[ "${TEST_TYPE}" != "" ]];
    then
        TEST_TYPE_PARAM="--test_type $TEST_TYPE"
    fi

    if [[ ${TEST_PARALLEL} -lt 2 ]];
    then
        # With xdist 0 and 1 are the same parallelsm but
        # 0 is more effecient
        TEST_PARALLEL_OPTS=()
        MEMORY_FRACTION='1'
    else
        MEMORY_FRACTION=`python -c "print(1/($TEST_PARALLEL + 1))"`
        TEST_PARALLEL_OPTS=("-n" "$TEST_PARALLEL")
    fi
    RUN_DIR="$SCRIPTPATH"/target/run_dir
    mkdir -p "$RUN_DIR"
    cd "$RUN_DIR"

    ## Under cloud environment, overwrite the '--rootdir' param to point to the working directory of each excutor
    LOCAL_ROOTDIR=${LOCAL_ROOTDIR:-"$SCRIPTPATH"}
    ## Under cloud environment, overwrite the '--std_input_path' param to point to the distributed file path
    INPUT_PATH=${INPUT_PATH:-"$SCRIPTPATH"}

    RUN_TESTS_COMMAND=("$SCRIPTPATH"/runtests.py
      --rootdir
      "$LOCAL_ROOTDIR"
      "$LOCAL_ROOTDIR"/src/main/python)

    TEST_COMMON_OPTS=(-v
          -rfExXs
          "$TEST_TAGS"
          --std_input_path="$INPUT_PATH"/src/test/resources
          --color=yes
          $TEST_TYPE_PARAM
          "$TEST_ARGS"
          $RUN_TEST_PARAMS
          "$@")

    NUM_LOCAL_EXECS=${NUM_LOCAL_EXECS:-0}
    MB_PER_EXEC=${MB_PER_EXEC:-1024}
    CORES_PER_EXEC=${CORES_PER_EXEC:-1}

    if ((NUM_LOCAL_EXECS > 0)); then
      export PYSP_TEST_spark_master="local-cluster[$NUM_LOCAL_EXECS,$CORES_PER_EXEC,$MB_PER_EXEC]"
    fi

    export PYSP_TEST_spark_driver_extraClassPath="${ALL_JARS// /:}"
    export PYSP_TEST_spark_executor_extraClassPath="${ALL_JARS// /:}"
    export PYSP_TEST_spark_driver_extraJavaOptions="-ea -Duser.timezone=UTC $COVERAGE_SUBMIT_FLAGS"
    export PYSP_TEST_spark_executor_extraJavaOptions='-ea -Duser.timezone=UTC'
    export PYSP_TEST_spark_ui_showConsoleProgress='false'
    export PYSP_TEST_spark_sql_session_timeZone='UTC'
    export PYSP_TEST_spark_sql_shuffle_partitions='12'
    if ((${#TEST_PARALLEL_OPTS[@]} > 0));
    then
        export PYSP_TEST_spark_rapids_memory_gpu_allocFraction=$MEMORY_FRACTION
        export PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=$MEMORY_FRACTION
        python "${RUN_TESTS_COMMAND[@]}" "${TEST_PARALLEL_OPTS[@]}" "${TEST_COMMON_OPTS[@]}"
    else
        "$SPARK_HOME"/bin/spark-submit --jars "${ALL_JARS// /,}" \
            --driver-java-options "$PYSP_TEST_spark_driver_extraJavaOptions" \
            $SPARK_SUBMIT_FLAGS "${RUN_TESTS_COMMAND[@]}" "${TEST_COMMON_OPTS[@]}"
    fi
fi
