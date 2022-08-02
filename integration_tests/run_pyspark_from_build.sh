#!/bin/bash
# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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
    # Spark 3.1.1 includes https://github.com/apache/spark/pull/31540
    # which helps with spurious task failures as observed in our tests. If you are running
    # Spark versions before 3.1.1, this sets the spark.max.taskFailures to 4 to allow for
    # more lineant configuration, else it will set them to 1 as spurious task failures are not expected
    # for Spark 3.1.1+
    VERSION_STRING=`$SPARK_HOME/bin/pyspark --version 2>&1|grep -v Scala|awk '/version\ [0-9.]+/{print $NF}'`
    VERSION_STRING="${VERSION_STRING/-SNAPSHOT/}"
    [[ -z $VERSION_STRING ]] && { echo "Unable to detect the Spark version at $SPARK_HOME"; exit 1; }
    [[ -z $SPARK_SHIM_VER ]] && { SPARK_SHIM_VER="spark${VERSION_STRING//./}"; }

    echo "Detected Spark version $VERSION_STRING (shim version: $SPARK_SHIM_VER)"

    INTEGRATION_TEST_VERSION=$SPARK_SHIM_VER

    if [[ ! -z $INTEGRATION_TEST_VERSION_OVERRIDE ]]; then
        # Override auto detected shim version in case of non-standard version string, e.g. `spark3113172702000-53`
        INTEGRATION_TEST_VERSION=$INTEGRATION_TEST_VERSION_OVERRIDE
    fi

    # support alternate local jars NOT building from the source code
    if [ -d "$LOCAL_JAR_PATH" ]; then
        AVRO_JARS=$(echo "$LOCAL_JAR_PATH"/spark-avro*.jar)
        PLUGIN_JARS=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark_*.jar)
        if [ -f $(echo $LOCAL_JAR_PATH/parquet-hadoop*.jar) ]; then
            export INCLUDE_PARQUET_HADOOP_TEST_JAR=true
            PARQUET_HADOOP_TESTS=$(echo $LOCAL_JAR_PATH/parquet-hadoop*.jar)
        else
            export INCLUDE_PARQUET_HADOOP_TEST_JAR=false
            PARQUET_HADOOP_TESTS=
        fi
        # the integration-test-spark3xx.jar, should not include the integration-test-spark3xxtest.jar
        TEST_JARS=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark-integration-tests*-$INTEGRATION_TEST_VERSION.jar)
    else
        AVRO_JARS=$(echo "$SCRIPTPATH"/target/dependency/spark-avro*.jar)
        PARQUET_HADOOP_TESTS=$(echo "$SCRIPTPATH"/target/dependency/parquet-hadoop*.jar)
        MIN_PARQUET_JAR="$SCRIPTPATH/target/dependency/parquet-hadoop-1.12.0-tests.jar"
        # Make sure we have Parquet version >= 1.12 in the dependency
        LOWEST_PARQUET_JAR=$(echo -e "$MIN_PARQUET_JAR\n$PARQUET_HADOOP_TESTS" | sort -V | head -1)
        export INCLUDE_PARQUET_HADOOP_TEST_JAR=$([[ "$LOWEST_PARQUET_JAR" == "MIN_PARQUET_JAR" ]] && echo true || echo false)
        PLUGIN_JARS=$(echo "$SCRIPTPATH"/../dist/target/rapids-4-spark_*.jar)
        # the integration-test-spark3xx.jar, should not include the integration-test-spark3xxtest.jar
        TEST_JARS=$(echo "$SCRIPTPATH"/target/rapids-4-spark-integration-tests*-$INTEGRATION_TEST_VERSION.jar)
    fi

    # `./run_pyspark_from_build.sh` runs all the tests excluding the avro tests in 'avro_test.py'.
    #
    # `INCLUDE_SPARK_AVRO_JAR=true ./run_pyspark_from_build.sh` runs all the tests, including the tests
    #                                                           in 'avro_test.py'.
    if [[ $( echo ${INCLUDE_SPARK_AVRO_JAR} | tr [:upper:] [:lower:] ) == "true" ]];
    then
        export INCLUDE_SPARK_AVRO_JAR=true
    else
        export INCLUDE_SPARK_AVRO_JAR=false
        AVRO_JARS=""
    fi

    # Only 3 jars: dist.jar integration-test.jar avro.jar
    ALL_JARS="$PLUGIN_JARS $TEST_JARS $AVRO_JARS $PARQUET_HADOOP_TESTS"
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
        # For integration tests we want to have at least
        #  - 1.5 GiB of GPU memory for the tests and 750 MiB for loading CUDF + CUDA.
        #    From profiling we saw that tests allocated under 200 MiB of GPU memory and
        #    1.5 GiB felt like it gave us plenty of room to grow.
        #  - 5 GiB of host memory. In testing with a limited number of tasks (4) we saw
        #    the amount of host memory not go above 3 GiB so 5 felt like a good number
        #    for future growth.
        #  - 1 CPU core
        # per Spark application. We reserve 2 GiB of GPU memory for general overhead also.

        # For now just assume that we are going to use the GPU on the system with the most
        # free memory. We use free memory to try and avoid issues if the GPU also is working
        # on graphics, which happens with many workstation GPUs. We also reserve 2 GiB for
        # CUDA/CUDF overhead which can be used because of JIT or launching large kernels.
       
        # If you need to increase the amount of GPU memory you need to change it here and
        # below where the processes are launched.
        GPU_MEM_PARALLEL=`nvidia-smi --query-gpu=memory.free --format=csv,noheader | awk '{if (MAX < $1){ MAX = $1}} END {print int((MAX - 2 * 1024) / ((1.5 * 1024) + 750))}'`
        CPU_CORES=`nproc` 
        HOST_MEM_PARALLEL=`cat /proc/meminfo | grep MemAvailable | awk '{print int($2 / (5 * 1024))}'`
        TMP_PARALLEL=$(( $GPU_MEM_PARALLEL > $CPU_CORES ? $CPU_CORES : $GPU_MEM_PARALLEL ))
        TMP_PARALLEL=$(( $TMP_PARALLEL > $HOST_MEM_PARALLEL ? $HOST_MEM_PARALLEL : $TMP_PARALLEL ))
        if [[ $TMP_PARALLEL -gt 1 ]];
        then
            # We subtract 1 from the parallel number because xdist launches a process to
            # control and monitor the other processes. It takes up one available parallel
            # slot, even if it is not truly using all of the resources we give it.
            TEST_PARALLEL=$(( $TMP_PARALLEL - 1 ))
        else
            TEST_PARALLEL=1
        fi

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
    else
        TEST_PARALLEL_OPTS=("-n" "$TEST_PARALLEL")
    fi
    RUN_DIR=${RUN_DIR-"$SCRIPTPATH"/target/run_dir}
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
          --junitxml=TEST-pytest-`date +%s%N`.xml
          "$@")

    NUM_LOCAL_EXECS=${NUM_LOCAL_EXECS:-0}
    MB_PER_EXEC=${MB_PER_EXEC:-1024}
    CORES_PER_EXEC=${CORES_PER_EXEC:-1}

    SPARK_TASK_MAXFAILURES=1
    [[ "$VERSION_STRING" < "3.1.1" ]] && SPARK_TASK_MAXFAILURES=4

    export PYSP_TEST_spark_driver_extraClassPath="${ALL_JARS// /:}"
    export PYSP_TEST_spark_executor_extraClassPath="${ALL_JARS// /:}"
    export PYSP_TEST_spark_driver_extraJavaOptions="-ea -Duser.timezone=UTC $COVERAGE_SUBMIT_FLAGS"
    export PYSP_TEST_spark_executor_extraJavaOptions='-ea -Duser.timezone=UTC'
    export PYSP_TEST_spark_ui_showConsoleProgress='false'
    export PYSP_TEST_spark_sql_session_timeZone='UTC'
    export PYSP_TEST_spark_sql_shuffle_partitions='4'
    # prevent cluster shape to change
    export PYSP_TEST_spark_dynamicAllocation_enabled='false'
    export PYSP_TEST_spark_rapids_memory_host_spillStorageSize='100m'
    # Not the default 2G but should be large enough for a single batch for all data (we found
    # 200 MiB being allocated by a single test at most, and we typically have 4 tasks.
    export PYSP_TEST_spark_rapids_sql_batchSizeBytes='100m'
    export PYSP_TEST_spark_rapids_sql_regexp_maxStateMemoryBytes='300m'

    # Extract Databricks version from deployed configs. This is set automatically on Databricks
    # notebooks but not when running Spark manually.
    DB_DEPLOY_CONF=/databricks/common/conf/deploy.conf
    if [[ -f $DB_DEPLOY_CONF ]]; then
      DB_VER=$(grep spark.databricks.clusterUsageTags.sparkVersion $DB_DEPLOY_CONF | sed -e 's/.*"\(.*\)".*/\1/')
      if [[ -z $DB_VER ]]; then
        echo >&2 "Unable to determine Databricks version"
        exit 1
      fi
      export PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion=$DB_VER
    fi

    # Set spark.task.maxFailures for most schedulers.
    #
    # Local (non-cluster) mode is the exception and does not work with `spark.task.maxFailures`.
    # It requires two arguments to the master specification "local[N, K]" where
    # N is the number of threads, and K is the maxFailures (otherwise this is hardcoded to 1,
    # see https://issues.apache.org/jira/browse/SPARK-2083).
    export PYSP_TEST_spark_task_maxFailures="$SPARK_TASK_MAXFAILURES"

    if ((NUM_LOCAL_EXECS > 0)); then
      export PYSP_TEST_spark_master="local-cluster[$NUM_LOCAL_EXECS,$CORES_PER_EXEC,$MB_PER_EXEC]"
    else
      # If a master is not specified, use "local[cores, $SPARK_TASK_MAXFAILURES]"
      if [ -z "${PYSP_TEST_spark_master}" ] && [[ "$SPARK_SUBMIT_FLAGS" != *"--master"* ]]; then
        CPU_CORES=`nproc` 
        # We are limiting the number of tasks in local mode to 4 because it helps to reduce the
        # total memory usage, especially host memory usage because when copying data to the GPU
        # buffers as large as batchSizeBytes can be allocated, and the fewer of them we have the better.
        LOCAL_PARALLEL=$(( $CPU_CORES > 4 ? 4 : $CPU_CORES ))
        export PYSP_TEST_spark_master="local[$LOCAL_PARALLEL,$SPARK_TASK_MAXFAILURES]"
      fi
    fi

    # If you want to change the amount of GPU memory allocated you have to change it here 
    # and where TEST_PARALLEL is calculated
    export PYSP_TEST_spark_rapids_memory_gpu_allocSize='1536m'

    if ((${#TEST_PARALLEL_OPTS[@]} > 0));
    then
        exec python "${RUN_TESTS_COMMAND[@]}" "${TEST_PARALLEL_OPTS[@]}" "${TEST_COMMON_OPTS[@]}"
    else
        # We set the GPU memory size to be a constant value even if only running with a parallelism of 1
        # because it helps us have consistent test runs.
        exec "$SPARK_HOME"/bin/spark-submit --jars "${ALL_JARS// /,}" \
            --driver-java-options "$PYSP_TEST_spark_driver_extraJavaOptions" \
            $SPARK_SUBMIT_FLAGS \
            --conf 'spark.rapids.memory.gpu.allocSize='"$PYSP_TEST_spark_rapids_memory_gpu_allocSize" \
            "${RUN_TESTS_COMMAND[@]}" "${TEST_COMMON_OPTS[@]}"
    fi
fi
