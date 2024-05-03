#!/bin/bash
# Copyright (c) 2020-2024, NVIDIA CORPORATION.
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
    [[ ! -x "$(command -v zip)" ]] && { echo "fail to find zip command in $PATH"; exit 1; }
    PY4J_TMP=("${SPARK_HOME}"/python/lib/py4j-*-src.zip)
    PY4J_FILE=${PY4J_TMP[0]}
    # PySpark uses ".dev0" for "-SNAPSHOT",  ".dev" for "preview"
    # https://github.com/apache/spark/blob/66f25e314032d562567620806057fcecc8b71f08/dev/create-release/release-build.sh#L267
    VERSION_STRING=$(PYTHONPATH=${SPARK_HOME}/python:${PY4J_FILE} python -c \
        "import pyspark, re; print(re.sub('\.dev0?$', '', pyspark.__version__))"
    )
    SCALA_VERSION=`$SPARK_HOME/bin/pyspark --version 2>&1| grep Scala | awk '{split($4,v,"."); printf "%s.%s", v[1], v[2]}'`

    [[ -z $VERSION_STRING ]] && { echo "Unable to detect the Spark version at $SPARK_HOME"; exit 1; }
    [[ -z $SCALA_VERSION ]] && { echo "Unable to detect the Scala version at $SPARK_HOME"; exit 1; }
    [[ -z $SPARK_SHIM_VER ]] && { SPARK_SHIM_VER="spark${VERSION_STRING//./}"; }

    echo "Detected Spark version $VERSION_STRING (shim version: $SPARK_SHIM_VER) (Scala version: $SCALA_VERSION)"

    INTEGRATION_TEST_VERSION=$SPARK_SHIM_VER

    if [[ ! -z $INTEGRATION_TEST_VERSION_OVERRIDE ]]; then
        # Override auto detected shim version in case of non-standard version string, e.g. `spark3113172702000-53`
        INTEGRATION_TEST_VERSION=$INTEGRATION_TEST_VERSION_OVERRIDE
    fi

    TARGET_DIR="$SCRIPTPATH"/target
    # support alternate local jars NOT building from the source code
    if [ -d "$LOCAL_JAR_PATH" ]; then
        AVRO_JARS=$(echo "$LOCAL_JAR_PATH"/spark-avro*.jar)
        PLUGIN_JARS=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark_*.jar)
        if [ -f $(echo $LOCAL_JAR_PATH/parquet-hadoop*.jar) ]; then
            export INCLUDE_PARQUET_HADOOP_TEST_JAR=true
            PARQUET_HADOOP_TESTS=$(echo $LOCAL_JAR_PATH/parquet-hadoop*.jar)
            # remove the log4j.properties file so it doesn't conflict with ours, ignore errors
            # if it isn't present or already removed
            zip -d $PARQUET_HADOOP_TESTS log4j.properties || true
        else
            export INCLUDE_PARQUET_HADOOP_TEST_JAR=false
            PARQUET_HADOOP_TESTS=
        fi
        # the integration-test-spark3xx.jar, should not include the integration-test-spark3xxtest.jar
        TEST_JARS=$(echo "$LOCAL_JAR_PATH"/rapids-4-spark-integration-tests*-$INTEGRATION_TEST_VERSION.jar)
    else
        [[ "$SCALA_VERSION" != "2.12"  ]] && TARGET_DIR=${TARGET_DIR/integration_tests/scala$SCALA_VERSION\/integration_tests}
        AVRO_JARS=$(echo "$TARGET_DIR"/dependency/spark-avro*.jar)
        PARQUET_HADOOP_TESTS=$(echo "$TARGET_DIR"/dependency/parquet-hadoop*.jar)
        # remove the log4j.properties file so it doesn't conflict with ours, ignore errors
        # if it isn't present or already removed
        zip -d $PARQUET_HADOOP_TESTS log4j.properties || true
        MIN_PARQUET_JAR="$TARGET_DIR"/dependency/parquet-hadoop-1.12.0-tests.jar
        # Make sure we have Parquet version >= 1.12 in the dependency
        LOWEST_PARQUET_JAR=$(echo -e "$MIN_PARQUET_JAR\n$PARQUET_HADOOP_TESTS" | sort -V | head -1)
        export INCLUDE_PARQUET_HADOOP_TEST_JAR=$([[ "$LOWEST_PARQUET_JAR" == "$MIN_PARQUET_JAR" ]] && echo true || echo false)
        PLUGIN_JARS=$(echo "$TARGET_DIR"/../../dist/target/rapids-4-spark_*.jar)
        # the integration-test-spark3xx.jar, should not include the integration-test-spark3xxtest.jar
        TEST_JARS=$(echo "$TARGET_DIR"/rapids-4-spark-integration-tests*-$INTEGRATION_TEST_VERSION.jar)
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

    # ALL_JARS includes dist.jar integration-test.jar avro.jar parquet.jar if they exist
    # Remove non-existing paths and canonicalize the paths including get rid of links and `..`
    ALL_JARS=$(readlink -e $PLUGIN_JARS $TEST_JARS $AVRO_JARS $PARQUET_HADOOP_TESTS || true)
    # `:` separated jars
    ALL_JARS="${ALL_JARS//$'\n'/:}"

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
        HOST_MEM_PARALLEL=`cat /proc/meminfo | grep MemAvailable | awk '{print int($2 / (5 * 1024 * 1024))}'`
        TMP_PARALLEL=$(( $GPU_MEM_PARALLEL > $CPU_CORES ? $CPU_CORES : $GPU_MEM_PARALLEL ))
        TMP_PARALLEL=$(( $TMP_PARALLEL > $HOST_MEM_PARALLEL ? $HOST_MEM_PARALLEL : $TMP_PARALLEL ))

        # Account for intra-Spark parallelism
        numGpuJVM=1
        if [[ "$NUM_LOCAL_EXECS" != "" ]]; then
            numGpuJVM=$NUM_LOCAL_EXECS
        elif [[ "$PYSP_TEST_spark_cores_max" != "" && "$PYSP_TEST_spark_executor_cores" != "" ]]; then
            numGpuJVM=$(( $PYSP_TEST_spark_cores_max /  $PYSP_TEST_spark_executor_cores ))
        fi
        TMP_PARALLEL=$(( $TMP_PARALLEL / $numGpuJVM ))

        if  (( $TMP_PARALLEL <= 1 )); then
            TEST_PARALLEL=1
        else
            TEST_PARALLEL=$TMP_PARALLEL
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
        # With xdist 0 and 1 are the same parallelism but
        # 0 is more efficient
        TEST_PARALLEL_OPTS=()
    else
        TEST_PARALLEL_OPTS=("-n" "$TEST_PARALLEL")
    fi

    mkdir -p "$TARGET_DIR"

    RUN_DIR=${RUN_DIR-$(mktemp -p "$TARGET_DIR" -d run_dir-$(date +%Y%m%d%H%M%S)-XXXX)}
    mkdir -p "$RUN_DIR"
    cd "$RUN_DIR"

    ## Under cloud environment, overwrite the '--rootdir' param to point to the working directory of each excutor
    LOCAL_ROOTDIR=${LOCAL_ROOTDIR:-"$SCRIPTPATH"}
    ## Under cloud environment, overwrite the '--std_input_path' param to point to the distributed file path
    INPUT_PATH=${INPUT_PATH:-"$SCRIPTPATH"}

    RUN_TESTS_COMMAND=(
        "$SCRIPTPATH"/runtests.py
        --rootdir "$LOCAL_ROOTDIR"
    )
    if [[ "${TESTS}" == "" ]]; then
        RUN_TESTS_COMMAND+=("${LOCAL_ROOTDIR}/src/main/python")
    else
        read -a RAW_TESTS <<< "${TESTS}"
        for raw_test in ${RAW_TESTS[@]}; do
            RUN_TESTS_COMMAND+=("${LOCAL_ROOTDIR}/src/main/python/${raw_test}")
        done
    fi

    REPORT_CHARS=${REPORT_CHARS:="fE"} # default as (f)ailed, (E)rror
    STD_INPUT_PATH="$INPUT_PATH"/src/test/resources
    TEST_COMMON_OPTS=(-v
          -r"$REPORT_CHARS"
          "$TEST_TAGS"
          --std_input_path="$STD_INPUT_PATH"
          --color=yes
          $TEST_TYPE_PARAM
          "$TEST_ARGS"
          $RUN_TEST_PARAMS
          --junitxml=TEST-pytest-`date +%s%N`.xml
          "$@")

    NUM_LOCAL_EXECS=${NUM_LOCAL_EXECS:-0}
    MB_PER_EXEC=${MB_PER_EXEC:-1024}
    CORES_PER_EXEC=${CORES_PER_EXEC:-1}

    SPARK_TASK_MAXFAILURES=${SPARK_TASK_MAXFAILURES:-1}

    if [[ "${PYSP_TEST_spark_shuffle_manager}" =~ "RapidsShuffleManager" ]]; then
        # If specified shuffle manager, set `extraClassPath` due to issue https://github.com/NVIDIA/spark-rapids/issues/5796
        # Remove this line if the issue is fixed
        export PYSP_TEST_spark_driver_extraClassPath="${ALL_JARS}"
        export PYSP_TEST_spark_executor_extraClassPath="${ALL_JARS}"
    else
        export PYSP_TEST_spark_jars="${ALL_JARS//:/,}"
    fi

    # time zone will be tested; use export TZ=time_zone_name before run this script
    export TZ=${TZ:-UTC}

    # Disable Spark UI by default since it is not needed for tests, and Spark can fail to start
    # due to Spark UI port collisions, especially in a parallel test setup.
    export PYSP_TEST_spark_ui_enabled=${PYSP_TEST_spark_ui_enabled:-false}

    # Set the Delta log cache size to prevent the driver from caching every Delta log indefinitely
    export PYSP_TEST_spark_databricks_delta_delta_log_cacheSize=${PYSP_TEST_spark_databricks_delta_delta_log_cacheSize:-10}
    deltaCacheSize=$PYSP_TEST_spark_databricks_delta_delta_log_cacheSize
    DRIVER_EXTRA_JAVA_OPTIONS="-ea -Duser.timezone=$TZ -Ddelta.log.cacheSize=$deltaCacheSize"
    export PYSP_TEST_spark_driver_extraJavaOptions="$DRIVER_EXTRA_JAVA_OPTIONS $COVERAGE_SUBMIT_FLAGS"
    export PYSP_TEST_spark_executor_extraJavaOptions="-ea -Duser.timezone=$TZ"
    export PYSP_TEST_spark_ui_showConsoleProgress='false'
    export PYSP_TEST_spark_sql_session_timeZone=$TZ
    export PYSP_TEST_spark_sql_shuffle_partitions='4'
    # prevent cluster shape to change
    export PYSP_TEST_spark_dynamicAllocation_enabled='false'
    export PYSP_TEST_spark_rapids_memory_host_spillStorageSize='100m'
    # Not the default 2G but should be large enough for a single batch for all data (we found
    # 200 MiB being allocated by a single test at most, and we typically have 4 tasks.
    export PYSP_TEST_spark_rapids_sql_batchSizeBytes='100m'
    export PYSP_TEST_spark_rapids_sql_regexp_maxStateMemoryBytes='300m'

    export PYSP_TEST_spark_hadoop_hive_exec_scratchdir="$RUN_DIR/hive"

    # Extract Databricks version from deployed configs.
    # spark.databricks.clusterUsageTags.sparkVersion is set automatically on Databricks
    # notebooks but not when running Spark manually.
    #
    # At the OS level the DBR version can be obtailed via
    # 1. DATABRICKS_RUNTIME_VERSION environment set by Databricks, e.g., 11.3
    # 2. File at /databricks/DBR_VERSION created by Databricks, e.g., 11.3
    # 3. The value for Spark conf in file /databricks/common/conf/deploy.conf created by Databricks,
    #    e.g. 11.3.x-gpu-ml-scala2.12
    #
    # For cases 1 and 2 append '.' for version matching in 3XYdb SparkShimServiceProvider
    #
    DBR_VERSION=/databricks/DBR_VERSION
    DB_DEPLOY_CONF=/databricks/common/conf/deploy.conf
    if [[ -n "${DATABRICKS_RUNTIME_VERSION}" ]]; then
      export PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion="${DATABRICKS_RUNTIME_VERSION}."
    elif [[ -f $DBR_VERSION || -f $DB_DEPLOY_CONF ]]; then
      DB_VER="$(< ${DBR_VERSION})." || \
        DB_VER=$(grep spark.databricks.clusterUsageTags.sparkVersion $DB_DEPLOY_CONF | sed -e 's/.*"\(.*\)".*/\1/')
      # if we did not error out on reads we should have at least four characters "x.y."
      if (( ${#DB_VER} < 4 )); then
          echo >&2 "Unable to determine Databricks version, unexpected length of: ${DB_VER}"
          exit 1
      fi
      export PYSP_TEST_spark_databricks_clusterUsageTags_sparkVersion=$DB_VER
    else
      cat << EOF
This node does not define
- DATABRICKS_RUNTIME_VERSION environment,
- Files containing version information: $DBR_VERSION, $DB_DEPLOY_CONF

Proceeding assuming a non-Databricks environment.
EOF

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
    if [[ "$SPARK_SUBMIT_FLAGS" == *"--master local"* || "$PYSP_TEST_spark_master" == "local"* ]]; then
        # The only case where we want worker logs is in local mode so we set the value here explicitly
        # We can't use the PYSP_TEST_spark_master as it's not always set e.g. when using --master
        export USE_WORKER_LOGS=1
    fi
    # Set a seed to be used in the tests, for datagen
    export SPARK_RAPIDS_TEST_DATAGEN_SEED=${SPARK_RAPIDS_TEST_DATAGEN_SEED:-${DATAGEN_SEED:-`date +%s`}}
    echo "SPARK_RAPIDS_TEST_DATAGEN_SEED used: $SPARK_RAPIDS_TEST_DATAGEN_SEED"

    # Set a seed to be used to pick random tests to inject with OOM
    export SPARK_RAPIDS_TEST_INJECT_OOM_SEED=${SPARK_RAPIDS_TEST_INJECT_OOM_SEED:-`date +%s`}
    echo "SPARK_RAPIDS_TEST_INJECT_OOM_SEED used: $SPARK_RAPIDS_TEST_INJECT_OOM_SEED"

    # If you want to change the amount of GPU memory allocated you have to change it here
    # and where TEST_PARALLEL is calculated
    if [[ -n "${PYSP_TEST_spark_rapids_memory_gpu_allocSize}" ]]; then
       >&2 echo "#### WARNING: using externally set" \
                "PYSP_TEST_spark_rapids_memory_gpu_allocSize" \
                "${PYSP_TEST_spark_rapids_memory_gpu_allocSize}." \
                "If needed permanently in CI please file an issue to accommodate" \
                "for new GPU memory requirements ####"
    fi
    export PYSP_TEST_spark_rapids_memory_gpu_allocSize=${PYSP_TEST_spark_rapids_memory_gpu_allocSize:-'1536m'}

    SPARK_SHELL_SMOKE_TEST="${SPARK_SHELL_SMOKE_TEST:-0}"
    if [[ "${SPARK_SHELL_SMOKE_TEST}" != "0" ]]; then
        echo "Running spark-shell smoke test..."
        SPARK_SHELL_ARGS_ARR=(
            --master local-cluster[1,2,1024]
            --conf spark.plugins=com.nvidia.spark.SQLPlugin
            --conf spark.deploy.maxExecutorRetries=0
        )
        if [[ "${PYSP_TEST_spark_shuffle_manager}" != "" ]]; then
            SPARK_SHELL_ARGS_ARR+=(
                --conf spark.shuffle.manager="${PYSP_TEST_spark_shuffle_manager}"
                --driver-class-path "${PYSP_TEST_spark_driver_extraClassPath}"
                --conf spark.executor.extraClassPath="${PYSP_TEST_spark_driver_extraClassPath}"
            )
        elif [[ -n "$PYSP_TEST_spark_jars_packages" ]]; then
            SPARK_SHELL_ARGS_ARR+=(--packages "${PYSP_TEST_spark_jars_packages}")
        else
            SPARK_SHELL_ARGS_ARR+=(--jars "${PYSP_TEST_spark_jars}")
        fi

        if [[ -n "$PYSP_TEST_spark_jars_repositories" ]]; then
            SPARK_SHELL_ARGS_ARR+=(--repositories "${PYSP_TEST_spark_jars_repositories}")
        fi
        # NOTE grep is used not only for checking the output but also
        # to workaround the fact that spark-shell catches all failures.
        # In this test it exits not because of the failure but because it encounters
        # an EOF on stdin and injects a ":quit" command. Without a grep check
        # the exit code would be success 0 regardless of the exceptions.
        #
        <<< 'spark.range(100).agg(Map("id" -> "sum")).collect()' \
            "${SPARK_HOME}"/bin/spark-shell "${SPARK_SHELL_ARGS_ARR[@]}" 2>/dev/null \
            | grep -F 'res0: Array[org.apache.spark.sql.Row] = Array([4950])'
        echo "SUCCESS spark-shell smoke test"
    elif ((${#TEST_PARALLEL_OPTS[@]} > 0));
    then
        exec python "${RUN_TESTS_COMMAND[@]}" "${TEST_PARALLEL_OPTS[@]}" "${TEST_COMMON_OPTS[@]}"
    else
        if [[ "$USE_WORKER_LOGS" == "1" ]]; then
          # Setting the extraJavaOptions again to set the log4j confs that will be needed for writing logs in the expected location
          # We have to export it again because we want to be able to let the user override these confs by setting them on the
          # command-line using the COVERAGE_SUBMIT_FLAGS which won't be possible if we were to just say
          # export $PYSP_TEST_spark_driver_extraJavaOptions = "$PYSP_TEST_spark_driver_extraJavaOptions $LOG4J_CONF"
          LOG4J_CONF="-Dlog4j.configuration=file://$STD_INPUT_PATH/pytest_log4j.properties -Dlogfile=$RUN_DIR/gw0_worker_logs.log"
          export PYSP_TEST_spark_driver_extraJavaOptions="$DRIVER_EXTRA_JAVA_OPTIONS $LOG4J_CONF $COVERAGE_SUBMIT_FLAGS"
        fi

        # We set the GPU memory size to be a constant value even if only running with a parallelism of 1
        # because it helps us have consistent test runs.
        jarOpts=()
        if [[ -n "$PYSP_TEST_spark_jars" ]]; then
            jarOpts+=(--jars "${PYSP_TEST_spark_jars}")
        fi

        if [[ -n "$PYSP_TEST_spark_jars_packages" ]]; then
            jarOpts+=(--packages "${PYSP_TEST_spark_jars_packages}")
        fi

        if [[ -n "$PYSP_TEST_spark_jars_repositories" ]]; then
            jarOpts+=(--repositories "${PYSP_TEST_spark_jars_repositories}")
        fi

        if [[ -n "$PYSP_TEST_spark_driver_extraClassPath" ]]; then
            jarOpts+=(--driver-class-path "${PYSP_TEST_spark_driver_extraClassPath}")
        fi

        driverJavaOpts="$PYSP_TEST_spark_driver_extraJavaOptions"
        gpuAllocSize="$PYSP_TEST_spark_rapids_memory_gpu_allocSize"

        # avoid double processing of variables passed to spark in
        # spark_conf_init
        unset PYSP_TEST_spark_databricks_delta_delta_log_cacheSize
        unset PYSP_TEST_spark_driver_extraClassPath
        unset PYSP_TEST_spark_driver_extraJavaOptions
        unset PYSP_TEST_spark_jars
        unset PYSP_TEST_spark_jars_packages
        unset PYSP_TEST_spark_jars_repositories
        unset PYSP_TEST_spark_rapids_memory_gpu_allocSize

        exec "$SPARK_HOME"/bin/spark-submit "${jarOpts[@]}" \
            --driver-java-options "$driverJavaOpts" \
            $SPARK_SUBMIT_FLAGS \
            --conf 'spark.rapids.memory.gpu.allocSize='"$gpuAllocSize" \
            --conf 'spark.databricks.delta.delta.log.cacheSize='"$deltaCacheSize" \
            "${RUN_TESTS_COMMAND[@]}" "${TEST_COMMON_OPTS[@]}"
    fi
fi
