#!/bin/bash
#
# Copyright (c) 2020-2024, NVIDIA CORPORATION. All rights reserved.
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

set -ex

BUILD_TYPE=all

if [[ $# -eq 1 ]]; then
    BUILD_TYPE=$1

elif [[ $# -gt 1 ]]; then
    >&2 echo "ERROR: too many parameters are provided"
    exit 1
fi

CUDA_CLASSIFIER=${CUDA_CLASSIFIER:-'cuda11'}
CLASSIFIER=${CLASSIFIER:-"$CUDA_CLASSIFIER"} # default as CUDA_CLASSIFIER for compatibility
MVN_CMD="mvn -Dmaven.wagon.http.retryHandler.count=3"
MVN_BUILD_ARGS="-Drat.skip=true -Dmaven.scaladoc.skip -Dmaven.scalastyle.skip=true -Dcuda.version=$CLASSIFIER"

mvn_verify() {
    echo "Run mvn verify..."

    # Download a Scala 2.12 build of spark
    prepare_spark $SPARK_VER 2.12

    # get merge BASE from merged pull request. Log message e.g. "Merge HEAD into BASE"
    BASE_REF=$(git --no-pager log --oneline -1 | awk '{ print $NF }')
    # file size check for pull request. The size of a committed file should be less than 1.5MiB
    pre-commit run check-added-large-files --from-ref $BASE_REF --to-ref HEAD

    MVN_INSTALL_CMD="env -u SPARK_HOME $MVN_CMD -U -B $MVN_URM_MIRROR clean install $MVN_BUILD_ARGS -DskipTests -pl aggregator -am"

    # For snapshot versions, we do mvn[compile,RAT,scalastyle,docgen] CI in github actions
    for version in "${SPARK_SHIM_VERSIONS_NOSNAPSHOTS_TAIL[@]}"
    do
        echo "Spark version: $version"
        # build and run unit tests on one specific version for each sub-version (e.g. 320, 330) except base version
        # separate the versions to two ci stages (mvn_verify, ci_2) for balancing the duration
        if [[ "${SPARK_SHIM_VERSIONS_PREMERGE_UT_1[@]}" =~ "$version" ]]; then
            env -u SPARK_HOME \
              $MVN_CMD -U -B $MVN_URM_MIRROR -Dbuildver=$version clean install $MVN_BUILD_ARGS -Dpytest.TEST_TAGS=''
            # Run filecache tests
            env -u SPARK_HOME SPARK_CONF=spark.rapids.filecache.enabled=true \
              $MVN_CMD -B $MVN_URM_MIRROR -Dbuildver=$version test -rf tests $MVN_BUILD_ARGS -Dpytest.TEST_TAGS='' \
              -DwildcardSuites=org.apache.spark.sql.rapids.filecache.FileCacheIntegrationSuite
        # build only for other versions
        # elif [[ "${SPARK_SHIM_VERSIONS_NOSNAPSHOTS_TAIL[@]}" =~ "$version" ]]; then
        #     $MVN_INSTALL_CMD -DskipTests -Dbuildver=$version
        fi
    done
    # build base shim version for following test step with PREMERGE_PROFILES
    $MVN_INSTALL_CMD -DskipTests -Dbuildver=$SPARK_BASE_SHIM_VERSION

    # enable UTF-8 for regular expression tests
    for version in "${SPARK_SHIM_VERSIONS_PREMERGE_UTF8[@]}"
    do
        echo "Spark version (regex): $version"
        env -u SPARK_HOME LC_ALL="en_US.UTF-8" $MVN_CMD $MVN_URM_MIRROR -Dbuildver=$version test $MVN_BUILD_ARGS \
          -Dpytest.TEST_TAGS='' \
          -DwildcardSuites=com.nvidia.spark.rapids.ConditionalsSuite,com.nvidia.spark.rapids.RegularExpressionSuite,com.nvidia.spark.rapids.RegularExpressionTranspilerSuite
    done

    # Here run Python integration tests tagged with 'premerge_ci_1' only, that would help balance test duration and memory
    # consumption from two k8s pods running in parallel, which executes 'mvn_verify()' and 'ci_2()' respectively.
    $MVN_CMD -B $MVN_URM_MIRROR $PREMERGE_PROFILES clean verify -Dpytest.TEST_TAGS="premerge_ci_1" \
        -Dpytest.TEST_TYPE="pre-commit" -Dcuda.version=$CLASSIFIER

    # The jacoco coverage should have been collected, but because of how the shade plugin
    # works and jacoco we need to clean some things up so jacoco will only report for the
    # things we care about
    SPK_VER=${JACOCO_SPARK_VER:-"320"}
    mkdir -p target/jacoco_classes/
    FILE=$(ls dist/target/rapids-4-spark_2.12-*.jar | grep -v test | xargs readlink -f)
    UDF_JAR=$(ls ./udf-compiler/target/spark${SPK_VER}/rapids-4-spark-udf_2.12-*-spark${SPK_VER}.jar | grep -v test | xargs readlink -f)
    pushd target/jacoco_classes/
    jar xf $FILE com org rapids spark-shared "spark${JACOCO_SPARK_VER:-320}/"
    # extract the .class files in udf jar and replace the existing ones in spark3xx-ommon and spark$SPK_VER
    # because the class files in udf jar will be modified in aggregator's shade phase
    jar xf "$UDF_JAR" com/nvidia/spark/udf
    # TODO Should clean up unused and duplicated 'org/roaringbitmap' in the spark3xx shim folders, https://github.com/NVIDIA/spark-rapids/issues/11175
    rm -rf com/nvidia/shaded/ org/openucx/ spark${SPK_VER}/META-INF/versions/*/org/roaringbitmap/ spark-shared/com/nvidia/spark/udf/ spark${SPK_VER}/com/nvidia/spark/udf/
    popd

    # Triggering here until we change the jenkins file
    rapids_shuffle_smoke_test

    # Test a portion of cases for non-UTC time zone because of limited GPU resources.
    # Here testing: parquet scan, orc scan, csv scan, cast, TimeZoneAwareExpression, FromUTCTimestamp
    # Nightly CIs will cover all the cases.
    source "$(dirname "$0")"/test-timezones.sh
    for tz in "${time_zones_test_cases[@]}"
    do
        TZ=$tz ./integration_tests/run_pyspark_from_build.sh -m tz_sensitive_test
    done
}

rapids_shuffle_smoke_test() {
    echo "Run rapids_shuffle_smoke_test..."

    # basic ucx check
    ucx_info -d

    # run in standalone mode
    export SPARK_MASTER_HOST=localhost
    export SPARK_MASTER=spark://$SPARK_MASTER_HOST:7077
    $SPARK_HOME/sbin/start-master.sh -h $SPARK_MASTER_HOST
    $SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 $SPARK_MASTER

    invoke_shuffle_integration_test() {
      # check out what else is on the GPU
      nvidia-smi

      # because the RapidsShuffleManager smoke tests work against a standalone cluster
      # we do not want the integration tests to launch N different applications, just one app
      # is what is expected.
      TEST_PARALLEL=0 \
      PYSP_TEST_spark_master=$SPARK_MASTER \
        PYSP_TEST_spark_cores_max=2 \
        PYSP_TEST_spark_executor_cores=1 \
        PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.$SHUFFLE_SPARK_SHIM.RapidsShuffleManager \
        PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0 \
        PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=0.1 \
        PYSP_TEST_spark_rapids_memory_gpu_allocFraction=0.1 \
        ./integration_tests/run_pyspark_from_build.sh -m shuffle_test
    }

    # using UCX shuffle
    # The UCX_TLS=^posix config is removing posix from the list of memory transports
    # so that IPC regions are obtained using SysV API instead. This was done because of
    # itermittent test failures. See: https://github.com/NVIDIA/spark-rapids/issues/6572
    PYSP_TEST_spark_rapids_shuffle_mode=UCX \
    PYSP_TEST_spark_executorEnv_UCX_ERROR_SIGNALS="" \
    PYSP_TEST_spark_executorEnv_UCX_TLS="^posix" \
        invoke_shuffle_integration_test

    # using MULTITHREADED shuffle
    PYSP_TEST_spark_rapids_shuffle_mode=MULTITHREADED \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_writer_threads=2 \
    PYSP_TEST_spark_rapids_shuffle_multiThreaded_reader_threads=2 \
        invoke_shuffle_integration_test

    $SPARK_HOME/sbin/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 1
    $SPARK_HOME/sbin/stop-master.sh
}

ci_2() {
    echo "Run premerge ci 2 testings..."
    $MVN_CMD -U -B $MVN_URM_MIRROR clean package $MVN_BUILD_ARGS -DskipTests=true
    export TEST_TAGS="not premerge_ci_1"
    export TEST_TYPE="pre-commit"

    # Download a Scala 2.12 build of spark
    prepare_spark $SPARK_VER 2.12
    ./integration_tests/run_pyspark_from_build.sh

    # enable avro test separately
    INCLUDE_SPARK_AVRO_JAR=true TEST='avro_test.py' ./integration_tests/run_pyspark_from_build.sh
    # export 'LC_ALL' to set locale with UTF-8 so regular expressions are enabled
    LC_ALL="en_US.UTF-8" TEST="regexp_test.py" ./integration_tests/run_pyspark_from_build.sh

    # put some mvn tests here to balance durations of parallel stages
    echo "Run mvn package..."
    for version in "${SPARK_SHIM_VERSIONS_PREMERGE_UT_2[@]}"
    do
        env -u SPARK_HOME $MVN_CMD -U -B $MVN_URM_MIRROR -Dbuildver=$version clean package $MVN_BUILD_ARGS \
          -Dpytest.TEST_TAGS=''
    done
}

ci_scala213() {
    echo "Run premerge ci (Scala 2.13) testing..."
    # Run scala2.13 build and test against JDK17
    export JAVA_HOME=$(echo /usr/lib/jvm/java-1.17.0-*)
    update-java-alternatives --set $JAVA_HOME
    java -version

    cd scala2.13
    ln -sf ../jenkins jenkins

    # Download a Scala 2.13 version of Spark
    prepare_spark 3.3.0 2.13

    # build Scala 2.13 versions
    for version in "${SPARK_SHIM_VERSIONS_PREMERGE_SCALA213[@]}"
    do
        echo "Spark version (Scala 2.13): $version"
        env -u SPARK_HOME \
            $MVN_CMD -U -B $MVN_URM_MIRROR -Dbuildver=$version clean install $MVN_BUILD_ARGS -Dpytest.TEST_TAGS=''
        # Run filecache tests
        env -u SPARK_HOME SPARK_CONF=spark.rapids.filecache.enabled=true \
            $MVN_CMD -B $MVN_URM_MIRROR -Dbuildver=$version test -rf tests $MVN_BUILD_ARGS -Dpytest.TEST_TAGS='' \
            -DwildcardSuites=org.apache.spark.sql.rapids.filecache.FileCacheIntegrationSuite
    done

    $MVN_CMD -U -B $MVN_URM_MIRROR clean package $MVN_BUILD_ARGS -DskipTests=true
    cd .. # Run integration tests in the project root dir to leverage test cases and resource files
    export TEST_TAGS="not premerge_ci_1"
    export TEST_TYPE="pre-commit"
    # SPARK_HOME (and related) must be set to a Spark built with Scala 2.13
    SPARK_HOME=$SPARK_HOME PYTHONPATH=$PYTHONPATH \
        ./integration_tests/run_pyspark_from_build.sh
    # enable avro test separately
    SPARK_HOME=$SPARK_HOME PYTHONPATH=$PYTHONPATH \
        INCLUDE_SPARK_AVRO_JAR=true TEST='avro_test.py' ./integration_tests/run_pyspark_from_build.sh
    # export 'LC_ALL' to set locale with UTF-8 so regular expressions are enabled
    SPARK_HOME=$SPARK_HOME PYTHONPATH=$PYTHONPATH \
        LC_ALL="en_US.UTF-8" TEST="regexp_test.py" ./integration_tests/run_pyspark_from_build.sh
}

prepare_spark() {
    spark_ver=${1:-'3.2.0'}
    scala_ver=${2:-'2.12'}

    ARTF_ROOT="$(pwd)/.download"
    rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT
    # Download a full version of spark
    . jenkins/hadoop-def.sh $spark_ver $scala_ver
    wget -P $ARTF_ROOT $SPARK_REPO/org/apache/spark/$spark_ver/spark-$spark_ver-$BIN_HADOOP_VER.tgz

    export SPARK_HOME="$ARTF_ROOT/spark-$spark_ver-$BIN_HADOOP_VER"
    export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
    tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && rm -f $SPARK_HOME.tgz
    # copy python path libs to container /tmp instead of workspace to avoid ephemeral PVC issue
    TMP_PYTHON=/tmp/$(date +"%Y%m%d")
    rm -rf $TMP_PYTHON && cp -r $SPARK_HOME/python $TMP_PYTHON
    export PYTHONPATH=$TMP_PYTHON/python:$TMP_PYTHON/python/pyspark/:$(echo -n $TMP_PYTHON/python/lib/py4j-*-src.zip)
}

nvidia-smi

. jenkins/version-def.sh

PREMERGE_PROFILES="-Ppre-merge"

# If possible create '~/.m2' cache from pre-created m2 tarball to minimize the impact of unstable network connection.
# Please refer to job 'update_premerge_m2_cache' on Blossom about building m2 tarball details.
M2_CACHE_TAR=${M2_CACHE_TAR:-"/home/jenkins/agent/m2_cache/premerge_m2_cache.tar"}
if [ -s "$M2_CACHE_TAR" ] ; then
    tar xf $M2_CACHE_TAR -C ~/
fi

case $BUILD_TYPE in

    all)
        echo "Run all testings..."
        mvn_verify
        ci_2
        ci_scala213
        ;;

    mvn_verify)
        mvn_verify
        ;;

    ci_2 )
        ci_2
        ;;

    ci_scala213 )
        ci_scala213
        ;;

    *)
        echo "ERROR: unknown parameter: $BUILD_TYPE"
        ;;
esac
