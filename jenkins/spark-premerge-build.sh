#!/bin/bash
#
# Copyright (c) 2020-2023, NVIDIA CORPORATION. All rights reserved.
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

MVN_CMD="mvn -Dmaven.wagon.http.retryHandler.count=3"
MVN_BUILD_ARGS="-Drat.skip=true -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER"

mvn_verify() {
    echo "Run mvn verify..."
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
            env -u SPARK_HOME $MVN_CMD -U -B $MVN_URM_MIRROR -Dbuildver=$version clean install $MVN_BUILD_ARGS \
              -Dpytest.TEST_TAGS=''
        # build only for other versions
        elif [[ "${SPARK_SHIM_VERSIONS_NOSNAPSHOTS_TAIL[@]}" =~ "$version" ]]; then
            $MVN_INSTALL_CMD -DskipTests -Dbuildver=$version
        fi
    done
    
    # enable UTF-8 for regular expression tests
    for version in "${SPARK_SHIM_VERSIONS_PREMERGE_UTF8[@]}"
    do
        env -u SPARK_HOME LC_ALL="en_US.UTF-8" $MVN_CMD $MVN_URM_MIRROR -Dbuildver=$version test $MVN_BUILD_ARGS \
          -Dpytest.TEST_TAGS='' \
          -DwildcardSuites=com.nvidia.spark.rapids.ConditionalsSuite,com.nvidia.spark.rapids.RegularExpressionSuite,com.nvidia.spark.rapids.RegularExpressionTranspilerSuite
    done
    
    # Here run Python integration tests tagged with 'premerge_ci_1' only, that would help balance test duration and memory
    # consumption from two k8s pods running in parallel, which executes 'mvn_verify()' and 'ci_2()' respectively.
    $MVN_CMD -B $MVN_URM_MIRROR $PREMERGE_PROFILES clean verify -Dpytest.TEST_TAGS="premerge_ci_1" \
        -Dpytest.TEST_TYPE="pre-commit" -Dpytest.TEST_PARALLEL=4 -Dcuda.version=$CUDA_CLASSIFIER

    # The jacoco coverage should have been collected, but because of how the shade plugin
    # works and jacoco we need to clean some things up so jacoco will only report for the
    # things we care about
    SPK_VER=${JACOCO_SPARK_VER:-"311"}
    mkdir -p target/jacoco_classes/
    FILE=$(ls dist/target/rapids-4-spark_2.12-*.jar | grep -v test | xargs readlink -f)
    UDF_JAR=$(ls ./udf-compiler/target/spark${SPK_VER}/rapids-4-spark-udf_2.12-*-spark${SPK_VER}.jar | grep -v test | xargs readlink -f)
    pushd target/jacoco_classes/
    jar xf $FILE com org rapids spark3xx-common "spark${JACOCO_SPARK_VER:-311}/"
    # extract the .class files in udf jar and replace the existing ones in spark3xx-ommon and spark$SPK_VER
    # because the class files in udf jar will be modified in aggregator's shade phase
    jar xf "$UDF_JAR" com/nvidia/spark/udf
    rm -rf com/nvidia/shaded/ org/openucx/ spark3xx-common/com/nvidia/spark/udf/ spark${SPK_VER}/com/nvidia/spark/udf/
    popd

    # Triggering here until we change the jenkins file
    rapids_shuffle_smoke_test
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
    export TEST_PARALLEL=5
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


nvidia-smi

. jenkins/version-def.sh

PREMERGE_PROFILES="-PnoSnapshots,pre-merge"

ARTF_ROOT="$WORKSPACE/.download"
MVN_GET_CMD="$MVN_CMD org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
    $MVN_URM_MIRROR -DremoteRepositories=$URM_URL \
    -Ddest=$ARTF_ROOT"

rm -rf $ARTF_ROOT && mkdir -p $ARTF_ROOT

# If possible create '~/.m2' cache from pre-created m2 tarball to minimize the impact of unstable network connection.
# Please refer to job 'update_premerge_m2_cache' on Blossom about building m2 tarball details.
M2_CACHE_TAR=${M2_CACHE_TAR:-"/home/jenkins/agent/m2_cache/premerge_m2_cache.tar"}
if [ -s "$M2_CACHE_TAR" ] ; then
    tar xf $M2_CACHE_TAR -C ~/
fi

# Download a full version of spark
. jenkins/hadoop-def.sh $SPARK_VER
wget -P $ARTF_ROOT $SPARK_REPO/org/apache/spark/$SPARK_VER/spark-$SPARK_VER-$BIN_HADOOP_VER.tgz

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-$BIN_HADOOP_VER"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz
# copy python path libs to container /tmp instead of workspace to avoid ephemeral PVC issue
TMP_PYTHON=/tmp/$(date +"%Y%m%d")
rm -rf $TMP_PYTHON && cp -r $SPARK_HOME/python $TMP_PYTHON
export PYTHONPATH=$TMP_PYTHON/python:$TMP_PYTHON/python/pyspark/:$TMP_PYTHON/python/lib/py4j-0.10.9-src.zip

case $BUILD_TYPE in

    all)
        echo "Run all testings..."
        mvn_verify
        ci_2
        ;;

    mvn_verify)
        mvn_verify
        ;;

    ci_2 )
        ci_2
        ;;

    *)
        echo "ERROR: unknown parameter: $BUILD_TYPE"
        ;;
esac
