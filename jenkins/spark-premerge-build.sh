#!/bin/bash
#
# Copyright (c) 2020-2022, NVIDIA CORPORATION. All rights reserved.
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
    echo "ERROR: too many parameters are provided"
    exit 1
fi


mvn_verify() {
    echo "Run mvn verify..."
    # get merge BASE from merged pull request. Log message e.g. "Merge HEAD into BASE"
    BASE_REF=$(git --no-pager log --oneline -1 | awk '{ print $NF }')
    # file size check for pull request. The size of a committed file should be less than 1.5MiB
    pre-commit run check-added-large-files --from-ref $BASE_REF --to-ref HEAD

    # build the Spark 2.x explain jar
    env -u SPARK_HOME mvn -B $MVN_URM_MIRROR -Dbuildver=24X clean install -DskipTests

    # build all the versions but only run unit tests on one 3.1.X version (base version covers this), and one 3.2.X version.
    # All others shims test should be covered in nightly pipelines
    env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=321cdh clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am
    env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=312 clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am
    env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=313 clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am
    [[ $BUILD_MAINTENANCE_VERSION_SNAPSHOTS == "true" ]] && env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=314 clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am

    # don't skip tests
    env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=320 clean install -Drat.skip=true -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -Dpytest.TEST_TAGS='' -pl '!tools'
    # enable UTF-8 for regular expression tests
    env -u SPARK_HOME LC_ALL="en_US.UTF-8" mvn $MVN_URM_MIRROR -Dbuildver=320 test -Drat.skip=true -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -Dpytest.TEST_TAGS='' -pl '!tools' -DwildcardSuites=com.nvidia.spark.rapids.ConditionalsSuite,com.nvidia.spark.rapids.RegularExpressionSuite,com.nvidia.spark.rapids.RegularExpressionTranspilerSuite
    env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=321 clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am
    [[ $BUILD_MAINTENANCE_VERSION_SNAPSHOTS == "true" ]] && env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=322 clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am
    env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=330 clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am
    [[ $BUILD_MAINTENANCE_VERSION_SNAPSHOTS == "true" ]] && env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=331 clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am
    [[ $BUILD_FEATURE_VERSION_SNAPSHOTS == "true" ]] && env -u SPARK_HOME mvn -U -B $MVN_URM_MIRROR -Dbuildver=340 clean install -Drat.skip=true -DskipTests -Dmaven.javadoc.skip=true -Dskip -Dmaven.scalastyle.skip=true -Dcuda.version=$CUDA_CLASSIFIER -pl aggregator -am

    # Here run Python integration tests tagged with 'premerge_ci_1' only, that would help balance test duration and memory
    # consumption from two k8s pods running in parallel, which executes 'mvn_verify()' and 'ci_2()' respectively.
    mvn -B $MVN_URM_MIRROR $PREMERGE_PROFILES clean verify -Dpytest.TEST_TAGS="premerge_ci_1" \
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

    # non-caller classloader smoke test in pseudo-distributed
    # standalone cluster
    echo "Running test_cartesian_join_special_case_count with spark.rapids.force.caller.classloader=false"
    PYSP_TEST_spark_rapids_force_caller_classloader=false \
        NUM_LOCAL_EXECS=1 \
        TEST_PARALLEL=0 \
        ./integration_tests/run_pyspark_from_build.sh -k 'test_cartesian_join_special_case_count[100]'
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

    # using UCX shuffle
    PYSP_TEST_spark_master=$SPARK_MASTER \
      TEST_PARALLEL=0 \
      PYSP_TEST_spark_cores_max=2 \
      PYSP_TEST_spark_executor_cores=1 \
      SPARK_SUBMIT_FLAGS="--conf spark.executorEnv.UCX_ERROR_SIGNALS=" \
      PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.$SHUFFLE_SPARK_SHIM.RapidsShuffleManager \
      PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0 \
      PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=0.1 \
      PYSP_TEST_spark_rapids_memory_gpu_allocFraction=0.1 \
      ./integration_tests/run_pyspark_from_build.sh -m shuffle_test

    # using MT shuffle
    PYSP_TEST_spark_master=$SPARK_MASTER \
      TEST_PARALLEL=0 \
      PYSP_TEST_spark_cores_max=2 \
      PYSP_TEST_spark_executor_cores=1 \
      SPARK_SUBMIT_FLAGS="--conf spark.rapids.shuffle.mode=MULTITHREADED \
        --conf spark.rapids.shuffle.multiThreaded.writer.threads=2" \
      PYSP_TEST_spark_shuffle_manager=com.nvidia.spark.rapids.$SHUFFLE_SPARK_SHIM.RapidsShuffleManager \
      PYSP_TEST_spark_rapids_memory_gpu_minAllocFraction=0 \
      PYSP_TEST_spark_rapids_memory_gpu_maxAllocFraction=0.1 \
      PYSP_TEST_spark_rapids_memory_gpu_allocFraction=0.1 \
      ./integration_tests/run_pyspark_from_build.sh -m shuffle_test

    $SPARK_HOME/sbin/spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 1
    $SPARK_HOME/sbin/stop-master.sh
}

ci_2() {
    echo "Run premerge ci 2 testings..."
    mvn -U -B $MVN_URM_MIRROR clean package -DskipTests=true -Dcuda.version=$CUDA_CLASSIFIER
    export TEST_TAGS="not premerge_ci_1"
    export TEST_TYPE="pre-commit"
    export TEST_PARALLEL=4
    # separate process to avoid OOM kill
    TEST='conditionals_test or window_function_test' ./integration_tests/run_pyspark_from_build.sh
    TEST_PARALLEL=5 TEST='struct_test or time_window_test' ./integration_tests/run_pyspark_from_build.sh
    TEST='not conditionals_test and not window_function_test and not struct_test and not time_window_test' \
      ./integration_tests/run_pyspark_from_build.sh
    INCLUDE_SPARK_AVRO_JAR=true TEST='avro_test.py' ./integration_tests/run_pyspark_from_build.sh
    # export 'LC_ALL' to set locale with UTF-8 so regular expressions are enabled
    LC_ALL="en_US.UTF-8" TEST="regexp_test.py" ./integration_tests/run_pyspark_from_build.sh
}


nvidia-smi

. jenkins/version-def.sh

# controls whether we build snapshots for the Spark maintenance versions like 3.1.4 and 3.2.2
BUILD_MAINTENANCE_VERSION_SNAPSHOTS="false"
# controls whether we build snapshots for the next Spark major or feature version like 3.4.0 or 4.0.0
BUILD_FEATURE_VERSION_SNAPSHOTS="false"
PREMERGE_PROFILES="-PnoSnapshots,pre-merge"
if [[ ${PROJECT_VER} =~ ^22\.08\. ]]; then # enable snapshot builds for active development branch only
  BUILD_MAINTENANCE_VERSION_SNAPSHOTS="true"
  BUILD_FEATURE_VERSION_SNAPSHOTS="false"
  PREMERGE_PROFILES="-Psnapshots,pre-merge"
elif [[ ${PROJECT_VER} =~ ^22\.10\. ]]; then
  BUILD_MAINTENANCE_VERSION_SNAPSHOTS="true"
  BUILD_FEATURE_VERSION_SNAPSHOTS="true"
  PREMERGE_PROFILES="-Psnapshots,pre-merge"
fi

ARTF_ROOT="$WORKSPACE/.download"
MVN_GET_CMD="mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -B \
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
$MVN_GET_CMD \
    -DgroupId=org.apache -DartifactId=spark -Dversion=$SPARK_VER -Dclassifier=bin-hadoop3.2 -Dpackaging=tgz

export SPARK_HOME="$ARTF_ROOT/spark-$SPARK_VER-bin-hadoop3.2"
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
tar zxf $SPARK_HOME.tgz -C $ARTF_ROOT && \
    rm -f $SPARK_HOME.tgz
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip

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
