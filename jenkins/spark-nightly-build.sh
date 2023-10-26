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

SCALA_BINARY_VER=${SCALA_BINARY_VER:-"2.12"}
if [ $SCALA_BINARY_VER == "2.13" ]; then
    cd scala2.13
    ln -sf ../jenkins jenkins
fi

. jenkins/version-def.sh

WORKSPACE=${WORKSPACE:-$(pwd)}
## export 'M2DIR' so that shims can get the correct Spark dependency info
export M2DIR=${M2DIR:-"$WORKSPACE/.m2"}

## MVN_OPT : maven options environment, e.g. MVN_OPT='-Dspark-rapids-jni.version=xxx' to specify spark-rapids-jni dependency's version.
MVN="mvn -Dmaven.wagon.http.retryHandler.count=3 -DretryFailedDeploymentCount=3 ${MVN_OPT}"

DIST_PL="dist"
function mvnEval {
    $MVN help:evaluate -q -pl $DIST_PL $MVN_URM_MIRROR -Prelease311 -Dmaven.repo.local=$M2DIR -DforceStdout -Dexpression=$1
}

ART_ID=$(mvnEval project.artifactId)
ART_GROUP_ID=$(mvnEval project.groupId)
ART_VER=$(mvnEval project.version)
DEFAULT_CUDA_CLASSIFIER=${DEFAULT_CUDA_CLASSIFIER:-$(mvnEval cuda.version)} # default cuda version
CUDA_CLASSIFIERS=${CUDA_CLASSIFIERS:-"$DEFAULT_CUDA_CLASSIFIER"} # e.g. cuda11,cuda12
IFS=',' read -a CUDA_CLASSIFIERS_ARR <<< "$CUDA_CLASSIFIERS"
TMP_PATH="/tmp/$(date '+%Y-%m-%d')-$$"

DIST_FPATH="$DIST_PL/target/$ART_ID-$ART_VER-$DEFAULT_CUDA_CLASSIFIER"
DIST_POM_FPATH="$DIST_PL/target/parallel-world/META-INF/maven/$ART_GROUP_ID/$ART_ID/pom.xml"

DIST_PROFILE_OPT=-Dincluded_buildvers=$(IFS=,; echo "${SPARK_SHIM_VERSIONS[*]}")
DIST_INCLUDES_DATABRICKS=${DIST_INCLUDES_DATABRICKS:-"true"}
if [[ "$DIST_INCLUDES_DATABRICKS" == "true" ]] && [[ -n ${SPARK_SHIM_VERSIONS_DATABRICKS[*]} ]] && [[ "$SCALA_BINARY_VER" == "2.12" ]]; then
    DIST_PROFILE_OPT="$DIST_PROFILE_OPT,"$(IFS=,; echo "${SPARK_SHIM_VERSIONS_DATABRICKS[*]}")
fi

DEPLOY_TYPES='jar'
DEPLOY_FILES="${DIST_FPATH}.jar"
DEPLOY_CLASSIFIERS="$DEFAULT_CUDA_CLASSIFIER"
# Make sure that the local m2 repo on the build machine has the same pom
# installed as the one being pushed to the remote repo. This to prevent
# discrepancies between the build machines regardless of how the local repo was populated.
function distWithReducedPom {
    cmd="$1"

    case $cmd in

        install)
            mvnCmd="install:install-file"
            mvnExtraFlags="-Dpackaging=jar"
            ;;

        deploy)
            mvnCmd="deploy:deploy-file"
            if (( ${#CUDA_CLASSIFIERS_ARR[@]} > 1 )); then
              # try move tmp artifacts back to target folder for simplifying separate release process
              mv ${TMP_PATH}/${ART_ID}-${ART_VER}-*.jar ${DIST_PL}/target/
            fi
            mvnExtraFlags="-Durl=${URM_URL}-local -DrepositoryId=snapshots -Dtypes=${DEPLOY_TYPES} -Dfiles=${DEPLOY_FILES} -Dclassifiers=${DEPLOY_CLASSIFIERS}"
            ;;

        *)
            echo "Unknown command: $cmd"
            ;;
    esac

    $MVN -B $mvnCmd $MVN_URM_MIRROR \
        -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER \
        -Dmaven.repo.local=$M2DIR \
        -Dfile="${DIST_FPATH}.jar" \
        -DpomFile="${DIST_POM_FPATH}" \
        -DgroupId="${ART_GROUP_ID}" \
        -DartifactId="${ART_ID}" \
        -Dversion="${ART_VER}" \
        $mvnExtraFlags
}

# build, install, and deploy all the versions we support, but skip deploy of individual dist module since we
# only want the combined jar to be pushed.
# Note this does not run any integration tests
# Deploy jars unless SKIP_DEPLOY is 'true'

# option to skip unit tests. Used in our CI to separate test runs in parallel stages
SKIP_TESTS=${SKIP_TESTS:-"false"}
for buildver in "${SPARK_SHIM_VERSIONS[@]:1}"; do
    $MVN -U -B clean install $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
        -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER \
        -DskipTests=$SKIP_TESTS \
        -Dbuildver="${buildver}"
    if [[ $SKIP_TESTS == "false" ]]; then
      # Run filecache tests
      SPARK_CONF=spark.rapids.filecache.enabled=true \
          $MVN -B test -rf tests $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
              -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER \
              -Dbuildver="${buildver}" \
              -DwildcardSuites=org.apache.spark.sql.rapids.filecache.FileCacheIntegrationSuite
    fi
    distWithReducedPom "install"
    [[ $SKIP_DEPLOY != 'true' ]] && \
        $MVN -B deploy -pl '!dist' $MVN_URM_MIRROR \
            -Dmaven.repo.local=$M2DIR \
            -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER \
            -DskipTests \
            -Dbuildver="${buildver}"
done

installDistArtifact() {
  local cuda_classifier="$1"
  $MVN -B clean install \
      $DIST_PROFILE_OPT \
      -Dbuildver=$SPARK_BASE_SHIM_VERSION \
      $MVN_URM_MIRROR \
      -Dmaven.repo.local=$M2DIR \
      -Dcuda.version=$cuda_classifier \
      -DskipTests=$SKIP_TESTS
}

# build extra cuda classifiers
if (( ${#CUDA_CLASSIFIERS_ARR[@]} > 1 )); then
  mkdir -p ${TMP_PATH}
  for classifier in "${CUDA_CLASSIFIERS_ARR[@]}"; do
    if [ "${classifier}" == "${DEFAULT_CUDA_CLASSIFIER}" ]; then
      echo "skip default: ${DEFAULT_CUDA_CLASSIFIER} in build extra cuda classifiers step..."
      continue
    fi
    installDistArtifact ${classifier}
    # move artifacts to temp for deployment later
    artifactFile="${ART_ID}-${ART_VER}-${classifier}.jar"
    mv ${DIST_PL}/target/${artifactFile} ${TMP_PATH}/
    # update deployment properties
    DEPLOY_TYPES="${DEPLOY_TYPES},jar"
    DEPLOY_FILES="${DEPLOY_FILES},${DIST_PL}/target/${artifactFile}"
    DEPLOY_CLASSIFIERS="${DEPLOY_CLASSIFIERS},${classifier}"
  done
fi
# build dist w/ default cuda classifier
installDistArtifact ${DEFAULT_CUDA_CLASSIFIER}

distWithReducedPom "install"

if [[ $SKIP_DEPLOY != 'true' ]]; then
    distWithReducedPom "deploy"

    # this deploys submodules except dist that is unconditionally built with Spark 3.1.1
    $MVN -B deploy -pl '!dist' \
        -Dbuildver=$SPARK_BASE_SHIM_VERSION \
        -DskipTests=$SKIP_TESTS \
        $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
        -Dcuda.version=$DEFAULT_CUDA_CLASSIFIER
fi

# Parse Spark files from local mvn repo
jenkins/printJarVersion.sh "SPARKVersion" "$M2DIR/org/apache/spark/spark-core_2.12/${SPARK_VER}" "spark-core_2.12-${SPARK_VER}" ".jar" $SERVER_ID
