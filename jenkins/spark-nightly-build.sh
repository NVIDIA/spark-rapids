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

. jenkins/version-def.sh

## export 'M2DIR' so that shims can get the correct Spark dependency info
export M2DIR=${M2DIR:-"$WORKSPACE/.m2"}

## MVN_OPT : maven options environment, e.g. MVN_OPT='-Dspark-rapids-jni.version=xxx' to specify spark-rapids-jni dependency's version.
MVN="mvn ${MVN_OPT}"

TOOL_PL=${TOOL_PL:-"tools"}
DIST_PL="dist"
function mvnEval {
    $MVN help:evaluate -q -pl $DIST_PL $MVN_URM_MIRROR -Prelease311 -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DforceStdout -Dexpression=$1
}

ART_ID=$(mvnEval project.artifactId)
ART_GROUP_ID=$(mvnEval project.groupId)
ART_VER=$(mvnEval project.version)

DIST_FPATH="$DIST_PL/target/$ART_ID-$ART_VER"
DIST_POM_FPATH="$DIST_PL/target/extra-resources/META-INF/maven/$ART_GROUP_ID/$ART_ID/pom.xml"

DIST_PROFILE_OPT=-Dincluded_buildvers=$(IFS=,; echo "${SPARK_SHIM_VERSIONS[*]}")
DIST_INCLUDES_DATABRICKS=${DIST_INCLUDES_DATABRICKS:-"true"}
if [[ "$DIST_INCLUDES_DATABRICKS" == "true" ]]; then
    DIST_PROFILE_OPT="$DIST_PROFILE_OPT,312db,321db"
fi

# Make sure that the local m2 repo on the build machine has the same pom
# installed as the one being pushed to the remote repo. This to prevent
# discrepancies between the build machines regardless of how the local repo was populated.
function distWithReducedPom {
    cmd="$1"

    case $cmd in

        install)
            mvnCmd="install:install-file"
            mvnExtaFlags="-Dpackaging=jar"
            ;;

        deploy)
            mvnCmd="deploy:deploy-file"
            mvnExtaFlags="-Durl=${URM_URL}-local -DrepositoryId=snapshots -Dtypes=jar -Dfiles=${DIST_FPATH}.jar -Dclassifiers=$CUDA_CLASSIFIER"
            ;;

        *)
            echo "Unknown command: $cmd"
            ;;
    esac

    $MVN -B $mvnCmd $MVN_URM_MIRROR \
        -Dcuda.version=$CUDA_CLASSIFIER \
        -Dmaven.repo.local=$M2DIR \
        -Dfile="${DIST_FPATH}.jar" \
        -DpomFile="${DIST_POM_FPATH}" \
        -DgroupId="${ART_GROUP_ID}" \
        -DartifactId="${ART_ID}" \
        -Dversion="${ART_VER}" \
        $mvnExtaFlags
}

# build the Spark 2.x explain jar
$MVN -B $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dbuildver=24X clean install -DskipTests
[[ $SKIP_DEPLOY != 'true' ]] && \
    $MVN -B deploy $MVN_URM_MIRROR \
        -Dmaven.repo.local=$M2DIR \
        -DskipTests \
        -Dbuildver=24X

# build, install, and deploy all the versions we support, but skip deploy of individual dist module since we
# only want the combined jar to be pushed.
# Note this does not run any integration tests
# Deploy jars unless SKIP_DEPLOY is 'true'

for buildver in "${SPARK_SHIM_VERSIONS[@]:1}"; do
    $MVN -U -B clean install -pl '!tools' $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
        -Dcuda.version=$CUDA_CLASSIFIER \
        -Dbuildver="${buildver}"
    distWithReducedPom "install"
    [[ $SKIP_DEPLOY != 'true' ]] && \
        $MVN -B deploy -pl '!tools,!dist' $MVN_URM_MIRROR \
            -Dmaven.repo.local=$M2DIR \
            -Dcuda.version=$CUDA_CLASSIFIER \
            -DskipTests \
            -Dbuildver="${buildver}"
done

$MVN -B clean install -pl '!tools' \
    $DIST_PROFILE_OPT \
    -Dbuildver=$SPARK_BASE_SHIM_VERSION \
    $MVN_URM_MIRROR \
    -Dmaven.repo.local=$M2DIR \
    -Dcuda.version=$CUDA_CLASSIFIER

distWithReducedPom "install"

if [[ $SKIP_DEPLOY != 'true' ]]; then
    DIST_FPATH="$DIST_FPATH-$CUDA_CLASSIFIER"
    distWithReducedPom "deploy"

    # this deploy includes 'tools' that is unconditionally built with Spark 3.1.1
    $MVN -B deploy -pl '!dist' \
        -Dbuildver=$SPARK_BASE_SHIM_VERSION \
        $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
        -Dcuda.version=$CUDA_CLASSIFIER \
        -DpomFile=${TOOL_PL}/dependency-reduced-pom.xml
fi

# Parse Spark files from local mvn repo
jenkins/printJarVersion.sh "SPARKVersion" "$M2DIR/org/apache/spark/spark-core_2.12/${SPARK_VER}" "spark-core_2.12-${SPARK_VER}" ".jar" $SERVER_ID
