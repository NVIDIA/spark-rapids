#!/bin/bash
#
# Copyright (c) 2020-2021, NVIDIA CORPORATION. All rights reserved.
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

## export 'M2DIR' so that shims can get the correct cudf/spark dependency info
export M2DIR="$WORKSPACE/.m2"

DIST_PL="dist"
function mvnEval {
    mvn help:evaluate -q -pl $DIST_PL $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR -Dcuda.version=$CUDA_CLASSIFIER -DforceStdout -Dexpression=$1
}

ART_ID=$(mvnEval project.artifactId)
ART_GROUP_ID=$(mvnEval project.groupId)
ART_VER=$(mvnEval project.version)

DIST_FPATH="$DIST_PL/target/$ART_ID-$ART_VER"

# build, install, and deploy all the versions we support, but skip deploy of individual dist module since we
# only want the combined jar to be pushed.
# Note this does not run any integration tests
# Deploy jars unless SKIP_DEPLOY is 'true'

for buildver in "${SPARK_SHIM_VERSIONS[@]:1}"; do
    # temporarily skip tests on Spark 3.3.0 - https://github.com/NVIDIA/spark-rapids/issues/4031
    [[ buildver == "330" ]] && skipTestsFor330=true || skipTestsFor330=false
    mvn -U -B clean install -pl '!tools' $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
        -Dcuda.version=$CUDA_CLASSIFIER \
        -Dbuildver="${buildver}" \
        -DskipTests="${skipTestsFor330}"
    # fix up dist jar with reduced pom in the local m2
    mvn -B install:install-file $MVN_URM_MIRROR \
        -Dcuda.version=$CUDA_CLASSIFIER \
        -Dmaven.repo.local=$M2DIR \
        -Dfile="${DIST_FPATH}.jar" \
        -DgroupId="${ART_GROUP_ID}" \
        -DartifactId="${ART_ID}" \
        -Dversion="${ART_VER}" \
        -Dpackaging=jar
    [[ $SKIP_DEPLOY != 'true' ]] && \
        mvn -B deploy -pl '!tools,!dist' $MVN_URM_MIRROR \
            -Dmaven.repo.local=$M2DIR \
            -Dcuda.version=$CUDA_CLASSIFIER \
            -DskipTests \
            -Dbuildver="${buildVer}"
done

mvn -B clean install -pl '!tools' \
    -PsnapshotsWithDatabricks \
    -Dbuildver=$SPARK_BASE_SHIM_VERSION \
    $MVN_URM_MIRROR \
    -Dmaven.repo.local=$M2DIR \
    -Dcuda.version=$CUDA_CLASSIFIER

# fix up dist jar with reduced pom in the local m2
mvn -B install:install-file $MVN_URM_MIRROR \
    -Dcuda.version=$CUDA_CLASSIFIER \
    -Dmaven.repo.local=$M2DIR \
    -Dfile="${DIST_FPATH}.jar" \
    -DgroupId="${ART_GROUP_ID}" \
    -DartifactId="${ART_ID}" \
    -Dversion="${ART_VER}" \
    -Dpackaging=jar

if [[ $SKIP_DEPLOY != 'true' ]]; then
    # deploy dist with reduced pom
    mvn -B deploy:deploy-file
        $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
        -Dcuda.version=$CUDA_CLASSIFIER \
        -Dfile="${DIST_FPATH}.jar" \
        -DgroupId="${ART_GROUP_ID}" \
        -DartifactId="${ART_ID}" \
        -Dversion="${ART_VER}"

    mvn -B deploy -pl '!tools,!dist' \
        -PsnapshotsWithDatabricks \
        -Dbuildver=$SPARK_BASE_SHIM_VERSION \
        $MVN_URM_MIRROR -Dmaven.repo.local=$M2DIR \
        -Dcuda.version=$CUDA_CLASSIFIER
fi

# Parse cudf and spark files from local mvn repo
jenkins/printJarVersion.sh "CUDFVersion" "$M2DIR/ai/rapids/cudf/${CUDF_VER}" "cudf-${CUDF_VER}" "-${CUDA_CLASSIFIER}.jar" $SERVER_ID
jenkins/printJarVersion.sh "SPARKVersion" "$M2DIR/org/apache/spark/spark-core_2.12/${SPARK_VER}" "spark-core_2.12-${SPARK_VER}" ".jar" $SERVER_ID
