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

# Argument(s):
#   SIGN_FILE:  true/false, whether to sign the jar/pom file to de deployed
#
# Used environment(s):
#   SQL_PL:         The path of module 'sql-plugin', relative to project root path.
#   DIST_PL:        The path of module 'dist', relative to project root path.
#   SERVER_ID:      The repository id for this deployment.
#   SERVER_URL:     The url where to deploy artifacts.
#   GPG_PASSPHRASE: The passphrase used to sign files, only required when <SIGN_FILE> is true.
#   SIGN_TOOL:      Tool to sign files, e.g., gpg, nvsec, only required when $1 is 'true'
#   GPG_PASSPHRASE: gpg passphrase to sign artifacts, only required when <SIGN_TOOL> is gpg
#   MVN_SETTINGS:   Maven configuration file
#   POM_FILE:       Project pom file to be deployed
#   OUT_PATH:       The path where jar files are
#   CLASSIFIERS:    Comma separated classifiers, e.g., "cuda11"
###

set -ex

SIGN_FILE=${1:-"false"}
DIST_PL=${DIST_PL:-"dist"}

###### Build the path of jar(s) to be deployed ######
MVN_SETTINGS=${MVN_SETTINGS:-"jenkins/settings.xml"}
MVN="mvn -B -Dmaven.wagon.http.retryHandler.count=3 -DretryFailedDeploymentCount=3 -s $MVN_SETTINGS"
ART_ID=`$MVN help:evaluate -q -pl $DIST_PL -Dexpression=project.artifactId -DforceStdout`
ART_GROUP_ID=`$MVN help:evaluate -q -pl $DIST_PL -Dexpression=project.groupId -DforceStdout`
ART_VER=`$MVN help:evaluate -q -f $DIST_PL -Dexpression=project.version -DforceStdout`
CUDA_CLASSIFIER=`mvn help:evaluate -q -pl $DIST_PL -Dexpression=cuda.version -DforceStdout`

SQL_PL=${SQL_PL:-"sql-plugin"}
POM_FILE=${POM_FILE:-"$DIST_PL/target/parallel-world/META-INF/maven/${ART_GROUP_ID}/${ART_ID}/pom.xml"}
OUT_PATH=${OUT_PATH:-"$DIST_PL/target"}
SIGN_TOOL=${SIGN_TOOL:-"gpg"}

FPATH="$OUT_PATH/$ART_ID-$ART_VER"
cp $FPATH-$CUDA_CLASSIFIER.jar $FPATH.jar

# dist does not have javadoc and sources jars, use 'sql-plugin' instead
source jenkins/version-def.sh >/dev/null 2&>1
echo $SPARK_BASE_SHIM_VERSION
SQL_ART_ID=`mvn help:evaluate -q -pl $SQL_PL -Dexpression=project.artifactId -DforceStdout`
SQL_ART_VER=`mvn help:evaluate -q -pl $SQL_PL -Dexpression=project.version -DforceStdout`
JS_FPATH="${SQL_PL}/target/spark${SPARK_BASE_SHIM_VERSION}/${SQL_ART_ID}-${SQL_ART_VER}"
cp $JS_FPATH-sources.jar $FPATH-sources.jar
cp $JS_FPATH-javadoc.jar $FPATH-javadoc.jar

echo "Plan to deploy ${FPATH}.jar to $SERVER_URL (ID:$SERVER_ID)"

###### Choose the deploy command ######
if [ "$SIGN_FILE" == true ]; then
    case $SIGN_TOOL in
        nvsec)
            DEPLOY_CMD="$MVN gpg:sign-and-deploy-file -Dgpg.executable=nvsec_sign"
            ;;
        gpg)
            DEPLOY_CMD="$MVN gpg:sign-and-deploy-file -Dgpg.passphrase=$GPG_PASSPHRASE "
            ;;
        *)
            echo "Error unsupported sign type : $SIGN_TYPE !"
            echo "Please set variable SIGN_TOOL 'nvsec'or 'gpg'"
            exit -1
            ;;
    esac
else
    DEPLOY_CMD="$MVN -B deploy:deploy-file -s jenkins/settings.xml"
fi
DEPLOY_CMD="$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID"
echo "Deploy CMD: $DEPLOY_CMD"

###### Deploy the parent pom file ######
$DEPLOY_CMD -Dfile=./pom.xml -DpomFile=./pom.xml

###### Deploy the artifact jar(s) ######
$DEPLOY_CMD -DpomFile=$POM_FILE \
            -Dfile=$FPATH.jar \
            -Dsources=$FPATH-sources.jar \
            -Djavadoc=$FPATH-javadoc.jar \
            -Dfiles=$FPATH-$CUDA_CLASSIFIER.jar \
            -Dtypes=jar \
            -Dclassifiers=$CUDA_CLASSIFIER
