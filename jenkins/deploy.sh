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

# Argument(s):
#   SIGN_FILE:  true/false, whether to sign the jar/pom file to de deployed
#   DATABRICKS: true/fasle, whether deploying for databricks
#
# Used environment(s):
#   SQL_PL:         The path of module 'sql-plugin', relative to project root path.
#   DIST_PL:        The path of module 'dist', relative to project root path.
#   SERVER_ID:      The repository id for this deployment.
#   SERVER_URL:     The url where to deploy artifacts.
#   GPG_PASSPHRASE: The passphrase used to sign files, only required when <SIGN_FILE> is true.
###

set -e
SIGN_FILE=$1
DATABRICKS=$2

###### Build the path of jar(s) to be deployed ######

cd $WORKSPACE

###### Databricks built tgz file so we need to untar and deploy from that
if [ "$DATABRICKS" == true ]; then
    rm -rf deploy
    mkdir -p deploy
    cd deploy
    tar -zxf ../spark-rapids-built.tgz
    cd spark-rapids
fi

ART_ID=`mvn help:evaluate -q -pl $DIST_PL -Dexpression=project.artifactId -DforceStdout`
ART_VER=`mvn help:evaluate -q -pl $DIST_PL -Dexpression=project.version -DforceStdout`

FPATH="$DIST_PL/target/$ART_ID-$ART_VER"

echo "Plan to deploy ${FPATH}.jar to $SERVER_URL (ID:$SERVER_ID)"


###### Choose the deploy command ######

if [ "$SIGN_FILE" == true ]; then
    # No javadoc and sources jar is generated for shade artifact only. Use 'sql-plugin' instead
    SQL_ART_ID=`mvn help:evaluate -q -pl $SQL_PL -Dexpression=project.artifactId -DforceStdout`
    SQL_ART_VER=`mvn help:evaluate -q -pl $SQL_PL -Dexpression=project.version -DforceStdout`
    JS_FPATH="${SQL_PL}/target/${SQL_ART_ID}-${SQL_ART_VER}"
    SRC_DOC_JARS="-Dsources=${JS_FPATH}-sources.jar -Djavadoc=${JS_FPATH}-javadoc.jar"
    DEPLOY_CMD="mvn -B gpg:sign-and-deploy-file -s jenkins/settings.xml -Dgpg.passphrase=$GPG_PASSPHRASE"
else
    DEPLOY_CMD="mvn -B deploy:deploy-file -s jenkins/settings.xml"
fi

echo "Deploy CMD: $DEPLOY_CMD"


###### Deploy the parent pom file ######

$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
            -Dfile=./pom.xml -DpomFile=./pom.xml

###### Deploy the artifact jar(s) ######

# Distribution jar is a shaded artifact so use the reduced dependency pom.
$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
            $SRC_DOC_JARS \
            -Dfile=$FPATH.jar -DpomFile=${DIST_PL}/dependency-reduced-pom.xml

###### Deploy integration tests jar(s) ######
TESTS_ART_ID=`mvn help:evaluate -q -pl $TESTS_PL -Dexpression=project.artifactId -DforceStdout`
TESTS_ART_VER=`mvn help:evaluate -q -pl $TESTS_PL -Dexpression=project.version -DforceStdout`
TESTS_FPATH="$TESTS_PL/target/$TESTS_ART_ID-$TESTS_ART_VER"
TESTS_DOC_JARS="-Dsources=${TESTS_FPATH}-sources.jar -Djavadoc=${TESTS_FPATH}-javadoc.jar"
$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
            $TESTS_DOC_JARS \
            -Dfile=$TESTS_FPATH.jar -DpomFile=${TESTS_PL}/pom.xml

###### Deploy profiling tool jar(s) ######
TOOL_PL=${TOOL_PL:-"rapids-4-spark-tools"}
TOOL_ART_ID=`mvn help:evaluate -q -pl $TOOL_PL -Dexpression=project.artifactId -DforceStdout`
TOOL_ART_VER=`mvn help:evaluate -q -pl $TOOL_PL -Dexpression=project.version -DforceStdout`
TOOL_FPATH="$TOOL_PL/target/$TOOL_ART_ID-$TOOL_ART_VER"
TOOL_DOC_JARS="-Dsources=${TOOL_FPATH}-sources.jar -Djavadoc=${TOOL_FPATH}-javadoc.jar"
$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
            $TOOL_DOC_JARS \
            -Dfile=$TOOL_FPATH.jar -DpomFile=${TOOL_PL}/pom.xml
