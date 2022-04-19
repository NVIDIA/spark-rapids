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

# Argument(s):
#   SIGN_FILE:  true/false, whether to sign the jar/pom file to de deployed
#   DATABRICKS: true/fasle, whether deploying for databricks
#   VERSIONS_BUILT: The spark versions built before calling this script
#
# Used environment(s):
#   SQL_PL:         The path of module 'sql-plugin', relative to project root path.
#   DIST_PL:        The path of module 'dist', relative to project root path.
#   AGGREGATOR_PL:  The path of the module 'aggregator', relative to project root path.
#   TESTS_PL:       The path of the module 'integration_tests', relative to the project root path.
#   SERVER_ID:      The repository id for this deployment.
#   SERVER_URL:     The url where to deploy artifacts.
#   GPG_PASSPHRASE: The passphrase used to sign files, only required when <SIGN_FILE> is true.
#   FINAL_AGG_VERSION_TOBUILD: The spark version of the final build and aggregation.
###

set -ex
SIGN_FILE=$1
DATABRICKS=$2
VERSIONS_BUILT=$3

export M2DIR=${M2DIR:-"$WORKSPACE/.m2"}

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
CUDA_CLASSIFIER=`mvn help:evaluate -q -pl $DIST_PL -Dexpression=cuda.version -DforceStdout`

FPATH="$DIST_PL/target/$ART_ID-$ART_VER-$CUDA_CLASSIFIER"
POM_FPATH="$DIST_PL/target/extra-resources/META-INF/maven/com.nvidia/$ART_ID/pom.xml"

echo "Plan to deploy ${FPATH}.jar to $SERVER_URL (ID:$SERVER_ID)"

FINAL_AGG_VERSION_TOBUILD=${FINAL_AGG_VERSION_TOBUILD:-'311'}
###### Choose the deploy command ######

if [ "$SIGN_FILE" == true ]; then
    # No javadoc and sources jar is generated for shade artifact only. Use 'sql-plugin' instead
    SQL_ART_ID=`mvn help:evaluate -q -pl $SQL_PL -Dexpression=project.artifactId -DforceStdout`
    SQL_ART_VER=`mvn help:evaluate -q -pl $SQL_PL -Dexpression=project.version -DforceStdout`
    JS_FPATH="${SQL_PL}/target/spark${FINAL_AGG_VERSION_TOBUILD}/${SQL_ART_ID}-${SQL_ART_VER}"
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
            -Dfile=$FPATH.jar -DgroupId=com.nvidia -DartifactId=$ART_ID -Dversion=$ART_VER \
            -DpomFile="$POM_FPATH" -Dclassifiers=$CUDA_CLASSIFIER

###### Deploy profiling tool jar(s) ######
TOOL_PL=${TOOL_PL:-"tools"}
TOOL_ART_ID=`mvn help:evaluate -q -pl $TOOL_PL -Dexpression=project.artifactId -DforceStdout -Prelease311`
TOOL_ART_VER=`mvn help:evaluate -q -pl $TOOL_PL -Dexpression=project.version -DforceStdout -Prelease311`
TOOL_FPATH="deployjars/$TOOL_ART_ID-$TOOL_ART_VER"
TOOL_DOC_JARS="-Dsources=${TOOL_FPATH}-sources.jar -Djavadoc=${TOOL_FPATH}-javadoc.jar"
$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
            $TOOL_DOC_JARS \
            -Dfile=$TOOL_FPATH.jar -DpomFile=${TOOL_PL}/pom.xml

###### Deploy Spark 2.x explain meta jar ######
SPARK2_PL=${SPARK2_PL:-"spark2-sql-plugin"}
SPARK2_ART_ID=`mvn help:evaluate -q -pl $SPARK2_PL -Dexpression=project.artifactId -DforceStdout -Dbuildver=24X`
SPARK2_ART_VER=`mvn help:evaluate -q -pl $SPARK2_PL -Dexpression=project.version -DforceStdout -Dbuildver=24X`
SPARK2_FPATH="$M2DIR/com/nvidia/$SPARK2_ART_ID/$SPARK2_ART_VER/$SPARK2_ART_ID-$SPARK2_ART_VER"
SPARK2_DOC_JARS="-Dsources=${SPARK2_FPATH}-sources.jar -Djavadoc=${SPARK2_FPATH}-javadoc.jar"
# a bit ugly but just hardcode to spark24 for now since only version supported
SPARK2_CLASSIFIER='spark24'
SPARK2_CLASSIFIER_JAR="${SPARK2_FPATH}-${SPARK2_CLASSIFIER}.jar"
# Oss requires a main jar file along with classifier jars
cp ${SPARK2_FPATH}-${SPARK2_CLASSIFIER}.jar ${SPARK2_FPATH}.jar
$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
            $SPARK2_DOC_JARS \
            -Dfile=${SPARK2_FPATH}.jar -DpomFile=${SPARK2_PL}/pom.xml \
            -Dfiles=$SPARK2_CLASSIFIER_JAR \
            -Dtypes=jar \
            -Dclassifiers=$SPARK2_CLASSIFIER
