#!/bin/bash
#
# Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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
    tar -zxvf ../spark-rapids-built.tgz
    cd spark-rapids
fi

ART_ID=`mvn exec:exec -q -pl $DIST_PL -Dexec.executable=echo -Dexec.args='${project.artifactId}'`
ART_VER=`mvn exec:exec -q -pl $DIST_PL -Dexec.executable=echo -Dexec.args='${project.version}'`

FPATH="$DIST_PL/target/$ART_ID-$ART_VER"

echo "Plan to deploy ${FPATH}.jar to $SERVER_URL (ID:$SERVER_ID)"


###### Choose the deploy command ######

if [ "$SIGN_FILE" == true ]; then
    # No javadoc and sources jar is generated for shade artifact only. Use 'sql-plugin' instead
    SQL_ART_ID=`mvn exec:exec -q -pl $SQL_PL -Dexec.executable=echo -Dexec.args='${project.artifactId}'`
    SQL_ART_VER=`mvn exec:exec -q -pl $SQL_PL -Dexec.executable=echo -Dexec.args='${project.version}'`
    JS_FPATH="${SQL_PL}/target/${SQL_ART_ID}-${SQL_ART_VER}"
    SRC_DOC_JARS="-Dsources=${JS_FPATH}-sources.jar -Djavadoc=${JS_FPATH}-javadoc.jar"
    DEPLOY_CMD="mvn -B '-Pinclude-databricks,!snapshot-shims' gpg:sign-and-deploy-file -s jenkins/settings.xml -Dgpg.passphrase=$GPG_PASSPHRASE"
else
    DEPLOY_CMD="mvn -B '-Pinclude-databricks,!snapshot-shims' deploy:deploy-file -s jenkins/settings.xml"
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
