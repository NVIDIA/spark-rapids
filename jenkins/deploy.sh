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

set -e
SIGN_FILE=$1

###### Build the path of jar(s) to be deployed ######

cd $WORKSPACE
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
    DEPLOY_CMD="mvn -B gpg:sign-and-deploy-file -s jenkins/settings.xml -Dgpg.passphrase=$GPG_PASSPHRASE"
else
    DEPLOY_CMD="mvn -B deploy:deploy-file -s jenkins/settings.xml"
fi

echo "Deploy CMD: $DEPLOY_CMD"


###### Deploy the parent pom file ######

$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
            -Dfile=./pom.xml -DpomFile=./pom.xml

###### Deploy the artifact jar(s) ######

$DEPLOY_CMD -Durl=$SERVER_URL -DrepositoryId=$SERVER_ID \
            $SRC_DOC_JARS \
            -Dfile=$FPATH.jar -DpomFile=${DIST_PL}/pom.xml

