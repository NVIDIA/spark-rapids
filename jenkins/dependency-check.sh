#!/bin/bash
#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
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
#   ARTIFACT_FILE :  Artifact(groupId:artifactId:version:[[packaging]:classifier]) list file
#
# Used environment(s):
#   SERVER_ID:      The repository id for this deployment.
#   SERVER_URL:     The url where to deploy artifacts.
#   M2_CACHE:       Maven local repo
###

set -ex

ARTIFACT_FILE=${1:-"/tmp/artifacts-list"}
SERVER_ID=${SERVER_ID:-"snapshots"}
SERVER_URL=${SERVER_URL:-"file:/tmp/local-release-repo"}
M2_CACHE=${M2_CACHE:-"/tmp/m2-cache"}

remote_maven_repo=$SERVER_ID::default::$SERVER_URL
# Get the spark-rapids-jni and spark-rapids-private jars from OSS Snapshot maven repo
if [ "$SERVER_ID" == "snapshots" ]; then
    oss_snapshot_url="https://oss.sonatype.org/content/repositories/snapshots"
    remote_maven_repo="$remote_maven_repo,$SERVER_ID::default::$oss_snapshot_url"
fi
while read line; do
    artifact=$line # artifact=groupId:artifactId:version:[[packaging]:classifier]
    mvn dependency:get -DremoteRepositories=$remote_maven_repo -Dmaven.repo.local=$M2_CACHE -Dartifact=$artifact
done < $ARTIFACT_FILE
