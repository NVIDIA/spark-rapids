#!/bin/bash
# Copyright (c) 2022, NVIDIA CORPORATION.
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

set -ex

REPO_BASE=${REPO_BASE:-"nvidia"}
TAG_NAME=${TAG_NAME:-"rapids-4-spark-databricks"}
VERSION=${VERSION:-"22.08.0"}
TAG_VERSION=${TAG_VERSION:-$VERSION}
CUDA_VERSION=${CUDA_VERSION:-"11.5.2"}
CUDA_MAJOR=${CUDA_VERSION%.*}
CUDA_MAJOR=${CUDA_MAJOR/./-}

DOCKERFILE=${DOCKERFILE:-"Dockerfile"} 
BASE_JAR_URL=${BASE_JAR_URL:-"https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12"}

JAR_VERSION=${JAR_VERSION:-$VERSION}
JAR_FILE=${JAR_FILE:-"rapids-4-spark_2.12-${JAR_VERSION}-cuda11.jar"}
JAR_URL="${BASE_JAR_URL}/${VERSION}/${JAR_FILE}"

DRIVER_CONF_FILE=${DRIVER_CONF_FILE:-"00-custom-spark-driver-defaults-alluxio.conf"}
STAGE="databricks-alluxio"

docker build \
  --build-arg CUDA_VERSION=${CUDA_VERSION} \
  --build-arg CUDA_MAJOR=${CUDA_MAJOR} \
  --build-arg JAR_URL=${JAR_URL} \
  --build-arg DRIVER_CONF_FILE=${DRIVER_CONF_FILE} \
  --target $STAGE \
  -f ${DOCKERFILE} \
  -t "${REPO_BASE}/${TAG_NAME}:${TAG_VERSION}" \
  .

if [[ $PUSH == "true" ]];  then
  docker push "${REPO_BASE}/${TAG_NAME}:${TAG_VERSION}"
fi
