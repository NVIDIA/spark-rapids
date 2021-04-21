#
# Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
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

# A container that can be used to build UDF native code against libcudf
ARG CUDA_VERSION=11.0
ARG LINUX_VERSION=ubuntu18.04

FROM nvidia/cuda:${CUDA_VERSION}-devel-${LINUX_VERSION}

ARG DEBIAN_FRONTEND=noninteractive
ARG PARALLEL_LEVEL=10
ENV PARALLEL_LEVEL=10

RUN GCC_VERSION=$(bash -c '\
CUDA_VERSION=$(nvcc --version | head -n4 | tail -n1 | cut -d" " -f5 | cut -d"," -f1); \
CUDA_VERSION_MAJOR=$(echo $CUDA_VERSION | tr -d '.' | cut -c 1-2); \
CUDA_VERSION_MINOR=$(echo $CUDA_VERSION | tr -d '.' | cut -c 3); \
  if [[ "$CUDA_VERSION_MAJOR" == 9 ]]; then echo "7"; \
  elif [[ "$CUDA_VERSION_MAJOR" == 10 ]]; then echo "8"; \
  elif [[ "$CUDA_VERSION_MAJOR" == 11 ]]; then echo "9"; \
  else echo "10"; \
  fi') \
&& apt update -y \
&& apt install -y software-properties-common \
&& add-apt-repository -y ppa:git-core/ppa \
&& add-apt-repository -y ppa:ubuntu-toolchain-r/test \
&& add-apt-repository ppa:deadsnakes/ppa \
&& apt update -y \
&& apt install -y \
   build-essential git wget \
   gcc-${GCC_VERSION} g++-${GCC_VERSION} \
   openjdk-8-jdk maven tzdata \
   # CMake dependencies
   curl libssl-dev libcurl4-openssl-dev zlib1g-dev \
   # cuDF dependencies
   libboost-filesystem-dev \
&& apt autoremove -y \
&& rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
&& update-alternatives \
   --install /usr/bin/gcc gcc /usr/bin/gcc-${GCC_VERSION} 100 \
# Set gcc-${GCC_VERSION} as the default gcc
&& update-alternatives --set gcc /usr/bin/gcc-${GCC_VERSION}  \
# Set JDK8 as the default Java
&& update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

ARG CMAKE_VERSION=3.18.5

# Install CMake
RUN cd /tmp \
 && curl -fsSLO --compressed "https://github.com/Kitware/CMake/releases/download/v$CMAKE_VERSION/cmake-$CMAKE_VERSION.tar.gz" -o /tmp/cmake-$CMAKE_VERSION.tar.gz \
 && tar -xvzf /tmp/cmake-$CMAKE_VERSION.tar.gz && cd /tmp/cmake-$CMAKE_VERSION \
 && /tmp/cmake-$CMAKE_VERSION/bootstrap \
    --system-curl \
    --parallel=${PARALLEL_LEVEL} \
 && make install -j${PARALLEL_LEVEL} \
 && cd /tmp && rm -rf /tmp/cmake-$CMAKE_VERSION*

