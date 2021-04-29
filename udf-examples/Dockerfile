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
ARG CUDA_VERSION=10.1
ARG CUDA_SHORT_VERSION=${CUDA_VERSION}
ARG LINUX_VERSION=ubuntu18.04
ARG CUDF_REPO=https://github.com/rapidsai/cudf
ARG CUDF_TAG=branch-0.19
ARG CC=7
ARG CXX=7

FROM nvidia/cuda:${CUDA_VERSION}-devel-${LINUX_VERSION}
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64:/usr/local/lib
# Needed for cudf.concat(), avoids "OSError: library nvvm not found"
ENV NUMBAPRO_NVVM=/usr/local/cuda/nvvm/lib64/libnvvm.so
ENV NUMBAPRO_LIBDEVICE=/usr/local/cuda/nvvm/libdevice/
ENV DEBIAN_FRONTEND=noninteractive

ARG CC
ARG CXX
RUN apt update -y --fix-missing && \
    apt upgrade -y && \
    apt install -y --no-install-recommends \
      git \
      gcc-${CC} \
      g++-${CXX} \
      libboost-all-dev \
      openjdk-8-jdk \
      maven \
      tzdata && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install conda
ADD https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh /miniconda.sh
RUN sh /miniconda.sh -b -p /conda && /conda/bin/conda update -n base conda
ENV PATH=${PATH}:/conda/bin
# Enables "source activate conda"
SHELL ["/bin/bash", "-c"]

# Pull the cudf source at the specified branch or tag
ARG CUDF_TAG
RUN git clone --recurse-submodules --depth 1 --single-branch --branch ${CUDF_TAG} https://github.com/rapidsai/cudf.git /cudf

# Create the cudf conda environment
ARG CUDA_SHORT_VERSION
RUN conda env create --name cudf --file /cudf/conda/environments/cudf_dev_cuda${CUDA_SHORT_VERSION}.yml

# libcudf build/install
ARG CC
ARG CXX
ENV CC=/usr/bin/gcc-${CC}
ENV CXX=/usr/bin/g++-${CXX}
RUN source activate cudf && \
    mkdir -p /cudf/cpp/build && \
    cd /cudf/cpp/build && \
    cmake .. -DCMAKE_INSTALL_PREFIX=${CONDA_PREFIX} && \
    make -j"$(nproc)" install

# Default to cudf conda environment
RUN echo "source activate cudf" > /.bash-init.sh
ENTRYPOINT /bin/bash --init-file /.bash-init.sh
