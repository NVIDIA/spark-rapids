# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

###
#
# Pre-merge CI image for the UDF skills integration tests (skills/tests), also reused
# as the skill-eval container environment (skills/*/evals/environment/Dockerfile).
# Based on `skills/udf-convert-to-cuda/templates/cuda/Dockerfile` + python3.11/pip.
#
# The gcc-toolset/Java/CMake/Maven toolchain is placed on PATH via ENV (not a login-shell
# ENTRYPOINT + /etc/profile.d scripts, as the CUDA template uses), so commands work in any
# shell — including the non-login `sh -c` that the eval harness (Harbor) runs in the
# container. See the ENV block below for the full rationale.
#
###

FROM nvidia/cuda:12.9.1-devel-rockylinux8

ARG TOOLSET_VERSION=14
ARG CMAKE_VERSION=4.0.3
ARG CMAKE_ARCH=x86_64
ARG CCACHE_VERSION=4.11.2
ARG MAVEN_VERSION=3.9.9
ARG PARALLEL_LEVEL=10
ARG PYTHON_VERSION=3.11

ENV TOOLSET_VERSION=${TOOLSET_VERSION}
ENV PARALLEL_LEVEL=${PARALLEL_LEVEL}
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

RUN dnf --enablerepo=powertools install -y \
  gcc-toolset-${TOOLSET_VERSION} \
  git \
  java-17-openjdk-devel \
  ninja-build \
  patch \
  python${PYTHON_VERSION} \
  python${PYTHON_VERSION}-pip \
  scl-utils \
  tar \
  unzip \
  wget \
  zlib-devel && \
  alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 100 && \
  alternatives --set python3 /usr/bin/python${PYTHON_VERSION} && \
  ln -sf /usr/bin/python${PYTHON_VERSION} /usr/bin/python && \
  ln -sf /usr/bin/pip${PYTHON_VERSION} /usr/bin/pip && \
  dnf clean all

RUN cd /usr/local && \
  wget --quiet https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz && \
  tar zxf cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz && \
  rm cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz

# The rockylinux8 `maven` package is 3.5.4, too old for the template build plugins
# (build-helper-maven-plugin, scoverage-maven-plugin require Maven >= 3.6.3), so install
# a modern Apache Maven binary on PATH instead.
RUN cd /usr/local && \
  wget --quiet https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
  tar zxf apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
  rm apache-maven-${MAVEN_VERSION}-bin.tar.gz

# KEY DIFFERENCE vs the CUDA template Dockerfile: the CUDA image activates the gcc-toolset
# SCL the "normal" way -- `ENTRYPOINT ["bash", "-l"]` plus /etc/profile.d scripts
# (`source .../gcc-toolset-N/enable`) sourced on every container start. That only works for
# a LOGIN shell. The skill-eval harness (Harbor) runs commands via a non-login `sh -c ...`
# (its compose command is `sleep infinity`, not bash -l), so a login-shell ENTRYPOINT never
# fires and the toolchain would be absent.
#
# So this image bakes the exact environment that `source gcc-toolset-N/enable` would set --
# PATH, LD_LIBRARY_PATH, PKG_CONFIG_PATH -- directly into ENV, which every shell (login or
# not) inherits. Hence, unlike the CUDA template, this image has NO `ENTRYPOINT ["bash", "-l"]`
# and NO /etc/profile.d activation scripts -- ENV is the single source of truth for the toolchain.
ENV PATH=/opt/rh/gcc-toolset-${TOOLSET_VERSION}/root/usr/bin:${JAVA_HOME}/bin:/usr/local/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}/bin:/usr/local/apache-maven-${MAVEN_VERSION}/bin:${PATH}
ENV LD_LIBRARY_PATH=/opt/rh/gcc-toolset-${TOOLSET_VERSION}/root/usr/lib64:/opt/rh/gcc-toolset-${TOOLSET_VERSION}/root/usr/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
ENV PKG_CONFIG_PATH=/opt/rh/gcc-toolset-${TOOLSET_VERSION}/root/usr/lib64/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}

RUN cd /tmp && \
  wget --quiet https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}.tar.gz && \
  tar zxf ccache-${CCACHE_VERSION}.tar.gz && \
  rm ccache-${CCACHE_VERSION}.tar.gz && \
  cd ccache-${CCACHE_VERSION} && \
  mkdir build && \
  cd build && \
  scl enable gcc-toolset-${TOOLSET_VERSION} \
    "cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DZSTD_FROM_INTERNET=ON \
      -DREDIS_STORAGE_BACKEND=OFF && \
    cmake --build . --parallel ${PARALLEL_LEVEL} --target install" && \
  cd ../.. && \
  rm -rf ccache-${CCACHE_VERSION}
