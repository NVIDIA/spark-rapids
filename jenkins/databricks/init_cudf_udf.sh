#!/bin/bash
#
# Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
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

# The initscript to set up environment for the cudf_udf tests on Databricks
# Will be automatically pushed into the dbfs:/databricks/init_scripts once it is updated.

set -x

CUDF_VER=${CUDF_VER:-23.02}
CUDA_VER=${CUDA_VER:-11.0}

# Need to explicitly add conda into PATH environment, to activate conda environment.
export PATH=/databricks/conda/bin:$PATH
# Set Python for the running instance
PYTHON_VERSION=$(${PYSPARK_PYTHON} -c 'import sys; print("{}.{}".format(sys.version_info.major, sys.version_info.minor))')

base=$(conda info --base)
# Create and activate 'cudf-udf' conda env for cudf-udf tests
conda create -y -n cudf-udf -c conda-forge python=$PYTHON_VERSION mamba && \
  source activate && \
  conda activate cudf-udf

# Use mamba to install cudf-udf packages to speed up conda resolve time
conda install -y -c conda-forge mamba python=$PYTHON_VERSION
${base}/envs/cudf-udf/bin/mamba remove -y c-ares zstd libprotobuf pandas

REQUIRED_PACKAGES=(
  cudatoolkit=$CUDA_VER
  cudf=$CUDF_VER
  findspark
  pandas
  pyarrow
  pytest
  pytest-order
  pytest-xdist
  requests
  sre_yield
)

${base}/envs/cudf-udf/bin/mamba install -y \
  -c rapidsai -c rapidsai-nightly -c nvidia -c conda-forge -c defaults \
  "${REQUIRED_PACKAGES[@]}"

source deactivate && conda deactivate