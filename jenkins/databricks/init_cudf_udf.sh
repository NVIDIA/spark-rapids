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

CUDF_VER=${CUDF_VER:-22.04}

# Need to explictly add conda into PATH environment, to activate conda environment.
export PATH=/databricks/conda/bin:$PATH

base=$(conda info --base)
# Create and activate 'cudf-udf' conda env for cudf-udf tests
conda create -y -n cudf-udf && source activate && conda activate cudf-udf
# Use mamba to install cudf-udf packages to speed up conda resolve time
conda install -c conda-forge mamba python=3.8
${base}/envs/cudf-udf/bin/mamba remove -y c-ares zstd libprotobuf pandas
${base}/envs/cudf-udf/bin/mamba install -y -c rapidsai -c rapidsai-nightly -c nvidia -c conda-forge -c defaults cudf=$CUDF_VER cudatoolkit=11.0
conda deactivate
