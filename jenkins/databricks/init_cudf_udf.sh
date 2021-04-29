#!/bin/bash
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

# The initscript to set up environment for the cudf_udf tests on Databrcks
# Will be automatically pushed into the dbfs:/databricks/init_scripts once it is updated.

CUDF_VER=${CUDF_VER:-0.19.2}

# Use mamba to install cudf-udf packages to speed up conda resolve time
base=$(conda info --base)
conda create -y -n mamba -c conda-forge mamba
pip uninstall -y pyarrow
${base}/envs/mamba/bin/mamba remove -y c-ares zstd libprotobuf pandas
${base}/envs/mamba/bin/mamba install -y pyarrow=1.0.1 -c conda-forge
${base}/envs/mamba/bin/mamba install -y -c rapidsai -c rapidsai-nightly -c nvidia -c conda-forge -c defaults cudf=$CUDF_VER cudatoolkit=10.1
conda env remove -n mamba
