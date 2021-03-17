#!/bin/bash
#
# Copyright (c) 2020-2021, NVIDIA CORPORATION. All rights reserved.
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

LOCAL_JAR_PATH=$1

# tests
export PATH=/databricks/conda/envs/databricks-ml-gpu/bin:/databricks/conda/condabin:$PATH
sudo /databricks/conda/envs/databricks-ml-gpu/bin/pip install pytest sre_yield requests pandas \
	pyarrow findspark pytest-xdist

export SPARK_HOME=/databricks/spark
# change to not point at databricks confs so we don't conflict with their settings
export SPARK_CONF_DIR=$PWD
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark/:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip
sudo ln -s /databricks/jars/ $SPARK_HOME/jars || true
sudo chmod 777 /databricks/data/logs/
sudo chmod 777 /databricks/data/logs/*
echo { \"port\":\"15002\" } > ~/.databricks-connect

if [ -d "$LOCAL_JAR_PATH" ]; then
    ## Run tests with jars in the LOCAL_JAR_PATH dir downloading from the denpedency repo
    LOCAL_JAR_PATH=$LOCAL_JAR_PATH bash $LOCAL_JAR_PATH/integration_tests/run_pyspark_from_build.sh  --runtime_env="databricks" 
else
    ## Run tests with jars building from the spark-rapids source code
    bash /home/ubuntu/spark-rapids/integration_tests/run_pyspark_from_build.sh --runtime_env="databricks"
fi
