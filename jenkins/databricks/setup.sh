#!/bin/bash
#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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
# Setup Spark local environment for integration testing with Databricks cluster

set -xe

sudo apt-get update
sudo apt-get install -y zip

# Configure spark environment on Databricks
export SPARK_HOME=$DB_HOME/spark

# Workaround to support local spark job
sudo ln -sf $DB_HOME/jars/ $SPARK_HOME/jars

# Set $SPARK_LOCAL_DIRS writable for ordinary user
if [ -f $SPARK_HOME/conf/spark-env.sh ]; then
    # Sample output: export SPARK_LOCAL_DIRS='/local_disk0'
    local_dir=`grep SPARK_LOCAL_DIRS $SPARK_HOME/conf/spark-env.sh`
    local_dir=${local_dir##*=}

    sudo chmod 777 `echo $local_dir | xargs`
fi

# Install Python modules for integration test
sudo pip install pytest sre_yield pandas pyarrow
