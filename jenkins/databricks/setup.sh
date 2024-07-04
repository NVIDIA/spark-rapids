#!/bin/bash
#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
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

# Set PYSPARK_PYTHON to keep the version of driver/workers python consistent.
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-"$(which python)"}
# Install if python pip does not exist.
if [ -z "$($PYSPARK_PYTHON -m pip --version || true)" ]; then
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
        $PYSPARK_PYTHON get-pip.py && rm get-pip.py
fi

# Get Python version (major.minor). i.e., python3.8 for DB10.4 and python3.9 for DB11.3
PYTHON_VERSION=$(${PYSPARK_PYTHON} -c 'import sys; print("python{}.{}".format(sys.version_info.major, sys.version_info.minor))')
# Set the path of python site-packages, and install packages here.
PYTHON_SITE_PACKAGES="$HOME/.local/lib/${PYTHON_VERSION}/site-packages"
# Use "python -m pip install" to make sure pip matches with python.
$PYSPARK_PYTHON -m pip install --target $PYTHON_SITE_PACKAGES pytest sre_yield requests pandas pyarrow findspark pytest-xdist pytest-order

# Install fastparquet (and numpy as its dependency).
echo -e "fastparquet==0.8.3;python_version=='3.8'\nfastparquet==2024.5.0;python_version>='3.9'" > fastparquet.txt
$PYSPARK_PYTHON -m pip install --target $PYTHON_SITE_PACKAGES -r fastparquet.txt
