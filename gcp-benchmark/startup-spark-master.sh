#!/bin/bash
tar -xzvf spark.tgz
rm spark.tgz
cd spark*
export SPARK_HOME=$PWD
export SPARK_CONF_DIR=$PWD/conf
./sbin/start-master.sh
