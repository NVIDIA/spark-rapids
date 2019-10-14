#!/bin/bash
cd $HOME/spark*
export SPARK_HOME=$PWD
export SPARK_CONF_DIR=$PWD/conf
./sbin/stop-slave.sh
