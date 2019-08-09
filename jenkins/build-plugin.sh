#!/bin/bash
##
#
# Script to build rapids-plugin jar files.
# Source tree is supposed to be ready by Jenkins before starting this script.
#
###
set -e

CLASSIFIERS=""
CUDA_VER=$1
#set cuda9 as default
if [ "$CUDA_VER" == "10.0" ]; then
    CUDA_VER="cuda10"
else
    CUDA_VER=""
fi

#pull cuda9 version of cudf from maven when the classifier is ""
BUILD_ARG="-Dcuda.version=$CUDA_VER"

echo "mvn -X clean package $BUILD_ARG"

mvn $BUILD_ARG -X clean package -DskipTests
