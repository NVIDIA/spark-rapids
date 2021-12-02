#!/usr/bin/env bash

# Copyright (c) 2021, NVIDIA CORPORATION.
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
# generate a jar for the purpose of integration tests compile
# install it to local
# the integration tests will use it
#
set -e

jar_name=$1
classifier=$2
target_path=$PWD
jar_path=$target_path/$jar_name
tmp_path="${target_path}/tmp-dist-for-compile"
mkdir -p "${tmp_path}"
unzip -q ${jar_path} -d "${tmp_path}"
cd $tmp_path
cp -r $classifier/* ./
cp -r spark3xx-common/* ./
rm -rf spark3*
rm libjucx.so
zip -rq dist-for-compile-1.jar *
mv dist-for-compile-1.jar /tmp/
cd ..
rm -rf $tmp_path
mvn install:install-file -Dfile=/tmp/dist-for-compile-1.jar -DgroupId=com.nvidia -DartifactId=dist-for-compile -Dversion=1 -Dpackaging=jar
