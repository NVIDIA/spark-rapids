# Copyright (c) 2021-2022, NVIDIA CORPORATION.
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
"""Parse input parameters."""

import getopt
import sys

workspace = 'https://dbc-9ff9942e-a9c4.cloud.databricks.com'
token = ''
private_key_file = "~/.ssh/id_rsa"
local_script = 'build.sh'
script_dest = '/home/ubuntu/build.sh'
source_tgz = 'spark-rapids-ci.tgz'
tgz_dest = '/home/ubuntu/spark-rapids-ci.tgz'
base_spark_pom_version = '3.1.1'
base_spark_version_to_install_databricks_jars = base_spark_pom_version
clusterid = ''
build_profiles = ''
jar_path = ''
# `spark_conf` can take comma seperated multiple spark configurations, e.g., spark.foo=1,spark.bar=2,...'
spark_conf = ''


def usage():
    """Define usage."""
    print('Usage: ' + sys.argv[0] +
          ' -s <workspace>'
          ' -t <token>'
          ' -c <clusterid>'
          ' -p <privatekeyfile>'
          ' -l <localscript>'
          ' -d <scriptdestination>'
          ' -z <sparktgz>'
          ' -v <basesparkpomversion>'
          ' -b <buildprofiles>'
          ' -j <jarpath>'
          ' -n <skipstartingcluster>'
          ' -f <sparkconf>'
          ' -i <sparkinstallver>')


try:
    opts, script_args = getopt.getopt(sys.argv[1:], 'hw:t:c:p:l:d:z:m:v:b:j:f:i:',
                                      ['workspace=',
                                       'token=',
                                       'clusterid=',
                                       'private=',
                                       'localscript=',
                                       'dest=',
                                       'sparktgz=',
                                       'basesparkpomversion=',
                                       'buildprofiles=',
                                       'jarpath',
                                       'sparkconf',
                                       'sparkinstallver='])
except getopt.GetoptError:
    usage()
    sys.exit(2)

for opt, arg in opts:
    if opt == '-h':
        usage()
        sys.exit(1)
    elif opt in ('-w', '--workspace'):
        workspace = arg
    elif opt in ('-t', '--token'):
        token = arg
    elif opt in ('-c', '--clusterid'):
        clusterid = arg
    elif opt in ('-p', '--private'):
        private_key_file = arg
    elif opt in ('-l', '--localscript'):
        local_script = arg
    elif opt in ('-d', '--dest'):
        script_dest = arg
    elif opt in ('-z', '--sparktgz'):
        source_tgz = arg
    elif opt in ('-v', '--basesparkpomversion'):
        base_spark_pom_version = arg
    elif opt in ('-b', '--bulidprofiles'):
        build_profiles = arg
    elif opt in ('-j', '--jarpath'):
        jar_path = arg
    elif opt in ('-f', '--sparkconf'):
        spark_conf = arg
    elif opt in ('-i', '--sparkinstallver'):
        base_spark_version_to_install_databricks_jars = arg

print('-w is ' + workspace)
print('-c is ' + clusterid)
print('-p is ' + private_key_file)
print('-l is ' + local_script)
print('-d is ' + script_dest)
print('script_args is ' + ' '.join(script_args))
print('-z is ' + source_tgz)
print('-v is ' + base_spark_pom_version)
print('-j is ' + jar_path)
print('-f is ' + spark_conf)
print('-i is ' + base_spark_version_to_install_databricks_jars)
