# Copyright (c) 2021-2023, NVIDIA CORPORATION.
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
import json
import requests
import sys
import getopt
import time
import os
import subprocess
from clusterutils import ClusterUtils
import params

def main():
  master_addr = ClusterUtils.cluster_get_master_addr(params.workspace, params.clusterid, params.token)
  if master_addr is None:
      print("Error, didn't get master address")
      sys.exit(1)
  print("Master node address is: %s" % master_addr)

  print("Copying script")
  ssh_args = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s" % params.private_key_file
  rsync_command = "rsync -I -Pave \"ssh %s\" %s ubuntu@%s:%s" % (ssh_args, params.local_script, master_addr, params.script_dest)
  print("rsync command: %s" % rsync_command)
  subprocess.check_call(rsync_command, shell = True)

  print("Copying source")
  rsync_command = "rsync -I -Pave \"ssh %s\" %s ubuntu@%s:%s" % (ssh_args, params.source_tgz, master_addr, params.tgz_dest)
  print("rsync command: %s" % rsync_command)
  subprocess.check_call(rsync_command, shell = True)

  ssh_command = "ssh %s ubuntu@%s " % (ssh_args, master_addr) + \
        "'SPARKSRCTGZ=%s BASE_SPARK_VERSION=%s BASE_SPARK_VERSION_TO_INSTALL_DATABRICKS_JARS=%s MVN_OPT=%s EXTRA_ENVS=%s \
        bash %s %s 2>&1 | tee buildout; if [ `echo ${PIPESTATUS[0]}` -ne 0 ]; then false; else true; fi'" % \
        (params.tgz_dest, params.base_spark_pom_version, params.base_spark_version_to_install_databricks_jars, params.mvn_opt, params.extra_envs, params.script_dest, ' '.join(params.script_args))
  print("ssh command: %s" % ssh_command)
  subprocess.check_call(ssh_command, shell = True)

  # Only the nightly build needs to copy the spark-rapids-built.tgz back
  if params.test_type == 'nightly':
      print("Copying built tarball back")
      rsync_command = "rsync -I -Pave \"ssh %s\" ubuntu@%s:/home/ubuntu/spark-rapids-built.tgz ./" % (ssh_args, master_addr)
      print("rsync command to get built tarball: %s" % rsync_command)
      subprocess.check_call(rsync_command, shell = True)

if __name__ == '__main__':
  main()
