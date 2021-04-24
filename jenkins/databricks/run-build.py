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
  rsync_command = "rsync -I -Pave \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s\" %s ubuntu@%s:%s" % (params.private_key_file, params.local_script, master_addr, params.script_dest)
  print("rsync command: %s" % rsync_command)
  subprocess.check_call(rsync_command, shell = True)

  print("Copying source")
  rsync_command = "rsync -I -Pave \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s\" %s ubuntu@%s:%s" % (params.private_key_file, params.source_tgz, master_addr, params.tgz_dest)
  print("rsync command: %s" % rsync_command)
  subprocess.check_call(rsync_command, shell = True)

  ssh_command = "bash -c 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@%s -p 2200 -i %s %s %s %s %s 2>&1 | tee buildout; if [ `echo ${PIPESTATUS[0]}` -ne 0 ]; then false; else true; fi'" % (master_addr, params.private_key_file, params.script_dest, params.tgz_dest, params.base_spark_pom_version, params.build_profiles)
  print("ssh command: %s" % ssh_command)
  subprocess.check_call(ssh_command, shell = True)

  print("Copying built tarball back")
  rsync_command = "rsync  -I -Pave \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s\" ubuntu@%s:/home/ubuntu/spark-rapids-built.tgz ./" % (params.private_key_file, master_addr)
  print("rsync command to get built tarball: %s" % rsync_command)
  subprocess.check_call(rsync_command, shell = True)

if __name__ == '__main__':
  main()
