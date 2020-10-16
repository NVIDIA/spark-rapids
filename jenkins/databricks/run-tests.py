# Copyright (c) 2020, NVIDIA CORPORATION.
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
from datetime import datetime
import json
import requests
import sys
import getopt
import time
import os
import subprocess
from clusterutils import ClusterUtils


def main():
  workspace = 'https://dbc-9ff9942e-a9c4.cloud.databricks.com'
  token = ''
  clusterid = '0617-140138-umiak14'
  private_key_file = "~/.ssh/id_rsa"
  skip_start = None
  local_script = 'build.sh'
  script_dest = '/home/ubuntu/build.sh'
  source_tgz = 'spark-rapids-ci.tgz'
  tgz_dest = '/home/ubuntu/spark-rapids-ci.tgz'
  ci_rapids_jar = 'rapids-4-spark_2.12-0.1-SNAPSHOT-ci.jar'
  db_version = '0.1-databricks-SNAPSHOT'
  scala_version = '2.12'
  spark_version = '3.0.0'
  cudf_version = '0.15'
  cuda_version = 'cuda10-1'
  ci_cudf_jar = 'cudf-0.14-cuda10-1.jar'
  base_spark_pom_version = '3.0.0'
  sshKey = ''

  try:
      opts, args = getopt.getopt(sys.argv[1:], 'hs:t:c:p:l:nd:z:j:b:k:a:f:u:m:v:w:',
                                 ['workspace=', 'token=', 'clusterid=', 'private=', 'nostart=', 'localscript=', 'dest=', 'sparktgz=', 'cirapidsjar=', 'databricksversion=', 'sparkversion=', 'scalaversion=', 'cudfversion=', 'cudaversion=', 'cicudfjar=', 'basesparkpomversion=', 'sshkey='])
  except getopt.GetoptError:
      print(
          'run-tests.py -s <workspace> -t <token> -c <clusterid> -p <privatekeyfile> -n <skipstartingcluster> -l <localscript> -d <scriptdestinatino> -z <sparktgz> -j <cirapidsjar> -b <databricksversion> -k <sparkversion> -a <scalaversion> -f <cudfversion> -u <cudaversion> -m <cicudfjar> -v <basesparkpomversion>')
      sys.exit(2)

  for opt, arg in opts:
      if opt == '-h':
          print(
              'run-tests.py -s <workspace> -t <token> -c <clusterid> -p <privatekeyfile> -n <skipstartingcluster> -l <localscript> -d <scriptdestinatino>, -z <sparktgz> -j <cirapidsjar> -b <databricksversion> -k <sparkversion> -a <scalaversion> -f <cudfversion> -u <cudaversion> -m <cicudfjar> -v <basesparkpomversion>')
          sys.exit()
      elif opt in ('-s', '--workspace'):
          workspace = arg
      elif opt in ('-t', '--token'):
          token = arg
      elif opt in ('-c', '--clusterid'):
          clusterid = arg
      elif opt in ('-p', '--private'):
          private_key_file = arg
      elif opt in ('-n', '--nostart'):
          skip_start = arg
      elif opt in ('-l', '--localscript'):
          local_script = arg
      elif opt in ('-d', '--dest'):
          script_dest = arg
      elif opt in ('-z', '--sparktgz'):
          source_tgz = arg
      elif opt in ('-j', '--cirapidsjar'):
          ci_rapids_jar = arg
      elif opt in ('-b', '--databricksversion'):
          db_version = arg
      elif opt in ('-k', '--sparkversion'):
          spark_version = arg
      elif opt in ('-a', '--scalaversion'):
          scala_version = arg
      elif opt in ('-f', '--cudfversion'):
          cudf_version = arg
      elif opt in ('-u', '--cudaversion'):
          cuda_version = arg
      elif opt in ('-m', '--cicudfjar'):
          ci_cudf_jar = arg
      elif opt in ('-v', '--basesparkpomversion'):
          base_spark_pom_version = arg
      elif opt in ('-w', '--ssheky'):
          sshKey = arg

  print('-s is ' + workspace)
  print('-c is ' + clusterid)
  print('-p is ' + private_key_file)
  if skip_start is not None:
      print("-n: skip start")
  else:
      print("-n: don't skip start")
  print('-l is ' + local_script)
  print('-d is ' + script_dest)
  print('-z is ' + source_tgz)
  print('-j is ' + ci_rapids_jar)
  print('-b is ' + db_version)
  print('-k is ' + spark_version)
  print('-a is ' + scala_version)
  print('-f is ' + cudf_version)
  print('-u is ' + cuda_version)
  print('-m is ' + ci_cudf_jar)
  print('-v is ' + base_spark_pom_version)

  master_addr = None
  if skip_start is None:
      # generate cluster template
      initScriptFile = ''
      templ = ClusterUtils.generate_create_templ(sshKey, initScriptFile)
      newclusterId = ClusterUtils.create_cluster(workspace, templ, token)
      clusterid = newclusterId
      print("Using cluster id is %s" % clusterid)
      jsonout = ClusterUtils.cluster_state(workspace, clusterid, token)
      current_state = jsonout['state']
      if current_state in ['RUNNING']:
          print("Cluster is already running - perhaps build/tests already running?")
          sys.exit(3)


      ClusterUtils.start_existing_cluster(workspace, clusterid, token)
      master_addr = ClusterUtils.wait_for_cluster_start(workspace, clusterid, token)
  else:
      jsonout = ClusterUtils.cluster_state(workspace, clusterid, token)
      master_addr = ClusterUtils.get_master_addr(jsonout)

  if master_addr is None:
      print("Error, didn't get master address")
      sys.exit(5)
  print("Master node address is: %s" % master_addr)

  print("Copying script")
  rsync_command = "rsync -I -Pave \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s\" %s ubuntu@%s:%s" % (private_key_file, local_script, master_addr, script_dest)
  print("rsync command: %s" % rsync_command)
  subprocess.check_call(rsync_command, shell = True)

  print("Copying source")
  rsync_command = "rsync -I -Pave \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s\" %s ubuntu@%s:%s" % (private_key_file, source_tgz, master_addr, tgz_dest)
  print("rsync command: %s" % rsync_command)
  subprocess.check_call(rsync_command, shell = True)

  ssh_command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@%s -p 2200 -i %s %s %s %s %s %s %s %s %s %s %s 2>&1 | tee buildout; if [ `echo ${PIPESTATUS[0]}` -ne 0 ]; then false; else true; fi" % (master_addr, private_key_file, script_dest, tgz_dest, db_version, scala_version, ci_rapids_jar, spark_version, cudf_version, cuda_version, ci_cudf_jar, base_spark_pom_version)
  print("ssh command: %s" % ssh_command)
  subprocess.check_call(ssh_command, shell = True)

  print("Copying built tarball back")
  rsync_command = "rsync  -I -Pave \"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s\" ubuntu@%s:/home/ubuntu/spark-rapids-built.tgz ./" % (private_key_file, master_addr)
  print("rsync command to get built tarball: %s" % rsync_command)
  subprocess.check_call(rsync_command, shell = True)

if __name__ == '__main__':
  main()
