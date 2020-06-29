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
import json
import requests
import sys
import getopt
import time
import os

def cluster_state(workspace, clusterid, token):
  clusterresp = requests.get(workspace + "/api/2.0/clusters/get?cluster_id=%s" % clusterid, headers={'Authorization': 'Bearer %s' % token})
  clusterjson = clusterresp.text
  print("cluster response is %s" % clusterjson)
  jsonout = json.loads(clusterjson)
  return jsonout

def main():
  workspace = 'https://dbc-9ff9942e-a9c4.cloud.databricks.com'
  token = ''
  clusterid = '0617-140138-umiak14'

  try:
      opts, args = getopt.getopt(sys.argv[1:], 'hs:t:c:',
                                 ['workspace=', 'token=', 'clusterid='])
  except getopt.GetoptError:
      print(
          'shutdown.py -s <workspace> -t <token> -c <clusterid>')
      sys.exit(2)

  for opt, arg in opts:
      if opt == '-h':
          print(
              'shutdown.py -s <workspace> -t <token> -c <clusterid>')
          sys.exit()
      elif opt in ('-s', '--workspace'):
          workspace = arg
      elif opt in ('-t', '--token'):
          token = arg
      elif opt in ('-c', '--clusterid'):
          clusterid = arg

  print('-s is ' + workspace)
  print('-c is ' + clusterid)

  jsonout = cluster_state(workspace, clusterid, token)
  current_state = jsonout['state']
  if current_state not in ['RUNNING']:
      print("Cluster is not running")
      sys.exit(1)

  print("Stopping cluster: " + clusterid)
  resp = requests.post(workspace + "/api/2.0/clusters/delete", headers={'Authorization': 'Bearer %s' % token}, json={'cluster_id': clusterid})
  print("stop response is %s" % resp.text)
  print("Done stopping cluster")

if __name__ == '__main__':
  main()
