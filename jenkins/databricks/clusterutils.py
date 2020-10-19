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
import time
import os
import requests
import sys

class ClusterUtils(object):

    @staticmethod
    def generate_create_templ(sshKey, initScriptFile, version):
        # TODO (in followup) - create directory if not there
        init_script_loc = 'dbfs:/databricks/ci_init_scripts/' + initScriptFile
        dt = datetime.now()
        databricks_runtime_version = '7.0.x-gpu-ml-scala2.12'
        idle_timeout = 240
        date = dt.microsecond
        uniq_name = version + "-" + str(date)
        cluster_name = "CI-GPU-databricks-" + uniq_name
        templ = {}
        templ['cluster_name'] = cluster_name
        print("cluster name is going to be %s" % cluster_name)
        templ['spark_version'] = databricks_runtime_version
        templ['aws_attributes'] = {
                    "zone_id": "us-west-2a",
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0
                }
        templ['autotermination_minutes'] = idle_timeout
        templ['enable_elastic_disk'] = 'false'
        templ['enable_local_disk_encryption'] = 'false'
        templ['node_type_id'] = 'g4dn.xlarge'
        templ['driver_node_type_id'] = 'g4dn.xlarge'
        templ['ssh_public_keys'] = [ sshKey ]
        templ['num_workers'] = 1
        if initScriptFile:
            # only need these when enabling the plugin
            templ['spark_conf'] = { 'spark.plugins': 'com.nvidia.spark.SQLPlugin', }
            templ['init_scripts'] = []
            templ['init_scripts'].append({ "dbfs": { "destination": init_script_loc } })

        return templ


    @staticmethod
    def create_cluster(workspace, jsonCreateTempl, token):
        resp = requests.post(workspace + "/api/2.0/clusters/create", headers={'Authorization': 'Bearer %s' % token}, json=jsonCreateTempl)
        print("create response is %s" % resp.text)
        clusterid = resp.json()['cluster_id']
        print("cluster id is %s" % clusterid)
        return clusterid


    @staticmethod
    def wait_for_cluster_start(workspace, clusterid, token):
        p = 0
        waiting = True
        master_addr = None
        while waiting:
            time.sleep(30)
            jsonout = ClusterUtils.cluster_state(workspace, clusterid, token)
            current_state = jsonout['state']
            print(clusterid + " state:" + current_state)
            if current_state in ['RUNNING']:
                master_addr = ClusterUtils.get_master_addr(jsonout)
                break
            if current_state in ['INTERNAL_ERROR', 'SKIPPED', 'TERMINATED'] or p >= 20:
                if p >= 20:
                   print("Waited %d times already, stopping" % p)
                sys.exit(4)
            p = p + 1
        print("Done starting cluster")
        return master_addr


    @staticmethod
    def terminate_cluster(workspace, clusterid, token):
        jsonout = ClusterUtils.cluster_state(workspace, clusterid, token)
        current_state = jsonout['state']
        if current_state not in ['RUNNING', 'RESIZING']:
            print("Cluster is not running")
            sys.exit(1)

        print("Stopping cluster: " + clusterid)
        resp = requests.post(workspace + "/api/2.0/clusters/delete", headers={'Authorization': 'Bearer %s' % token}, json={'cluster_id': clusterid})
        print("stop response is %s" % resp.text)
        print("Done stopping cluster")


    @staticmethod
    def delete_cluster(workspace, clusterid, token):
        print("Deleting cluster: " + clusterid)
        resp = requests.post(workspace + "/api/2.0/clusters/permanent-delete", headers={'Authorization': 'Bearer %s' % token}, json={'cluster_id': clusterid})
        print("delete response is %s" % resp.text)
        print("Done deleting cluster")


    @staticmethod
    def start_existing_cluster(workspace, clusterid, token):
        print("Starting cluster: " + clusterid)
        resp = requests.post(workspace + "/api/2.0/clusters/start", headers={'Authorization': 'Bearer %s' % token}, json={'cluster_id': clusterid})
        print("start response is %s" % resp.text)


    @staticmethod
    def cluster_state(workspace, clusterid, token):
        clusterresp = requests.get(workspace + "/api/2.0/clusters/get?cluster_id=%s" % clusterid, headers={'Authorization': 'Bearer %s' % token})
        clusterjson = clusterresp.text
        print("cluster response is %s" % clusterjson)
        jsonout = json.loads(clusterjson)
        return jsonout


    @staticmethod
    def get_master_addr(jsonout):
        current_state = jsonout['state']
        master_addr = None
        if current_state in ['RUNNING']:
            driver = jsonout['driver']
            master_addr = driver["public_dns"]
            return master_addr
        else:
            return None


    @staticmethod
    def cluster_list(workspace, token):
        clusterresp = requests.get(workspace + "/api/2.0/clusters/list", headers={'Authorization': 'Bearer %s' % token})
        clusterjson = clusterresp.text
        print("cluster list is %s" % clusterjson)
        jsonout = json.loads(clusterjson)
        return jsonout

