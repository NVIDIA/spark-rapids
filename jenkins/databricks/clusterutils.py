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

class ClusterUtils(object):

    @staticmethod
    def generate_create_templ(sshKey):
        ssh_key = 'SSH public key\\nssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDB+ValakyoKn7w+iBRoAi1KlLVH4yVmRXhLCZs1qUECBAhbck2o8Lgjp5wJ+epdT3+EAP2+t/zlK1mU9tTylidapR4fZuIk9ApoQSLOUEXcUtHkPpZulIoGAq78HoyiEs1sKovc6ULvpymjdnQ3ogCZlTlP9uqmL2E4kbtrNCNL0SVj/w10AqzrJ5lqQgO5dIdDRMHW2sv88JI1VLlfiSsofa9RdI7hDRuCnfZ0+dv2URJGGzGt2BkdEmk9t5F1BMpuXvZ8HzOYdACzw0U+THBOk9d4CACUYMyO1XqlXwoYweNKRnigXDCRaTWGFBzTkugRuW/BZBccTR1ON430uRB svcngcc@nvidia.com'
        init_script_loc = 'dbfs:/databricks/ci_init_scripts'
        branch = branch-0.3
        runtime = 70
        dt = datetime.now()
        date = dt.microsecond
        print("date is %s" % date)
        uniq_name = runtime + "-" + branch # + "-" date
        cluster_name = "CI-GPU-databricks-" + uniq_name
        templ = {}
        templ['cluster_name'] = cluster_name
        print("cluster name is going to be %s" % cluster_name)
        templ['spark_version'] = "7.0.x-gpu-ml-scala2.12"
        templ['aws_attributes'] = {
                    "zone_id": "us-west-2a",
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0
                }
        templ['node_type_id'] = "g4dn.xlarge"
        templ['driver_node_type_id'] = "g4dn.xlarge"
        templ['ssh_public_keys'] = [ ssh_key ]
        templ['num_workers'] = 1
        # only need these when enabling the plugin
        templ['spark_conf'] = { 'spark.plugins': 'com.nvidia.spark.SQLPlugin', }
        templ['init_scripts'] = []
        templ['init_scripts'].append({ "dbfs": { "destination": init_script_loc } })

        return templ


    @staticmethod
    def create_cluster(jsonCreateTempl):
        resp = requests.post(workspace + "api/2.0/clusters/create", headers={'Authorization': 'Bearer %s' % token}, json=jsonCreateTempl)
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
