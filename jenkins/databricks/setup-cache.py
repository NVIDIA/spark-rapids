# Copyright (c) 2025, NVIDIA CORPORATION.
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
"""Setup cache by syncing from Databricks Workspace to local cluster."""

import os
import subprocess
import sys

from clusterutils import ClusterUtils

import params


def main():
    """Setup cache on Databricks cluster."""
    master_addr = ClusterUtils.cluster_get_master_addr(params.workspace, params.clusterid, params.token)
    if master_addr is None:
        print("Error, didn't get master address")
        sys.exit(1)
    print("Master node address is: %s" % master_addr)

    ssh_args = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i %s" % params.private_key_file

    # Install latest databricks CLI using official install script
    print("Installing latest Databricks CLI")
    install_cli_command = "ssh %s ubuntu@%s 'curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh && $HOME/bin/databricks --version'" % (ssh_args, master_addr)
    print("install CLI command: %s" % install_cli_command)
    subprocess.call(install_cli_command, shell=True)  # Use call instead of check_call to allow failure

    # Get DATABRICKS credentials from environment (set by Jenkinsfile)
    databricks_host = os.getenv('DATABRICKS_HOST', params.workspace)
    databricks_token = os.getenv('DATABRICKS_TOKEN', params.token)

    # Sync cache from Workspace to local cluster
    print("Syncing cache from Workspace to cluster")
    ws_cache_dir = "/databricks/cached_jars"
    local_cache_dir = "/tmp/workspace_cache"
    
    sync_command = "ssh %s ubuntu@%s " % (ssh_args, master_addr) + \
        "'DATABRICKS_HOST=%s DATABRICKS_TOKEN=%s $HOME/bin/databricks workspace export-dir %s %s 2>&1 || echo \"No cache found in Workspace\"'" % \
        (databricks_host, databricks_token, ws_cache_dir, local_cache_dir)
    print("sync command: %s" % sync_command)
    subprocess.call(sync_command, shell=True)  # Use call to allow failure if no cache exists

    print("Cache setup completed")


if __name__ == '__main__':
    main()

