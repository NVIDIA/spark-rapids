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


# Define files to cache with their download URLs
CACHE_FILES = [
    {
        'name': 'apache-maven-3.6.3-bin.tar.gz',
        'url': 'https://archive.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz'
    },
    {
        'name': 'spark-3.2.0-bin-hadoop3.2.tgz',
        'url': 'https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz'
    }
]


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

    # Sync cache from Workspace to local cluster (per-file to handle size limits)
    print("Syncing cache from Workspace to cluster")
    ws_cache_dir = "/databricks/cached_jars"
    local_cache_dir = "/tmp/workspace_cache"
    
    # Create local cache directory
    mkdir_command = "ssh %s ubuntu@%s 'mkdir -p %s'" % (ssh_args, master_addr, local_cache_dir)
    subprocess.call(mkdir_command, shell=True)
    
    # Try to export each file individually (small files will succeed, large files will fail gracefully)
    print("\nAttempting to export files from Workspace:")
    for file in CACHE_FILES:
        file_name = file['name']
        ws_file_path = "%s/%s" % (ws_cache_dir, file_name)
        local_file_path = "%s/%s" % (local_cache_dir, file_name)
        
        print("  Trying to export %s..." % file_name)
        export_command = "ssh %s ubuntu@%s " % (ssh_args, master_addr) + \
            "'DATABRICKS_HOST=%s DATABRICKS_TOKEN=%s $HOME/bin/databricks workspace export %s --file %s --format AUTO 2>&1'" % \
            (databricks_host, databricks_token, ws_file_path, local_file_path)
        
        result = subprocess.call(export_command, shell=True)
        if result == 0:
            print("    -> Exported successfully")
        else:
            print("    -> Export failed (file may be too large or not exist)")

    # Check each file and download if missing
    print("\nVerifying cached files...")
    missing_files = []
    
    for file in CACHE_FILES:
        file_name = file['name']
        print("Checking for %s..." % file_name)
        
        check_command = "ssh %s ubuntu@%s 'test -f %s/%s && echo EXISTS || echo MISSING'" % \
            (ssh_args, master_addr, local_cache_dir, file_name)
        result = subprocess.check_output(check_command, shell=True).decode('utf-8').strip()
        
        if 'MISSING' in result:
            print("  -> Missing: %s" % file_name)
            missing_files.append(file)
        else:
            print("  -> Found: %s" % file_name)
    
    # Download and upload missing files
    if missing_files:
        print("\nDownloading and caching %d missing file(s)..." % len(missing_files))
        
        for file in missing_files:
            file_name = file['name']
            file_url = file['url']
            
            print("\nProcessing %s:" % file_name)
            print("  1. Downloading from %s" % file_url)
            
            download_command = "ssh %s ubuntu@%s 'cd %s && wget -q --show-progress %s -O %s'" % \
                (ssh_args, master_addr, local_cache_dir, file_url, file_name)
            
            download_result = subprocess.call(download_command, shell=True)
            
            if download_result == 0:
                print("  2. Uploading to Workspace cache...")
                
                # Import the single file to Workspace
                local_file_path = "%s/%s" % (local_cache_dir, file_name)
                ws_file_path = "%s/%s" % (ws_cache_dir, file_name)
                
                upload_command = "ssh %s ubuntu@%s " % (ssh_args, master_addr) + \
                    "'DATABRICKS_HOST=%s DATABRICKS_TOKEN=%s $HOME/bin/databricks workspace mkdirs %s 2>/dev/null || true && " \
                    "DATABRICKS_HOST=%s DATABRICKS_TOKEN=%s $HOME/bin/databricks workspace import --file %s %s --format AUTO --overwrite 2>&1 || echo \"Warning: Could not upload to Workspace (file may be too large)\"'" % \
                    (databricks_host, databricks_token, ws_cache_dir, databricks_host, databricks_token, local_file_path, ws_file_path)
                
                subprocess.call(upload_command, shell=True)
                print("  -> Completed: %s" % file_name)
            else:
                print("  -> Failed to download: %s" % file_name)
    else:
        print("\nAll files found in cache!")

    print("\nCache setup completed")


if __name__ == '__main__':
    main()

