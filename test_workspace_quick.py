#!/usr/bin/env python3
"""Quick script to test Workspace permissions on a running Databricks cluster."""

import subprocess
import sys

# Use the cluster that's currently running from the Jenkins build
CLUSTER_IP = "20.169.80.73"  # From the Jenkins log
PRIVATE_KEY = "~/.ssh/id_rsa"  # Or use your key path

def main():
    print("Testing Workspace permissions on cluster...")
    
    # Copy test script to cluster
    ssh_args = f"-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2200 -i {PRIVATE_KEY}"
    
    print("\n1. Copying test script...")
    rsync_cmd = f'rsync -Pave "ssh {ssh_args}" ./jenkins/databricks/test_workspace_permissions.sh ubuntu@{CLUSTER_IP}:/tmp/'
    subprocess.run(rsync_cmd, shell=True, check=True)
    
    print("\n2. Running permission test on cluster...")
    ssh_cmd = f'ssh {ssh_args} ubuntu@{CLUSTER_IP} "bash /tmp/test_workspace_permissions.sh"'
    subprocess.run(ssh_cmd, shell=True)
    
    print("\n✓ Test complete!")

if __name__ == '__main__':
    main()

