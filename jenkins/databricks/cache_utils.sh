#!/bin/bash
#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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
#

# Shared utility functions for caching artifacts

# Use databricks CLI installed by official script at $HOME/bin/databricks
DATABRICKS_CLI=${DATABRICKS_CLI:-$HOME/bin/databricks}

# Print CLI version for debugging
if [[ -x "$DATABRICKS_CLI" ]]; then
    echo "Using Databricks CLI: $DATABRICKS_CLI"
    $DATABRICKS_CLI --version 2>/dev/null || echo "Unable to determine CLI version"
else
    echo "Warning: Databricks CLI not found at $DATABRICKS_CLI"
fi


# Download and cache artifact to Workspace cache directory
# Arguments:
#   $1: jar_file_name - Name of the file to download/cache
#   $2: download_url - URL to download from if not in cache
#   $3: extract_dir - Directory to extract to (optional, defaults to $HOME)
# Returns:
#   0 on success, 1 on failure
download_and_cache_artifact() {
    local jar_file_name=$1
    local download_url=$2
    local extract_dir=${3:-$HOME}
    
    # Workspace path for caching (accessed via Databricks CLI API, not local FUSE)
    local ws_cache_dir=${WS_CACHE_DIR:-"/databricks/cached_jars"}
    local local_cache_dir="/tmp/workspace_cache"
    local local_file="$local_cache_dir/$jar_file_name"
    
    mkdir -p "$local_cache_dir"
    
    # Check if file exists in local cache first (from setup-cache stage)
    if [[ -f "$local_file" ]]; then
        echo "Found $jar_file_name in local cache"
    else
        echo "$jar_file_name not found in local cache"
        
        # Try to sync from Workspace using Databricks CLI (if CLI is available and configured)
        echo "Syncing from Workspace cache ($ws_cache_dir -> $local_cache_dir)..."
        $DATABRICKS_CLI workspace export-dir "$ws_cache_dir" "$local_cache_dir" 2>/dev/null || \
            echo "Workspace cache not found or empty"
        
        # Check again after sync attempt
        if [[ -f "$local_file" ]]; then
            echo "Found $jar_file_name in Workspace cache"
        else
            # Download from URL as fallback
            echo "Downloading $jar_file_name from $download_url..."
            if wget "$download_url" -O "$local_file"; then
                echo "Download successful"
                # Try to cache to Workspace if CLI is available
                echo "Caching to Workspace..."
                $DATABRICKS_CLI workspace mkdirs "$ws_cache_dir" 2>/dev/null || true
                $DATABRICKS_CLI workspace import-dir "$local_cache_dir" "$ws_cache_dir" 2>/dev/null || \
                    echo "Warning: Could not cache to Workspace (continuing anyway)"
            else
                echo "Download failed"
                return 1
            fi
        fi
    fi
    
    # Extract the file
    tar xf "$local_file" -C "$extract_dir"
    return 0
}

