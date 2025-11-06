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
    
    local ws_cache_dir=${WS_CACHE_DIR:-"/Workspace/databricks/cached_jars"}
    local cache_file="$ws_cache_dir/$jar_file_name"
    
    # Create cache directory if it doesn't exist
    mkdir -p "$ws_cache_dir"
    
    # Check if file exists in Workspace cache
    if [[ -f "$cache_file" ]]; then
        echo "Found $jar_file_name in Workspace cache, copying to /tmp..."
        cp "$cache_file" "/tmp/$jar_file_name"
    else
        echo "$jar_file_name not found in Workspace cache, downloading from $download_url..."
        if wget "$download_url" -P /tmp; then
            echo "Download successful, caching to Workspace..."
            cp "/tmp/$jar_file_name" "$cache_file" || true
        else
            echo "Download failed"
            return 1
        fi
    fi
    
    # Extract the file
    tar xf "/tmp/$jar_file_name" -C "$extract_dir"
    rm -f "/tmp/$jar_file_name"
    return 0
}

