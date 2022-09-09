
#!/bin/bash
# Copyright (c) 2022, NVIDIA CORPORATION.
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

# Try to run Alluxio directly in init_script
# Driver side:
#   1. Update alluxio-site.properties
#   2. Format master (No need to copyDir conf/)
#   3. Start alluxio master only.
# Worker side:
#   1. Update alluxio-site.properties as same as Driver side
#   2. Start alluxio worker only. Make sure succeed to connect to master. Wait master? 

####################
# Global constants #
####################
readonly ALLUXIO_VERSION="2.8.0"
readonly ALLUXIO_HOME="/opt/alluxio-${ALLUXIO_VERSION}"
readonly ALLUXIO_SITE_PROPERTIES="${ALLUXIO_HOME}/conf/alluxio-site.properties"
readonly ALLUXIO_METRICS_PROPERTIES_TEMPLATE="${ALLUXIO_HOME}/conf/metrics.properties.template"
readonly ALLUXIO_METRICS_PROPERTIES="${ALLUXIO_HOME}/conf/metrics.properties"
readonly ALLUXIO_STORAGE_PERCENT="70"

# Run a command as a specific user
# Assumes the provided user already exists on the system and user running script has sudo access
#
# Args:
#   $1: user
#   $2: cmd
doas() {
  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function doas, expecting 2"
    exit 2
  fi
  local user="$1"
  local cmd="$2"

  runuser -l "${user}" -c "${cmd}"
}

# Appends or replaces a property KV pair to the alluxio-site.properties file
#
# Args:
#   $1: property
#   $2: value
set_alluxio_property() {
  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function set_alluxio_property, expecting 2"
    exit 2
  fi
  local property="$1"
  local value="$2"

  if grep -qe "^\s*${property}=" ${ALLUXIO_SITE_PROPERTIES} 2> /dev/null; then
    doas ubuntu "sed -i 's;${property}=.*;${property}=${value};g' ${ALLUXIO_SITE_PROPERTIES}"
    echo "Property ${property} already exists in ${ALLUXIO_SITE_PROPERTIES} and is replaced with value ${value}" >&2
  else
    doas ubuntu "echo '${property}=${value}' >> ${ALLUXIO_SITE_PROPERTIES}"
  fi
}

# Appends or replaces a property KV pair to the metrics.properties file
set_metrics_property() {
  if [[ "$#" -ne "2" ]]; then
    echo "Incorrect number of arguments passed into function set_metrics_property, expecting 2"
    exit 2
  fi
  local property="$1"
  local value="$2"

  if [ ! -f "$ALLUXIO_METRICS_PROPERTIES" ]; then
    doas ubuntu "cp ${ALLUXIO_METRICS_PROPERTIES_TEMPLATE} ${ALLUXIO_METRICS_PROPERTIES}"
  fi
  if grep -qe "^\s*${property}=" ${ALLUXIO_METRICS_PROPERTIES} 2> /dev/null; then
    doas ubuntu "sed -i 's;${property}=.*;${property}=${value};g' ${ALLUXIO_METRICS_PROPERTIES}"
    echo "Property ${property} already exists in ${ALLUXIO_METRICS_PROPERTIES} and is replaced with value ${value}" >&2
  else
    doas ubuntu "echo '${property}=${value}' >> ${ALLUXIO_METRICS_PROPERTIES}"
  fi
}


# Configures Alluxio to use NVMe mounts as storage
# Returns "true" if Alluxio should configure MEM tier when no NVMe mounts are available
#
# Args:
#   $1: nvme_capacity_usage - Argument value of [-n <storage percentage>]
configure_nvme() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect number of arguments passed into function configure_nvme, expecting 1"
    exit 2
  fi
  nvme_capacity_usage=$1

  local use_mem="true"
  local paths=""
  local quotas=""
  local medium_type=""
  # In databricks instance, /local_disk0 is the local NVME disk. 
  # in the format of "<dev name> <capacity> <mount path>"
  # The block size parameter (-B) is in MB (1024 * 1024)
  local -r mount_points="$(df -B 1048576 | grep 'local_disk' | awk '{print $1, $4, $6}')"
  set +e
  # read returns 1 unless EOF is reached, but we specify -d '' which means always read until EOF
  IFS=$'\n' read -d '' -ra mounts <<< "${mount_points}"
  set -e
  # attempt to configure NVMe, otherwise fallback to MEM
  # Should only 1 NVMe disk, I need to 
  if [[ "${#mounts[@]}" -gt 1 ]]; then
    echo "More than 1 NVMe device, need to modify the script to cover it"
    exit 2
  fi
  if [[ "${#mounts[@]}" == 1 ]]; then
    for mount_point in "${mounts[@]}"; do
      local path_cap
      local mnt_path
      local quota_p
      path_cap="$(echo "${mount_point}" | awk '{print $2}')"
      # mnt_path="$(echo "${mount_point}" | awk '{print $3}')"
      quota_p=$((path_cap * nvme_capacity_usage / 100))
      # if alluxio doesn't have permissions to write to this directory it will fail
      # fixed to use the /cache folder in alluxio folder
      mnt_path="/local_disk0/cache"
      paths+="${mnt_path},"
      quotas+="${quota_p}MB,"
      medium_type+="SSD,"
    done
    paths="${paths::-1}"
    quotas="${quotas::-1}"
    medium_type="${medium_type::-1}"

    use_mem="false"
    set_alluxio_property alluxio.worker.tieredstore.level0.alias "SSD"
    set_alluxio_property alluxio.worker.tieredstore.level0.dirs.mediumtype "${medium_type}"
    set_alluxio_property alluxio.worker.tieredstore.level0.dirs.path "${paths}"
    set_alluxio_property alluxio.worker.tieredstore.level0.dirs.quota "${quotas}"
  fi
  echo "${use_mem}"
}

# add crontab to rsync alluxio log to /dbfs/cluster-logs/alluxio
set_crontab_alluxio_log() {
  if [[ "$#" -ne "1" ]]; then
    echo "Incorrect"
    exit 2
  fi
  local folder=$1
  mkdir -p /dbfs/cluster-logs/alluxio/$folder
  # add crond to copy alluxio logs
  crontab -l > cron_bkp || true
  echo "* * * * * /usr/bin/rsync -a /opt/alluxio-2.8.0/logs /dbfs/cluster-logs/alluxio/$folder >/dev/null 2>&1" >> cron_bkp
  crontab cron_bkp
  rm cron_bkp
}


# create the folder for NVMe caching
mkdir -p /local_disk0/cache
chown ubuntu:ubuntu /local_disk0/cache

# create the folder for domain socket
mkdir -p /local_disk0/alluxio_domain_socket
chown ubuntu:ubuntu /local_disk0/alluxio_domain_socket

set_alluxio_property alluxio.master.hostname "${DB_DRIVER_IP}"
set_alluxio_property alluxio.underfs.s3.inherit.acl false
set_alluxio_property alluxio.underfs.s3.default.mode 0755
set_alluxio_property alluxio.worker.tieredstore.levels "1"
set_alluxio_property alluxio.worker.data.server.domain.socket.address /local_disk0/alluxio_domain_socket
set_alluxio_property alluxio.worker.data.server.domain.socket.as.uuid true
set_alluxio_property alluxio.worker.network.async.cache.manager.queue.max 4000
set_alluxio_property alluxio.user.short.circuit.preferred true

set_alluxio_property alluxio.job.worker.threadpool.size 20
set_alluxio_property alluxio.worker.network.block.writer.threads.max 2048
set_alluxio_property alluxio.worker.network.block.reader.threads.max 2048
set_alluxio_property alluxio.master.ufs.block.location.cache.capacity 2000000
set_alluxio_property alluxio.master.ufs.path.cache.capacity 200000

sed -i "s/localhost/${DB_DRIVER_IP}/g" /opt/alluxio-${ALLUXIO_VERSION}/conf/masters

configure_nvme "${ALLUXIO_STORAGE_PERCENT}"

if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  # On Driver
  set_crontab_alluxio_log "${DB_DRIVER_IP}-master"
  doas ubuntu "ALLUXIO_MASTER_JAVA_OPTS='-Xms16g -Xmx16g' ${ALLUXIO_HOME}/bin/alluxio-start.sh master"
else
  # On Workers
  set_crontab_alluxio_log "${DB_CONTAINER_IP}-worker"
  echo "alluxio.worker.hostname=${DB_CONTAINER_IP}" >> ${ALLUXIO_SITE_PROPERTIES}
  echo "alluxio.user.hostname=${DB_CONTAINER_IP}" >> ${ALLUXIO_SITE_PROPERTIES}
  
  n=0
  until [ "$n" -ge 5 ]
  do     
     doas ubuntu "${ALLUXIO_HOME}/bin/alluxio-start.sh worker" && break
     n=$((n+1)) 
     sleep 10
  done
fi

