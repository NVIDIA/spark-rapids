#
# Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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
#!/bin/bash
set -euxo pipefail

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly OS_NAME
OS_DIST=$(lsb_release -cs)
readonly OS_DIST
CUDA_VERSION='11.0'
readonly CUDA_VERSION

readonly NVIDIA_BASE_DL_URL='https://developer.download.nvidia.com/compute'
# Parameters for NVIDIA-provided Ubuntu GPU driver
readonly NVIDIA_UBUNTU_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/ubuntu1804/x86_64"
readonly NVIDIA_UBUNTU_REPO_KEY_PACKAGE="${NVIDIA_UBUNTU_REPO_URL}/cuda-keyring_1.0-1_all.deb"
readonly NVIDIA_UBUNTU_REPO_CUDA_PIN="${NVIDIA_UBUNTU_REPO_URL}/cuda-ubuntu1804.pin"
# Dataproc configurations
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
readonly HIVE_CONF_DIR='/etc/hive/conf'
readonly SPARK_CONF_DIR='/etc/spark/conf'

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function set_hadoop_property() {
  local -r config_file=$1
  local -r property=$2
  local -r value=$3
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/${config_file}" \
    --name "${property}" --value "${value}" \
    --clobber
}

function install_nvidia_gpu_driver() {

  execute_with_retries "apt-get update"
  execute_with_retries "apt-get install -y -q pciutils"
  execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"

  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NVIDIA_UBUNTU_REPO_KEY_PACKAGE}" -o /tmp/cuda-keyring.deb
  dpkg -i "/tmp/cuda-keyring.deb"

  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NVIDIA_UBUNTU_REPO_CUDA_PIN}" -o /etc/apt/preferences.d/cuda-repository-pin-600

  add-apt-repository "deb ${NVIDIA_UBUNTU_REPO_URL} /"
  execute_with_retries "apt-get update"

  if [[ -n "${CUDA_VERSION}" ]]; then
    local -r cuda_package=cuda-toolkit-${CUDA_VERSION//./-}
  else
    local -r cuda_package=cuda-toolkit
  fi
  # Without --no-install-recommends this takes a very long time.
  execute_with_retries "apt-get install -y -q --no-install-recommends cuda-drivers-460"
  execute_with_retries "apt-get install -y -q --no-install-recommends ${cuda_package}"
  ldconfig
  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
}

function configure_yarn() {
  if [[ ! -f ${HADOOP_CONF_DIR}/resource-types.xml ]]; then
    printf '<?xml version="1.0" ?>\n<configuration/>' >"${HADOOP_CONF_DIR}/resource-types.xml"
  fi
  set_hadoop_property 'resource-types.xml' 'yarn.resource-types' 'yarn.io/gpu'
  set_hadoop_property 'capacity-scheduler.xml' \
    'yarn.scheduler.capacity.resource-calculator' \
    'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator'
  set_hadoop_property 'yarn-site.xml' 'yarn.resource-types' 'yarn.io/gpu'
}

function configure_yarn_nodemanager() {
  set_hadoop_property 'yarn-site.xml' 'yarn.nodemanager.resource-plugins' 'yarn.io/gpu'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices' 'auto'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables' '/usr/bin'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.mount' 'true'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.mount-path' '/sys/fs/cgroup'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.hierarchy' 'yarn'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.container-executor.class' \
    'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor'
  set_hadoop_property 'yarn-site.xml' 'yarn.nodemanager.linux-container-executor.group' 'yarn'

}

function configure_gpu_isolation() {
  # Download GPU discovery script
  local -r spark_gpu_script_dir='/usr/lib/spark/scripts/gpu'
  mkdir -p ${spark_gpu_script_dir}
  local -r gpu_resources_url=https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${gpu_resources_url}" -o ${spark_gpu_script_dir}/getGpusResources.sh
  chmod a+rwx -R ${spark_gpu_script_dir}

  # enable GPU isolation
  sed -i "s/yarn.nodemanager\.linux\-container\-executor\.group\=/yarn\.nodemanager\.linux\-container\-executor\.group\=yarn/g" "${HADOOP_CONF_DIR}/container-executor.cfg"
  printf '\n[gpu]\nmodule.enabled=true\n[cgroups]\nroot=/sys/fs/cgroup\nyarn-hierarchy=yarn\n' >>"${HADOOP_CONF_DIR}/container-executor.cfg"

  # Configure a systemd unit to ensure that permissions are set on restart
  cat >/etc/systemd/system/dataproc-cgroup-device-permissions.service<<EOF
[Unit]
Description=Set permissions to allow YARN to access device directories
[Service]
ExecStart=/bin/bash -c "chmod a+rwx -R /sys/fs/cgroup/cpu,cpuacct; chmod a+rwx -R /sys/fs/cgroup/devices"
[Install]
WantedBy=multi-user.target
EOF

  systemctl enable dataproc-cgroup-device-permissions
  systemctl start dataproc-cgroup-device-permissions
}

readonly DEFAULT_SPARK_RAPIDS_VERSION="22.06.0"
readonly DEFAULT_CUDA_VERSION="11.0"
readonly DEFAULT_XGBOOST_VERSION="1.6.1"
readonly SPARK_VERSION="3.0"

# SPARK config
readonly SPARK_RAPIDS_VERSION=${DEFAULT_SPARK_RAPIDS_VERSION}
readonly XGBOOST_VERSION=${DEFAULT_XGBOOST_VERSION}
readonly XGBOOST_GPU_SUB_VERSION=${DEFAULT_XGBOOST_GPU_SUB_VERSION}

function install_spark_rapids() {
  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r dmlc_repo_url='https://repo.maven.apache.org/maven2/ml/dmlc'

  # Convert . to - for URL formatting
  local cudf_cuda_version="${CUDA_VERSION//\./-}"

  # There's only one release for all CUDA 11 versions
  # The version formatting does not have a '.'
  if [[ ${cudf_cuda_version} == 11* ]]; then
    cudf_cuda_version="11"
  fi
    
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${dmlc_repo_url}/xgboost4j-spark-gpu_2.12/${XGBOOST_VERSION}/xgboost4j-spark-gpu_2.12-${XGBOOST_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${dmlc_repo_url}/xgboost4j-gpu_2.12/${XGBOOST_VERSION}/xgboost4j-gpu_2.12-${XGBOOST_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${nvidia_repo_url}/rapids-4-spark_2.12/${SPARK_RAPIDS_VERSION}/rapids-4-spark_2.12-${SPARK_RAPIDS_VERSION}.jar" \
    -P /usr/lib/spark/jars/
}

# For PySpark based XGBoost, please refer to the version 22.04 branch that uses NVIDIA’s Spark XGBoost version. 
# https://repo1.maven.org/maven2/com/nvidia/xgboost4j_3.0
# https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/

function configure_spark() {
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF
###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
# Rapids Accelerator for Spark can utilize AQE, but when plan is not finalized, 
# query explain output won't show GPU operator, if user have doubt
# they can uncomment the line before to see the GPU plan explan, but AQE on give user the best performance.
# spark.sql.adaptive.enabled=false
spark.executor.resource.gpu.amount=1
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
}

function main() {

  install_nvidia_gpu_driver

  # This configuration should run on all nodes regardless of attached GPUs
  configure_yarn
  configure_yarn_nodemanager
  configure_gpu_isolation

  install_spark_rapids
  configure_spark
  echo "RAPIDS initialized with Spark runtime"
}

main
