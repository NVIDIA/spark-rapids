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
readonly NVIDIA_UBUNTU_REPOSITORY_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/ubuntu1804/x86_64"
readonly NVIDIA_UBUNTU_REPOSITORY_KEY="${NVIDIA_UBUNTU_REPOSITORY_URL}/7fa2af80.pub"
readonly NVIDIA_UBUNTU_REPOSITORY_CUDA_PIN="${NVIDIA_UBUNTU_REPOSITORY_URL}/cuda-ubuntu1804.pin"
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
    "${NVIDIA_UBUNTU_REPOSITORY_KEY}" | apt-key add -
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NVIDIA_UBUNTU_REPOSITORY_CUDA_PIN}" -o /etc/apt/preferences.d/cuda-repository-pin-600

  add-apt-repository "deb ${NVIDIA_UBUNTU_REPOSITORY_URL} /"
  execute_with_retries "apt-get update"

  if [[ -n "${CUDA_VERSION}" ]]; then
    local -r cuda_package=cuda-toolkit-${CUDA_VERSION//./-}
  else
    local -r cuda_package=cuda-toolkit
  fi
  # Without --no-install-recommends this takes a very long time.
  execute_with_retries "apt-get install -y -q --no-install-recommends cuda-drivers-460"
  execute_with_retries "apt-get install -y -q --no-install-recommends ${cuda_package}"

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

  chmod a+rwx -R /sys/fs/cgroup/cpu,cpuacct
  chmod a+rwx -R /sys/fs/cgroup/devices
}

readonly RAPIDS_VERSION="0.19"
readonly DEFAULT_SPARK_RAPIDS_VERSION="0.5.0"
readonly DEFAULT_CUDA_VERSION="11.0"
readonly DEFAULT_CUDF_VERSION="0.19.2"
readonly DEFAULT_XGBOOST_VERSION="1.3.0"
readonly DEFAULT_XGBOOST_GPU_SUB_VERSION="0.1.0"
readonly SPARK_VERSION="3.0"

readonly CUDF_VERSION=${DEFAULT_CUDF_VERSION}
# SPARK config
readonly SPARK_RAPIDS_VERSION=${DEFAULT_SPARK_RAPIDS_VERSION}
readonly XGBOOST_VERSION=${DEFAULT_XGBOOST_VERSION}
readonly XGBOOST_GPU_SUB_VERSION=${DEFAULT_XGBOOST_GPU_SUB_VERSION}

function install_spark_rapids() {
  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'
  local cudf_cuda_version="${CUDA_VERSION//\./-}"
  # Convert "11-0" to "11"
  cudf_cuda_version="${cudf_cuda_version%-0}"
    
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${nvidia_repo_url}/xgboost4j-spark_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${nvidia_repo_url}/xgboost4j_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${nvidia_repo_url}/rapids-4-spark_2.12/${SPARK_RAPIDS_VERSION}/rapids-4-spark_2.12-${SPARK_RAPIDS_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${rapids_repo_url}/cudf/${CUDF_VERSION}/cudf-${CUDF_VERSION}-cuda${cudf_cuda_version}.jar" \
    -P /usr/lib/spark/jars/
}

function configure_spark() {
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF
###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
# Rapids Accelerator for Spark can utilize AQE, but when plan is not finalized, 
# query explain output won't show GPU operator, if user have doubt
# they can uncomment the line before to see the GPU plan explan, but AQE on give user the best performance.
# spark.sql.adaptive.enabled=false
spark.rapids.sql.concurrentGpuTasks=2
spark.executor.resource.gpu.amount=1
spark.executor.cores=4
spark.executor.memory=8G
spark.task.cpus=1
spark.task.resource.gpu.amount=0.25
spark.rapids.memory.pinnedPool.size=2G
spark.executor.memoryOverhead=2G
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.extraJavaOptions='-Dai.rapids.cudf.prefer-pinned=true'
spark.locality.wait=0s
spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh
spark.sql.shuffle.partitions=48
spark.sql.files.maxPartitionBytes=512m
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar
spark.dynamicAllocation.enabled=false
spark.shuffle.service.enabled=false
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

}

main