#!/bin/bash

# Parameters:
#  1 spark master
#  2 spark master port
#  3 spark master public DNS
#  4 spark master web ui port
#  5 optional: cores to use

# Write the spark.defaults file for HIPAA compliance if necessary
${DB_HOME}/spark/scripts/hipaa-setup.sh spark-defaults

export SPARK_HOME=$DB_HOME/spark

. "${SPARK_HOME}/bin/load-spark-env.sh"

# Appending ML Runtime Python Path items, if they exist to PYTHONPATH
if [[ -f $MLR_PYTHONPATH ]];
then
  MLR_PYTHONPATH_CONTENT=`cat $MLR_PYTHONPATH`
  export PYTHONPATH=$SPARK_HOME/python:$MLR_PYTHONPATH_CONTENT
fi

# Set R_LIBS to configure library installation locations.
R_LIBS_PRIMARY=$SPARK_HOME/R/lib
# Set environment variables for NFS library directories. These should be kept in sync with
# setup_driver_env.sh. Additionally, DATABRICKS_LIBS_NFS_ROOT_DIR needs to match the value of
# NFS_PATH_SUFFIX set for the sidecar container in Kubernetes-based dataplane, see
# DbrClusterJsonnetInputParams.
export DATABRICKS_LIBS_NFS_ROOT_DIR="${DATABRICKS_LIBS_NFS_ROOT_DIR:=.ephemeral_nfs}"
export DATABRICKS_CLUSTER_LIBS_ROOT_DIR="${DATABRICKS_CLUSTER_LIBS_ROOT_DIR:=cluster_libraries}"
export DATABRICKS_CLUSTER_LIBS_R_ROOT_DIR="${DATABRICKS_CLUSTER_LIBS_R_ROOT_DIR:=r}"
# Get the first element of $SPARK_LOCAL_DIRS.
SPARK_LOCAL_DIR=$(echo $SPARK_LOCAL_DIRS | cut -d , -f 1)
R_CLUSTER_LIBS=$SPARK_LOCAL_DIR/$DATABRICKS_LIBS_NFS_ROOT_DIR/$DATABRICKS_CLUSTER_LIBS_ROOT_DIR/$DATABRICKS_CLUSTER_LIBS_R_ROOT_DIR
export R_LIBS=$R_LIBS_PRIMARY:$R_CLUSTER_LIBS

SECONDS=0
mkdir -p $R_LIBS_PRIMARY
R CMD INSTALL --library=$R_LIBS_PRIMARY --no-docs --no-html $SPARK_HOME/R/pkg/
echo "Took $SECONDS seconds to install Spark R"

echo "$@" > /tmp/slave-params

export SPARK_PUBLIC_DNS=$3
export SPARK_WORKER_WEBUI_PORT=$4

if [ "$5" -gt "0" ]; then
    export SPARK_WORKER_CORES=$5
fi

# Config directory
DBCONF_DIR=$DB_HOME/spark/dbconf
# Log4j config
LOG4J_DIR=$DBCONF_DIR/log4j/master-worker
# The slave must have SPARK_JARS in its classpath in addition
# to $DB_HOME/spark/assembly/target/scala-2.10/spark-assembly-1.0.0-hadoop1.2.1.jar.
# The reason is that this assembly jar is empty, according to ami/charms/worker/runnext.
#
# Include the driver-daemon jars (which includes all relevant spark jars).
SPARK_JARS=`echo /databricks/jars/* | tr " " :`

SHUFFLE_SERVICE_OPT=""
SPARK_EXECUTOR_LOG_CONF=""
SPARK_VERSION=$(head -n 1 $DB_HOME/spark/VERSION)

if [[ ${SPARK_VERSION:0:1} -ge 2 ]]; then
  # Shuffle service is only enabled if the Spark version is greater than or equal to 2.0.0
  SHUFFLE_SERVICE_OPT="-Dspark.shuffle.service.enabled=true -Dspark.shuffle.service.port=4048"
  # Only need to configure executor logging strategy if the Spark version is greater than or equal to 2.0.0
  SPARK_EXECUTOR_LOG_CONF="-Dspark.executor.logs.rolling.strategy=time"
  SPARK_EXECUTOR_LOG_CONF="$SPARK_EXECUTOR_LOG_CONF -Dspark.executor.logs.rolling.time.interval=hourly"
  SPARK_EXECUTOR_LOG_CONF="$SPARK_EXECUTOR_LOG_CONF -Dspark.executor.logs.rolling.maxRetainedFiles=72"
  SPARK_EXECUTOR_LOG_CONF="$SPARK_EXECUTOR_LOG_CONF -Dspark.executor.logs.rolling.enableCompression=true"
fi

$SPARK_HOME/scripts/install_gpu_driver.sh

# If it is GPU image (check nvidia-smi) and the spark version is greater or equal to 3.0.0,
# we will set up the configs for GPU scheduling
if [[ ${SPARK_VERSION:0:1} -ge 3 ]] && [[ -x "$(command -v nvidia-smi)" ]]; then
  NUM_GPUS=`nvidia-smi --query-gpu=name --format=csv,noheader | wc -l`
  SPARK_RESOURCE_CONF="-Dspark.worker.resource.gpu.discoveryScript=/databricks/spark/scripts/gpu/get_gpus_resources.sh -Dspark.worker.resource.gpu.amount=$NUM_GPUS"
fi

# LOG4J_DIR must be specified before SPARK_JARS
export SPARK_DAEMON_JAVA_OPTS="$SHUFFLE_SERVICE_OPT -XX:-UseContainerSupport -Dspark.worker.cleanup.enabled=false $SPARK_RESOURCE_CONF $SPARK_EXECUTOR_LOG_CONF -cp $LOG4J_DIR:$SPARK_JARS"

# Inject allowlisted custom spark conf to spark worker
# Same code snippet in start_spark_master.sh
get_custom_spark_conf_location() {
  local deploy_conf_dir="$DB_HOME/common/conf"

  local value="$($DB_HOME/spark/scripts/conf_reader \
    --key "databricks.common.driver.sparkConfFilePath" \
    --project-group "untrusted-projects" \
    --conf-dir "$deploy_conf_dir" \
    --quiet 2> /dev/null)"
  if [[ -z "$value" ]]; then
    echo "/tmp/custom-spark.conf"
  else
    echo "$value"
  fi
}

custom_spark_conf_file="$(get_custom_spark_conf_location)"

SPARK_CONF_KEYS_TO_PROPAGATE=(
    'spark.ui.reverseProxy'
    'spark.ui.reverseProxyUrl'
    'spark.worker.aioaLazyConfig.enabled'
    'spark.worker.aioaLazyConfig.iamRoleCheckEnabled'
    'spark.worker.register.initialRegistrationRetries'
    'spark.worker.register.proLongedRegistrationRetries'
    'spark.worker.register.initialRegistrationIntervalSec'
    'spark.worker.register.prolongedRegistrationIntervalSec'
)

for KEY in "${SPARK_CONF_KEYS_TO_PROPAGATE[@]}"; do
    VALUE="$(cat "$custom_spark_conf_file" | grep "^$KEY " | cut -d' ' -f2-)"
    if [[ ! -z "$VALUE" ]]; then
        SPARK_DAEMON_JAVA_OPTS+=" -D$KEY='$VALUE'"
    fi
done

export SPARK_DIST_CLASSPATH="$LOG4J_DIR:$SPARK_JARS"

source $SPARK_HOME/scripts/rotate_logs.sh
rotate_active_spark_log $SPARK_HOME/logs

# On slaves, we use single thread OpenBLAS.
export OPENBLAS_NUM_THREADS=1

# The default malloc() implementation is rather aggresive in creating new arenas to handle memory allocations in a multi-threaded workload.
# By default it can create up to 8 x NUM_CORES arenas. It leads to increased memory fragmentation. The Spark executor doesn't typically have
# much space for memory from malloc() as most of it is used by java heap. Therefore, it benefits from a much more conservative malloc() settings.
# See:
#  https://github.com/prestodb/presto/issues/8993 
#  https://devcenter.heroku.com/articles/tuning-glibc-memory-behavior
export MALLOC_ARENA_MAX=4

# Configure metrics.properties to send metrics to gmond on the driver node
echo "*.sink.ganglia.host=$1" >> $SPARK_HOME/conf/metrics.properties

# START SPARK SLAVE. We will always launch a single worker at here.
# Activate the root python env before start Spark slave.
#   - For conda runtime, this is DATABRICKS_ROOT_CONDA_ENV
#   - For non-conda runtime, this is DATABRICKS_ROOT_VIRTUALENV_ENV
# The conda script is a no-op in non-conda runtimes.
(
  /databricks/spark/scripts/copy_conda_pkgs_dir.sh
  . /databricks/spark/scripts/activate_root_python_environment.sh
  $SPARK_HOME/sbin/start-slave.sh spark://$1:$2
)

# Start gmond and setup monit configurations
$SPARK_HOME/scripts/ganglia/start_ganglia.sh --services ganglia-monitor --driver_ip $1

# Enable monit for the slave
if [ ! -e  /etc/monit/conf.d/spark-slave.cfg ]; then
  cp /etc/monit/conf.d/spark-slave-not-active /etc/monit/conf.d/spark-slave.cfg
  /etc/init.d/monit start
  /etc/init.d/monit reload
fi

# Make sure that Python is fully initialized before we start spark worker.
# So, when user ubuntu is used to interact with Python, user ubuntu
# will not see permission related issue.
# We ignore the return code of this call. So, even the python environment
# is in a bad state because of any customization by users, we can still
# start services in the container.
echo "Trigger Python initialization"
/databricks/python/bin/python --version || true

# Note: This should be the highest priority (most likely to be killed)
nohup $SPARK_HOME/scripts/set_jvms_oom_priority.sh 2000 &> /tmp/set_jvms_oom_priority_output &
