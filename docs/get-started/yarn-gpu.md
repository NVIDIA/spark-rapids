---
layout: page
title: yarn-gpu
nav_exclude: true
---

## Spark3 GPU Configuration Guide on Yarn 3.2.1 

Following files recommended to be configured to enable GPU scheduling on Yarn 3.2.1 and later. 

GPU resource discovery script - `/usr/lib/spark/scripts/gpu/getGpusResources.sh`:
```bash
mkdir -p /usr/lib/spark/scripts/gpu/
cd /usr/lib/spark/scripts/gpu/
wget https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh
chmod a+rwx -R /usr/lib/spark/scripts/gpu/
```

Spark config - `/etc/spark/conf/spark-default.conf`:
```bash
spark.rapids.sql.concurrentGpuTasks=2
spark.executor.resource.gpu.amount=1
spark.executor.cores=8
spark.task.cpus=1
spark.task.resource.gpu.amount=0.125
spark.rapids.memory.pinnedPool.size=2G
spark.executor.memoryOverhead=2G
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.extraJavaOptions='-Dai.rapids.cudf.prefer-pinned=true'
spark.locality.wait=0s
spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh # this match the location of discovery script
spark.sql.shuffle.partitions=40
spark.sql.files.maxPartitionBytes=512m
```

Yarn Scheduler config - `/etc/hadoop/conf/capacity-scheduler.xml`:
```xml
<configuration>
  <property>
    <name>yarn.scheduler.capacity.resource-calculator</name>     
    <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
  </property>
</configuration>
```

Yarn config - `/etc/hadoop/conf/yarn-site.xml`:
```xml
<configuration>
  <property>
    <name>yarn.nodemanager.resource-plugins</name>
    <value>yarn.io/gpu</value>
  </property>
  <property>
     <name>yarn.resource-types</name>
     <value>yarn.io/gpu</value>
  </property>
  <property>
     <name>yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices</name>
     <value>auto</value>
  </property>
  <property>
     <name>yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables</name>
     <value>/usr/bin</value>
  </property>
  <property>
     <name>yarn.nodemanager.linux-container-executor.cgroups.mount</name>
     <value>true</value>
  </property>
  <property>
     <name>yarn.nodemanager.linux-container-executor.cgroups.mount-path</name>
     <value>/sys/fs/cgroup</value>
  </property>
  <property>
     <name>yarn.nodemanager.linux-container-executor.cgroups.hierarchy</name>
     <value>yarn</value>
  </property>
  <property>
    <name>yarn.nodemanager.container-executor.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value>
  </property>
  <property>
    <name>yarn.nodemanager.linux-container-executor.group</name>
    <value>yarn</value>
  </property>
</configuration>
```

`/etc/hadoop/conf/container-executor.cfg` - user yarn as service account:
```bash
yarn.nodemanager.linux-container-executor.group=yarn

#--Original container-exectuor.cfg Content--

[gpu]
module.enabled=true
[cgroups]
root=/sys/fs/cgroup
yarn-hierarchy=yarn
```

Need to share node manager local dir to all user, run below in bash:
```bash
chmod a+rwx -R /sys/fs/cgroup/cpu,cpuacct
chmod a+rwx -R /sys/fs/cgroup/devices
local_dirs=$(bdconfig get_property_value \
    --configuration_file /etc/hadoop/conf/yarn-site.xml \
    --name yarn.nodemanager.local-dirs 2>/dev/null)
mod_local_dirs=${local_dirs//\,/ }
chmod a+rwx -R ${mod_local_dirs}
```

In the end, restart node manager and resource manager service:
On all workers:
```bash
sudo systemctl restart hadoop-yarn-nodemanager.service
```
On all masters:
```bash
sudo systemctl restart hadoop-yarn-resourcemanager.service
```
