---
layout: page
title: NVTX Ranges
nav_order: 2
parent: Developer Overview
---
# Using NVTX Ranges with the RAPIDS Plugin for Spark
NVTX ranges are typically used to profile applications that use the GPU. Such NVTX profiles,
once captured can be visually analyzed using
[NVIDIA NSight Systems](https://developer.nvidia.com/nsight-systems).
This document is specific to the RAPIDS Spark Plugin profiling.

### STEP 1:

In order to get NVTX ranges to work you need to recompile your cuDF with NVTX flag enabled:

```
//from the cpp/build directory

cmake .. -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX -DCMAKE_CXX11_ABI=ON -DUSE_NVTX=1

make -j <num_threads>
```
If you are using the java cuDF layer, recompile your jar as usual using maven.
```
mvn clean package -DskipTests
```
### STEP 2:

We need to pass a flag to the spark executors / driver in order to enable NVTX collection.
This can be done for spark shell by adding the following configuration keys:
```
--conf spark.driver.extraJavaOptions=-Dai.rapids.cudf.nvtx.enabled=true
--conf spark.executor.extraJavaOptions=-Dai.rapids.cudf.nvtx.enabled=true
```
For java based profile tests add this to `JAVA_OPTS`
```
export JAVA_OPTS=”-Dai.rapids.cudf.nvtx.enabled=true”
```
To capture the process’ profile run :
`nsys profile <command>`

where command can be your Spark shell / Java program etc.
This works typically in non-distributed mode.

To make it run in Spark’s distributed mode, start the worker with "nsys profile " in front of the
worker start command.
Here is an example that starts up a slave in standalone mode, profiles it and the shell
until the shell exits (using Ctrl+D) while stopping the slave process at the end."
```
nsys profile bash -c " \
CUDA_VISIBLE_DEVICES=0 ${SPARK_HOME}/sbin/start-slave.sh $master_url & \
$SPARK_HOME/bin/spark-shell; \
${SPARK_HOME}/sbin/stop-slave.sh"

```
If you need to kill the worker process that is being traced, do not use `kill -9`.

You should have a *.qdrep file once the trace completes. This can now be opened in NSight UI.

## How to add NVTX ranges to your code?

If you are in Java or Scala land you can do the following:

```
val nvtxRange = new NvtxRange(<name of the range>, NvtxColor.YELLOW)
try {
  // the code you want to profile
} finally {
  nvtxRange.close()
}
```
In C++ land:
```
gdf_nvtx_range_push_hex("write_orc_all", 0xffff0000);
// the code you want to profile
gdf_nvtx_range_pop();
```

To use CPU profiling features, run the following command before running `nsys profile`:
```
sudo sh -c 'echo [level] >/proc/sys/kernel/perf_event_paranoid'
```
where valid values are { 1, 2 }. Refer to
[NVIDIA Nsight Systems documentation](https://docs.nvidia.com/nsight-systems/)
for further details.
