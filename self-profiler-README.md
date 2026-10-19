# cuDF for Spark Built-in Self Profiler

## Overview

cuDF for Spark includes a self-contained GPU profiler that can generate data
compatible with NVIDIA Nsight Systems.

This feature does **not** launch:

```bash
nsys profile ...
```

at runtime. Instead, selected executor JVMs load `libprofilerjni.so` and capture
CUDA activities in process through CUPTI and NVTX injection. Each executor
writes a compressed flatbuffer file. The file is converted to `.nsys-rep`
offline with `spark_rapids_profile_converter`.

Do not confuse this feature with:

- manually wrapping Spark workers with `nsys profile`;
- `spark.rapids.flameGraph.*`, which uses async-profiler and produces JFR/flame
  graph data.

## Default Capture

When profiling is enabled, the native profiler enables these CUPTI activity
kinds:

- `CUPTI_ACTIVITY_KIND_DEVICE`
  - GPU device metadata.
- `CUPTI_ACTIVITY_KIND_DRIVER`
  - CUDA Driver API calls.
- `CUPTI_ACTIVITY_KIND_RUNTIME`
  - CUDA Runtime API calls.
- `CUPTI_ACTIVITY_KIND_MEMCPY`
  - HtoD, DtoH, DtoD and other CUDA memory-copy activities.
- `CUPTI_ACTIVITY_KIND_MEMSET`
  - CUDA memset activities.
- `CUPTI_ACTIVITY_KIND_NAME`
  - CUPTI name activities are enabled. The current serializer does not have a
    dedicated `NAME` record branch, while kernel records still contain kernel
    names.
- `CUPTI_ACTIVITY_KIND_MARKER`
  - NVTX markers and ranges.
- `CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL`
  - GPU kernel execution.
- `CUPTI_ACTIVITY_KIND_OVERHEAD`
  - CUPTI/driver profiling overhead.

The closest `nsys` analogy is:

```text
cuda + nvtx
```

It is not equivalent to the full default trace set of the `nsys profile` CLI.

## Not Captured

The built-in profiler does not enable:

- OS runtime/syscall tracing (`osrt`);
- CPU sampling;
- CPU stack traces/backtraces;
- thread scheduling or context-switch tracing;
- MPI or OpenMP tracing;
- Vulkan/OpenGL tracing;
- separate cuDNN/cuBLAS trace categories;
- full `CUPTI_ACTIVITY_KIND_MEMORY` allocation-lifetime activity;
- Python worker processes;
- the Spark driver JVM.

Only selected executor JVM processes are captured.

## Runtime API Filtering

The serializer drops several common Runtime APIs to reduce output size:

```text
cudaGetDevice
cudaGetLastError
cudaPeekAtLastError
cudaDeviceGetAttribute
```

The following APIs are also dropped by default:

```text
cudaMallocAsync
cudaMallocAsync_ptsz
cudaMallocFromPoolAsync
cudaMallocFromPoolAsync_ptsz
cudaFreeAsync
cudaFreeAsync_ptsz
```

To include these async allocation/free Runtime API calls:

```text
--conf spark.rapids.profile.asyncAllocCapture=true
```

This setting captures the API calls only. It does not enable the full CUPTI
memory-activity trace.

## Enabling the Profiler

The profiler is disabled unless `spark.rapids.profile.pathPrefix` is set.

Minimal configuration:

```text
--conf spark.rapids.profile.pathPrefix=hdfs:///tmp/rapids-profiles
```

With only this setting:

- only executor `0` is profiled;
- capture starts during executor initialization;
- capture continues until executor plugin shutdown;
- output is compressed with Zstandard.

## Selecting Executors

Default:

```text
spark.rapids.profile.executors=0
```

The value accepts comma-separated executor IDs and integer ranges:

```text
--conf spark.rapids.profile.executors=0,2-4,8
```

An empty value matches no executors.

Profiling many executors simultaneously can generate large output and add
profiling overhead. Start with a small representative subset.

## Capture Ranges

### Relative Time Ranges

Capture selected time ranges relative to executor startup:

```text
--conf spark.rapids.profile.timeRangesInSeconds=10-30,100-110
```

This example:

1. waits 10 seconds;
2. captures for 20 seconds;
3. waits 70 seconds;
4. captures for another 10 seconds.

### Spark Jobs

Capture selected Spark job IDs:

```text
--conf spark.rapids.profile.jobs=3,5-7
```

Job-level profiling depends on Hadoop `CallerContext`.

### Spark Stages

Capture selected Spark stage IDs:

```text
--conf spark.rapids.profile.stages=12,15-18
```

### Limit Tasks Per Stage

```text
--conf spark.rapids.profile.taskLimitPerStage=4
```

Default is `0`, meaning all tasks in each selected stage.

### Range Constraint

Time ranges cannot be combined with job or stage ranges. Choose either:

```text
timeRangesInSeconds
```

or:

```text
jobs / stages
```

## Configuration Reference

### `spark.rapids.profile.pathPrefix`

- Default: unset.
- Enables profiling and specifies the Hadoop-compatible output directory.

### `spark.rapids.profile.executors`

- Default: `0`.
- Executor IDs and ranges to capture.

### `spark.rapids.profile.timeRangesInSeconds`

- Default: unset.
- Capture windows relative to executor startup.

### `spark.rapids.profile.jobs`

- Default: unset.
- Spark job IDs and ranges to capture.

### `spark.rapids.profile.stages`

- Default: unset.
- Spark stage IDs and ranges to capture.

### `spark.rapids.profile.taskLimitPerStage`

- Default: `0`.
- Maximum profiled tasks per selected stage; values less than or equal to zero
  mean unlimited.

### `spark.rapids.profile.asyncAllocCapture`

- Default: `false`.
- Includes async CUDA allocation/free Runtime API calls.

### `spark.rapids.profile.driverPollMillis`

- Default: `1000`.
- Executor polling interval for job/stage completion.

### `spark.rapids.profile.compression`

- Default: `zstd`.
- Supported values: `zstd`, `none`.

### `spark.rapids.profile.flushPeriodMillis`

- Default: `0`.
- Periodic CUPTI flush interval in milliseconds.
- Values less than or equal to zero disable periodic flushing.
- Records are still flushed when capture stops or the profiler shuts down.

### `spark.rapids.profile.writeBufferSize`

- Default: `8388608` bytes (8 MiB).
- Host buffer used to collect and write profile records.

## Output

Each selected executor writes:

```text
<pathPrefix>/rapids-profile-<application-id>-<executor-id>.bin.zstd
```

If compression is `none`, the `.zstd` suffix is omitted.

The output path is resolved through Hadoop `FileSystem` and must be writable by
every selected executor.

## Converting to Nsight Systems

The runtime `.bin` file must be converted offline with
`spark_rapids_profile_converter`.

Example:

```bash
spark_rapids_profile_converter \
  -w \
  --nvtxw-backend=/opt/nvidia/nsight-systems/<version>/host-linux-x64/libNvtxwBackend.so \
  -o executor-0.nsys-rep \
  rapids-profile-<application-id>-0.bin.zstd
```

The converter supports:

```text
-j, --json
-o, --output=PATH
-t, --nvtxt
-w, --nvtxw
--nvtxw-backend=PATH
--ignore-truncated
--nvtxw-chunk-records=N
```

The NVTXW backend can also be specified with:

```text
NVTXW_BACKEND=/path/to/libNvtxwBackend.so
```

## Recommended Configurations

### Profile one executor for the full application

```text
--conf spark.rapids.profile.pathPrefix=hdfs:///tmp/rapids-profiles
--conf spark.rapids.profile.executors=0
```

### Profile selected stages

```text
--conf spark.rapids.profile.pathPrefix=hdfs:///tmp/rapids-profiles
--conf spark.rapids.profile.executors=0-1
--conf spark.rapids.profile.stages=10-12
--conf spark.rapids.profile.taskLimitPerStage=2
```

### Profile a short time window

```text
--conf spark.rapids.profile.pathPrefix=hdfs:///tmp/rapids-profiles
--conf spark.rapids.profile.executors=0
--conf spark.rapids.profile.timeRangesInSeconds=60-90
```

### Include async allocation APIs

```text
--conf spark.rapids.profile.asyncAllocCapture=true
```

Async allocation capture increases profile volume and should only be enabled
when allocation behavior is part of the investigation.

## Operational Considerations

- Profiling adds CUPTI processing, serialization, buffering, compression, and
  filesystem-write overhead.
- Profiling all executors or a long-running application can create large output.
- Start with one executor and a narrow stage/time range.
- A lost executor may leave a truncated profile; the converter supports
  `--ignore-truncated`.
- The output path must be writable by every selected executor.
- Job and stage IDs must match the current Spark application.
- Profiles can contain kernel/API timing and application-level NVTX labels.
  Treat output as potentially sensitive diagnostic data.
- The native profiler requires a compatible NVIDIA CUDA/CUPTI environment.
- Nsight Systems is required only for offline `.nsys-rep` conversion/viewing,
  not for runtime collection.

## Version History

- [PR #10870](https://github.com/NVIDIA/cudf-spark/pull/10870)
  - Introduced self-contained profiling.
  - First released in v24.06.0.
- [PR #10965](https://github.com/NVIDIA/cudf-spark/pull/10965)
  - Disabled async-allocation capture by default.
  - Increased the write buffer to 8 MiB.
  - First released in v24.08.1.
- [PR #11708](https://github.com/NVIDIA/cudf-spark/pull/11708)
  - Added stage selection and per-stage task limits.
  - First released in v24.12.0.
- [PR #13563](https://github.com/NVIDIA/cudf-spark/pull/13563)
  - Prevented profiler initialization failures from terminating executors.
  - First released in v25.10.0.

## Source Locations

cuDF for Spark:

```text
sql-plugin/src/main/scala/com/nvidia/spark/rapids/profiler.scala
sql-plugin/src/main/scala/com/nvidia/spark/rapids/RapidsConf.scala
sql-plugin/src/main/scala/com/nvidia/spark/rapids/Plugin.scala
sql-plugin/src/main/scala/com/nvidia/spark/rapids/RangeConfMatcher.scala
```

cuDF for Spark JNI:

```text
src/main/java/com/nvidia/spark/rapids/jni/Profiler.java
src/main/cpp/profiler/ProfilerJni.cpp
src/main/cpp/profiler/profiler_serializer.cpp
src/main/cpp/profiler/README.md
```

