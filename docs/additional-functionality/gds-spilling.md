---
layout: page
title: GPUDirect Storage (GDS) Spilling
parent: Additional Functionality
nav_order: 6
---
# GPUDirect Storage (GDS) Spilling
---
**NOTE**

_GPUDirect Storage (GDS) Spilling_ is a beta feature!

---
The [RAPIDS Shuffle Manager](rapids-shuffle.md) has a spillable cache that keeps GPU data in device
memory, but can spill to host memory and then to disk when the GPU is out of memory. Using
[GPUDirect Storage (GDS)](https://docs.nvidia.com/gpudirect-storage/), device buffers can be spilled
directly to storage. This direct path increases system bandwidth, decreases latency and 
utilization load on the CPU.

### System Setup
In order to enable GDS spilling, GDS must be installed on the host. GDS software can be 
downloaded [here](https://developer.nvidia.com/gpudirect-storage). Follow the
[GDS Installation and Troubleshooting Guide](
https://docs.nvidia.com/gpudirect-storage/troubleshooting-guide/index.html)
to install and configure GDS.

### Spark App Configuration
After GDS is installed on the host, to enable GDS spilling:
* Make sure the [RAPIDS Shuffle Manager](rapids-shuffle.md) is enabled and configured correctly.
* Make sure the Spark "scratch" directory configured by `spark.local.dir` supports GDS.
* Set `spark.rapids.memory.gpu.direct.storage.spill.enabled=true` in the Spark app.

To verify that GDS spilling is working correctly, add the following line to
`${SPARK_HOME}/conf/log4j.properties`:
```properties
log4j.logger.com.nvidia.spark.rapids.RapidsGdsStore=DEBUG
```
When spilling happens, the log file should show information for writing to and reading from GDS.

### Fine-Tuning
Writing many small device buffers through GDS incurs overhead that may affect spilling performance.
To combat this issue, small device buffers are concatenated together before written to disk in a 
batch. The batch write buffer used for this purpose takes up PCI Base Address Register (BAR) space, 
which can be very limited on some GPUs. For example, the NVIDIA T4 only has 256 MiB. On GPUs with a
larger BAR space (e.g. the NVIDIA V100 or the NVIDIA A100), you can increase the size of the 
batch write buffer, which may further improve spilling performance. To change the batch write buffer
size from the default 8 MiB to, say, 64 MiB, set
`spark.rapids.memory.gpu.direct.storage.spill.batchWriteBuffer.size=64m` in the Spark app.
