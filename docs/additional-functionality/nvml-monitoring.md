---
layout: page
title: NVML GPU Monitoring
parent: Additional Functionality
nav_order: 6
---

The RAPIDS Accelerator for Apache Spark provides NVML (NVIDIA Management Library) monitoring functionality to track GPU utilization and memory usage during Spark job execution. This feature helps with performance analysis and understanding GPU resource consumption patterns.

## Overview

The NVML monitor provides GPU statistics including:

- GPU utilization
- GPU memory utilization
- Other metrics like GPU temporature, etc.

The monitor supports two distinct monitoring modes:

1. **Executor Lifecycle Mode** (default): Monitors GPU usage from executor startup to shutdown
2. **Stage-based Mode**: Similar to AsyncProfiler, monitors each stage independently and switches monitoring contexts based on stage transitions

Regardless of the monitoring mode selected, the system performs periodic updates at configurable intervals to collect and log GPU statistics. This ensures continuous monitoring and provides regular feedback about GPU resource utilization throughout the execution.



## Configuration Options

| Configuration | Explanation | Default Value | Type |
|--------------|-------------|---------------|------|
| spark.rapids.monitor.nvml.enabled | Enable NVML GPU monitoring to track GPU usage statistics during executor or stage execution | false | Runtime |
| spark.rapids.monitor.nvml.intervalMs | Interval in milliseconds for NVML GPU monitoring data collection. Lower values provide more frequent monitoring but may impact performance | 1000 | Runtime |
| spark.rapids.monitor.nvml.logFrequency | Number of GPU monitoring updates before logging a progress message. Set to 0 to disable periodic logging | 10 | Runtime |
| spark.rapids.monitor.nvml.stageMode | When enabled, NVML monitoring will track GPU usage per stage instead of executor lifecycle. Each stage epoch will have separate GPU statistics | false | Runtime |
| spark.rapids.monitor.stageEpochInterval | Unified interval in seconds to determine stage epoch transitions for all monitoring systems. This setting affects all monitoring systems that use stage-based profiling | 5 | Runtime |

## Basic Usage

### Enable NVML Monitoring

To enable basic NVML monitoring for the entire executor lifecycle:

```bash
--conf spark.rapids.monitor.nvml.enabled=true
```

This will start monitoring GPU usage from executor startup to shutdown and log periodic updates to the executor logs.

### Adjust Monitoring Frequency

To change the monitoring interval and logging frequency:

```bash
--conf spark.rapids.monitor.nvml.enabled=true \
--conf spark.rapids.monitor.nvml.intervalMs=500 \
--conf spark.rapids.monitor.nvml.logFrequency=5
```

This configuration:

- Collects GPU statistics every 500ms (twice as frequent)
- Logs progress messages every 5 updates instead of the default 10

### Disable Periodic Logging

To enable monitoring but disable periodic log messages (only lifecycle report will be logged):

```bash
--conf spark.rapids.monitor.nvml.enabled=true \
--conf spark.rapids.monitor.nvml.logFrequency=0
```

## Monitoring Modes

### Executor Lifecycle Mode

This is the default mode where GPU monitoring runs for the entire executor lifetime:

```bash
--conf spark.rapids.monitor.nvml.enabled=true
```

#### Characteristics

- Starts monitoring when executor initializes
- Continues monitoring until executor shuts down
- Provides aggregate GPU usage statistics for the entire executor session
- Reports are generated with executor ID as identifier

#### Use Cases

- Overall application performance analysis
- Resource utilization assessment across entire job
- Long-running applications or streaming jobs

### Stage-based Mode

This mode provides per-stage GPU monitoring similar to the AsyncProfiler flame graph functionality:

```bash
--conf spark.rapids.monitor.nvml.enabled=true \
--conf spark.rapids.monitor.nvml.stageMode=true
```

#### Characteristics

- Monitors GPU usage per stage epoch
- Automatically switches monitoring context when stage transitions occur
- Generates separate reports for each stage epoch
- Uses task-count-based approach to determine stage boundaries

#### Use Cases

- Fine-grained performance analysis per stage
- Identifying GPU usage patterns in specific stages
- Debugging performance issues in particular stages
- Comparing GPU utilization across different stages

### Stage Epoch Determination

Similar to the AsyncProfiler functionality, the stage-based mode handles stage boundaries intelligently:

1. The system periodically checks which stage has the most running tasks
2. Switches profiling to the dominant stage if:
   - The dominant stage has more than 50% of all running tasks
   - The dominant stage is different from the currently monitored stage
3. The check interval is controlled by `spark.rapids.monitor.stageEpochInterval` (default 5 seconds)




## Report Generation

The NVML monitor automatically generates lifecycle reports when monitoring stops:

### Executor Mode Report

- Report name: `Executor-{executorId}`
- Contains aggregate statistics for entire executor lifecycle

### Stage Mode Report

- Report name: `Stage-{stageId}-Epoch-{epochId}`
- Contains statistics for specific stage epoch
- Generated when stage transitions occur or at shutdown

Setting `spark.scheduler.mode=FIFO` is recommended for cleaner stage boundaries when using stage-based monitoring.

## Log Output Examples

### Lifecycle Report

```text
25/09/02 15:12:03.612 ScalaTest-main-running-NVMLMonitorSuite INFO NVMLMonitor: Stage-2-Epoch-0 - LIFECYCLE REPORT: GPU_0 (NVIDIA RTX 5000 Ada Generation): 1.9s, 13 samples, GPU 0% (avg), Mem 0% (avg), 37°C (avg), 15W (avg) | GPU_1 (NVIDIA RTX A5000): 1.9s, 13 samples, GPU 7% (avg), Mem 0% (avg), 65°C (avg), 94W (avg)

>>> GPU_0 Detailed Statistics: NVIDIA RTX 5000 Ada Generation
Duration: 1.87s, Samples: 13 (7.0/s), Hardware: 12800 SMs, PCIe Gen1 x16
Utilization - GPU: Min:   0, Max:   0, Avg:   0%, Memory: Min:   0, Max:   0, Avg:   0%
Memory - Used: Min:  408, Max:  408, Avg:  408 MB, Free: Min: 32351, Max: 32351, Avg: 32351 MB, Total: 32760 MB
Thermal/Power - Temp: Min:  37, Max:  37, Avg:  37°C, Power: Min:  15, Max:  16, Avg:  15 W (limit: 250 W)
Clocks - Graphics: Min: 210, Max: 210, Avg: 210 MHz, Memory: Min: 405, Max: 405, Avg: 405 MHz, SM: Min: 210, Max: 210, Avg: 210 MHz
Other - Fan: Min:  30, Max:  30, Avg:  30%, Performance State: Min:   8, Max:   8, Avg:   8 (avg P8)
>>> GPU_1 Detailed Statistics: NVIDIA RTX A5000
Duration: 1.87s, Samples: 13 (7.0/s), Hardware: 8192 SMs, PCIe Gen4 x16
Utilization - GPU: Min:   6, Max:  10, Avg:   7%, Memory: Min:   0, Max:   1, Avg:   0%
Memory - Used: Min: 1547, Max: 1579, Avg: 1549 MB, Free: Min: 22984, Max: 23016, Avg: 23013 MB, Total: 24564 MB
Thermal/Power - Temp: Min:  65, Max:  65, Avg:  65°C, Power: Min:  94, Max:  95, Avg:  94 W (limit: 230 W)
Clocks - Graphics: Min: 1695, Max: 1695, Avg: 1695 MHz, Memory: Min: 7600, Max: 7600, Avg: 7600 MHz, SM: Min: 1695, Max: 1695, Avg: 1695 MHz
Other - Fan: Min:  38, Max:  39, Avg:  38%, Performance State: Min:   2, Max:   2, Avg:   2 (avg P2)
```

### Periodic Update

```text
25/09/02 16:21:47 INFO NVMLMonitorOnExecutor: NVML Update #10:
25/09/02 16:21:47 INFO NVMLMonitorOnExecutor:   GPU_0 (NVIDIA RTX 5000 Ada Generation): Util: 8%, Mem: 0% (32146MB/32760MB), Temp: 50°C, Power: 107W/250W, Clocks: 2730/8550 MHz
25/09/02 16:21:47 INFO NVMLMonitorOnExecutor:   GPU_1 (NVIDIA RTX A5000): Util: 9%, Mem: 1% (1547MB/24564MB), Temp: 66°C, Power: 95W/230W, Clocks: 1695/7600 MHz
```