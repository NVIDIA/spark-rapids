# NVML Monitor Integration Example

This document shows how to use the new NVML GPU monitoring feature in RAPIDS Spark plugin.

## Configuration Options

The following new configuration options are available:

### Basic Configuration

- `spark.rapids.monitor.nvml.enabled`: Enable/disable NVML monitoring (default: false)
- `spark.rapids.monitor.nvml.intervalMs`: Monitoring interval in milliseconds (default: 1000)
- `spark.rapids.monitor.nvml.logFrequency`: Log every N updates (default: 10, set 0 to disable)

### Advanced Configuration (Stage-based Monitoring)

- `spark.rapids.monitor.nvml.stageMode`: Enable per-stage monitoring like AsyncProfiler (default: false)
- `spark.rapids.monitor.nvml.stageEpochInterval`: Stage transition interval in seconds (default: 5)

## Usage Examples

### Example 1: Basic Executor Lifecycle Monitoring

```bash
$SPARK_HOME/bin/spark-shell \
--jars rapids-4-spark_2.12-25.10.0-SNAPSHOT-cuda12.jar \
--conf spark.plugins=com.nvidia.spark.SQLPlugin \
--conf spark.rapids.monitor.nvml.enabled=true \
--conf spark.rapids.monitor.nvml.intervalMs=500 \
--conf spark.rapids.monitor.nvml.logFrequency=5
```

This will:
- Enable NVML monitoring
- Collect GPU stats every 500ms
- Log progress every 5 updates
- Print final GPU lifecycle stats when executor shuts down

### Example 2: Stage-based Monitoring

```bash
$SPARK_HOME/bin/spark-shell \
--jars rapids-4-spark_2.12-25.10.0-SNAPSHOT-cuda12.jar \
--conf spark.plugins=com.nvidia.spark.SQLPlugin \
--conf spark.rapids.monitor.nvml.enabled=true \
--conf spark.rapids.monitor.nvml.stageMode=true \
--conf spark.rapids.monitor.nvml.stageEpochInterval=3 \
--conf spark.scheduler.mode=FIFO
```

This will:
- Enable stage-based GPU monitoring
- Switch monitoring contexts every 3 seconds based on dominant stage
- Print GPU statistics for each stage epoch
- Work best with FIFO scheduling for clean stage boundaries

### Example 3: High Frequency Monitoring with Minimal Logging

```bash
$SPARK_HOME/bin/spark-shell \
--jars rapids-4-spark_2.12-25.10.0-SNAPSHOT-cuda12.jar \
--conf spark.plugins=com.nvidia.spark.SQLPlugin \
--conf spark.rapids.monitor.nvml.enabled=true \
--conf spark.rapids.monitor.nvml.intervalMs=100 \
--conf spark.rapids.monitor.nvml.logFrequency=0
```

This will:
- Monitor GPU every 100ms (high frequency)
- Disable periodic logging (logFrequency=0)
- Only show start/stop messages and final statistics

## Sample Output

When monitoring is enabled, you'll see log messages like:

```
24/01/15 10:30:15 INFO NVMLMonitorOnExecutor: Initializing NVML Monitor: stageMode=false, intervalMs=1000, logFreq=10
24/01/15 10:30:15 INFO NVMLMonitorOnExecutor: NVML detected 1 GPU device(s)
24/01/15 10:30:15 INFO NVMLMonitorOnExecutor: 🚀 NVML monitoring started for executor 1
24/01/15 10:30:25 INFO NVMLMonitorOnExecutor: NVML Update #10:
24/01/15 10:30:25 INFO NVMLMonitorOnExecutor:   GPU 0: Util=85%, Mem=4.2GB/8.0GB (52%), Temp=75°C
24/01/15 10:30:35 INFO NVMLMonitorOnExecutor: NVML Update #20:
24/01/15 10:30:35 INFO NVMLMonitorOnExecutor:   GPU 0: Util=92%, Mem=6.1GB/8.0GB (76%), Temp=78°C
...
24/01/15 10:31:00 INFO NVMLMonitorOnExecutor: ⏹️  NVML monitoring stopped for executor 1
[GPU Lifecycle Statistics Report would be printed here]
```

In stage mode, you'll also see stage transition messages:

```
24/01/15 10:30:20 INFO NVMLMonitorOnExecutor: Stage epoch transition: -1 -> 0 (8/10 tasks)
24/01/15 10:30:23 INFO NVMLMonitorOnExecutor: 🚀 NVML monitoring started for stage 0 epoch 1
...
24/01/15 10:30:28 INFO NVMLMonitorOnExecutor: Stage epoch transition: 0 -> 1 (12/15 tasks)
24/01/15 10:30:28 INFO NVMLMonitorOnExecutor: ⏹️  NVML monitoring stopped for stage 0 epoch 1
[Stage 0 Epoch 1 GPU Statistics would be printed here]
```

## Requirements

- NVIDIA GPU with NVML support
- CUDA toolkit properly installed
- Rapids plugin with JNI support
- NVMLMonitor.java available in spark-rapids-jni

## Troubleshooting

If you see "Failed to initialize NVML", check:
1. NVIDIA drivers are properly installed
2. CUDA is available
3. Rapids JNI library includes NVML support
4. Executor has proper GPU access permissions
