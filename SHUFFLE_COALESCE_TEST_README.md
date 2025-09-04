# Shuffle Coalesce Before Shuffle Performance Test

This directory contains tests for the new "coalesce before shuffle" feature that was added to optimize GPU shuffle performance.

## Feature Description

The feature adds a configurable coalescing step before shuffle partitioning to potentially reduce memory fragmentation and improve performance. It's controlled by the configuration:

- **Config**: `spark.rapids.shuffle.coalesceBeforeShuffleTargetSizeRatio`
- **Default**: `0.0` (disabled)
- **Range**: `0.0` to `1.0`
- **Description**: When > 0, coalesces batches to `targetBatchSize * ratio` before shuffle partitioning

## Test Files

### Interactive Performance Test
**File**: `performance_test_shuffle_coalesce.scala`

Interactive Scala script for manual performance testing and analysis.

**Run with spark-shell**:
```bash
# Start spark-shell with RAPIDS plugin
spark-shell \
  --jars path/to/rapids-4-spark_*.jar \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=0.25 \
  --conf spark.rapids.sql.exec.ShuffleExchangeExec=true \
  -i performance_test_shuffle_coalesce.scala
```

## Test Scenarios

### Scenario 1: Broadcast Join with Small Batches
Tests the specific scenario where coalesce before shuffle provides maximum benefit:
- Each mapper task reads 100 batches of 64KB each from data source
- Broadcast inner join with small table reduces each batch size by 90%
- Results in many small batches (6.4KB each) going into shuffle
- Demonstrates clear performance difference with/without coalescing

### Scenario 2: Performance Comparison
Compares execution times across different ratio values:
- Disabled (0.0)
- Low coalescing (0.3)
- Medium coalescing (0.6)
- High coalescing (0.9)

### Scenario 3: Large Dataset Test
Tests with larger datasets in the broadcast join scenario to observe more pronounced effects

## Expected Results

### When Coalescing Helps
- Many small batches going into shuffle
- High memory fragmentation
- Network-bound shuffle operations

### When Coalescing May Not Help
- Already well-sized batches
- CPU-bound operations
- Limited memory available

## Metrics to Observe

The feature adds a new metric:
- **Name**: `rapidsShuffleCoalesceBeforeShuffleTime`
- **Description**: Time spent coalescing batches before shuffle
- **Unit**: Nanoseconds

## Configuration Recommendations

Based on your workload characteristics:

1. **Start with ratio = 0.0 (disabled)** - This is the default and maintains existing performance
2. **For many small batches**: Try ratio = 0.6
3. **For memory-constrained environments**: Try ratio = 0.3
4. **Monitor metrics** to ensure the coalescing time doesn't exceed the benefits

## Troubleshooting

### Test Fails to Run
1. Ensure RAPIDS plugin is properly loaded
2. Check GPU availability: `nvidia-smi`
3. Verify Spark configuration includes GPU settings

### No Performance Difference
1. Dataset might be too small to show effects
2. Batches might already be well-sized
3. Other bottlenecks might dominate performance

### Performance Regression
1. Coalescing overhead exceeds benefits
2. Set ratio back to 0.0 to disable
3. Consider tuning batch size instead

## Example Output

```
=== Performance Analysis ===
Configuration        Ratio   Time (ms)       Relative Performance
----------------------------------------------------------------------
Disabled             0.0     1250.45         +0.00% (baseline)
Ratio 0.3           0.3     1180.23         +5.62% (IMPROVEMENT)
Ratio 0.6           0.6     1156.78         +7.49% (IMPROVEMENT)
Ratio 0.9           0.9     1201.34         +3.93% (IMPROVEMENT)

=== Configuration Recommendations ===
Best performing configuration: Ratio 0.6 (ratio: 0.6)
Recommendation: Enable coalesce before shuffle with ratio 0.6
Expected improvement: 7.49%
```

## Advanced Usage

### Custom Workload Testing
Modify the test data generation in the script to match your workload patterns:
```scala
// The script includes createBroadcastJoinScenario() which creates the optimal test case:
// - 64KB batches initially
// - Broadcast join reduces batch size by 90%
// - Creates many small batches perfect for testing coalesce before shuffle
```

### Integration with Existing Tests
Add the configuration to your existing Spark applications:
```scala
spark.conf.set("spark.rapids.shuffle.coalesceBeforeShuffleTargetSizeRatio", "0.6")
```

## Questions or Issues

If you encounter issues or have questions:
1. Check the test output for error messages
2. Verify GPU and RAPIDS plugin setup
3. Monitor Spark UI for detailed execution metrics
4. Consult RAPIDS documentation for plugin configuration

