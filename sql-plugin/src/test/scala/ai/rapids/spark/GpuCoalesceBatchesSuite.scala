/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.rapids.spark

import java.io.File

import org.sparkproject.guava.io.Files

class GpuCoalesceBatchesSuite extends SparkQueryCompareTestSuite {

  test("require single batch") {

    val conf = makeBatchedBytes(1)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "1")
      .set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "50000")
      .set("spark.sql.shuffle.partitions", "1")

    withGpuSparkSession(spark => {

      val df = longsCsvDf(spark)

      // currently, GpuSortExec requires a single batch but this is likely to change in the
      // future, making this test invalid
      val df2 = df
        .sort(df.col("longs"))

      val coalesce = df2.queryExecution.executedPlan
        .find(_.isInstanceOf[GpuCoalesceBatches]).get
        .asInstanceOf[GpuCoalesceBatches]

      assert(coalesce.goal == RequireSingleBatch)
      assert(coalesce.goal.targetSizeBytes == Long.MaxValue)

      // assert the metrics start out at zero
      assert(coalesce.additionalMetrics("numInputBatches").value == 0)
      assert(coalesce.longMetric(GpuMetricNames.NUM_OUTPUT_BATCHES).value == 0)

      // execute the plan
      df2.collect()

      // assert the metrics are correct
      assert(coalesce.additionalMetrics("numInputBatches").value == 7)
      assert(coalesce.longMetric(GpuMetricNames.NUM_OUTPUT_BATCHES).value == 1)

    }, conf)
  }

  test("coalesce HostColumnarToGpu") {

    val conf = makeBatchedBytes(1)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "1")
      .set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "50000")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "FileSourceScanExec")
      .set("spark.rapids.sql.exec.FileSourceScanExec", "false") // force Parquet read onto CPU
      .set("spark.sql.shuffle.partitions", "1")

    val dir = Files.createTempDir()
    val path = new File(dir, s"HostColumnarToGpu-${System.currentTimeMillis()}.parquet").getAbsolutePath

    try {
      // convert csv test data to parquet
      withCpuSparkSession(spark => {
        longsCsvDf(spark).write.parquet(path)
      }, conf)

      withGpuSparkSession(spark => {
        val df = spark.read.parquet(path)
        val df2 = df
          .sort(df.col("longs"))

        // ensure that the plan does include the HostColumnarToGpu step
        val hostColumnarToGpu = df2.queryExecution.executedPlan
          .find(_.isInstanceOf[HostColumnarToGpu]).get
          .asInstanceOf[HostColumnarToGpu]

        assert(hostColumnarToGpu.goal == TargetSize(50000))

        val gpuCoalesceBatches = df2.queryExecution.executedPlan
          .find(_.isInstanceOf[GpuCoalesceBatches]).get
          .asInstanceOf[GpuCoalesceBatches]

        assert(gpuCoalesceBatches.goal == RequireSingleBatch)
        assert(gpuCoalesceBatches.goal.targetSizeBytes == Long.MaxValue)

        // execute the plan
        df2.collect()

      }, conf)
    } finally {
      dir.delete()
    }
  }

  test("not require single batch") {

    val conf = makeBatchedBytes(1)
      .set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")
      .set(RapidsConf.MAX_READER_BATCH_SIZE_BYTES.key, "1")
      .set("spark.sql.shuffle.partitions", "1")

    withGpuSparkSession(spark => {

      val df = longsCsvDf(spark)

      // A coalesce step is added after the filter to help with the case where much of the
      // data is filtered out
      val df2 = df
        .filter(df.col("six").gt(5))

      df2.explain()

      val coalesce = df2.queryExecution.executedPlan
        .find(_.isInstanceOf[GpuCoalesceBatches]).get
        .asInstanceOf[GpuCoalesceBatches]

      assert(coalesce.goal != RequireSingleBatch)
      assert(coalesce.goal.targetSizeBytes == 1)

      // assert the metrics start out at zero
      assert(coalesce.additionalMetrics("numInputBatches").value == 0)
      assert(coalesce.longMetric(GpuMetricNames.NUM_OUTPUT_BATCHES).value == 0)

      // execute the plan
      df2.collect()

      // assert the metrics are correct
      assert(coalesce.additionalMetrics("numInputBatches").value == 7)
      assert(coalesce.longMetric(GpuMetricNames.NUM_OUTPUT_BATCHES).value == 7)

    }, conf)
  }

}
