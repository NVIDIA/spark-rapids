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

import org.apache.spark.metrics.source.MockTaskContext
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.sparkproject.guava.io.Files

class GpuCoalesceBatchesSuite extends SparkQueryCompareTestSuite {

  test("test with small input batches") {
    withGpuSparkSession(spark => {
      val testData = doubleCsvDf(spark).coalesce(1)
      val gpuRowToColumnarExec = GpuRowToColumnarExec(testData.queryExecution.sparkPlan,
        TargetSize(1))
      val gpuCoalesceBatches = GpuCoalesceBatches(gpuRowToColumnarExec, TargetSize(100000))
      val rdd = gpuCoalesceBatches.doExecuteColumnar()
      val part = rdd.partitions.head
      val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
      val batches = rdd.compute(part, context)

      // assert final results are correct
      assert(batches.hasNext)
      val batch = batches.next()
      assert(batch.numCols() == 2)
      assert(batch.numRows() == 14)
      assert(!batches.hasNext)
      batch.close()

      // assert metrics are correct
      assert(gpuRowToColumnarExec.metrics(GpuMetricNames.NUM_OUTPUT_ROWS).value == 14)
      assert(gpuRowToColumnarExec.metrics(GpuMetricNames.NUM_OUTPUT_BATCHES).value == 14)
      assert(gpuCoalesceBatches.metrics(GpuMetricNames.NUM_INPUT_ROWS).value == 14)
      assert(gpuCoalesceBatches.metrics(GpuMetricNames.NUM_INPUT_BATCHES).value == 14)
      assert(gpuCoalesceBatches.metrics(GpuMetricNames.NUM_OUTPUT_ROWS).value == 14)
      assert(gpuCoalesceBatches.metrics(GpuMetricNames.NUM_OUTPUT_BATCHES).value == 1)
    })
  }

  test("limit batches by string size") {

    val schema = new StructType(Array(
      StructField("a", DataTypes.DoubleType),
      StructField("b", DataTypes.StringType)
    ))

    // create input with 2 rows where the combined string length is > Integer.MAX_VALUE
    val input = new BatchIterator(schema, rowCount = 2)

    val numInputRows = createMetric()
    val numInputBatches = createMetric()
    val numOutputRows = createMetric()
    val numOutputBatches = createMetric()
    val collectTime = createMetric()
    val concatTime = createMetric()
    val totalTime = createMetric()
    val peakDevMemory = createMetric()

    val it = new GpuCoalesceIterator(input,
      schema,
      TargetSize(Long.MaxValue),
      numInputRows,
      numInputBatches,
      numOutputRows,
      numOutputBatches,
      collectTime,
      concatTime,
      totalTime,
      peakDevMemory,
      opName = "opname"
    ) {
      // override for this test so we can mock the response to make it look the strings are large
      override def getColumnSizes(cb: ColumnarBatch): Array[Long] = Array(64, Int.MaxValue)

      override def getColumnDataSize(cb: ColumnarBatch, index: Int, default: Long): Long =
        index match {
          case 0 => 64L
          case 1 => (Int.MaxValue / 4 * 3).toLong
        }
    }

    while (it.hasNext) {
      val batch = it.next()
      batch.close()
    }

    assert(numInputBatches.value == 2)
    assert(numOutputBatches.value == 2)
  }

  private def createMetric() = new SQLMetric("sum")

  class BatchIterator(schema: StructType, var rowCount: Int) extends Iterator[ColumnarBatch] {
    override def hasNext: Boolean = {
      val hasNext = rowCount > 0
      rowCount -= 1
      hasNext
    }
    override def next(): ColumnarBatch = FuzzerUtils.createColumnarBatch(schema, 3, 64)
  }

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
    val path = new File(dir,
      s"HostColumnarToGpu-${System.currentTimeMillis()}.parquet").getAbsolutePath

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
