/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable

import ai.rapids.cudf.{HostColumnVector, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf.ShuffleKudoMode
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.jni.kudo.DumpOption
import com.nvidia.spark.rapids.shims.GpuHashPartitioning
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBase, TrampolineUtil}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch


object GpuKudoWritePartitioningSuite {
  /**
   * Extract all rows from a ColumnarBatch as tuples of (row index, integer value, string value)
   * This is a standalone function to avoid serialization issues
   */
  def extractRowsFromBatch(batch: ColumnarBatch): Seq[(Int, Option[Int], Option[String])] = {
    val rows = mutable.ArrayBuffer[(Int, Option[Int], Option[String])]()

    // Regular GpuColumnVector - extract directly
    val gpuVecs = GpuColumnVector.extractBases(batch)

    withResource(gpuVecs.safeMap(_.copyToHost())) { hostVecs =>
      val intCol = hostVecs(0).asInstanceOf[HostColumnVector]
      val stringCol = hostVecs(1).asInstanceOf[HostColumnVector]

      for (i <- 0 until batch.numRows()) {
        val intVal = if (intCol.isNull(i)) None else Some(intCol.getInt(i))
        val stringVal = if (stringCol.isNull(i)) None else Some(stringCol.getJavaString(i))
        rows.append((i, intVal, stringVal))
      }
    }
    rows.toSeq
  }

  /**
   * Deserialize SlicedSerializedColumnVector batches by serializing them to a stream,
   * then deserializing back and using the coalesce iterator to fully deserialize them.
   * This follows the same pattern as GpuShuffleCoalesceSuite.
   */
  def deserializeSlicedBatches(
      slicedBatches: Seq[ColumnarBatch],
      dataTypes: Array[DataType],
      serializer: GpuColumnarBatchSerializer): Seq[ColumnarBatch] = {
    // Serialize the sliced batches to a stream
    val byteOutputStream = new ByteArrayOutputStream()
    withResource(serializer.newInstance().serializeStream(byteOutputStream)) {
        serializationStream =>
      slicedBatches.foreach { batch =>
        serializationStream.writeKey(0)
        serializationStream.writeValue(batch)
      }
    }

    // Deserialize from the stream to get KudoSerializedTableColumn batches
    val byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray)
    val deserializationStream = serializer.newInstance().deserializeStream(byteInputStream)
    val kudoBatchesIter = deserializationStream.asKeyValueIterator.map(_._2)
      .asInstanceOf[Iterator[ColumnarBatch]]

    // Set up minimal metrics for the coalescing operation (required by API)
    val metricsMap = Map[String, GpuMetric]().withDefaultValue(NoopMetric)

    // Create coalesce read option for kudo GPU mode
    val readOption = com.nvidia.spark.rapids.CoalesceReadOption(
      kudoEnabled = true,
      kudoMode = ShuffleKudoMode.GPU,
      kudoDebugMode = DumpOption.Never,
      kudoDebugDumpPrefix = None,
      useAsync = false
    )

    // Use the coalesce iterator to fully deserialize the batches
    val coalesceIter =
      com.nvidia.spark.rapids.GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
        kudoBatchesIter, Long.MaxValue, dataTypes, readOption, metricsMap)

    // Collect all deserialized batches
    coalesceIter.toSeq
  }
}

class GpuKudoWritePartitioningSuite extends AnyFunSuite with BeforeAndAfterEach {
  var rapidsConf = new RapidsConf(Map[String, String]())

  override def beforeEach(): Unit = {
    super.beforeEach()
    TrampolineUtil.cleanupAnyExistingSession()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    TrampolineUtil.cleanupAnyExistingSession()
  }


  test("GPU Kudo write partitioning and serialization - vanilla") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
      .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
      .set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")
      .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
    TestUtils.withGpuSparkSession(conf) { spark =>
      GpuShuffleEnv.init(new RapidsConf(conf))

      val numPartitions = 4
      val gpuPartitioning = GpuHashPartitioning(
        Seq(GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "key")),
        numPartitions)

      val dataTypes: Array[DataType] = Array(IntegerType, StringType)

      // Define the test data once (10 rows per batch) - used for both building batches
      // and expected values
      val testIntValues: Array[java.lang.Integer] = Array(9,
        null.asInstanceOf[java.lang.Integer], 8, 7, 6, 5, 4, 3, 2, 1)
      val testStringValues: Array[String] = Array("nine", "eight", null, null, "six",
        "five", "four", "three", "two", "one")

      // Create serializer with GPU Kudo write mode
      val serializerMetrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)
      val serializer = new GpuColumnarBatchSerializer(serializerMetrics, dataTypes,
        ShuffleKudoMode.GPU, useKudo = true, kudoMeasureBufferCopy = false)

      // Create an RDD that produces batches (simulating child.executeColumnar())
      // We can't parallelize ColumnarBatch directly as it's not serializable,
      // so we create an RDD that builds batches in each partition
      def buildBatchInPartition(): ColumnarBatch = {
        // Use the same testIntValues and testStringValues defined above
        withResource(new Table.TestBuilder()
          .column(testIntValues(0), testIntValues(1), testIntValues(2),
            testIntValues(3), testIntValues(4), testIntValues(5),
            testIntValues(6), testIntValues(7), testIntValues(8),
            testIntValues(9))
          .column(testStringValues(0), testStringValues(1),
            testStringValues(2), testStringValues(3), testStringValues(4),
            testStringValues(5), testStringValues(6), testStringValues(7),
            testStringValues(8), testStringValues(9))
          .build()) { table =>
          GpuColumnVector.from(table, dataTypes)
        }
      }

      val inputRDD = spark.sparkContext.parallelize(Seq(0, 1), numSlices = 1)
        .mapPartitions { partitionIndex =>
          // Return 2 batches (one for each element in Seq(0, 1))
          Iterator(buildBatchInPartition(), buildBatchInPartition())
        }

      // Create output attributes for the partitioning
      val outputAttributes = Seq(
        AttributeReference("id", IntegerType, nullable = true)(ExprId(0)),
        AttributeReference("name", StringType, nullable = true)(ExprId(1)))

      // Create write metrics (empty map is fine for this test)
      val writeMetrics = Map[String, SQLMetric]()

      // Call prepareBatchShuffleDependency which exercises rddWithPartitionIds
      // This internally creates rddWithPartitionIds which includes the withRetry logic
      val dependency = GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(
        inputRDD,
        outputAttributes,
        gpuPartitioning,
        dataTypes,
        serializer,
        useGPUShuffle = false,
        useMultiThreadedShuffle = false,
        serializerMetrics,
        writeMetrics,
        Map.empty,
        None,
        Seq.empty,
        enableOpTimeTrackingRdd = false)

      // Exercise rddWithPartitionIds by iterating through it locally on the driver
      // The dependency.rdd is finalRddWithPartitionIds which wraps rddWithPartitionIds
      // We iterate locally (not in distributed context) to collect batches for verification
      val allPartitionedBatches = mutable.ArrayBuffer[(Int, ColumnarBatch)]()
      var totalRowsSeen = 0L
      val partitionIdsSeen = mutable.Set[Int]()

      // Iterate through all partitions locally to collect batches
      dependency.rdd.partitions.foreach { partition =>
        val partitionIterator = dependency.rdd.iterator(partition,
          org.apache.spark.TaskContext.get())

        while (partitionIterator.hasNext) {
          val result = partitionIterator.next()
          val partitionId = result._1
          val batch = result._2

          // Increment ref counts before collecting (batches are managed by rddWithPartitionIds
          // and will be closed automatically, so we need to take ownership)
          if (batch.numCols() > 0 &&
              batch.column(0).isInstanceOf[SlicedSerializedColumnVector]) {
            SlicedSerializedColumnVector.incRefCount(batch)
          } else {
            GpuColumnVector.incRefCounts(batch)
          }
          allPartitionedBatches.append((partitionId, batch))
          totalRowsSeen += batch.numRows()
          partitionIdsSeen.add(partitionId)
        }
      }

      // Note: The batches in allPartitionedBatches now have incremented ref counts,
      // so we own them and must close them. The original batches in rddWithPartitionIds
      // will also be closed, but our ref counts prevent double-close issues.

      // Verify total rows are preserved (10 rows per batch, 2 batches = 20 total)
      assert(totalRowsSeen == 20,
        s"Expected 20 total rows (10 per batch × 2 batches), got $totalRowsSeen")

      // Deserialize SlicedSerializedColumnVector batches using the same approach as
      // GpuShuffleCoalesceSuite: serialize to stream, deserialize, then use coalesce iterator
      val slicedBatches = allPartitionedBatches.map(_._2)
      val deserializedBatches = try {
        GpuKudoWritePartitioningSuite.deserializeSlicedBatches(
          slicedBatches, dataTypes, serializer)
      } finally {
        // Clean up the sliced batches after deserialization
        slicedBatches.foreach(_.close())
      }

      try {
        // Extract row data from all deserialized batches
        val allPartitionedRows = deserializedBatches.flatMap { batch =>
          GpuKudoWritePartitioningSuite.extractRowsFromBatch(batch)
        }

        val partitionedDataValues = allPartitionedRows.map { case (_, intVal, stringVal) =>
          (intVal, stringVal)
        }.toSet

        // Create expected data set (twice since we have 2 batches)
        // Handle null properly: null.asInstanceOf[Int] should become None, not Some(0)
        val expectedDataValues = (testIntValues ++ testIntValues).zip(
          testStringValues ++ testStringValues).map { case (intVal, stringVal) =>
          val intOpt = if (intVal == null) None else Option(intVal.asInstanceOf[Integer])
          (intOpt, Option(stringVal))
        }.toSet

        // Verify deserialized result matches original
        assert(partitionedDataValues == expectedDataValues,
          s"Deserialized data doesn't match original. " +
          s"Partitioned: $partitionedDataValues, Expected: $expectedDataValues")
      } finally {
        // Clean up deserialized batches
        deserializedBatches.foreach(_.close())
      }
    }
  }

  test("GPU Kudo write partitioning and serialization - with split retry") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
      .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
      .set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")
      .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
    TestUtils.withGpuSparkSession(conf) { spark =>
      GpuShuffleEnv.init(new RapidsConf(conf))

      val numPartitions = 4
      val gpuPartitioning = GpuHashPartitioning(
        Seq(GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "key")),
        numPartitions)

      val dataTypes: Array[DataType] = Array(IntegerType, StringType)

      // Define the test data once (10 rows per batch) - used for both building batches
      // and expected values
      val testIntValues: Array[java.lang.Integer] = Array(9,
        null.asInstanceOf[java.lang.Integer], 8, 7, 6, 5, 4, 3, 2, 1)
      val testStringValues: Array[String] = Array("nine", "eight", null, null, "six",
        "five", "four", "three", "two", "one")

      // Create serializer with GPU Kudo write mode
      val serializerMetrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)
      val serializer = new GpuColumnarBatchSerializer(serializerMetrics, dataTypes,
        ShuffleKudoMode.GPU, useKudo = true, kudoMeasureBufferCopy = false)

      // Create an RDD that produces batches (simulating child.executeColumnar())
      // We can't parallelize ColumnarBatch directly as it's not serializable,
      // so we create an RDD that builds batches in each partition
      def buildBatchInPartition(): ColumnarBatch = {
        // Use the same testIntValues and testStringValues defined above
        withResource(new Table.TestBuilder()
          .column(testIntValues(0), testIntValues(1), testIntValues(2),
            testIntValues(3), testIntValues(4), testIntValues(5),
            testIntValues(6), testIntValues(7), testIntValues(8),
            testIntValues(9))
          .column(testStringValues(0), testStringValues(1),
            testStringValues(2), testStringValues(3), testStringValues(4),
            testStringValues(5), testStringValues(6), testStringValues(7),
            testStringValues(8), testStringValues(9))
          .build()) { table =>
          GpuColumnVector.from(table, dataTypes)
        }
      }

      val inputRDD = spark.sparkContext.parallelize(Seq(0, 1), numSlices = 1)
        .mapPartitions { partitionIndex =>
          // Return 2 batches (one for each element in Seq(0, 1))
          Iterator(buildBatchInPartition(), buildBatchInPartition())
        }

      // Create output attributes for the partitioning
      val outputAttributes = Seq(
        AttributeReference("id", IntegerType, nullable = true)(ExprId(0)),
        AttributeReference("name", StringType, nullable = true)(ExprId(1)))

      // Create write metrics (empty map is fine for this test)
      val writeMetrics = Map[String, SQLMetric]()

      // Call prepareBatchShuffleDependency which exercises rddWithPartitionIds
      // This internally creates rddWithPartitionIds which includes the withRetry logic
      val dependency = GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(
        inputRDD,
        outputAttributes,
        gpuPartitioning,
        dataTypes,
        serializer,
        useGPUShuffle = false,
        useMultiThreadedShuffle = false,
        serializerMetrics,
        writeMetrics,
        Map.empty,
        None,
        Seq.empty,
        enableOpTimeTrackingRdd = false)

      // Associate the current thread with a task (required for OOM injection)
      // This follows the pattern from RmmSparkRetrySuiteBase
      RmmSpark.currentThreadIsDedicatedToTask(1)

      // Exercise rddWithPartitionIds by iterating through it locally on the driver
      // The dependency.rdd is finalRddWithPartitionIds which wraps rddWithPartitionIds
      // We iterate locally (not in distributed context) to collect batches for verification
      val allPartitionedBatches = mutable.ArrayBuffer[(Int, ColumnarBatch)]()
      var totalRowsSeen = 0L
      var firstIteration = true

      // Iterate through all partitions locally to collect batches
      dependency.rdd.partitions.foreach { partition =>
        val partitionIterator = dependency.rdd.iterator(partition,
          org.apache.spark.TaskContext.get())

        // Inject a split OOM right before the first call to next() - this follows the pattern
        // from BatchWithPartitionDataSuite where OOM is injected after creating the iterator
        // but before calling next()
        if (firstIteration && partitionIterator.hasNext) {
          val threadId = RmmSpark.getCurrentThreadId
          RmmSpark.forceSplitAndRetryOOM(threadId, 1,
            RmmSpark.OomInjectionType.GPU.ordinal, 0)
          firstIteration = false
        }

        while (partitionIterator.hasNext) {
          val result = partitionIterator.next()
          val partitionId = result._1
          val batch = result._2

          // Increment ref counts before collecting (batches are managed by rddWithPartitionIds
          // and will be closed automatically, so we need to take ownership)
          if (batch.numCols() > 0 &&
              batch.column(0).isInstanceOf[SlicedSerializedColumnVector]) {
            SlicedSerializedColumnVector.incRefCount(batch)
          } else {
            GpuColumnVector.incRefCounts(batch)
          }
          allPartitionedBatches.append((partitionId, batch))
          totalRowsSeen += batch.numRows()
        }
      }

      // Clean up thread association
      RmmSpark.removeAllCurrentThreadAssociation()

      // Verify that a retry occurred
      val retryCount = RmmSpark.getAndResetNumSplitRetryThrow(1)
      assert(retryCount > 0,
        s"Expected at least one split retry, but saw $retryCount retries")

      // Note: The batches in allPartitionedBatches now have incremented ref counts,
      // so we own them and must close them. The original batches in rddWithPartitionIds
      // will also be closed, but our ref counts prevent double-close issues.

      // Verify total rows are preserved (10 rows per batch, 2 batches = 20 total)
      // Even after split retry, all rows should be preserved
      assert(totalRowsSeen == 20,
        s"Expected 20 total rows (10 per batch × 2 batches), got $totalRowsSeen")

      // Deserialize SlicedSerializedColumnVector batches using the same approach as
      // GpuShuffleCoalesceSuite: serialize to stream, deserialize, then use coalesce iterator
      val slicedBatches = allPartitionedBatches.map(_._2)
      val deserializedBatches = try {
        GpuKudoWritePartitioningSuite.deserializeSlicedBatches(
          slicedBatches, dataTypes, serializer)
      } finally {
        // Clean up the sliced batches after deserialization
        slicedBatches.foreach(_.close())
      }

      try {
        // Extract row data from all deserialized batches
        val allPartitionedRows = deserializedBatches.flatMap { batch =>
          GpuKudoWritePartitioningSuite.extractRowsFromBatch(batch)
        }

        val partitionedDataValues = allPartitionedRows.map { case (_, intVal, stringVal) =>
          (intVal, stringVal)
        }.toSet

        // Create expected data set (twice since we have 2 batches)
        // Handle null properly: null.asInstanceOf[Int] should become None, not Some(0)
        val expectedDataValues = (testIntValues ++ testIntValues).zip(
          testStringValues ++ testStringValues).map { case (intVal, stringVal) =>
          val intOpt = if (intVal == null) None else Option(intVal.asInstanceOf[Integer])
          (intOpt, Option(stringVal))
        }.toSet

        // Verify deserialized result matches original (even after split retry)
        assert(partitionedDataValues == expectedDataValues,
          s"Deserialized data doesn't match original after split retry. " +
          s"Partitioned: $partitionedDataValues, Expected: $expectedDataValues")
      } finally {
        // Clean up deserialized batches
        deserializedBatches.foreach(_.close())
      }
    }
  }
}
