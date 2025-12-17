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
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBase, TrampolineUtil}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch


object GpuKudoWritePartitioningSuite {
  // Test data constants (10 rows per batch)
  val testIntValues: Array[java.lang.Integer] = Array(9,
    null.asInstanceOf[java.lang.Integer], 8, 7, 6, 5, 4, 3, 2, 1)
  val testStringValues: Array[String] = Array("nine", "eight", null, null, "six",
    "five", "four", "three", "two", "one")
  val dataTypes: Array[DataType] = Array(IntegerType, StringType)

  /**
   * Builds a ColumnarBatch using the test data
   * This is a standalone function to avoid serialization issues
   */
  def buildBatchInPartition(): ColumnarBatch = {
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

  private val numPartitions = 1
  private val expectedTotalRows = 20 // 10 rows per batch × 2 batches

  /**
   * Creates a SparkConf configured for GPU Kudo write mode
   */
  private def createKudoSparkConf(): SparkConf = {
    new SparkConf()
      .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
      .set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")
      .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
  }

  /**
   * Creates a GPU Kudo serializer for the test data types
   */
  private def createSerializer(): GpuColumnarBatchSerializer = {
    val serializerMetrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)
    new GpuColumnarBatchSerializer(serializerMetrics, GpuKudoWritePartitioningSuite.dataTypes,
      ShuffleKudoMode.GPU, useKudo = true, kudoMeasureBufferCopy = false)
  }

  /**
   * Creates an input RDD that produces test batches
   */
  private def createInputRDD(spark: org.apache.spark.sql.SparkSession):
      org.apache.spark.rdd.RDD[ColumnarBatch] = {
    spark.sparkContext.parallelize(Seq(0, 1), numSlices = 1)
      .mapPartitions { _ =>
        // Return 2 batches (one for each element in Seq(0, 1))
        Iterator(GpuKudoWritePartitioningSuite.buildBatchInPartition(),
          GpuKudoWritePartitioningSuite.buildBatchInPartition())
      }
  }

  /**
   * Sets up the shuffle dependency for testing
   * Returns both the dependency and the metrics map for reading metric values
   */
  private def setupShuffleDependency(
      spark: org.apache.spark.sql.SparkSession,
      inputRDD: org.apache.spark.rdd.RDD[ColumnarBatch],
      serializer: GpuColumnarBatchSerializer):
      (org.apache.spark.ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
          Map[String, GpuMetric]) = {
    val gpuPartitioning = GpuHashPartitioning(
      Seq(GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "key")),
      numPartitions)

    val outputAttributes = Seq(
      AttributeReference("id", IntegerType, nullable = true)(ExprId(0)),
      AttributeReference("name", StringType, nullable = true)(ExprId(1)))

    val writeMetrics = Map[String, SQLMetric]()

    // Create real metrics that we can read back, including required metrics
    val partitionedArraysMetric = GpuMetric.wrap(
      SQLMetrics.createMetric(spark.sparkContext,
        GpuMetric.DESCRIPTION_NUM_OUTPUT_BATCHES))
    val metrics = Map[String, GpuMetric](
      GpuMetric.NUM_OUTPUT_BATCHES -> partitionedArraysMetric
    ).withDefaultValue(NoopMetric)

    val dependency = GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(
      inputRDD,
      outputAttributes,
      gpuPartitioning,
      GpuKudoWritePartitioningSuite.dataTypes,
      serializer,
      useGPUShuffle = false,
      useMultiThreadedShuffle = false,
      metrics,
      writeMetrics,
      Map.empty,
      None,
      Seq.empty,
      enableOpTimeTrackingRdd = false)
    (dependency, metrics)
  }

  /**
   * Collects batches from the shuffle dependency, optionally injecting OOM for retry testing
   */
  private def collectBatches(
      dependency: org.apache.spark.ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
      injectOOM: Boolean): (mutable.ArrayBuffer[(Int, ColumnarBatch)], Long) = {
    val allPartitionedBatches = mutable.ArrayBuffer[(Int, ColumnarBatch)]()
    var totalRowsSeen = 0L
    var firstIteration = true

    if (injectOOM) {
      // Associate the current thread with a task (required for OOM injection)
      RmmSpark.currentThreadIsDedicatedToTask(1)
    }

    try {
      dependency.rdd.partitions.foreach { partition =>
        val partitionIterator = dependency.rdd.iterator(partition,
          org.apache.spark.TaskContext.get())

        // Inject a split OOM right before the first call to next() if requested
        if (injectOOM && firstIteration && partitionIterator.hasNext) {
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
    } finally {
      if (injectOOM) {
        RmmSpark.removeAllCurrentThreadAssociation()
      }
    }

    (allPartitionedBatches, totalRowsSeen)
  }

  /**
   * Verifies that the collected batches match the expected test data.
   * Note: This method does NOT close the input batches - caller is responsible for cleanup.
   */
  private def verifyBatchContents(
      allPartitionedBatches: mutable.ArrayBuffer[(Int, ColumnarBatch)],
      totalRowsSeen: Long,
      serializer: GpuColumnarBatchSerializer,
      contextMessage: String): Unit = {
    // Verify total rows are preserved
    assert(totalRowsSeen == expectedTotalRows,
      s"Expected $expectedTotalRows total rows (10 per batch × 2 batches), got $totalRowsSeen")

    // Deserialize SlicedSerializedColumnVector batches
    // Note: We create a copy of the batch references to avoid closing the originals
    val slicedBatches = allPartitionedBatches.map(_._2).toSeq
    val deserializedBatches = GpuKudoWritePartitioningSuite.deserializeSlicedBatches(
      slicedBatches, GpuKudoWritePartitioningSuite.dataTypes, serializer)

    try {
      // Extract row data from all deserialized batches
      val allPartitionedRows = deserializedBatches.flatMap { batch =>
        GpuKudoWritePartitioningSuite.extractRowsFromBatch(batch)
      }

      val partitionedDataValues = allPartitionedRows.map { case (_, intVal, stringVal) =>
        (intVal, stringVal)
      }.toSet

      // Create expected data set (twice since we have 2 batches)
      val testIntValues = GpuKudoWritePartitioningSuite.testIntValues
      val testStringValues = GpuKudoWritePartitioningSuite.testStringValues
      val expectedDataValues = (testIntValues ++ testIntValues).zip(
        testStringValues ++ testStringValues).map { case (intVal, stringVal) =>
        val intOpt = if (intVal == null) None else Option(intVal.asInstanceOf[Integer])
        (intOpt, Option(stringVal))
      }.toSet

      // Verify deserialized result matches original
      val messagePrefix = if (contextMessage.nonEmpty) s"$contextMessage: " else ""
      assert(partitionedDataValues == expectedDataValues,
        s"${messagePrefix}Deserialized data doesn't match original. " +
        s"Partitioned: $partitionedDataValues, Expected: $expectedDataValues")
    } finally {
      deserializedBatches.foreach(_.close())
    }
  }


  test("GPU Kudo write partitioning and serialization") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = createKudoSparkConf()
    TestUtils.withGpuSparkSession(conf) { spark =>
      GpuShuffleEnv.init(new RapidsConf(conf))

      val serializer = createSerializer()
      val inputRDD = createInputRDD(spark)
      val (dependency, metrics) = setupShuffleDependency(spark, inputRDD, serializer)

      // Collect batches without OOM injection
      val (allPartitionedBatches, totalRowsSeen) = collectBatches(dependency, injectOOM = false)

      // Verify partitioned arrays count equals number of input batches (2)
      val batchesCount = metrics(GpuMetric.NUM_OUTPUT_BATCHES).value
      assert(batchesCount == 2,
        s"Expected 2 partitioned arrays (one per input batch), but got $batchesCount")

      // Verify batch contents match expected data
      verifyBatchContents(allPartitionedBatches, totalRowsSeen, serializer, "")

      // Clean up collected batches
      allPartitionedBatches.foreach(_._2.close())
    }
  }

  test("GPU Kudo write partitioning and serialization - with split retry") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = createKudoSparkConf()
    TestUtils.withGpuSparkSession(conf) { spark =>
      GpuShuffleEnv.init(new RapidsConf(conf))

      val serializer = createSerializer()
      val inputRDD = createInputRDD(spark)
      val (dependency, metrics) = setupShuffleDependency(spark, inputRDD, serializer)

      // Collect batches with OOM injection to trigger split retry
      val (allPartitionedBatches, totalRowsSeen) = collectBatches(dependency, injectOOM = true)

      // Verify that a retry occurred
      val retryCount = RmmSpark.getAndResetNumSplitRetryThrow(1)
      assert(retryCount > 0,
        s"Expected at least one split retry, but saw $retryCount retries")

      // Verify partitioned arrays count: one batch splits (1->2), plus one normal batch = 3
      val batchesCount = metrics(GpuMetric.NUM_OUTPUT_BATCHES).value
      assert(batchesCount == 3,
        s"Expected 3 partitioned arrays (split first batch produces 2, " +
        s"second batch produces 1), but got $batchesCount")

      // Verify batch contents match expected data (even after split retry)
      verifyBatchContents(allPartitionedBatches, totalRowsSeen, serializer,
        "After split retry")

      // Clean up collected batches
      allPartitionedBatches.foreach(_._2.close())
    }
  }
}
