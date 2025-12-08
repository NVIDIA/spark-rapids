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

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsConf.ShuffleKudoMode
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.nvidia.spark.rapids.shims.GpuHashPartitioning

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

  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
      .column(9, null.asInstanceOf[java.lang.Integer], 8, 7, 6, 5, 4, 3, 2, 1)
      .column("nine", "eight", null, null, "six", "five", "four", "three",
        "two", "one")
      .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType))
    }
  }

  test("GPU Kudo write partitioning and serialization - vanilla") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
      .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
      .set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")
      .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))

      val numPartitions = 4
      val partitioner = GpuHashPartitioning(
        Seq(GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "key")),
        numPartitions)

      val batch = buildBatch()
      val originalNumRows = batch.numRows()
      val dataTypes: Array[DataType] = Array(IntegerType, StringType)

      // Create serializer with GPU Kudo write mode
      val serializerMetrics = Map[String, GpuMetric]().withDefaultValue(NoopMetric)
      val serializer = new GpuColumnarBatchSerializer(serializerMetrics, dataTypes,
        ShuffleKudoMode.GPU, useKudo = true, kudoMeasureBufferCopy = false)

      // Partition the batch (simulating what happens in rddWithPartitionIds)
      // Note: columnarEvalAny closes the input batch
      val partitioned: Array[(ColumnarBatch, Int)] =
        partitioner.columnarEvalAny(batch)
          .asInstanceOf[Array[(ColumnarBatch, Int)]]

      try {
        // Verify we got partitioned batches
        assert(partitioned.length > 0, "Should have at least one partition")

        // Get row counts before closing batches
        val totalRowsInPartitions = partitioned.map(_._1.numRows()).sum

        // Serialize each partitioned batch using GPU Kudo serializer
        // When GPU Kudo write mode is enabled, batches contain
        // SlicedSerializedColumnVector (already serialized)
        val serializedData = new ArrayBuffer[Array[Byte]]()

        partitioned.foreach { case (partBatch, partitionId) =>
          val outputStream = new ByteArrayOutputStream()
          val serializerInstance = serializer.newInstance()
          val serializationStream =
            serializerInstance.serializeStream(outputStream)

          try {
            serializationStream.writeKey(partitionId)
            serializationStream.writeValue(partBatch)
            serializationStream.flush()
            serializedData.append(outputStream.toByteArray)
          } finally {
            serializationStream.close()
          }
        }

        // Verify serialization produced data
        assert(serializedData.nonEmpty, "Should have serialized data")
        serializedData.foreach { data =>
          assert(data.length > 0, "Serialized data should not be empty")
        }

        // Verify total rows are preserved
        assert(totalRowsInPartitions == originalNumRows,
          s"Total rows in partitions ($totalRowsInPartitions) should match " +
            s"original batch rows ($originalNumRows)")
      } finally {
        // Clean up partitioned batches
        partitioned.foreach(_._1.close())
      }
    }
  }
}
