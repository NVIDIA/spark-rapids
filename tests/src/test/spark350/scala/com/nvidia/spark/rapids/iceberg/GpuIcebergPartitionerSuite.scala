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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import com.nvidia.spark.rapids.{GpuColumnVector, RapidsConf}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.FuzzerUtils.createColumnarBatch
import com.nvidia.spark.rapids.iceberg.GpuIcebergPartitionerSuite.assertEqual
import com.nvidia.spark.rapids.spill.SpillFramework
import org.apache.iceberg.{PartitionKey, PartitionSpec, Schema, StructLike}
import org.apache.iceberg.spark.{GpuTypeToSparkType, SparkStructLike}
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.StructType
import org.scalatest.Assertions.assertResult
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuIcebergPartitionerSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var seed = 0L

  override def beforeAll: Unit = {
    SpillFramework.initialize(new RapidsConf(new SparkConf))
    seed = System.currentTimeMillis()
    println(s"Random seed set to $seed")
  }

  override def afterAll: Unit = {
    SpillFramework.shutdown()
  }

  test("bucket") {
    val icebergSchema = new Schema(
      Types.NestedField.required(1, "x", Types.LongType.get),
      Types.NestedField.required(2, "y", Types.LongType.get, "comment"),
      Types.NestedField.required(3, "z", Types.LongType.get),
      Types.NestedField.required(4, "d", Types.IntegerType.get),
    )

    val sparkType = GpuTypeToSparkType.toSparkType(icebergSchema)

    val icebergPartitionSpec = PartitionSpec.builderFor(icebergSchema)
        .bucket("x", 16)
        .bucket("d", 20)
        .build()

    val partitioner = new GpuIcebergPartitioner(icebergPartitionSpec, sparkType)

    val partitioned = withResource(createColumnarBatch(sparkType, 1000, seed = seed)) { batch =>
      partitioner.partition(batch)
    }

    val icebergPartitionKey = new PartitionKey(icebergPartitionSpec, icebergSchema)
    val icebergRow = new SparkStructLike(icebergSchema.asStruct())

    withResource(partitioned) { _ =>
      for ((part, partIdx) <- partitioned.zipWithIndex) {
        withResource(part.batch.getColumnarBatch()) { batch =>
          assert(batch.numRows() > 0, s"partition $partIdx is empty")
          val numCols = batch.numCols()
          val hostCols = closeOnExcept(new Array[ColumnVector](numCols)) { hostCols =>
            for (i <- 0 until numCols) {
              hostCols(i) = batch.column(i).asInstanceOf[GpuColumnVector].copyToHost()
            }
            hostCols
          }

          withResource(new ColumnarBatch(hostCols, batch.numRows())) { hostBatch =>
            for (rowIdx <- 0 until hostBatch.numRows()) {
              val internalRow = hostBatch.getRow(rowIdx)
              val row = Row.fromSeq(internalRow.toSeq(sparkType))
              icebergRow.wrap(row)

              icebergPartitionKey.partition(icebergRow)
              val expectedPartitionKey = icebergPartitionKey
              val actualPartitionKey = part.partition

              assertEqual(expectedPartitionKey, actualPartitionKey,
                icebergPartitionSpec.partitionType(),
                s"in partition $partIdx for #$rowIdx row $row")
            }
          }
        }
      }
    }
  }
}

object GpuIcebergPartitionerSuite {
  def assertEqual(expected: StructLike, actual: StructLike, structType: StructType, clue: => Any)
  : Unit = {
    assertResult(expected.size(), clue) {
      actual.size()
    }
    (0 until expected.size()).foreach { i =>
      val javaClass = structType.fields().get(i).`type`().typeId().javaClass()
      assertResult(expected.get(i, javaClass), s"$clue at #$i partition key") {
        actual.get(i, javaClass)
      }
    }
  }
}
