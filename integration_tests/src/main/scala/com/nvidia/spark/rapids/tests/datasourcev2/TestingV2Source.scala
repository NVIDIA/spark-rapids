/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
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
package com.nvidia.spark.rapids.tests.datasourcev2.parquet

import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.reader._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession

trait TestingV2Source extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    TestingV2Source.schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties))
  }

  def getTable(options: CaseInsensitiveStringMap): Table
}

class ColumnarDataSourceV2 extends TestingV2Source {

  class MyScanBuilder extends SimpleScanBuilder {

    override def planInputPartitions(): Array[InputPartition] = {
      Array(RangeInputPartition(0, 50), RangeInputPartition(50, 90))
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      ColumnarReaderFactory
    }
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder()
    }
  }
}

object ColumnarReaderFactory extends PartitionReaderFactory {
  private final val BATCH_SIZE = 20

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val RangeInputPartition(start, end) = partition
    new PartitionReader[ColumnarBatch] {
      private lazy val i = new OnHeapColumnVector(BATCH_SIZE, IntegerType)
      private lazy val j = new OnHeapColumnVector(BATCH_SIZE, IntegerType)
      private lazy val batch = new ColumnarBatch(Array(i, j))

      private var current = start

      override def next(): Boolean = {
        i.reset()
        j.reset()

        var count = 0
        while (current < end && count < BATCH_SIZE) {
          i.putInt(count, current)
          j.putInt(count, -current)
          current += 1
          count += 1
        }

        if (count == 0) {
          false
        } else {
          batch.setNumRows(count)
          true
        }
      }

      override def get(): ColumnarBatch = batch

      override def close(): Unit = batch.close()
    }
  }
}

