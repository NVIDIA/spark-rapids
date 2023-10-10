/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Unit tests for utility methods in [[ BatchWithPartitionDataUtils ]]
 */
class BatchWithPartitionDataSuite extends SparkQueryCompareTestSuite {

  private def generateRowData(): (Array[InternalRow], Array[Int], StructType) = {
    val schema = StructType(Seq(
      StructField("v0", IntegerType),
      StructField("k0", StringType),
      StructField("k1", IntegerType)
    ))
    val rowNums = Array(50, 70, 25, 100, 50)
    val rowValues = GpuBatchUtilsSuite.createRows(schema, rowNums.length)
    (rowValues, rowNums, schema)
  }

  test("test splitting partition data into groups") {
    val conf = new SparkConf(false)
      .set(RapidsConf.CUDF_COLUMN_SIZE_LIMIT.key, "1000")
    withCpuSparkSession(_ => {
      val (rowValues, rowNums, schema) = generateRowData()
      val partitionRowData = PartitionRowData.from(rowValues, rowNums)
      val resultPartitions = BatchWithPartitionDataUtils.splitPartitionDataIntoGroups(
        partitionRowData, schema)
      val resultRowCounts = resultPartitions.flatMap(_.map(_.rowNum)).sum
      val expectedRowCounts = partitionRowData.map(_.rowNum).sum
      assert(resultRowCounts == expectedRowCounts)
    }, conf)
  }

  test("test splitting partition data into halves") {
    val conf = new SparkConf(false)
      .set(RapidsConf.CUDF_COLUMN_SIZE_LIMIT.key, "1000")
    withCpuSparkSession(_ => {
      val (rowValues, rowNums, _) = generateRowData()
      val partitionRowData = PartitionRowData.from(rowValues, rowNums)
      val resultPartitions = BatchWithPartitionDataUtils.splitPartitionDataInHalf(partitionRowData)
      val resultRowCounts = resultPartitions.flatMap(_.map(_.rowNum)).sum
      val expectedRowCounts = partitionRowData.map(_.rowNum).sum
      assert(resultRowCounts == expectedRowCounts)
    }, conf)
  }
}
