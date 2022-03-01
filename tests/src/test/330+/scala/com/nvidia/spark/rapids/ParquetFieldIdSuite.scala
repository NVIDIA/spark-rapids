/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StructType}

/**
 * TODO should update after cuDF supports field id
 * See https://github.com/NVIDIA/spark-rapids/issues/4846
 */
class ParquetFieldIdSuite extends SparkQueryCompareTestSuite {

  // this should failed
  test("try to write field id") {
    val tmpFile = File.createTempFile("field-id", ".parquet")
    try {
      def withId(id: Int) =
        new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id).build()
      // not support writing field id
      val schema = new StructType().add("c1", IntegerType, nullable = true, withId(1))
      val data = (1 to 4).map(i => Row(i))

      assertThrows[IllegalArgumentException] {
        withGpuSparkSession(
          spark => spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
              .write.mode("overwrite").parquet(tmpFile.getAbsolutePath)
        )
      }
    } finally {
      tmpFile.delete()
    }
  }

  // this should failed
  test("try to read field id") {
    val tmpFile = File.createTempFile("field-id", ".parquet")
    try {
      def withId(id: Int) =
        new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id).build()
      val schema = new StructType().add("c1", IntegerType, nullable = true, withId(1))
      val data = (1 to 4).map(i => Row(i))

      withCpuSparkSession(
        spark => spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
            .write.mode("overwrite").parquet(tmpFile.getAbsolutePath)
      )

      assertThrows[IllegalArgumentException] {
        withGpuSparkSession(
          spark => spark.read.parquet(tmpFile.getAbsolutePath).collect(),
          // not support read field id
          new SparkConf().set("spark.sql.parquet.fieldId.read.enabled", "true")
        )
      }
    } finally {
      tmpFile.delete()
    }
  }
}
