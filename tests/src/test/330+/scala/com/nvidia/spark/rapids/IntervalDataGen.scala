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

package com.nvidia.spark.rapids;

import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, StructField, StructType, TimestampType}

object IntervalDataGen {
  def genDf(spark: SparkSession): DataFrame = {
    val data = (1 to 1024).map(
      i => Row(i, Timestamp.valueOf("2022-01-01 01:01:01"), Duration.ofDays(i).plusSeconds(i)))
    val schema = StructType(Array(
      StructField("i", IntegerType, false),
      StructField("t", TimestampType, false),
      StructField("d", DayTimeIntervalType(), false)))
    spark.createDataFrame(spark.sparkContext.parallelize(data, numSlices = 1), schema)
  }
}
