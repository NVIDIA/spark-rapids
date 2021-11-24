/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

class RowBasedHiveUDFSuite extends SparkQueryCompareTestSuite {

  val cpuEnabledConf: SparkConf = new SparkConf()
    .set(RapidsConf.ENABLE_CPU_BASED_UDF.key, "true")

  // It is impossible to cover all the case for Hive UDFs, so here just
  // verifies the basic functionality.
  testSparkResultsAreEqualWithHiveSupport("Row Based Hive UDF-string",
      mixedDfWithNulls,
      cpuEnabledConf) { df =>
    df.createOrReplaceTempView("mixed_table")
    val ss = df.sparkSession
    ss.sql("DROP TEMPORARY FUNCTION IF EXISTS empty_simple_udf")
    ss.sql("CREATE TEMPORARY FUNCTION empty_simple_udf AS" +
      " 'com.nvidia.spark.rapids.EmptyHiveSimpleUDF'")
    ss.sql("DROP TEMPORARY FUNCTION IF EXISTS empty_generic_udf")
    ss.sql("CREATE TEMPORARY FUNCTION empty_generic_udf AS" +
      " 'com.nvidia.spark.rapids.EmptyHiveGenericUDF'")
    ss.sql("SELECT empty_simple_udf(strings), empty_generic_udf(strings) from mixed_table")
  }

}
