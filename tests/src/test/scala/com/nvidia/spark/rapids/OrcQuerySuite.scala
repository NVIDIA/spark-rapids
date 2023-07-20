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

import java.io.File

import org.apache.hadoop.fs.FileUtil.fullyDelete

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.rapids.{MyDenseVector, MyDenseVectorUDT}
import org.apache.spark.sql.types._

class OrcQuerySuite extends SparkQueryCompareTestSuite {
  val tempFile = File.createTempFile("orc-test-udt", ".orc")

  try {
    testGpuWriteFallback(
      "Write User Defined Type(UDT) to ORC fall back",
      "DataWritingCommandExec",
      spark => {
        val data = Seq(Row(1, new MyDenseVector(Array(0.25, 2.25, 4.25))))
        val schema = new StructType(Array(
          StructField("c0", DataTypes.IntegerType),
          StructField("c1", new MyDenseVectorUDT)
        ))
        spark.createDataFrame(
          SparkContext.getOrCreate().parallelize(data, numSlices = 1),
          schema)
      },
      execsAllowedNonGpu = Seq("DataWritingCommandExec", "ShuffleExchangeExec")
    ) {
      frame => frame.write.mode("overwrite").orc("/tmp/a.orc")
    }
  } finally {
    fullyDelete(tempFile)
  }
}
