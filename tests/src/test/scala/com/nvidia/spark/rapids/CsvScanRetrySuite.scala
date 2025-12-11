/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import ai.rapids.cudf.CSVOptions
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVOptions => SparkCSVOptions}
import org.apache.spark.sql.types._

class CsvScanRetrySuite extends RmmSparkRetrySuiteBase {
  test("test simple retry") {
    val bufferer = HostLineBuffererFactory.createBufferer(100, Array('\n'.toByte))
    bufferer.add("1,2".getBytes, 0, 3)

    val cudfSchema = GpuColumnVector.from(StructType(Seq(StructField("a", IntegerType),
      StructField("b", IntegerType))))
    val opts = CSVOptions.builder().hasHeader(false)
    RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    val table = CSVPartitionReader.readToTable(bufferer, cudfSchema, NoopMetric,
      opts, "CSV", null)
    table.close()
    // We don't have any good way to verify that the retry was thrown, but we are going to trust
    // that it was.
  }

  test("cast table to desired types is retried on OOM") {
    val csvFile = Files.createTempFile("csv-cast-retry", ".csv")
    Files.write(csvFile, "1,2\n".getBytes(StandardCharsets.UTF_8))

    val reader = new CSVPartitionReader(
      new Configuration(),
      PartitionedFileUtilsShim.newPartitionedFile(
        InternalRow.empty, csvFile.toString, 0, Files.size(csvFile)),
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType))),
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType))),
      new SparkCSVOptions(Map.empty[String, String], false, "UTC", "_corrupt_record"),
      1024, 128 * 1024,
      Map[String, GpuMetric]().withDefaultValue(NoopMetric))

    try {
      // skipCount=1 to skip the decode retry and target the cast retry
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 1)
      assert(reader.next())
      reader.get().close()
    } finally {
      reader.close()
      Files.deleteIfExists(csvFile)
    }
  }

}
