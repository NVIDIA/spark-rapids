/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.csv.{CSVOptions => SparkCSVOptions}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class CsvBomShimSuite extends SparkQueryCompareTestSuite {
  private val cases = Seq(
    ("BOM only", "\uFEFF", Map.empty[String, String]),
    ("BOM and literal NUL", "\uFEFF\u0000", Map.empty[String, String]),
    ("custom line separator", "\uFEFF", Map("lineSep" -> "|")),
    ("custom null value", "\uFEFF", Map("nullValue" -> "NULL")),
    ("custom null marker", "\uFEFFNULL,tail", Map("nullValue" -> "NULL")),
    ("quote disabled", "\uFEFF", Map("quote" -> "")),
    ("quoted delimiter", "\uFEFF\"quoted,value\",tail", Map("lineSep" -> "\r\n")))

  for ((name, record, options) <- cases; numColumns <- Seq(1, 2)) {
    test(s"BOM record matches the CPU CSV file reader: $name, columns=$numColumns") {
      val schema = StructType((0 until numColumns).map(i => StructField(s"c$i", StringType)))
      val readOptions = options ++ Map("header" -> "false", "comment" -> "#")
      val separator = options.getOrElse("lineSep", "\n")
      val file = Files.createTempFile("csv-bom-shim", ".csv")
      try {
        // Hadoop consumes the file BOM; the following BOM belongs to the first CSV record.
        Files.write(file, ("\uFEFF" + record + separator)
          .getBytes(UTF_8))
        withCpuSparkSession({ spark =>
          val frame = spark.read.schema(schema).options(readOptions).csv(file.toUri.toString)
          val rows = frame.collect()
          assert(rows.length == 1)
          val expected = rows.head.toSeq.map(_.asInstanceOf[String])
          val factory = new FilterCsvEmptyHostLineBuffererFactory(
            new SparkCSVOptions(readOptions, false, "UTC"), numColumns, false)
          val bytes = ("prefix" + record + "suffix").getBytes(UTF_8)
          val actual = factory.parseBomRecord(bytes, "prefix".length,
            record.getBytes(UTF_8).length).toSeq
          assert(actual == expected)
          if (record.contains('\u0000')) {
            assert(actual.head == "\u0000")
          }
        }, new SparkConf().set("spark.sql.sources.useV1SourceList", "csv")
          .set("spark.sql.files.maxPartitionBytes", "1m"))
      } finally {
        Files.deleteIfExists(file)
      }
    }
  }
}
