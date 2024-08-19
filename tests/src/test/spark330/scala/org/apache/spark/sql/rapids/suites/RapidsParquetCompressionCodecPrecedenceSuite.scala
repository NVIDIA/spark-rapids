/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.parquet.ParquetCompressionCodecPrecedenceSuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait
import org.apache.spark.util.VersionUtils

class RapidsParquetCompressionCodecPrecedenceSuite
  extends ParquetCompressionCodecPrecedenceSuite
    with RapidsSQLTestsBaseTrait {

  private def getTableCompressionCodec(path: String): Seq[String] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val codecs = for {
      footer <- readAllFootersWithoutSummaryFiles(new Path(path), hadoopConf)
      block <- footer.getParquetMetadata.getBlocks.asScala
      column <- block.getColumns.asScala
    } yield column.getCodec.name()
    codecs.distinct
  }

  private def createTableWithCompression(
                                          tableName: String,
                                          isPartitioned: Boolean,
                                          compressionCodec: String,
                                          rootDir: File): Unit = {
    val options =
      s"""
         |OPTIONS('path'='${rootDir.toURI.toString.stripSuffix("/")}/$tableName',
         |'parquet.compression'='$compressionCodec')
       """.stripMargin
    val partitionCreate = if (isPartitioned) "PARTITIONED BY (p)" else ""
    sql(
      s"""
         |CREATE TABLE $tableName USING Parquet $options $partitionCreate
         |AS SELECT 1 AS col1, 2 AS p
       """.stripMargin)
  }

  private def checkCompressionCodec(compressionCodec: String, isPartitioned: Boolean): Unit = {
    withTempDir { tmpDir =>
      val tempTableName = "TempParquetTable"
      withTable(tempTableName) {
        createTableWithCompression(tempTableName, isPartitioned, compressionCodec, tmpDir)
        val partitionPath = if (isPartitioned) "p=2" else ""
        val path = s"${tmpDir.getPath.stripSuffix("/")}/$tempTableName/$partitionPath"
        val realCompressionCodecs = getTableCompressionCodec(path)
        assert(realCompressionCodecs.forall(_ == compressionCodec))
      }
    }
  }

  test("rapids Create parquet table with compression") {
    Seq(true, false).foreach { isPartitioned =>
      val codecs = Seq("UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD") ++ {
        if (VersionUtils.isHadoop3) Seq("LZ4") else Seq()
      }
      codecs.foreach { compressionCodec =>
        checkCompressionCodec(compressionCodec, isPartitioned)
      }
    }
  }
}
