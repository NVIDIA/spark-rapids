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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg.spark.source

import java.nio.file.Files
import java.util.Collections

import org.apache.commons.io.FileUtils
import org.apache.iceberg.spark.SparkReadOptions
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{spy, verify}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * End-to-end test for the rapids iceberg session-catalog drop-in. Configures
 * `spark.sql.catalog.spark_catalog` with `RapidsSparkSessionCatalog` against a
 * hadoop iceberg catalog rooted in a tmpdir warehouse, creates a real iceberg
 * table, runs a scan, and asserts that all three per-table session confs set
 * under `spark.rapids.iceberg.<catalog>.<namespace>.<table>.<suffix>` are
 * merged into the scan options that the wrapper forwards to iceberg.
 *
 * The merged options are observed via a spy on the loaded table's delegate so
 * the assertion stays on the public `Table.newScanBuilder` boundary rather
 * than reaching into RapidsSparkTable's internals.
 */
class RapidsSparkSessionCatalogE2ESuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouse = Files.createTempDirectory("rapids-iceberg-e2e").toFile
  private val tbl = "test_tbl"
  // Non-default values, picked so a "merged from session conf" assertion can't
  // be satisfied by iceberg defaults or by an accidental fallback.
  private val splitSize = "111111111"
  private val planningLookback = "444"
  private val openFileCost = "5555555"

  override def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    val confPrefix = s"spark.rapids.iceberg.spark_catalog.default.$tbl"
    val conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("RapidsSparkSessionCatalogE2E")
        .set("spark.sql.catalog.spark_catalog",
            "com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog")
        .set("spark.sql.catalog.spark_catalog.type", "hadoop")
        .set("spark.sql.catalog.spark_catalog.warehouse", warehouse.toURI.toString)
        .set("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set(s"$confPrefix.read-split-target-size", splitSize)
        .set(s"$confPrefix.read-split-planning-lookback", planningLookback)
        .set(s"$confPrefix.read-split-open-file-cost", openFileCost)
    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    TrampolineUtil.cleanupAnyExistingSession()
    if (warehouse.exists()) {
      FileUtils.deleteDirectory(warehouse)
    }
  }

  test("catalog wraps loaded tables and all session confs merge into scan options") {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
    spark.sql(s"CREATE TABLE default.$tbl (id long) USING iceberg")
    spark.sql(s"INSERT INTO default.$tbl VALUES (1), (2)")

    // Running an actual scan succeeds — the rapids catalog wrapper must not
    // break the read path for a table that has active session-conf overrides.
    assert(spark.table(s"default.$tbl").count() == 2)

    // The catalog returns a RapidsSparkTable wrapper for loaded iceberg tables,
    // proving the spark.sql.catalog.spark_catalog config is honored end-to-end.
    val catalog = spark.sessionState.catalogManager
        .catalog("spark_catalog").asInstanceOf[TableCatalog]
    val loaded = catalog.loadTable(Identifier.of(Array("default"), tbl))
    assert(loaded.isInstanceOf[RapidsSparkTable])

    // Observe the merged scan options via the public Table.newScanBuilder
    // boundary: wrap a spy of the loaded table's iceberg delegate in a fresh
    // RapidsSparkTable, call newScanBuilder with no DataFrame options, and
    // capture what the wrapper forwards to the delegate. All three per-table
    // overrides must appear in the merged options under the corresponding
    // SparkReadOptions keys.
    val delegateSpy = spy(loaded.asInstanceOf[RapidsSparkTable].delegate())
    val testWrapper = new RapidsSparkTable(delegateSpy, "spark_catalog")
    val empty = new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())
    testWrapper.newScanBuilder(empty)
    val captor = ArgumentCaptor.forClass(classOf[CaseInsensitiveStringMap])
    verify(delegateSpy).newScanBuilder(captor.capture())
    val merged = captor.getValue
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    assert(merged.get(SparkReadOptions.LOOKBACK) == planningLookback)
    assert(merged.get(SparkReadOptions.FILE_OPEN_COST) == openFileCost)
  }
}
