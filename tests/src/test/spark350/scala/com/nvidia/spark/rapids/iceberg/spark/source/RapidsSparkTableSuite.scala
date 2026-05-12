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
import java.util.{Collections, HashMap => JHashMap}

import com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog
import org.apache.commons.io.FileUtils
import org.apache.iceberg.spark.SparkReadOptions
import org.apache.iceberg.spark.source.SparkTable
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, spy, verify, when}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * End-to-end suite for the rapids iceberg session-catalog drop-in. Configures
 * `spark.sql.catalog.spark_catalog` with `RapidsSparkSessionCatalog` against a
 * hadoop iceberg catalog rooted in a tmpdir warehouse and exercises the
 * per-table session conf merging path through `Table.newScanBuilder`.
 *
 * Merged scan options are observed via a spy on the loaded table's iceberg
 * delegate so assertions stay on the public `Table.newScanBuilder` boundary
 * rather than reaching into RapidsSparkTable's internals.
 */
class RapidsSparkTableSuite
    extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val warehouse = Files.createTempDirectory("rapids-iceberg-e2e").toFile
  private val ns = Array("default")
  private val tbl = "test_tbl"
  private val confPrefix = s"spark.rapids.iceberg.spark_catalog.default.$tbl"

  // Non-default values, picked so a "merged from session conf" assertion can't
  // be satisfied by iceberg defaults or by an accidental fallback.
  private val splitSize = "111111111"
  private val planningLookback = "444"
  private val openFileCost = "5555555"

  override def beforeAll(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("RapidsSparkTableSuite")
        .set("spark.sql.catalog.spark_catalog",
            "com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog")
        .set("spark.sql.catalog.spark_catalog.type", "hadoop")
        .set("spark.sql.catalog.spark_catalog.warehouse", warehouse.toURI.toString)
        .set("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
    spark.sql(s"CREATE TABLE default.$tbl (id long) USING iceberg")
    spark.sql(s"INSERT INTO default.$tbl VALUES (1), (2)")
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

  override def afterEach(): Unit = {
    // Sweep any per-test spark.rapids.iceberg.* runtime confs. The baseline
    // catalog wiring lives in SparkConf (set in beforeAll) so nothing else
    // needs to be preserved.
    spark.conf.getAll.keys.toList
        .filter(_.startsWith("spark.rapids.iceberg."))
        .foreach(spark.conf.unset)
  }

  private def emptyOptions: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())

  private def loadedRapidsTable: RapidsSparkTable = {
    val catalog = spark.sessionState.catalogManager
        .catalog("spark_catalog").asInstanceOf[TableCatalog]
    catalog.loadTable(Identifier.of(ns, tbl)).asInstanceOf[RapidsSparkTable]
  }

  // Wrap a spy of the loaded iceberg delegate in a fresh RapidsSparkTable so
  // the caller can capture the options the wrapper forwards to the delegate.
  private def captureMergedOptions(
      catalogName: String = "spark_catalog",
      explicitOptions: CaseInsensitiveStringMap = emptyOptions): CaseInsensitiveStringMap = {
    val delegateSpy = spy(loadedRapidsTable.delegate())
    val wrapper = new RapidsSparkTable(delegateSpy, catalogName)
    wrapper.newScanBuilder(explicitOptions)
    val captor = ArgumentCaptor.forClass(classOf[CaseInsensitiveStringMap])
    verify(delegateSpy).newScanBuilder(captor.capture())
    captor.getValue
  }

  test("scan succeeds and catalog wraps loaded tables with RapidsSparkTable") {
    // Sanity: an actual scan returns rows; the wrapper does not break reads.
    assert(spark.table(s"default.$tbl").count() == 2)
    assert(loadedRapidsTable.isInstanceOf[RapidsSparkTable])
  }

  test("session conf is a pure pass-through when no override is set") {
    val merged = captureMergedOptions()
    // No spark.rapids.iceberg.* conf is set for this table, so the wrapper
    // must forward the original (empty) options unchanged.
    assert(merged.isEmpty)
  }

  test("single recognized suffix is merged into the scan options") {
    spark.conf.set(s"$confPrefix.read-split-target-size", splitSize)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    assert(!merged.containsKey(SparkReadOptions.LOOKBACK))
    assert(!merged.containsKey(SparkReadOptions.FILE_OPEN_COST))
  }

  test("all three recognized suffixes are merged into the scan options") {
    spark.conf.set(s"$confPrefix.read-split-target-size", splitSize)
    spark.conf.set(s"$confPrefix.read-split-planning-lookback", planningLookback)
    spark.conf.set(s"$confPrefix.read-split-open-file-cost", openFileCost)
    val merged = captureMergedOptions()
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == splitSize)
    assert(merged.get(SparkReadOptions.LOOKBACK) == planningLookback)
    assert(merged.get(SparkReadOptions.FILE_OPEN_COST) == openFileCost)
  }

  test("explicit DataFrame option wins over session override") {
    spark.conf.set(s"$confPrefix.read-split-target-size", splitSize)
    val explicit = new JHashMap[String, String]()
    explicit.put(SparkReadOptions.SPLIT_SIZE, "536870912")
    val merged = captureMergedOptions(
      explicitOptions = new CaseInsensitiveStringMap(explicit))
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == "536870912")
  }

  test("confs for a different table are ignored") {
    spark.conf.set(
      "spark.rapids.iceberg.spark_catalog.default.other_tbl.read-split-target-size",
      splitSize)
    val merged = captureMergedOptions()
    assert(merged.isEmpty)
  }

  test("unrecognized suffix under our prefix is rejected") {
    spark.conf.set(s"$confPrefix.read-split-bogus", "1")
    val ex = intercept[IllegalArgumentException] {
      captureMergedOptions()
    }
    assert(ex.getMessage.contains("Unrecognized suffix"))
    assert(ex.getMessage.contains("read-split-bogus"))
  }

  test("dot in catalog component is rejected when a conf would apply") {
    spark.conf.set(
      s"spark.rapids.iceberg.hadoop.prod.default.$tbl.read-split-target-size",
      splitSize)
    val ex = intercept[IllegalArgumentException] {
      captureMergedOptions(catalogName = "hadoop.prod")
    }
    assert(ex.getMessage.contains("ambiguous"))
    assert(ex.getMessage.contains("hadoop.prod"))
  }

  test("dot in catalog component is a pure pass-through when no conf applies") {
    // Even with a catalog name containing '.', simply having the wrapper in
    // place must not break scans for tables the user has not explicitly tuned.
    val merged = captureMergedOptions(catalogName = "hadoop.prod")
    assert(merged.isEmpty)
  }

  test("wrap returns a RapidsSparkTable for SparkTable inputs") {
    // RapidsSparkTable's constructor eagerly reads delegate.table().name() to
    // derive namespace and tableName, so the mock has to wire that chain.
    val sparkTable = mock(classOf[SparkTable])
    val icebergTable = mock(classOf[org.apache.iceberg.Table])
    when(sparkTable.table()).thenReturn(icebergTable)
    when(icebergTable.name()).thenReturn("iceCat.default.store_sales")
    val wrapped = RapidsSparkSessionCatalog.wrap("spark_catalog", sparkTable)
    assert(wrapped.isInstanceOf[RapidsSparkTable])
    assert(wrapped.asInstanceOf[RapidsSparkTable].delegate() eq sparkTable)
  }

  test("wrap passes through non-SparkTable inputs unchanged") {
    val other = mock(classOf[Table])
    val wrapped = RapidsSparkSessionCatalog.wrap("spark_catalog", other)
    assert(wrapped eq other)
  }
}
