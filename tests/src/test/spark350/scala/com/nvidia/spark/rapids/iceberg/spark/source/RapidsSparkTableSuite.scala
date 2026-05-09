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

import java.util.{HashMap => JHashMap, Map => JMap}
import java.util.Collections

import com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog
import org.apache.iceberg.spark.SparkReadOptions
import org.apache.iceberg.spark.source.SparkTable
import org.mockito.Mockito.mock
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class RapidsSparkTableSuite extends AnyFunSuite {

  private val catalog = "spark_catalog"
  private val ns = Array("default")
  private val tbl = "store_sales"
  private val prefix = s"${RapidsSparkTable.CONF_PREFIX}$catalog.${ns.mkString(".")}.$tbl."

  private def emptyOptions: CaseInsensitiveStringMap =
    new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())

  test("mergeFromConfs returns the same map when no matching confs are set") {
    val options = emptyOptions
    val confs: JMap[String, String] = Collections.singletonMap("unrelated.conf", "v")
    val result = RapidsSparkTable.mergeFromConfs(options, catalog, ns, tbl, confs)
    assert(result eq options)
  }

  test("mergeFromConfs adds a single recognized session override") {
    val confs = new JHashMap[String, String]()
    confs.put(prefix + "read-split-target-size", "2147483648")
    val merged = RapidsSparkTable.mergeFromConfs(emptyOptions, catalog, ns, tbl, confs)
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == "2147483648")
  }

  test("mergeFromConfs merges all three recognized suffixes") {
    val confs = new JHashMap[String, String]()
    confs.put(prefix + "read-split-target-size", "2147483648")
    confs.put(prefix + "read-split-planning-lookback", "1000")
    confs.put(prefix + "read-split-open-file-cost", "8388608")
    val merged = RapidsSparkTable.mergeFromConfs(emptyOptions, catalog, ns, tbl, confs)
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == "2147483648")
    assert(merged.get(SparkReadOptions.LOOKBACK) == "1000")
    assert(merged.get(SparkReadOptions.FILE_OPEN_COST) == "8388608")
  }

  test("explicit DataFrame option wins over session override") {
    val explicit = new JHashMap[String, String]()
    explicit.put(SparkReadOptions.SPLIT_SIZE, "536870912")
    val options = new CaseInsensitiveStringMap(explicit)
    val confs = new JHashMap[String, String]()
    confs.put(prefix + "read-split-target-size", "2147483648")
    val merged = RapidsSparkTable.mergeFromConfs(options, catalog, ns, tbl, confs)
    assert(merged.get(SparkReadOptions.SPLIT_SIZE) == "536870912")
  }

  test("dot in catalog component is rejected when a conf would apply") {
    val confs = new JHashMap[String, String]()
    confs.put(s"${RapidsSparkTable.CONF_PREFIX}hadoop.prod.${ns.mkString(".")}.$tbl." +
      "read-split-target-size", "2147483648")
    val ex = intercept[IllegalArgumentException] {
      RapidsSparkTable.mergeFromConfs(emptyOptions, "hadoop.prod", ns, tbl, confs)
    }
    assert(ex.getMessage.contains("ambiguous"))
    assert(ex.getMessage.contains("hadoop.prod"))
  }

  test("dot in namespace component is rejected when a conf would apply") {
    val confs = new JHashMap[String, String]()
    confs.put(s"${RapidsSparkTable.CONF_PREFIX}$catalog.a.b.$tbl." +
      "read-split-target-size", "2147483648")
    val ex = intercept[IllegalArgumentException] {
      RapidsSparkTable.mergeFromConfs(emptyOptions, catalog, Array("a.b"), tbl, confs)
    }
    assert(ex.getMessage.contains("ambiguous"))
  }

  test("identifier with dots is a pure pass-through when no conf applies") {
    // Even if catalog/namespace/table contains '.', simply enabling the wrapper
    // must not break scans on tables the user has not explicitly tuned.
    val confs = new JHashMap[String, String]()
    confs.put("unrelated.conf", "v")
    val result = RapidsSparkTable.mergeFromConfs(
      emptyOptions, "hadoop.prod", Array("a.b"), tbl, confs)
    assert(result eq emptyOptions)
  }

  test("unrecognized suffix under our prefix is rejected") {
    val confs = new JHashMap[String, String]()
    confs.put(prefix + "read-split-bogus", "1")
    val ex = intercept[IllegalArgumentException] {
      RapidsSparkTable.mergeFromConfs(emptyOptions, catalog, ns, tbl, confs)
    }
    assert(ex.getMessage.contains("Unrecognized suffix"))
    assert(ex.getMessage.contains("read-split-bogus"))
  }

  test("confs with the right suffix but different table prefix are ignored") {
    val confs = new JHashMap[String, String]()
    val otherKey = s"${RapidsSparkTable.CONF_PREFIX}other_catalog.default" +
      ".other_tbl.read-split-target-size"
    confs.put(otherKey, "2147483648")
    val merged = RapidsSparkTable.mergeFromConfs(emptyOptions, catalog, ns, tbl, confs)
    assert(merged.isEmpty)
  }

  test("wrap returns a RapidsSparkTable for SparkTable inputs") {
    val sparkTable = mock(classOf[SparkTable])
    val wrapped = RapidsSparkSessionCatalog.wrap(catalog, sparkTable)
    assert(wrapped.isInstanceOf[RapidsSparkTable])
    assert(wrapped.asInstanceOf[RapidsSparkTable].delegate() eq sparkTable)
  }

  test("wrap passes through non-SparkTable inputs unchanged") {
    val other = mock(classOf[Table])
    val wrapped = RapidsSparkSessionCatalog.wrap(catalog, other)
    assert(wrapped eq other)
  }
}
