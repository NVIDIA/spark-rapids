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

import com.nvidia.spark.rapids.iceberg.spark.RapidsSparkSessionCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.connector.catalog.Table

class RapidsSparkTableSuite extends AnyFunSuite {

  private val catalog = "spark_catalog"

  private def mockSparkTable(qualifiedName: String): SparkTable = {
    // RapidsSparkTable's constructor eagerly reads delegate.table().name() to
    // derive namespace and tableName, so the mock has to wire that chain.
    val sparkTable = mock(classOf[SparkTable])
    val icebergTable = mock(classOf[org.apache.iceberg.Table])
    when(sparkTable.table()).thenReturn(icebergTable)
    when(icebergTable.name()).thenReturn(qualifiedName)
    sparkTable
  }

  test("wrap returns a RapidsSparkTable for SparkTable inputs") {
    val sparkTable = mockSparkTable("iceCat.default.store_sales")
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
