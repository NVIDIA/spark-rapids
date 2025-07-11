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

/*** spark-rapids-shim-json-lines
{"spark": "356"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.v2.rapids

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, TableSpec}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, StagingTableCatalog, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.V2CreateTableAsSelectBaseExec
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * GPU version of AtomicReplaceTableAsSelectExec.
 *
 * Physical plan node for v2 replace table as select when the catalog supports staging
 * table replacement.
 *
 * A new table will be created using the schema of the query, and rows from the query are appended.
 * If the table exists, its contents and schema should be replaced with the schema and the contents
 * of the query. This implementation is atomic. The table replacement is staged, and the commit
 * operation at the end should perform the replacement of the table's metadata and contents. If the
 * write fails, the table is instructed to roll back staged changes and any previously written table
 * is left untouched.
 */
case class GpuAtomicReplaceTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    tableSpec: TableSpec,
    writeOptions: Map[String, String],
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit)
  extends V2CreateTableAsSelectBaseExec with GpuExec {

  val properties = CatalogV2Util.convertTableProperties(tableSpec)

  override def supportsColumnar: Boolean = false

  override protected def run(): Seq[InternalRow] = {
    val columns = getV2Columns(query.schema, catalog.useNullableQuerySchema)
    if (catalog.tableExists(ident)) {
      val table = catalog.loadTable(ident)
      invalidateCache(catalog, table, ident)
    }
    val staged = if (orCreate) {
      catalog.stageCreateOrReplace(
        ident, columns, partitioning.toArray, properties.asJava)
    } else if (catalog.tableExists(ident)) {
      try {
        catalog.stageReplace(
          ident, columns, partitioning.toArray, properties.asJava)
      } catch {
        case e: NoSuchTableException =>
          throw QueryCompilationErrors.cannotReplaceMissingTableError(ident, Some(e))
      }
    } else {
      throw QueryCompilationErrors.cannotReplaceMissingTableError(ident)
    }
    writeToTable(catalog, staged, writeOptions, ident, query, overwrite = true)
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] =
    throw new IllegalStateException("Columnar execution not supported")
}
