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

package com.nvidia.spark.rapids.shims.spark311cdh

import java.net.URI

import com.nvidia.spark.ParquetCachedBatchSerializer
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.v2._
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.rapids.shims.spark311cdh._
import org.apache.spark.sql.rapids.shims.v2.{GpuColumnarToRowTransitionExec, GpuInMemoryTableScanExec}
import org.apache.spark.sql.sources.BaseRelation

class Spark311CDHShims extends SparkBaseShims with Spark30Xuntil32XShims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    super.getExecs ++ Seq(
      GpuOverrides.exec[InMemoryTableScanExec](
        "Implementation of InMemoryTableScanExec to use GPU accelerated Caching",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_64 + TypeSig.STRUCT
            + TypeSig.ARRAY).nested().withPsNote(TypeEnum.DECIMAL,
              "Negative scales aren't supported at the moment even with " +
                  "spark.sql.legacy.allowNegativeScaleOfDecimal set to true. " +
                  "This is because Parquet doesn't support negative scale for decimal values"),
          TypeSig.all),
        (scan, conf, p, r) => new SparkPlanMeta[InMemoryTableScanExec](scan, conf, p, r) {
          override def tagPlanForGpu(): Unit = {
            if (!scan.relation.cacheBuilder.serializer.isInstanceOf[ParquetCachedBatchSerializer]) {
              willNotWorkOnGpu("ParquetCachedBatchSerializer is not being used")
            }
            if (SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
              willNotWorkOnGpu("Parquet doesn't support negative scales for Decimal values")
            }
          }

          /**
           * Convert InMemoryTableScanExec to a GPU enabled version.
           */
          override def convertToGpu(): GpuExec = {
            GpuInMemoryTableScanExec(scan.attributes, scan.predicates, scan.relation)
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r))
  }

  override def getGpuColumnarToRowTransition(plan: SparkPlan,
     exportColumnRdd: Boolean): GpuColumnarToRowExecParent = {
    val serName = plan.conf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
    val serClass = ShimLoader.loadClass(serName)
    if (serClass == classOf[ParquetCachedBatchSerializer]) {
      GpuColumnarToRowTransitionExec(plan)
    } else {
      GpuColumnarToRowExec(plan)
    }
  }

  override def createTable(
    table: CatalogTable,
    sessionCatalog: SessionCatalog,
    tableLocation: Option[URI],
    result: BaseRelation): Unit = {
    val newTable = table.copy(
      storage = table.storage.copy(locationUri = tableLocation),
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      schema = result.schema)
    // Table location is already validated. No need to check it again during table creation.
    sessionCatalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)
  }

  override def hasCastFloatTimestampUpcast: Boolean = false

  override def getParquetFilters(
      schema: MessageType,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean,
      datetimeRebaseMode: SQLConf.LegacyBehaviorPolicy.Value): ParquetFilters = {
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive)
  }
}
