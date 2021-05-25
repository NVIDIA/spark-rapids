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

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.spark311.{GpuBroadcastHashJoinMeta, GpuShuffledHashJoinMeta, GpuSortMergeJoinMeta, Spark311Shims}
import com.nvidia.spark.rapids.spark311cdh.RapidsShuffleManager

import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.shims.spark311._
import org.apache.spark.sql.sources.BaseRelation

class Spark311CDHShims extends Spark311Shims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    super.getExecs ++ Seq(
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.DECIMAL).nested(), TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)

          override def convertToGpu(): GpuExec = {
            val sparkSession = wrapped.relation.sparkSession
            val options = wrapped.relation.options

            val location = replaceWithAlluxioPathIfNeeded(
              conf,
              wrapped.relation,
              wrapped.partitionFilters,
              wrapped.dataFilters)

            val newRelation = HadoopFsRelation(
              location,
              wrapped.relation.partitionSchema,
              wrapped.relation.dataSchema,
              wrapped.relation.bucketSpec,
              GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
              options)(sparkSession)

            GpuFileSourceScanExec(
              newRelation,
              wrapped.output,
              wrapped.requiredSchema,
              wrapped.partitionFilters,
              wrapped.optionalBucketSet,
              wrapped.optionalNumCoalescedBuckets,
              wrapped.dataFilters,
              wrapped.tableIdentifier)(conf)
          }
        }),
      GpuOverrides.exec[InMemoryTableScanExec](
        "Implementation of InMemoryTableScanExec to use GPU accelerated Caching",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.STRUCT).nested()
          .withPsNote(TypeEnum.DECIMAL,
            "Negative scales aren't supported at the moment even with " +
              "spark.sql.legacy.allowNegativeScaleOfDecimal set to true. This is because Parquet " +
              "doesn't support negative scale for decimal values"),
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
        }),
      GpuOverrides.exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
          TypeSig.STRUCT).nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL
        ), TypeSig.all),
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[BroadcastHashJoinExec](
        "Implementation of join using broadcast data",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
          TypeSig.STRUCT).nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL
        ), TypeSig.all),
        (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
      GpuOverrides.exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
          TypeSig.STRUCT).nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL
        ), TypeSig.all),
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r))
  }

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }

  override def getGpuColumnarToRowTransition(plan: SparkPlan,
     exportColumnRdd: Boolean): GpuColumnarToRowExecParent = {
    val serName = plan.conf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
    val serClass = Class.forName(serName)
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
    result: BaseRelation) = {
    val newTable = table.copy(
      storage = table.storage.copy(locationUri = tableLocation),
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      schema = result.schema)
    // Table location is already validated. No need to check it again during table creation.
    sessionCatalog.createTable(newTable, ignoreIfExists = false, validateLocation = false)
  }

}
