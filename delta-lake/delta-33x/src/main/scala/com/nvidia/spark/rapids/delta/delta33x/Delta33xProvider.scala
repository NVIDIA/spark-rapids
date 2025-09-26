/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.delta33x

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.delta.{DeltaIOProvider, GpuDeltaDataSource, RapidsDeltaUtils}
import com.nvidia.spark.rapids.shims._
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.delta.{DeltaLog, DeltaParquetFileFormat}
import org.apache.spark.sql.delta.DeltaParquetFileFormat.{IS_ROW_DELETED_COLUMN_NAME, ROW_INDEX_COLUMN_NAME}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, OptimizeTableCommand, UpdateCommand}
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.delta.rapids.DeltaRuntimeShim
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils.PROP_CLUSTERING_COLUMNS
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterByTransform
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExecV1, AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec, OverwriteByExpressionExecV1}
import org.apache.spark.sql.execution.datasources.v2.rapids.{GpuAtomicCreateTableAsSelectExec, GpuAtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.rapids.ExternalSource
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object Delta33xProvider extends DeltaIOProvider {

  override def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
    CreatableRelationProviderRule[_ <: CreatableRelationProvider]] = {
    Seq(
      ExternalSource.toCreatableRelationProviderRule[DeltaDataSource](
        "Write to Delta Lake table",
        (a, conf, p, r) => {
          require(p.isDefined, "Must provide parent meta")
          new DeltaCreatableRelationProviderMeta(a, conf, p, r)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[CreatableRelationProvider]), r)).toMap
  }

  override def isSupportedWrite(write: Class[_ <: SupportsWrite]): Boolean = {
    write == classOf[DeltaTableV2] || write == classOf[GpuDeltaCatalog#GpuStagedDeltaTableV2]
  }

  override def tagForGpu(cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    super.tagForGpu(cpuExec, meta)

    if (cpuExec.partitioning.exists(_.isInstanceOf[ClusterByTransform])) {
      meta.willNotWorkOnGpu("Delta Lake liquid clustering not supported on gpu yet.")
    }
  }

  override def tagForGpu(cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    super.tagForGpu(cpuExec, meta)

    if (cpuExec.partitioning.exists(_.isInstanceOf[ClusterByTransform])) {
      meta.willNotWorkOnGpu("Delta Lake liquid clustering not supported on gpu yet.")
    }
  }

  override def tagForGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): Unit = {
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }

    if (cpuExec.table.properties().containsKey(PROP_CLUSTERING_COLUMNS)) {
      meta.willNotWorkOnGpu("Delta Lake liquid clustering not supported on gpu yet.")
    }

    cpuExec.table match {
      case _: DeltaTableV2 => super.tagForGpu(cpuExec, meta)
      case _: GpuDeltaCatalog#GpuStagedDeltaTableV2 =>
      case _ => meta.willNotWorkOnGpu(s"${cpuExec.table} table class not supported on GPU")
    }
  }

  override def tagForGpu(cpuExec: OverwriteByExpressionExecV1,
      meta: OverwriteByExpressionExecV1Meta): Unit = {
    super.tagForGpu(cpuExec, meta)

    if (cpuExec.table.properties().containsKey(PROP_CLUSTERING_COLUMNS)) {
      meta.willNotWorkOnGpu("Delta Lake liquid clustering not supported on gpu yet.")
    }
  }

  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Seq(
      GpuOverrides.runnableCmd[DeleteCommand](
          "Delete rows from a Delta Lake table",
          (a, conf, p, r) => new DeleteCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[UpdateCommand](
          "Update rows from a Delta Lake table",
          (a, conf, p, r) => new UpdateCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[MergeIntoCommand](
          "Merge of a source query/table into a Delta Lake table",
          (a, conf, p, r) => new MergeIntoCommandMeta(a, conf, p, r)),
      GpuOverrides.runnableCmd[OptimizeTableCommand](
          "Optimize a Delta Lake table",
          (a, conf, p, r) => new OptimizeTableCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }

  case class GpuIncrementMeticMeta(
    cpuInc: IncrementMetric,
    override val conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[IncrementMetric](cpuInc, conf, p, r) {
    override def convertToGpu(): GpuExpression = {
      val gpuChild = childExprs.head.convertToGpu()
      GpuIncrementMetic(cpuInc, gpuChild)
    }
  }

  case class GpuIncrementMetic(cpuInc: IncrementMetric, override val child: Expression) 
    extends ShimUnaryExpression with GpuExpression {

    override def dataType: DataType = child.dataType
    override lazy val deterministic: Boolean = cpuInc.deterministic

    // metric update for a particular branch
    override def hasSideEffects: Boolean = true

    override def prettyName: String = "gpu_" + cpuInc.prettyName

    override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
      cpuInc.metric.add(batch.numRows())
      child.columnarEval(batch)
    }
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[IncrementMetric](
      "IncrementMetric",
      ExprChecks.unaryProject(TypeSig.all, TypeSig.all, TypeSig.all, TypeSig.all),
      GpuIncrementMeticMeta
    )
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  override def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val format = meta.wrapped.relation.fileFormat
    if (format.getClass == classOf[DeltaParquetFileFormat]) {
      val requiredSchema = meta.wrapped.requiredSchema
      if (requiredSchema.exists(_.name == IS_ROW_DELETED_COLUMN_NAME)) {
        meta.willNotWorkOnGpu(
          s"reading metadata column $IS_ROW_DELETED_COLUMN_NAME is not supported")
      }
      if (requiredSchema.exists(_.name == ROW_INDEX_COLUMN_NAME)) {
        meta.willNotWorkOnGpu(
          s"reading metadata column $ROW_INDEX_COLUMN_NAME is not supported")
      }
      GpuReadParquetFileFormat.tagSupport(meta)
    } else {
      meta.willNotWorkOnGpu(s"format ${format.getClass} is not supported")
    }
  }

  override def getReadFileFormat(relation: HadoopFsRelation): FileFormat = {
    val fmt = relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    GpuDelta33xParquetFileFormat(fmt.protocol, fmt.metadata, fmt.nullableRowTrackingFields,
      fmt.optimizationsEnabled, fmt.tablePath, fmt.isCDCRead)
  }

  override def convertToGpu(
    cpuExec: AtomicCreateTableAsSelectExec,
    meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    val cpuCatalog = cpuExec.catalog.asInstanceOf[DeltaCatalog]
    GpuAtomicCreateTableAsSelectExec(
      DeltaRuntimeShim.getGpuDeltaCatalog(cpuCatalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.ifNotExists)
  }

  override def convertToGpu(
    cpuExec: AtomicReplaceTableAsSelectExec,
    meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    val cpuCatalog = cpuExec.catalog.asInstanceOf[DeltaCatalog]
    GpuAtomicReplaceTableAsSelectExec(
      DeltaRuntimeShim.getGpuDeltaCatalog(cpuCatalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.orCreate,
      cpuExec.invalidateCache)
  }

  override def convertToGpu(
      cpuExec: AppendDataExecV1,
      meta: AppendDataExecV1Meta): GpuExec = {
    cpuExec.table match {
      case _: DeltaTableV2 =>
        super.convertToGpu(cpuExec, meta)
      case _: GpuDeltaCatalog#GpuStagedDeltaTableV2 =>
        GpuAppendDataExecV1(cpuExec.table, cpuExec.plan, cpuExec.refreshCache, cpuExec.write)
      case unknown => throw new IllegalStateException(s"$unknown doesn't match any of the known ")
    }
  }
}

class DeltaCreatableRelationProviderMeta(
    source: DeltaDataSource,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends CreatableRelationProviderMeta[DeltaDataSource](source, conf, parent, rule) {
  require(parent.isDefined, "Must provide parent meta")
  private val saveCmd = parent.get.wrapped match {
    case s: SaveIntoDataSourceCommand => s
    case s =>
      throw new IllegalStateException(s"Expected SaveIntoDataSourceCommand, found ${s.getClass}")
  }

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    val path = saveCmd.options.get("path")
    if (path.isDefined) {
      val deltaLog = DeltaLog.forTable(SparkSession.active, new Path(path.get), saveCmd.options)
      RapidsDeltaUtils.tagForDeltaWrite(this, saveCmd.query.schema, Some(deltaLog),
        saveCmd.options, SparkSession.active)

      val table = source.getTable(saveCmd.schema, Array.empty, saveCmd.options.asJava)
      if (table.properties().containsKey(PROP_CLUSTERING_COLUMNS)) {
        willNotWorkOnGpu("Delta Lake liquid clustering not supported on gpu yet.")
      }
    } else {
      willNotWorkOnGpu("no path specified for Delta Lake table")
    }

  }

  override def convertToGpu(): GpuCreatableRelationProvider = new GpuDeltaDataSource(conf)
}