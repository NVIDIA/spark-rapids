/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.common

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.delta.{DeltaIOProvider, GpuDeltaDataSource, RapidsDeltaUtils}
import com.nvidia.spark.rapids.shims._
import com.nvidia.spark.rapids.shims.InvalidateCacheShims
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.delta.{DeltaLog, DeltaParquetFileFormat}
import org.apache.spark.sql.delta.DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.delta.rapids.DeltaRuntimeShim
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.{AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.execution.datasources.v2.rapids.{GpuAtomicCreateTableAsSelectExec, GpuAtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

// Expression support shared across versions - defined outside class to avoid serialization issues
case class GpuIncrementMetricMeta(
  cpuInc: IncrementMetric,
  override val conf: RapidsConf,
  p: Option[RapidsMeta[_, _, _]],
  r: DataFromReplacementRule) extends ExprMeta[IncrementMetric](cpuInc, conf, p, r) {
  override def convertToGpuImpl(): GpuExpression = {
    val gpuChild = childExprs.head.convertToGpu()
    GpuIncrementMetric(cpuInc, gpuChild)
  }
}

case class GpuIncrementMetric(cpuInc: IncrementMetric, override val child: Expression)
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

abstract class DeltaProviderBase extends DeltaIOProvider {

  override def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
      CreatableRelationProviderRule[_ <: CreatableRelationProvider]] = {
    Seq(
      ExternalSource.toCreatableRelationProviderRule[
        org.apache.spark.sql.delta.sources.DeltaDataSource](
        "Write to Delta Lake table",
        (a, conf, p, r) => {
          require(p.isDefined, "Must provide parent meta")
          new DeltaCreatableRelationProviderMeta(a, conf, p, r)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[CreatableRelationProvider]), r)).toMap
  }

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[IncrementMetric](
      "IncrementMetric",
      ExprChecks.unaryProject(TypeSig.all, TypeSig.all, TypeSig.all, TypeSig.all),
      (cpuInc, conf, p, r) => GpuIncrementMetricMeta(cpuInc, conf, p, r)
    )
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  override def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val format = meta.wrapped.relation.fileFormat
    if (format.getClass == classOf[DeltaParquetFileFormat]) {
      GpuReadParquetFileFormat.tagSupport(meta)
    } else {
      meta.willNotWorkOnGpu(s"format ${format.getClass} is not supported")
    }
  }

  override def getReadFileFormat(relation: HadoopFsRelation): FileFormat = {
    val fmt = relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    toGpuParquetFileFormat(fmt)
  }

  protected def toGpuParquetFileFormat(fmt: DeltaParquetFileFormat): FileFormat

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
      InvalidateCacheShims.getInvalidateCache(cpuExec.invalidateCache))
  }

  override def pushDVPredicateDownToScan(plan: SparkPlan): SparkPlan = {
    val pushed = DVPredicatePushdown.pushToScan(plan)

    // Spark often generates a plan that looks like below for deletion vector scans:
    // ...
    // ProjectExec <- prune is_row_deleted
    //   FilterExec <- condition on is_row_deleted
    //     ProjectExec <- project is_row_deleted
    //       FileSourceScanExec
    //
    // The FilterExec can be removed during the pushdown if the only remaining predicates
    // are deletion vector predicates. If the FilterExec is removed, the `is_row_deleted`
    // column is pruned from the subtree below the FilterExec. This can leave two consecutive
    // identical ProjectExecs. We merge them here to simplify the plan.
    DVPredicatePushdown.mergeIdenticalProjects(pushed)
  }

  override def pruneFileMetadata(plan: SparkPlan): SparkPlan = {
    plan match {
      // This logic is a special case of eliminating of unused columns.
      //
      // Delta modifies the logical plan (if there is a deletion vector present on the Delta table)
      //
      // https://github.com/delta-io/delta/blob/f405c3fc4ea3a3ed420f58fb8581aa34e0f0826c
      // /spark/src/main/scala/org/apache/spark/sql/delta/PreprocessTableWithDVs.scala#L69
      //
      // to compute is_deleted from row_index. Not only Plugin's current logic is not taking
      // advantage of this but it also requires producing the rest of completely unrelated
      // file metadata.
      //
      // The following logic along with isDVScan matches DV-enabled scan produced by the Delta
      // rule and cleans out _metadata. If _metadata is used above the DV-Scan we fallback on CPU
      //
      case dvRoot @ GpuProjectExec(outputList,
      dvFilter @ GpuFilterExec(condition,
      dvFilterInput @ GpuProjectExec(inputList, fsse: GpuFileSourceScanExec, _)), _)
        if condition.references.exists(_.name == IS_ROW_DELETED_COLUMN_NAME) &&
          !outputList.exists(_.name == "_metadata") && inputList.exists(_.name == "_metadata") =>
        dvRoot.withNewChildren(Seq(
          dvFilter.withNewChildren(Seq(
            dvFilterInput.copy(projectList = inputList.filterNot(_.name == "_metadata"))
              .withNewChildren(Seq(
                fsse.copy(
                  originalOutput =
                    fsse.originalOutput.filterNot(_.name == "_tmp_metadata_row_index"),
                  requiredSchema = StructType(
                    fsse.requiredSchema.filterNot(_.name == "_tmp_metadata_row_index")
                  ))(fsse.rapidsConf)))))))
      case _ =>
        plan.withNewChildren(plan.children.map(pruneFileMetadata))
    }
  }

  override def isDVScan(meta: SparkPlanMeta[FileSourceScanExec]): Boolean = {
    val maybeDVScan = meta.parent // project input
      .flatMap(_.parent) // filter
      .flatMap(_.parent) // project output
      .map(_.wrapped)

    maybeDVScan.map {
      case ProjectExec(outputList, FilterExec(condition, ProjectExec(inputList, _))) =>
        condition.references.exists(_.name == IS_ROW_DELETED_COLUMN_NAME) &&
          inputList.exists(_.name == "_metadata") && !outputList.exists(_.name == "_metadata")
      case _ =>
        false
    }.getOrElse(false)
  }

}

object DVPredicatePushdown extends ShimPredicateHelper {

  /**
   * Pushes down deletion vector predicates to scan level by removing them from the FilterExec.
   * The deletion vector predicates will be processed in the scan instead.
   */
  def pushToScan(plan: SparkPlan): SparkPlan = {

    /**
     * Is the condition a form of "IS_ROW_DELETED_COLUMN_NAME == 0"?
     */
    def isDVCondition(condition: Expression): Boolean = {
      condition match {
        case GpuEqualTo(left, right) =>
          isRowDeletedColumnRef(left) && isLiteralZero(right) ||
            isRowDeletedColumnRef(right) && isLiteralZero(left)
        case _ => false
      }
    }

    def isRowDeletedColumnRef(expr: Expression): Boolean = {
      expr match {
        case attr: AttributeReference if attr.name == IS_ROW_DELETED_COLUMN_NAME => true
        case _ => false
      }
    }

    def isLiteralZero(expr: Expression): Boolean = {
      expr match {
        case GpuLiteral(value: Number, _) if value.longValue() == 0L => true
        case _ => false
      }
    }

    def pruneIsRowDeletedColumn(plan: SparkPlan): SparkPlan = {
      plan.transformUp {
        case project @ GpuProjectExec(projectList, _, _) =>
          val newProjList = projectList.filterNot(isRowDeletedColumnRef(_))
          project.copy(projectList = newProjList)
        case fsse: GpuFileSourceScanExec =>
          fsse.copy(originalOutput = fsse.originalOutput.filterNot(
            _.name == IS_ROW_DELETED_COLUMN_NAME),
            requiredSchema = StructType(
              fsse.requiredSchema.filterNot(_.name == IS_ROW_DELETED_COLUMN_NAME)
            ))(fsse.rapidsConf)
      }
    }

    plan.transformUp {
      case filter @ GpuFilterExec(condition, child)
        if condition.references.exists(_.name == IS_ROW_DELETED_COLUMN_NAME) =>
        // Decompose the condition into CNF
        val conjuncts = splitConjunctivePredicates(condition)
        // the dv condition should be "IS_ROW_DELETED_COLUMN_NAME == 0"
        val (dvPredicate, otherPredicates) = conjuncts.partition(p =>
          p.references.size == 1 &&
            p.references.exists(_.name == IS_ROW_DELETED_COLUMN_NAME) &&
            isDVCondition(p)
        )

        val otherPredicatesReadingIsRowDeleted = otherPredicates.filter(
          p => p.references.exists(_.name == IS_ROW_DELETED_COLUMN_NAME)
        )

        val newChild = if (dvPredicate.nonEmpty && otherPredicatesReadingIsRowDeleted.isEmpty) {
          // Since we are going to drop this dvPredicate and isRowDeleted is not used in other
          // predicates, we can prune isRowDeleted column from the child plan
          pruneIsRowDeletedColumn(child)
        } else {
          child
        }

        if (otherPredicates.isEmpty) {
          newChild
        } else {
          filter.copy(condition = otherPredicates.reduce(GpuAnd),
            child = newChild)(filter.coalesceAfter)
        }
    }
  }

  /**
   * Merges consecutive ProjectExecs into one if their project lists are identical.
   */
  def mergeIdenticalProjects(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p @ GpuProjectExec(projList1,
      GpuProjectExec(projList2, child, enablePreSplit1), enablePreSplit2) =>
        val projSet1 = projList1.map(_.exprId).toSet
        val projSet2 = projList2.map(_.exprId).toSet
        if (projSet1 == projSet2) {
          GpuProjectExec(projList1, child, enablePreSplit1 && enablePreSplit2)
        } else {
          p
        }
    }
  }
}

class DeltaCreatableRelationProviderMeta(
    source: org.apache.spark.sql.delta.sources.DeltaDataSource,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends CreatableRelationProviderMeta[org.apache.spark.sql.delta.sources.DeltaDataSource](
    source, conf, parent, rule) {
  require(parent.isDefined, "Must provide parent meta")
  private val saveCmd = parent.get.wrapped.asInstanceOf[SaveIntoDataSourceCommand]

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
    } else {
      willNotWorkOnGpu("no path specified for Delta Lake table")
    }

  }

  override def convertToGpu(): GpuCreatableRelationProvider = new GpuDeltaDataSource(conf)
}
