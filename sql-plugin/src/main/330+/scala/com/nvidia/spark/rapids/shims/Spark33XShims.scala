/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import org.apache.parquet.schema.MessageType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Coalesce, DynamicPruningExpression, Expression, FileSourceMetadataAttribute, TimeAdd}
import org.apache.spark.sql.execution.{BaseSubqueryExec, CoalesceExec, FileSourceScanExec, InSubqueryExec, ProjectExec, ReusedSubqueryExec, SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FilePartition, FileScanRDD, HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.rapids.shims.GpuTimeAdd
import org.apache.spark.sql.types.{CalendarIntervalType, DayTimeIntervalType, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

trait Spark33XShims extends Spark321PlusShims {

  /**
   * For spark3.3+ optionally return null if element not exists.
   */
  override def shouldFailOnElementNotExists(): Boolean = SQLConf.get.strictIndexOperator

  override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = null

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = {
    new FileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns)
  }

  override def getParquetFilters(
      schema: MessageType,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean,
      lookupFileMeta: String => String,
      dateTimeRebaseModeFromConf: String): ParquetFilters = {
    val datetimeRebaseMode = DataSourceUtils
      .datetimeRebaseSpec(lookupFileMeta, dateTimeRebaseModeFromConf)
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive, datetimeRebaseMode)
  }

  override def tagFileSourceScanExec(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    if (meta.wrapped.expressions.exists {
      case FileSourceMetadataAttribute(_) => true
      case _ => false
    }) {
      meta.willNotWorkOnGpu("hidden metadata columns are not supported on GPU")
    }
    super.tagFileSourceScanExec(meta)
  }

  // 330+ supports DAYTIME interval types
  override def getFileFormats: Map[FileFormatType, Map[FileFormatOp, FileFormatChecks]] = {
    Map(
      (ParquetFormatType, FileFormatChecks(
        cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.DAYTIME).nested(),
        cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.DAYTIME).nested(),
        sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
            TypeSig.UDT + TypeSig.DAYTIME).nested())))
  }

  // 330+ supports DAYTIME interval types
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val _gpuCommonTypes = TypeSig.commonCudfTypes + TypeSig.NULL
    val map: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[Coalesce](
        "Returns the first non-null argument if exists. Otherwise, null",
        ExprChecks.projectOnly(
          (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
              TypeSig.DAYTIME).nested(),
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param",
            (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
                TypeSig.DAYTIME).nested(),
            TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[Coalesce](a, conf, p, r) {
          override def convertToGpu():
          GpuExpression = GpuCoalesce(childExprs.map(_.convertToGpu()))
        }),
      GpuOverrides.expr[AttributeReference](
        "References an input column",
        ExprChecks.projectAndAst(
          TypeSig.astTypes,
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.STRUCT + TypeSig.DECIMAL_128 + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (att, conf, p, r) => new BaseExprMeta[AttributeReference](att, conf, p, r) {
          // This is the only NOOP operator.  It goes away when things are bound
          override def convertToGpu(): Expression = att

          // There are so many of these that we don't need to print them out, unless it
          // will not work on the GPU
          override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {
            if (!this.canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan) {
              super.print(append, depth, all)
            }
          }
        }),
      GpuOverrides.expr[TimeAdd](
        "Adds interval to timestamp",
        ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
          ("start", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
          // interval support DAYTIME column or CALENDAR literal
          ("interval", TypeSig.DAYTIME + TypeSig.lit(TypeEnum.CALENDAR)
              .withPsNote(TypeEnum.CALENDAR, "month intervals are not supported"),
              TypeSig.DAYTIME + TypeSig.CALENDAR)),
        (timeAdd, conf, p, r) => new BinaryExprMeta[TimeAdd](timeAdd, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            GpuOverrides.extractLit(timeAdd.interval).foreach { lit =>
              lit.dataType match {
                case CalendarIntervalType =>
                  val intvl = lit.value.asInstanceOf[CalendarInterval]
                  if (intvl.months != 0) {
                    willNotWorkOnGpu("interval months isn't supported")
                  }
                case _: DayTimeIntervalType => // Supported
              }
            }
            checkTimeZoneId(timeAdd.timeZoneId)
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuTimeAdd(lhs, rhs)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ map
  }

  // 330+ supports DAYTIME interval types
  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val _gpuCommonTypes = TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64
    val map: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[CoalesceExec](
        "The backend for the dataframe coalesce method",
        ExecChecks((_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT + TypeSig.ARRAY +
            TypeSig.MAP + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (coalesce, conf, parent, r) => new SparkPlanMeta[CoalesceExec](coalesce, conf, parent, r) {
          override def convertToGpu(): GpuExec =
            GpuCoalesceExec(coalesce.numPartitions, childPlans.head.convertIfNeeded())
        }),
      GpuOverrides.exec[DataWritingCommandExec](
        "Writing data",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128.withPsNote(
          TypeEnum.DECIMAL, "128bit decimal only supported for Orc and Parquet") +
            TypeSig.STRUCT.withPsNote(TypeEnum.STRUCT, "Only supported for Parquet") +
            TypeSig.MAP.withPsNote(TypeEnum.MAP, "Only supported for Parquet") +
            TypeSig.ARRAY.withPsNote(TypeEnum.ARRAY, "Only supported for Parquet") +
            TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new SparkPlanMeta[DataWritingCommandExec](p, conf, parent, r) {
          override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
            Seq(GpuOverrides.wrapDataWriteCmds(p.cmd, conf, Some(this)))

          override def convertToGpu(): GpuExec =
            GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(),
              childPlans.head.convertIfNeeded())
        }),
      // this is copied, only added TypeSig.DAYTIME check
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {

          // Replaces SubqueryBroadcastExec inside dynamic pruning filters with GPU counterpart
          // if possible. Instead regarding filters as childExprs of current Meta, we create
          // a new meta for SubqueryBroadcastExec. The reason is that the GPU replacement of
          // FileSourceScan is independent from the replacement of the partitionFilters. It is
          // possible that the FileSourceScan is on the CPU, while the dynamic partitionFilters
          // are on the GPU. And vice versa.
          private lazy val partitionFilters = {
            val convertBroadcast = (bc: SubqueryBroadcastExec) => {
              val meta = GpuOverrides.wrapAndTagPlan(bc, conf)
              meta.tagForExplain()
              meta.convertIfNeeded().asInstanceOf[BaseSubqueryExec]
            }
            wrapped.partitionFilters.map { filter =>
              filter.transformDown {
                case dpe@DynamicPruningExpression(inSub: InSubqueryExec) =>
                  inSub.plan match {
                    case bc: SubqueryBroadcastExec =>
                      dpe.copy(inSub.copy(plan = convertBroadcast(bc)))
                    case reuse@ReusedSubqueryExec(bc: SubqueryBroadcastExec) =>
                      dpe.copy(inSub.copy(plan = reuse.copy(convertBroadcast(bc))))
                    case _ =>
                      dpe
                  }
              }
            }
          }

          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = tagFileSourceScanExec(this)

          override def convertToCpu(): SparkPlan = {
            wrapped.copy(partitionFilters = partitionFilters)
          }

          override def convertToGpu(): GpuExec = {
            val sparkSession = wrapped.relation.sparkSession
            val options = wrapped.relation.options

            val location = replaceWithAlluxioPathIfNeeded(
              conf,
              wrapped.relation,
              partitionFilters,
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
              partitionFilters,
              wrapped.optionalBucketSet,
              wrapped.optionalNumCoalescedBuckets,
              wrapped.dataFilters,
              wrapped.tableIdentifier,
              wrapped.disableBucketedScan)(conf)
          }
        }),
      GpuOverrides.exec[ProjectExec](
        "The backend for most select, withColumn and dropColumn statements",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
              TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (proj, conf, p, r) => new GpuProjectExecMeta(proj, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
    super.getExecs ++ map
  }

}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
