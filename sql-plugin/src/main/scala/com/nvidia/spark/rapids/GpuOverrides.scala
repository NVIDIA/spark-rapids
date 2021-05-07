/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.time.ZoneId

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf.DType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.rapids.TimeStamp
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, CustomShuffleReaderExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.v2.{AlterNamespaceSetPropertiesExec, AlterTableExec, AtomicReplaceTableExec, BatchScanExec, CreateNamespaceExec, CreateTableExec, DeleteFromTableExec, DescribeNamespaceExec, DescribeTableExec, DropNamespaceExec, DropTableExec, RefreshTableExec, RenameTableExec, ReplaceTableExec, SetCatalogAndNamespaceExec, ShowCurrentNamespaceExec, ShowNamespacesExec, ShowTablePropertiesExec, ShowTablesExec}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.rapids.GpuHiveOverrides
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.rapids.execution.{GpuBroadcastMeta, GpuBroadcastNestedLoopJoinMeta, GpuCustomShuffleReaderExec, GpuShuffleExchangeExecBase, GpuShuffleMeta}
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Base class for all ReplacementRules
 * @param doWrap wraps a part of the plan in a [[RapidsMeta]] for further processing.
 * @param desc a description of what this part of the plan does.
 * @param tag metadata used to determine what INPUT is at runtime.
 * @tparam INPUT the exact type of the class we are wrapping.
 * @tparam BASE the generic base class for this type of stage, i.e. SparkPlan, Expression, etc.
 * @tparam WRAP_TYPE base class that should be returned by doWrap.
 */
abstract class ReplacementRule[INPUT <: BASE, BASE, WRAP_TYPE <: RapidsMeta[INPUT, BASE, _]](
    protected var doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => WRAP_TYPE,
    protected var desc: String,
    protected val checks: Option[TypeChecks[_]],
    final val tag: ClassTag[INPUT]) extends DataFromReplacementRule {

  private var _incompatDoc: Option[String] = None
  private var _disabledDoc: Option[String] = None
  private var _visible: Boolean = true

  def isVisible: Boolean = _visible
  def description: String = desc

  override def incompatDoc: Option[String] = _incompatDoc
  override def disabledMsg: Option[String] = _disabledDoc
  override def getChecks: Option[TypeChecks[_]] = checks

  /**
   * Mark this expression as incompatible with the original Spark version
   * @param str a description of how it is incompatible.
   * @return this for chaining.
   */
  final def incompat(str: String) : this.type = {
    _incompatDoc = Some(str)
    this
  }

  /**
   * Mark this expression as disabled by default.
   * @param str a description of why it is disabled by default.
   * @return this for chaining.
   */
  final def disabledByDefault(str: String) : this.type = {
    _disabledDoc = Some(str)
    this
  }

  final def invisible(): this.type = {
    _visible = false
    this
  }

  /**
   * Provide a function that will wrap a spark type in a [[RapidsMeta]] instance that is used for
   * conversion to a GPU version.
   * @param func the function
   * @return this for chaining.
   */
  final def wrap(func: (
      INPUT,
      RapidsConf,
      Option[RapidsMeta[_, _, _]],
      DataFromReplacementRule) => WRAP_TYPE): this.type = {
    doWrap = func
    this
  }

  /**
   * Set a description of what the operation does.
   * @param str the description.
   * @return this for chaining
   */
  final def desc(str: String): this.type = {
    this.desc = str
    this
  }

  private var confKeyCache: String = null
  protected val confKeyPart: String

  override def confKey: String = {
    if (confKeyCache == null) {
      confKeyCache = "spark.rapids.sql." + confKeyPart + "." + tag.runtimeClass.getSimpleName
    }
    confKeyCache
  }

  def notes(): Option[String] = if (incompatDoc.isDefined) {
    Some(s"This is not 100% compatible with the Spark version because ${incompatDoc.get}")
  } else if (disabledMsg.isDefined) {
    Some(s"This is disabled by default because ${disabledMsg.get}")
  } else {
    None
  }

  def confHelp(asTable: Boolean = false, sparkSQLFunctions: Option[String] = None): Unit = {
    if (_visible) {
      val notesMsg = notes()
      if (asTable) {
        import ConfHelper.makeConfAnchor
        print(s"${makeConfAnchor(confKey)}")
        if (sparkSQLFunctions.isDefined) {
          print(s"|${sparkSQLFunctions.get}")
        }
        print(s"|$desc|${notesMsg.isEmpty}|")
        if (notesMsg.isDefined) {
          print(s"${notesMsg.get}")
        } else {
          print("None")
        }
        println("|")
      } else {
        println(s"$confKey:")
        println(s"\tEnable (true) or disable (false) the $tag $operationName.")
        if (sparkSQLFunctions.isDefined) {
          println(s"\tsql function: ${sparkSQLFunctions.get}")
        }
        println(s"\t$desc")
        if (notesMsg.isDefined) {
          println(s"\t${notesMsg.get}")
        }
        println(s"\tdefault: ${notesMsg.isEmpty}")
        println()
      }
    }
  }

  final def wrap(
      op: BASE,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      r: DataFromReplacementRule): WRAP_TYPE = {
    doWrap(op.asInstanceOf[INPUT], conf, parent, r)
  }

  def getClassFor: Class[_] = tag.runtimeClass
}

/**
 * Holds everything that is needed to replace an Expression with a GPU enabled version.
 */
class ExprRule[INPUT <: Expression](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => BaseExprMeta[INPUT],
    desc: String,
    checks: Option[ExprChecks],
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Expression, BaseExprMeta[INPUT]](
    doWrap, desc, checks, tag) {

  override val confKeyPart = "expression"
  override val operationName = "Expression"
}

/**
 * Holds everything that is needed to replace a `Scan` with a GPU enabled version.
 */
class ScanRule[INPUT <: Scan](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => ScanMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Scan, ScanMeta[INPUT]](
    doWrap, desc, None, tag) {

  override val confKeyPart: String = "input"
  override val operationName: String = "Input"
}

/**
 * Holds everything that is needed to replace a `Partitioning` with a GPU enabled version.
 */
class PartRule[INPUT <: Partitioning](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => PartMeta[INPUT],
    desc: String,
    checks: Option[PartChecks],
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Partitioning, PartMeta[INPUT]](
    doWrap, desc, checks, tag) {

  override val confKeyPart: String = "partitioning"
  override val operationName: String = "Partitioning"
}

/**
 * Holds everything that is needed to replace a `SparkPlan` with a GPU enabled version.
 */
class ExecRule[INPUT <: SparkPlan](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => SparkPlanMeta[INPUT],
    desc: String,
    checks: Option[ExecChecks],
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, SparkPlan, SparkPlanMeta[INPUT]](
    doWrap, desc, checks, tag) {

  // TODO finish this...

  override val confKeyPart: String = "exec"
  override val operationName: String = "Exec"
}

/**
 * Holds everything that is needed to replace a `DataWritingCommand` with a
 * GPU enabled version.
 */
class DataWritingCommandRule[INPUT <: DataWritingCommand](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        DataFromReplacementRule) => DataWritingCommandMeta[INPUT],
    desc: String,
    tag: ClassTag[INPUT])
    extends ReplacementRule[INPUT, DataWritingCommand, DataWritingCommandMeta[INPUT]](
      doWrap, desc, None, tag) {

  override val confKeyPart: String = "output"
  override val operationName: String = "Output"
}

final class InsertIntoHadoopFsRelationCommandMeta(
    cmd: InsertIntoHadoopFsRelationCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends DataWritingCommandMeta[InsertIntoHadoopFsRelationCommand](cmd, conf, parent, rule) {

  private var fileFormat: Option[ColumnarFileFormat] = None

  override def tagSelfForGpu(): Unit = {
    if (cmd.bucketSpec.isDefined) {
      willNotWorkOnGpu("bucketing is not supported")
    }

    val spark = SparkSession.active

    fileFormat = cmd.fileFormat match {
      case _: CSVFileFormat =>
        willNotWorkOnGpu("CSV output is not supported")
        None
      case _: JsonFileFormat =>
        willNotWorkOnGpu("JSON output is not supported")
        None
      case f if GpuOrcFileFormat.isSparkOrcFormat(f) =>
        GpuOrcFileFormat.tagGpuSupport(this, spark, cmd.options, cmd.query.schema)
      case _: ParquetFileFormat =>
        GpuParquetFileFormat.tagGpuSupport(this, spark, cmd.options, cmd.query.schema)
      case _: TextFileFormat =>
        willNotWorkOnGpu("text output is not supported")
        None
      case f =>
        willNotWorkOnGpu(s"unknown file format: ${f.getClass.getCanonicalName}")
        None
    }
  }

  override def convertToGpu(): GpuDataWritingCommand = {
    val format = fileFormat.getOrElse(
      throw new IllegalStateException("fileFormat missing, tagSelfForGpu not called?"))

    GpuInsertIntoHadoopFsRelationCommand(
      cmd.outputPath,
      cmd.staticPartitions,
      cmd.ifPartitionNotExists,
      cmd.partitionColumns,
      cmd.bucketSpec,
      format,
      cmd.options,
      cmd.query,
      cmd.mode,
      cmd.catalogTable,
      cmd.fileIndex,
      cmd.outputColumnNames)
  }
}

final class CreateDataSourceTableAsSelectCommandMeta(
    cmd: CreateDataSourceTableAsSelectCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends DataWritingCommandMeta[CreateDataSourceTableAsSelectCommand](cmd, conf, parent, rule) {

  private var origProvider: Class[_] = _
  private var gpuProvider: Option[ColumnarFileFormat] = None

  override def tagSelfForGpu(): Unit = {
    if (cmd.table.bucketSpec.isDefined) {
      willNotWorkOnGpu("bucketing is not supported")
    }
    if (cmd.table.provider.isEmpty) {
      willNotWorkOnGpu("provider must be defined")
    }

    val spark = SparkSession.active
    origProvider =
      GpuDataSource.lookupDataSourceWithFallback(cmd.table.provider.get, spark.sessionState.conf)
    // Note that the data source V2 always fallsback to the V1 currently.
    // If that changes then this will start failing because we don't have a mapping.
    gpuProvider = origProvider.getConstructor().newInstance() match {
      case f: FileFormat if GpuOrcFileFormat.isSparkOrcFormat(f) =>
        GpuOrcFileFormat.tagGpuSupport(this, spark, cmd.table.storage.properties, cmd.query.schema)
      case _: ParquetFileFormat =>
        GpuParquetFileFormat.tagGpuSupport(this, spark,
          cmd.table.storage.properties, cmd.query.schema)
      case ds =>
        willNotWorkOnGpu(s"Data source class not supported: ${ds}")
        None
    }
  }

  override def convertToGpu(): GpuDataWritingCommand = {
    val newProvider = gpuProvider.getOrElse(
      throw new IllegalStateException("fileFormat unexpected, tagSelfForGpu not called?"))

    GpuCreateDataSourceTableAsSelectCommand(
      cmd.table,
      cmd.mode,
      cmd.query,
      cmd.outputColumnNames,
      origProvider,
      newProvider)
  }
}

/**
 * Listener trait so that tests can confirm that the expected optimizations are being applied
 */
trait GpuOverridesListener {
  def optimizedPlan(
      plan: SparkPlanMeta[SparkPlan],
      sparkPlan: SparkPlan,
      costOptimizations: Seq[Optimization])
}

sealed trait FileFormatType
object CsvFormatType extends FileFormatType {
  override def toString = "CSV"
}
object ParquetFormatType extends FileFormatType {
  override def toString = "Parquet"
}
object OrcFormatType extends FileFormatType {
  override def toString = "ORC"
}

sealed trait FileFormatOp
object ReadFileOp extends FileFormatOp {
  override def toString = "read"
}
object WriteFileOp extends FileFormatOp {
  override def toString = "write"
}

object GpuOverrides {
  val FLOAT_DIFFERS_GROUP_INCOMPAT =
    "when enabling these, there may be extra groups produced for floating point grouping " +
    "keys (e.g. -0.0, and 0.0)"
  val CASE_MODIFICATION_INCOMPAT =
    "in some cases unicode characters change byte width when changing the case. The GPU string " +
    "conversion does not support these characters. For a full list of unsupported characters " +
    "see https://github.com/rapidsai/cudf/issues/3132"
  val UTC_TIMEZONE_ID = ZoneId.of("UTC").normalized()
  // Based on https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
  private[this] lazy val regexList: Seq[String] = Seq("\\", "\u0000", "\\x", "\t", "\n", "\r",
    "\f", "\\a", "\\e", "\\cx", "[", "]", "^", "&", ".", "*", "\\d", "\\D", "\\h", "\\H", "\\s",
    "\\S", "\\v", "\\V", "\\w", "\\w", "\\p", "$", "\\b", "\\B", "\\A", "\\G", "\\Z", "\\z", "\\R",
    "?", "|", "(", ")", "{", "}", "\\k", "\\Q", "\\E", ":", "!", "<=", ">")

  private[this] val _commonTypes = TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL

  val pluginSupportedOrderableSig: TypeSig = _commonTypes + TypeSig.STRUCT.nested(_commonTypes)

  private[this] def isStructType(dataType: DataType) = dataType match {
    case StructType(_) => true
    case _ => false
  }

  // this listener mechanism is global and is intended for use by unit tests only
  private val listeners: ListBuffer[GpuOverridesListener] = new ListBuffer[GpuOverridesListener]()

  def addListener(listener: GpuOverridesListener): Unit = {
    listeners += listener
  }

  def removeListener(listener: GpuOverridesListener): Unit = {
    listeners -= listener
  }

  def removeAllListeners(): Unit = {
    listeners.clear()
  }

  def canRegexpBeTreatedLikeARegularString(strLit: UTF8String): Boolean = {
    val s = strLit.toString
    !regexList.exists(pattern => s.contains(pattern))
  }

  private def convertExprToGpuIfPossible(expr: Expression, conf: RapidsConf): Expression = {
    if (expr.find(_.isInstanceOf[GpuExpression]).isDefined) {
      // already been converted
      expr
    } else {
      val wrapped = wrapExpr(expr, conf, None)
      wrapped.tagForGpu()
      if (wrapped.canExprTreeBeReplaced) {
        wrapped.convertToGpu()
      } else {
        expr
      }
    }
  }

  private def gpuOrderingSemanticEquals(
      found: Expression,
      required: Expression,
      conf: RapidsConf): Boolean =
    found.deterministic &&
        required.deterministic &&
        convertExprToGpuIfPossible(found, conf).canonicalized ==
            convertExprToGpuIfPossible(required, conf).canonicalized

  private def orderingSatisfies(
      found: SortOrder,
      required: SortOrder,
      conf: RapidsConf): Boolean = {
    val foundChildren = ShimLoader.getSparkShims.sortOrderChildren(found)
    found.direction == required.direction &&
        found.nullOrdering == required.nullOrdering &&
        foundChildren.exists(gpuOrderingSemanticEquals(_, required.child, conf))
  }

  private def orderingSatisfies(
      ordering1: Seq[SortOrder],
      ordering2: Seq[SortOrder],
      conf: RapidsConf): Boolean = {
    // We cannot use SortOrder.orderingSatisfies because there is a corner case where
    // some operators like a Literal can be a part of SortOrder, which then results in errors
    // because we may have converted it over to a GpuLiteral at that point and a Literal
    // is not equivalent to a GpuLiteral, even though it should be.
    if (ordering2.isEmpty) {
      true
    } else if (ordering2.length > ordering1.length) {
      false
    } else {
      ordering2.zip(ordering1).forall {
        case (o2, o1) => orderingSatisfies(o1, o2, conf)
      }
    }
  }

  private def convertPartToGpuIfPossible(part: Partitioning, conf: RapidsConf): Partitioning = {
    part match {
      case _: GpuPartitioning => part
      case _ =>
        val wrapped = wrapPart(part, conf, None)
        wrapped.tagForGpu()
        if (wrapped.canThisBeReplaced) {
          wrapped.convertToGpu()
        } else {
          part
        }
    }
  }

  /**
   * Removes unnecessary CPU shuffles that Spark can add to the plan when it does not realize
   * a GPU partitioning satisfies a CPU distribution because CPU and GPU expressions are not
   * semantically equal.
   */
  def removeExtraneousShuffles(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    plan.transformUp {
      case cpuShuffle: ShuffleExchangeExec =>
        cpuShuffle.child match {
          case sqse: ShuffleQueryStageExec =>
            GpuTransitionOverrides.getNonQueryStagePlan(sqse) match {
              case gpuShuffle: GpuShuffleExchangeExecBase =>
                val converted = convertPartToGpuIfPossible(cpuShuffle.outputPartitioning, conf)
                if (converted == gpuShuffle.outputPartitioning) {
                  sqse
                } else {
                  cpuShuffle
                }
              case _ => cpuShuffle
            }
          case _ => cpuShuffle
        }
    }
  }

  @scala.annotation.tailrec
  def extractLit(exp: Expression): Option[Literal] = exp match {
    case l: Literal => Some(l)
    case a: Alias => extractLit(a.child)
    case _ => None
  }

  def isOfType(l: Option[Literal], t: DataType): Boolean = l.exists(_.dataType == t)

  def isStringLit(exp: Expression): Boolean =
    isOfType(extractLit(exp), StringType)

  def extractStringLit(exp: Expression): Option[String] = extractLit(exp) match {
    case Some(Literal(v: UTF8String, StringType)) =>
      val s = if (v == null) null else v.toString
      Some(s)
    case _ => None
  }

  def isLit(exp: Expression): Boolean = extractLit(exp).isDefined

  def isNullLit(lit: Literal): Boolean = {
    lit.value == null
  }

  def isNullOrEmptyOrRegex(exp: Expression): Boolean = {
    val lit = extractLit(exp)
    if (!isOfType(lit, StringType)) {
      false
    } else if (isNullLit(lit.get)) {
      //isOfType check above ensures that this lit.get does not throw
      true
    } else {
      val strLit = lit.get.value.asInstanceOf[UTF8String].toString
      if (strLit.isEmpty) {
        true
      } else {
        regexList.exists(pattern => strLit.contains(pattern))
      }
    }
  }

  def areAllSupportedTypes(types: DataType*): Boolean = types.forall(isSupportedType(_))

  /**
   * Is this particular type supported or not.
   * @param dataType the type to check
   * @param allowNull should NullType be allowed
   * @param allowDecimal should DecimalType be allowed
   * @param allowBinary should BinaryType be allowed
   * @param allowCalendarInterval should CalendarIntervalType be allowed
   * @param allowArray should ArrayType be allowed
   * @param allowStruct should StructType be allowed
   * @param allowStringMaps should a Map[String, String] specifically be allowed
   * @param allowMaps should MapType be allowed generically
   * @param allowNesting should nested types like array struct and map allow nested types
   *                     within them, or only primitive types.
   * @return true if it is allowed else false
   */
  def isSupportedType(dataType: DataType,
      allowNull: Boolean = false,
      allowDecimal: Boolean = false,
      allowBinary: Boolean = false,
      allowCalendarInterval: Boolean = false,
      allowArray: Boolean = false,
      allowStruct: Boolean = false,
      allowStringMaps: Boolean = false,
      allowMaps: Boolean = false,
      allowNesting: Boolean = false): Boolean = {
    def checkNested(dataType: DataType): Boolean = {
      isSupportedType(dataType,
        allowNull = allowNull,
        allowDecimal = allowDecimal,
        allowBinary = allowBinary && allowNesting,
        allowCalendarInterval = allowCalendarInterval && allowNesting,
        allowArray = allowArray && allowNesting,
        allowStruct = allowStruct && allowNesting,
        allowStringMaps = allowStringMaps && allowNesting,
        allowMaps = allowMaps && allowNesting,
        allowNesting = allowNesting)
    }
    dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case DateType => true
      case TimestampType => ZoneId.systemDefault().normalized() == GpuOverrides.UTC_TIMEZONE_ID
      case StringType => true
      case dt: DecimalType if allowDecimal => dt.precision <= DType.DECIMAL64_MAX_PRECISION
      case NullType => allowNull
      case BinaryType => allowBinary
      case CalendarIntervalType => allowCalendarInterval
      case ArrayType(elementType, _) if allowArray => checkNested(elementType)
      case MapType(StringType, StringType, _) if allowStringMaps => true
      case MapType(keyType, valueType, _) if allowMaps =>
        checkNested(keyType) && checkNested(valueType)
      case StructType(fields) if allowStruct =>
        fields.map(_.dataType).forall(checkNested)
      case _ => false
    }
  }

  /**
   * Checks to see if any expressions are a String Literal
   */
  def isAnyStringLit(expressions: Seq[Expression]): Boolean =
    expressions.exists(isStringLit)

  def expr[INPUT <: Expression](
      desc: String,
      pluginChecks: ExprChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => BaseExprMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ExprRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExprRule[INPUT](doWrap, desc, Some(pluginChecks), tag)
  }

  def scan[INPUT <: Scan](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => ScanMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ScanRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ScanRule[INPUT](doWrap, desc, tag)
  }

  def part[INPUT <: Partitioning](
      desc: String,
      checks: PartChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => PartMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): PartRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new PartRule[INPUT](doWrap, desc, Some(checks), tag)
  }

  /**
   * Create an exec rule that should never be replaced, because it is something that should always
   * run on the CPU, or should just be ignored totally for what ever reason.
   */
  def neverReplaceExec[INPUT <: SparkPlan](desc: String)
      (implicit tag: ClassTag[INPUT]): ExecRule[INPUT] = {
    assert(desc != null)
    def doWrap(
        exec: INPUT,
        conf: RapidsConf,
        p: Option[RapidsMeta[_, _, _]],
        cc: DataFromReplacementRule) =
      new DoNotReplaceOrWarnSparkPlanMeta[INPUT](exec, conf, p)
    new ExecRule[INPUT](doWrap, desc, None, tag).invisible()
  }

  def exec[INPUT <: SparkPlan](
      desc: String,
      pluginChecks: ExecChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => SparkPlanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ExecRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExecRule[INPUT](doWrap, desc, Some(pluginChecks), tag)
  }

  def dataWriteCmd[INPUT <: DataWritingCommand](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], DataFromReplacementRule)
          => DataWritingCommandMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): DataWritingCommandRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new DataWritingCommandRule[INPUT](doWrap, desc, tag)
  }

  def wrapExpr[INPUT <: Expression](
      expr: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): BaseExprMeta[INPUT] =
    expressions.get(expr.getClass)
      .map(r => r.wrap(expr, conf, parent, r).asInstanceOf[BaseExprMeta[INPUT]])
      .getOrElse(new RuleNotFoundExprMeta(expr, conf, parent))

  val fileFormats: Map[FileFormatType, Map[FileFormatOp, FileFormatChecks]] = Map(
    (CsvFormatType, FileFormatChecks(
      cudfRead = TypeSig.commonCudfTypes,
      cudfWrite = TypeSig.none,
      sparkSig = TypeSig.atomics)),
    (ParquetFormatType, FileFormatChecks(
      cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.STRUCT + TypeSig.ARRAY +
          TypeSig.MAP).nested(),
      cudfWrite = TypeSig.commonCudfTypes + TypeSig.DECIMAL,
      sparkSig = (TypeSig.atomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.UDT).nested())),
    (OrcFormatType, FileFormatChecks(
      cudfReadWrite = TypeSig.commonCudfTypes,
      sparkSig = (TypeSig.atomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.UDT).nested())))

  val commonExpressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    expr[Literal](
      "Holds a static value from the query",
      ExprChecks.projectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.CALENDAR
          + TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL),
        TypeSig.all),
      (lit, conf, p, r) => new LiteralExprMeta(lit, conf, p, r)),
    expr[Signum](
      "Returns -1.0, 0.0 or 1.0 as expr is negative, 0 or positive",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Signum](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSignum(child)
      }),
    expr[Alias](
      "Gives a column a name",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT
            + TypeSig.DECIMAL).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryExprMeta[Alias](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuAlias(child, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      }),
    expr[AttributeReference](
      "References an input column",
      ExprChecks.projectNotLambda((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP +
                TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.DECIMAL).nested(), TypeSig.all),
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
    expr[PromotePrecision](
      "PromotePrecision before arithmetic operations between DecimalType data",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(TypeSig.DECIMAL, TypeSig.DECIMAL),
      (a, conf, p, r) => new UnaryExprMeta[PromotePrecision](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuPromotePrecision(child)
      }),
    expr[CheckOverflow](
      "CheckOverflow after arithmetic operations between DecimalType data",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(TypeSig.DECIMAL, TypeSig.DECIMAL),
      (a, conf, p, r) => new UnaryExprMeta[CheckOverflow](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuCheckOverflow(child, wrapped.dataType, wrapped.nullOnOverflow)
      }),
    expr[ToDegrees](
      "Converts radians to degrees",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[ToDegrees](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuToDegrees = GpuToDegrees(child)
      }),
    expr[ToRadians](
      "Converts degrees to radians",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[ToRadians](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuToRadians = GpuToRadians(child)
      }),
    expr[WindowExpression](
      "Calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      ExprChecks.windowOnly(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL +
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.STRUCT),
        TypeSig.all,
        Seq(ParamCheck("windowFunction",
          TypeSig.commonCudfTypes + TypeSig.DECIMAL +
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.STRUCT),
          TypeSig.all),
          ParamCheck("windowSpec",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL,
            TypeSig.numericAndInterval))),
      (windowExpression, conf, p, r) => new GpuWindowExpressionMeta(windowExpression, conf, p, r)),
    expr[SpecifiedWindowFrame](
      "Specification of the width of the group (or \"frame\") of input rows " +
        "around which a window function is evaluated",
      ExprChecks.projectOnly(
        TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral,
        TypeSig.numericAndInterval,
        Seq(
          ParamCheck("lower",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral,
            TypeSig.numericAndInterval),
          ParamCheck("upper",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral,
            TypeSig.numericAndInterval))),
      (windowFrame, conf, p, r) => new GpuSpecifiedWindowFrameMeta(windowFrame, conf, p, r) ),
    expr[WindowSpecDefinition](
      "Specification of a window function, indicating the partitioning-expression, the row " +
        "ordering, and the width of the window",
      WindowSpecCheck,
      (windowSpec, conf, p, r) => new GpuWindowSpecDefinitionMeta(windowSpec, conf, p, r)),
    expr[CurrentRow.type](
      "Special boundary for a window frame, indicating stopping at the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      (currentRow, conf, p, r) => new ExprMeta[CurrentRow.type](currentRow, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(currentRow)
      }),
    expr[UnboundedPreceding.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      (unboundedPreceding, conf, p, r) =>
        new ExprMeta[UnboundedPreceding.type](unboundedPreceding, conf, p, r) {
          override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(unboundedPreceding)
        }),
    expr[UnboundedFollowing.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      (unboundedFollowing, conf, p, r) =>
        new ExprMeta[UnboundedFollowing.type](unboundedFollowing, conf, p, r) {
          override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(unboundedFollowing)
        }),
    expr[RowNumber](
      "Window function that returns the index for the row within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT),
      (rowNumber, conf, p, r) => new ExprMeta[RowNumber](rowNumber, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuRowNumber()
      }),
    expr[Lead](
      "Window function that returns N entries ahead of this one",
      ExprChecks.windowOnly(TypeSig.numeric + TypeSig.BOOLEAN +
          TypeSig.DATE + TypeSig.TIMESTAMP, TypeSig.all,
        Seq(ParamCheck("input", TypeSig.numeric + TypeSig.BOOLEAN +
            TypeSig.DATE + TypeSig.TIMESTAMP, TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default", TypeSig.numeric + TypeSig.BOOLEAN +
              TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.NULL, TypeSig.all))),
      (lead, conf, p, r) => new OffsetWindowFunctionMeta[Lead](lead, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuLead(input.convertToGpu(), offset.convertToGpu(), default.convertToGpu())
      }),
    expr[Lag](
      "Window function that returns N entries behind this one",
      ExprChecks.windowOnly(TypeSig.numeric + TypeSig.BOOLEAN +
          TypeSig.DATE + TypeSig.TIMESTAMP, TypeSig.all,
        Seq(ParamCheck("input", TypeSig.numeric + TypeSig.BOOLEAN +
            TypeSig.DATE + TypeSig.TIMESTAMP, TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default", TypeSig.numeric + TypeSig.BOOLEAN +
              TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.NULL, TypeSig.all))),
      (lag, conf, p, r) => new OffsetWindowFunctionMeta[Lag](lag, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuLag(input.convertToGpu(), offset.convertToGpu(), default.convertToGpu())
      }),
    expr[UnaryMinus](
      "Negate a numeric value",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        TypeSig.numeric,
        TypeSig.numericAndInterval),
      (a, conf, p, r) => new UnaryExprMeta[UnaryMinus](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUnaryMinus(child)
      }),
    expr[UnaryPositive](
      "A numeric value with a + in front of it",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        TypeSig.numeric,
        TypeSig.numericAndInterval),
      (a, conf, p, r) => new UnaryExprMeta[UnaryPositive](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUnaryPositive(child)
      }),
    expr[Year](
      "Returns the year from a date or timestamp",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Year](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuYear(child)
      }),
    expr[Month](
      "Returns the month from a date or timestamp",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Month](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuMonth(child)
      }),
    expr[Quarter](
      "Returns the quarter of the year for date, in the range 1 to 4",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Quarter](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuQuarter(child)
      }),
    expr[DayOfMonth](
      "Returns the day of the month from a date or timestamp",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfMonth](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuDayOfMonth(child)
      }),
    expr[DayOfYear](
      "Returns the day of the year from a date or timestamp",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfYear](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuDayOfYear(child)
      }),
    expr[Abs](
      "Absolute value",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        TypeSig.numeric, TypeSig.numeric),
      (a, conf, p, r) => new UnaryExprMeta[Abs](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAbs(child)
      }),
    expr[Acos](
      "Inverse cosine",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Acos](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAcos(child)
      }),
    expr[Acosh](
      "Inverse hyperbolic cosine",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Acosh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          if (conf.includeImprovedFloat) {
            GpuAcoshImproved(child)
          } else {
            GpuAcoshCompat(child)
          }
      }),
    expr[Asin](
      "Inverse sine",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Asin](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAsin(child)
      }),
    expr[Asinh](
      "Inverse hyperbolic sine",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Asinh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          if (conf.includeImprovedFloat) {
            GpuAsinhImproved(child)
          } else {
            GpuAsinhCompat(child)
          }
      }),
    expr[Sqrt](
      "Square root",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Sqrt](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSqrt(child)
      }),
    expr[Cbrt](
      "Cube root",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Cbrt](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCbrt(child)
      }),
    expr[Floor](
      "Floor of a number",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL,
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL),
      (a, conf, p, r) => new UnaryExprMeta[Floor](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuFloor(child)
      }),
    expr[Ceil](
      "Ceiling of a number",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL,
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL),
      (a, conf, p, r) => new UnaryExprMeta[Ceil](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCeil(child)
      }),
    expr[Not](
      "Boolean not operator",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(TypeSig.BOOLEAN, TypeSig.BOOLEAN),
      (a, conf, p, r) => new UnaryExprMeta[Not](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuNot(child)
      }),
    expr[IsNull](
      "Checks if a value is null",
      ExprChecks.unaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryExprMeta[IsNull](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNull(child)
      }),
    expr[IsNotNull](
      "Checks if a value is not null",
      ExprChecks.unaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryExprMeta[IsNotNull](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNotNull(child)
      }),
    expr[IsNaN](
      "Checks if a value is NaN",
      ExprChecks.unaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        TypeSig.DOUBLE + TypeSig.FLOAT, TypeSig.DOUBLE + TypeSig.FLOAT),
      (a, conf, p, r) => new UnaryExprMeta[IsNaN](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuIsNan(child)
      }),
    expr[Rint](
      "Rounds up a double value to the nearest double equal to an integer",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Rint](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuRint(child)
      }),
    expr[BitwiseNot](
      "Returns the bitwise NOT of the operands",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        TypeSig.integral, TypeSig.integral),
      (a, conf, p, r) => new UnaryExprMeta[BitwiseNot](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuBitwiseNot(child)
      }),
    expr[AtLeastNNonNulls](
      "Checks if number of non null/Nan values is greater than a given value",
      ExprChecks.projectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[AtLeastNNonNulls](a, conf, p, r) {
        def convertToGpu(): GpuExpression =
          GpuAtLeastNNonNulls(a.n, childExprs.map(_.convertToGpu()))
      }),
    expr[DateAdd](
      "Returns the date that is num_days after start_date",
      ExprChecks.binaryProjectNotLambda(TypeSig.DATE, TypeSig.DATE,
        ("startDate", TypeSig.DATE, TypeSig.DATE),
        ("days",
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE,
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE)),
      (a, conf, p, r) => new BinaryExprMeta[DateAdd](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDateAdd(lhs, rhs)
      }),
    expr[DateSub](
      "Returns the date that is num_days before start_date",
      ExprChecks.binaryProjectNotLambda(TypeSig.DATE, TypeSig.DATE,
        ("startDate", TypeSig.DATE, TypeSig.DATE),
        ("days",
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE,
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE)),
      (a, conf, p, r) => new BinaryExprMeta[DateSub](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDateSub(lhs, rhs)
      }),
    expr[NaNvl](
      "Evaluates to `left` iff left is not NaN, `right` otherwise",
      ExprChecks.binaryProjectNotLambda(TypeSig.fp, TypeSig.fp,
        ("lhs", TypeSig.fp, TypeSig.fp),
        ("rhs", TypeSig.fp, TypeSig.fp)),
      (a, conf, p, r) => new BinaryExprMeta[NaNvl](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuNaNvl(lhs, rhs)
      }),
    expr[ShiftLeft](
      "Bitwise shift left (<<)",
      ExprChecks.binaryProjectNotLambda(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftLeft](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftLeft(lhs, rhs)
      }),
    expr[ShiftRight](
      "Bitwise shift right (>>)",
      ExprChecks.binaryProjectNotLambda(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftRight](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftRight(lhs, rhs)
      }),
    expr[ShiftRightUnsigned](
      "Bitwise unsigned shift right (>>>)",
      ExprChecks.binaryProjectNotLambda(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftRightUnsigned](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuShiftRightUnsigned(lhs, rhs)
      }),
    expr[BitwiseAnd](
      "Returns the bitwise AND of the operands",
      ExprChecks.binaryProjectNotLambda(TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryExprMeta[BitwiseAnd](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseAnd(lhs, rhs)
      }),
    expr[BitwiseOr](
      "Returns the bitwise OR of the operands",
      ExprChecks.binaryProjectNotLambda(TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryExprMeta[BitwiseOr](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseOr(lhs, rhs)
      }),
    expr[BitwiseXor](
      "Returns the bitwise XOR of the operands",
      ExprChecks.binaryProjectNotLambda(TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryExprMeta[BitwiseXor](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBitwiseXor(lhs, rhs)
      }),
    expr[Coalesce] (
      "Returns the first non-null argument if exists. Otherwise, null",
      ExprChecks.projectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[Coalesce](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuCoalesce(childExprs.map(_.convertToGpu()))
      }),
    expr[Least] (
      "Returns the least value of all parameters, skipping null values",
      ExprChecks.projectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.orderable,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
          TypeSig.orderable))),
      (a, conf, p, r) => new ExprMeta[Least](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuLeast(childExprs.map(_.convertToGpu()))
      }),
    expr[Greatest] (
      "Returns the greatest value of all parameters, skipping null values",
      ExprChecks.projectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.orderable,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
          TypeSig.orderable))),
      (a, conf, p, r) => new ExprMeta[Greatest](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuGreatest(childExprs.map(_.convertToGpu()))
      }),
    expr[Atan](
      "Inverse tangent",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Atan](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAtan(child)
      }),
    expr[Atanh](
      "Inverse hyperbolic tangent",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Atanh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuAtanh(child)
      }),
    expr[Cos](
      "Cosine",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Cos](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCos(child)
      }),
    expr[Exp](
      "Euler's number e raised to a power",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Exp](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuExp(child)
      }),
    expr[Expm1](
      "Euler's number e raised to a power minus 1",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Expm1](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuExpm1(child)
      }),
    expr[InitCap](
      "Returns str with the first letter of each word in uppercase. " +
      "All other letters are in lowercase",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[InitCap](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuInitCap(child)
      }).incompat(CASE_MODIFICATION_INCOMPAT + " Spark also only sees the space character as " +
      "a word deliminator, but this uses more white space characters."),
    expr[Log](
      "Natural log",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLog(child)
      }),
    expr[Log1p](
      "Natural log 1 + expr",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log1p](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuLog(GpuAdd(child, GpuLiteral(1d, DataTypes.DoubleType)))
      }),
    expr[Log2](
      "Log base 2",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log2](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuLogarithm(child, GpuLiteral(2d, DataTypes.DoubleType))
      }),
    expr[Log10](
      "Log base 10",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log10](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuLogarithm(child, GpuLiteral(10d, DataTypes.DoubleType))
      }),
    expr[Logarithm](
      "Log variable base",
      ExprChecks.binaryProjectNotLambda(TypeSig.DOUBLE, TypeSig.DOUBLE,
        ("value", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("base", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      (a, conf, p, r) => new BinaryExprMeta[Logarithm](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          // the order of the parameters is transposed intentionally
          GpuLogarithm(rhs, lhs)
      }),
    expr[Sin](
      "Sine",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Sin](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSin(child)
      }),
    expr[Sinh](
      "Hyperbolic sine",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Sinh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuSinh(child)
      }),
    expr[Cosh](
      "Hyperbolic cosine",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Cosh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCosh(child)
      }),
    expr[Cot](
      "Cotangent",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Cot](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuCot(child)
      }),
    expr[Tanh](
      "Hyperbolic tangent",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Tanh](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuTanh(child)
      }),
    expr[Tan](
      "Tangent",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Tan](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuTan(child)
      }),
    expr[NormalizeNaNAndZero](
      "Normalize NaN and zero",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.FLOAT,
        TypeSig.DOUBLE + TypeSig.FLOAT),
      (a, conf, p, r) => new UnaryExprMeta[NormalizeNaNAndZero](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuNormalizeNaNAndZero(child)
      }),
    expr[KnownFloatingPointNormalized](
      "Tag to prevent redundant normalization",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.FLOAT,
        TypeSig.DOUBLE + TypeSig.FLOAT),
      (a, conf, p, r) => new UnaryExprMeta[KnownFloatingPointNormalized](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuKnownFloatingPointNormalized(child)
      }),
    expr[DateDiff](
      "Returns the number of days from startDate to endDate",
      ExprChecks.binaryProjectNotLambda(TypeSig.INT, TypeSig.INT,
        ("lhs", TypeSig.DATE, TypeSig.DATE),
        ("rhs", TypeSig.DATE, TypeSig.DATE)),
      (a, conf, p, r) => new BinaryExprMeta[DateDiff](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          GpuDateDiff(lhs, rhs)
        }
    }),
    expr[TimeAdd](
      "Adds interval to timestamp",
      ExprChecks.binaryProjectNotLambda(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("start", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("interval", TypeSig.lit(TypeEnum.CALENDAR)
          .withPsNote(TypeEnum.CALENDAR, "month intervals are not supported"),
          TypeSig.CALENDAR)),
      (timeAdd, conf, p, r) => new BinaryExprMeta[TimeAdd](timeAdd, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          GpuOverrides.extractLit(timeAdd.interval).foreach { lit =>
            val intvl = lit.value.asInstanceOf[CalendarInterval]
            if (intvl.months != 0) {
              willNotWorkOnGpu("interval months isn't supported")
            }
          }
          checkTimeZoneId(timeAdd.timeZoneId)
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuTimeAdd(lhs, rhs)
    }),
    expr[DateAddInterval](
      "Adds interval to date",
      ExprChecks.binaryProjectNotLambda(TypeSig.DATE, TypeSig.DATE,
        ("start", TypeSig.DATE, TypeSig.DATE),
        ("interval", TypeSig.lit(TypeEnum.CALENDAR)
          .withPsNote(TypeEnum.CALENDAR, "month intervals are not supported"),
          TypeSig.CALENDAR)),
      (dateAddInterval, conf, p, r) =>
        new BinaryExprMeta[DateAddInterval](dateAddInterval, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            GpuOverrides.extractLit(dateAddInterval.interval).foreach { lit =>
              val intvl = lit.value.asInstanceOf[CalendarInterval]
              if (intvl.months != 0) {
                willNotWorkOnGpu("interval months isn't supported")
              }
            }
            checkTimeZoneId(dateAddInterval.timeZoneId)
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuDateAddInterval(lhs, rhs)
        }),
    expr[DateFormatClass](
      "Converts timestamp to a value of string in the format specified by the date format",
      ExprChecks.binaryProjectNotLambda(TypeSig.STRING, TypeSig.STRING,
        ("timestamp", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("strfmt", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[DateFormatClass](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDateFormatClass(lhs, rhs, strfFormat)
      }
    ),
    expr[ToUnixTimestamp](
      "Returns the UNIX timestamp of the given time",
      ExprChecks.binaryProjectNotLambda(TypeSig.LONG, TypeSig.LONG,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[ToUnixTimestamp](a, conf, p, r){
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          if (conf.isImprovedTimestampOpsEnabled) {
            // passing the already converted strf string for a little optimization
            GpuToUnixTimestampImproved(lhs, rhs, sparkFormat, strfFormat)
          } else {
            GpuToUnixTimestamp(lhs, rhs, sparkFormat, strfFormat)
          }
        }
      }),
    expr[UnixTimestamp](
      "Returns the UNIX timestamp of current or specified time",
      ExprChecks.binaryProjectNotLambda(TypeSig.LONG, TypeSig.LONG,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[UnixTimestamp](a, conf, p, r){
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          if (conf.isImprovedTimestampOpsEnabled) {
            // passing the already converted strf string for a little optimization
            GpuUnixTimestampImproved(lhs, rhs, sparkFormat, strfFormat)
          } else {
            GpuUnixTimestamp(lhs, rhs, sparkFormat, strfFormat)
          }
        }
      }),
    expr[Hour](
      "Returns the hour component of the string/timestamp",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (hour, conf, p, r) => new UnaryExprMeta[Hour](hour, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          checkTimeZoneId(hour.timeZoneId)
        }

        override def convertToGpu(expr: Expression): GpuExpression = GpuHour(expr)
      }),
    expr[Minute](
      "Returns the minute component of the string/timestamp",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (minute, conf, p, r) => new UnaryExprMeta[Minute](minute, conf, p, r) {
        override def tagExprForGpu(): Unit = {
         checkTimeZoneId(minute.timeZoneId)
        }

        override def convertToGpu(expr: Expression): GpuExpression =
          GpuMinute(expr)
      }),
    expr[Second](
      "Returns the second component of the string/timestamp",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (second, conf, p, r) => new UnaryExprMeta[Second](second, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          checkTimeZoneId(second.timeZoneId)
        }

        override def convertToGpu(expr: Expression): GpuExpression =
          GpuSecond(expr)
      }),
    expr[WeekDay](
      "Returns the day of the week (0 = Monday...6=Sunday)",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[WeekDay](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuWeekDay(expr)
      }),
    expr[DayOfWeek](
      "Returns the day of the week (1 = Sunday...7=Saturday)",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfWeek](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuDayOfWeek(expr)
      }),
    expr[LastDay](
      "Returns the last day of the month which the date belongs to",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[LastDay](a, conf, p, r) {
        override def convertToGpu(expr: Expression): GpuExpression =
          GpuLastDay(expr)
      }),
    expr[FromUnixTime](
      "Get the string from a unix timestamp",
      ExprChecks.binaryProjectNotLambda(TypeSig.STRING, TypeSig.STRING,
        ("sec", TypeSig.LONG, TypeSig.LONG),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "Only a limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[FromUnixTime](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          // passing the already converted strf string for a little optimization
          GpuFromUnixTime(lhs, rhs, strfFormat)
      }),
    expr[Pmod](
      "Pmod",
      ExprChecks.binaryProjectNotLambda(TypeSig.integral + TypeSig.fp, TypeSig.numeric,
        ("lhs", TypeSig.integral + TypeSig.fp, TypeSig.numeric),
        ("rhs", TypeSig.integral + TypeSig.fp, TypeSig.numeric)),
      (a, conf, p, r) => new BinaryExprMeta[Pmod](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuPmod(lhs, rhs)
      }),
    expr[Add](
      "Addition",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.numeric, TypeSig.numericAndInterval,
        ("lhs", TypeSig.numeric, TypeSig.numericAndInterval),
        ("rhs", TypeSig.numeric, TypeSig.numericAndInterval)),
      (a, conf, p, r) => new BinaryExprMeta[Add](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuAdd(lhs, rhs)
      }),
    expr[Subtract](
      "Subtraction",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.numeric, TypeSig.numericAndInterval,
        ("lhs", TypeSig.numeric, TypeSig.numericAndInterval),
        ("rhs", TypeSig.numeric, TypeSig.numericAndInterval)),
      (a, conf, p, r) => new BinaryExprMeta[Subtract](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuSubtract(lhs, rhs)
      }),
    expr[Multiply](
      "Multiplication",
      ExprChecks.binaryProjectNotLambda(TypeSig.numeric +
          TypeSig.psNote(TypeEnum.DECIMAL, "Because of Spark's inner workings the full range " +
              "of decimal precision (even for 64-bit values) is not supported."),
        TypeSig.numeric,
        ("lhs", TypeSig.numeric, TypeSig.numeric),
        ("rhs", TypeSig.numeric, TypeSig.numeric)),
      (a, conf, p, r) => new BinaryExprMeta[Multiply](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // Multiplication of Decimal types is a little odd. Spark will cast the inputs
          // to a common wider value where scale is max of the two input scales, and precision is
          // max of the two input non-scale portions + the new scale. Then it will do the multiply,
          // which will produce a return scale that is 2x that of the wider scale, but lie about it
          // in the return type of the Multiply operator. Then in CheckOverflow it will reset the
          // scale and check the precision so that they know it fits in final desired result which
          // is precision1 + precision2 + 1 for precision and scale1 + scale2 for scale, based off
          // of the precision and scale for the original input values. We would like to avoid all
          // of this if possible because having a temporary intermediate value that can have a
          // scale quite a bit larger than the final result reduces the maximum precision that
          // we could support, as we don't have unlimited precision. But sadly because of how
          // the logical plan is compiled down to the physical plan we have lost what the original
          // types were and cannot recover it. As such for now we are going to do what Spark does,
          // but we have to recompute/recheck the temporary precision to be sure it will fit
          // on the GPU.
          val Seq(leftDataType, rightDataType) = childExprs.map(_.dataType)
          (leftDataType, rightDataType) match {
            case (l: DecimalType, r: DecimalType) =>
              val intermediateResult = GpuMultiplyUtil.decimalDataType(l, r)
              if (intermediateResult.precision > DType.DECIMAL64_MAX_PRECISION) {
                willNotWorkOnGpu("The actual output precision of the multiply is too large" +
                    s" to fit on the GPU $intermediateResult")
              }
            case _ => // NOOP
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuMultiply(lhs, rhs)
      }),
    expr[And](
      "Logical AND",
      ExprChecks.binaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
        ("rhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN)),
      (a, conf, p, r) => new BinaryExprMeta[And](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuAnd(lhs, rhs)
      }),
    expr[Or](
      "Logical OR",
      ExprChecks.binaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
        ("rhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN)),
      (a, conf, p, r) => new BinaryExprMeta[Or](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuOr(lhs, rhs)
      }),
    expr[EqualNullSafe](
      "Check if the values are equal including nulls <=>",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all)),
      (a, conf, p, r) => new BinaryExprMeta[EqualNullSafe](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEqualNullSafe(lhs, rhs)
      }),
    expr[EqualTo](
      "Check if the values are equal",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all)),
      (a, conf, p, r) => new BinaryExprMeta[EqualTo](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEqualTo(lhs, rhs)
      }),
    expr[GreaterThan](
      "> operator",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all)),
      (a, conf, p, r) => new BinaryExprMeta[GreaterThan](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuGreaterThan(lhs, rhs)
      }),
    expr[GreaterThanOrEqual](
      ">= operator",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all)),
      (a, conf, p, r) => new BinaryExprMeta[GreaterThanOrEqual](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuGreaterThanOrEqual(lhs, rhs)
      }),
    expr[In](
      "IN operator",
      ExprChecks.projectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        Seq(ParamCheck("value", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
          TypeSig.all)),
        Some(RepeatingParamCheck("list", (TypeSig.commonCudfTypes + TypeSig.DECIMAL).withAllLit(),
          TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[In](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val unaliased = in.list.map(extractLit)
          val hasNullLiteral = unaliased.exists {
            case Some(l) => l.value == null
            case _ => false
          }
          if (hasNullLiteral) {
            willNotWorkOnGpu("nulls are not supported")
          }
        }
        override def convertToGpu(): GpuExpression =
          GpuInSet(childExprs.head.convertToGpu(), in.list.asInstanceOf[Seq[Literal]])
      }),
    expr[InSet](
      "INSET operator",
      ExprChecks.unaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
      (in, conf, p, r) => new ExprMeta[InSet](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (in.hset.contains(null)) {
            willNotWorkOnGpu("nulls are not supported")
          }
        }
        override def convertToGpu(): GpuExpression =
          GpuInSet(childExprs.head.convertToGpu(), in.hset.map(LiteralHelper(_)).toSeq)
      }),
    expr[LessThan](
      "< operator",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all)),
      (a, conf, p, r) => new BinaryExprMeta[LessThan](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuLessThan(lhs, rhs)
      }),
    expr[LessThanOrEqual](
      "<= operator",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all)),
      (a, conf, p, r) => new BinaryExprMeta[LessThanOrEqual](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuLessThanOrEqual(lhs, rhs)
      }),
    expr[CaseWhen](
      "CASE WHEN expression",
      CaseWhenCheck,
      (a, conf, p, r) => new ExprMeta[CaseWhen](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val anyLit = a.branches.exists { case (predicate, _) => isLit(predicate) }
          if (anyLit) {
            willNotWorkOnGpu("literal predicates are not supported")
          }
        }
        override def convertToGpu(): GpuExpression = {
          val branches = childExprs.grouped(2).flatMap {
            case Seq(cond, value) => Some((cond.convertToGpu(), value.convertToGpu()))
            case Seq(_) => None
          }.toArray.toSeq  // force materialization to make the seq serializable
          val elseValue = if (childExprs.size % 2 != 0) {
            Some(childExprs.last.convertToGpu())
          } else {
            None
          }
          GpuCaseWhen(branches, elseValue)
        }
      }),
    expr[If](
      "IF expression",
      ExprChecks.projectNotLambda(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
        TypeSig.all,
        Seq(ParamCheck("predicate", TypeSig.psNote(TypeEnum.BOOLEAN,
          "literal values are not supported"), TypeSig.BOOLEAN),
          ParamCheck("trueValue", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
            TypeSig.all),
          ParamCheck("falseValue", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
            TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[If](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (isLit(a.predicate)) {
            willNotWorkOnGpu(s"literal predicate ${a.predicate} is not supported")
          }
        }
        override def convertToGpu(): GpuExpression = {
          val boolExpr :: trueExpr :: falseExpr :: Nil = childExprs.map(_.convertToGpu())
          GpuIf(boolExpr, trueExpr, falseExpr)
        }
      }),
    expr[Pow](
      "lhs ^ rhs",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        ("lhs", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("rhs", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      (a, conf, p, r) => new BinaryExprMeta[Pow](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuPow(lhs, rhs)
      }),
    expr[Divide](
      "Division",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.DOUBLE +
            TypeSig.psNote(TypeEnum.DECIMAL, "Because of Spark's inner workings the full range " +
                "of decimal precision (even for 64-bit values) is not supported."),
        TypeSig.DOUBLE + TypeSig.DECIMAL,
        ("lhs", TypeSig.DOUBLE + TypeSig.DECIMAL, TypeSig.DOUBLE + TypeSig.DECIMAL),
        ("rhs", TypeSig.DOUBLE + TypeSig.DECIMAL, TypeSig.DOUBLE + TypeSig.DECIMAL)),
      (a, conf, p, r) => new BinaryExprMeta[Divide](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // Division of Decimal types is a little odd. Spark will cast the inputs
          // to a common wider value where scale is max of the two input scales, and precision is
          // max of the two input non-scale portions + the new scale. Then it will do the divide,
          // which the rules for it are a little complex, but lie about it
          // in the return type of the Divide operator. Then in CheckOverflow it will reset the
          // scale and check the precision so that they know it fits in final desired result.
          // We would like to avoid all of this if possible because having a temporary intermediate
          // value that can have a scale quite a bit larger than the final result reduces the
          // maximum precision that we could support, as we don't have unlimited precision. But
          // sadly because of how the logical plan is compiled down to the physical plan we have
          // lost what the original types were and cannot recover it. As such for now we are going
          // to do what Spark does, but we have to recompute/recheck the temporary precision to be
          // sure it will fit on the GPU. In addition to this we have it a little harder because
          // the decimal divide itself will do rounding on the result before it is returned,
          // effectively calculating an extra digit of precision. Because cudf does not support this
          // right now we actually increase the scale (and corresponding precision) to get an extra
          // decimal place so we can round it in GpuCheckOverflow
          val Seq(leftDataType, rightDataType) = childExprs.map(_.dataType)
          (leftDataType, rightDataType) match {
            case (l: DecimalType, r: DecimalType) =>
              val outputType = GpuDivideUtil.decimalDataType(l, r)
              // Case 1: OutputType.precision doesn't get truncated
              //   We will never hit a case where outputType.precision < outputType.scale + r.scale.
              //   So there is no need to protect against that.
              //   The only two cases in which there is a possibility of the intermediary scale
              //   exceeding the intermediary precision is when l.precision < l.scale or l
              //   .precision < 0, both of which aren't possible.
              //   Proof:
              //   case 1:
              //   outputType.precision = p1 - s1 + s2 + s1 + p2 + 1 + 1
              //   outputType.scale = p1 + s2 + p2 + 1 + 1
              //   To find out if outputType.precision < outputType.scale simplifies to p1 < s1,
              //   which is never possible
              //
              //   case 2:
              //   outputType.precision = p1 - s1 + s2 + 6 + 1
              //   outputType.scale = 6 + 1
              //   To find out if outputType.precision < outputType.scale simplifies to p1 < 0
              //   which is never possible
              // Case 2: OutputType.precision gets truncated to 38
              //   In this case we have to make sure the r.precision + l.scale + r.scale + 1 <= 38
              //   Otherwise the intermediate result will overflow
              // TODO We should revisit the proof one more time after we support 128-bit decimals
              if (l.precision + l.scale + r.scale + 1 > 38) {
                willNotWorkOnGpu("The intermediate output precision of the divide is too " +
                  s"large to be supported on the GPU i.e. Decimal(${outputType.precision}, " +
                  s"${outputType.scale + r.scale})")
              } else {
                val intermediateResult =
                  DecimalType(outputType.precision, outputType.scale + r.scale)
                if (intermediateResult.precision > DType.DECIMAL64_MAX_PRECISION) {
                  willNotWorkOnGpu("The actual output precision of the divide is too large" +
                    s" to fit on the GPU $intermediateResult")
                }
              }
            case _ => // NOOP
          }
        }

        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuDivide(lhs, rhs)
      }),
    expr[IntegralDivide](
      "Division with a integer result",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.LONG, TypeSig.LONG,
        ("lhs", TypeSig.LONG + TypeSig.DECIMAL, TypeSig.LONG + TypeSig.DECIMAL),
        ("rhs", TypeSig.LONG + TypeSig.DECIMAL, TypeSig.LONG + TypeSig.DECIMAL)),
      (a, conf, p, r) => new BinaryExprMeta[IntegralDivide](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuIntegralDivide(lhs, rhs)
      }),
    expr[Remainder](
      "Remainder or modulo",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.integral + TypeSig.fp, TypeSig.numeric,
        ("lhs", TypeSig.integral + TypeSig.fp, TypeSig.numeric),
        ("rhs", TypeSig.integral + TypeSig.fp, TypeSig.numeric)),
      (a, conf, p, r) => new BinaryExprMeta[Remainder](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuRemainder(lhs, rhs)
      }),
    expr[AggregateExpression](
      "Aggregate expression",
      ExprChecks.fullAgg(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL +
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.STRUCT),
        TypeSig.all,
        Seq(ParamCheck(
          "aggFunc",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL +
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.STRUCT),
          TypeSig.all)),
        Some(RepeatingParamCheck("filter", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      (a, conf, p, r) => new ExprMeta[AggregateExpression](a, conf, p, r) {
        private val filter: Option[BaseExprMeta[_]] =
          a.filter.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val childrenExprMeta: Seq[BaseExprMeta[Expression]] =
          a.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          childrenExprMeta ++ filter.toSeq

        override def convertToGpu(): GpuExpression = {
          // handle the case AggregateExpression has the resultIds parameter where its
          // Seq[ExprIds] instead of single ExprId.
          val resultId = try {
            val resultMethod = a.getClass.getMethod("resultId")
            resultMethod.invoke(a).asInstanceOf[ExprId]
          } catch {
            case _: Exception =>
              val resultMethod = a.getClass.getMethod("resultIds")
              resultMethod.invoke(a).asInstanceOf[Seq[ExprId]].head
          }
          GpuAggregateExpression(childExprs.head.convertToGpu().asInstanceOf[GpuAggregateFunction],
            a.mode, a.isDistinct, filter.map(_.convertToGpu()), resultId)
        }
      }),
    expr[SortOrder](
      "Sort order",
      ExprChecks.projectOnly(
        pluginSupportedOrderableSig,
        TypeSig.orderable,
        Seq(ParamCheck(
          "input",
          pluginSupportedOrderableSig,
          TypeSig.orderable))),
      (sortOrder, conf, p, r) => new BaseExprMeta[SortOrder](sortOrder, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (isStructType(sortOrder.dataType)) {
            val nullOrdering = sortOrder.nullOrdering
            val directionDefaultNullOrdering = sortOrder.direction.defaultNullOrdering
            val direction = sortOrder.direction.sql
            if (nullOrdering != directionDefaultNullOrdering) {
              willNotWorkOnGpu(s"only default null ordering $directionDefaultNullOrdering " +
                s"for direction $direction is supported for nested types; actual: ${nullOrdering}")
            }
          }
        }

        // One of the few expressions that are not replaced with a GPU version
        override def convertToGpu(): Expression =
          sortOrder.withNewChildren(childExprs.map(_.convertToGpu()))
      }),
    expr[PivotFirst](
      "PivotFirst operator",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL +
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL),
        TypeSig.all,
        Seq(ParamCheck(
          "pivotColumn",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
          TypeSig.all),
          ParamCheck("valueColumn",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
          TypeSig.all))),
      (pivot, conf, p, r) => new ImperativeAggExprMeta[PivotFirst](pivot, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (conf.hasNans &&
            (pivot.pivotColumn.dataType.equals(FloatType) ||
              pivot.pivotColumn.dataType.equals(DoubleType))) {
            willNotWorkOnGpu("Pivot expressions over floating point columns " +
              "that may contain NaN is disabled. You can bypass this by setting " +
              s"${RapidsConf.HAS_NANS}=false")
          }
          // If pivotColumnValues doesn't have distinct values, fall back to CPU
          if (pivot.pivotColumnValues.distinct.lengthCompare(pivot.pivotColumnValues.length) != 0) {
            willNotWorkOnGpu("PivotFirst does not work on the GPU when there are duplicate" +
                " pivot values provided")
          }
        }
        override def convertToGpu(childExprs: Seq[Expression]): GpuExpression = {
          val Seq(pivotColumn, valueColumn) = childExprs
          GpuPivotFirst(pivotColumn, valueColumn, pivot.pivotColumnValues)
        }
      }),
    expr[Count](
      "Count aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.LONG, TypeSig.LONG,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "input", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all))),
      (count, conf, p, r) => new ExprMeta[Count](count, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (count.children.size > 1) {
            willNotWorkOnGpu("count of multiple columns not supported")
          }
        }
        override def convertToGpu(): GpuExpression = GpuCount(childExprs.map(_.convertToGpu()))
      }),
    expr[Max](
      "Max aggregate operator",
      ExprChecksImpl(
        ExprChecks.fullAgg(
          TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.orderable,
          Seq(ParamCheck("input",
            TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.orderable))
        ).asInstanceOf[ExprChecksImpl].contexts
          ++
          ExprChecks.windowOnly(
            TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL, TypeSig.orderable,
            Seq(ParamCheck("input",
              TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL, TypeSig.orderable))
          ).asInstanceOf[ExprChecksImpl].contexts
      ),
      (max, conf, p, r) => new AggExprMeta[Max](max, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val dataType = max.child.dataType
          if (conf.hasNans && (dataType == DoubleType || dataType == FloatType)) {
            willNotWorkOnGpu("Max aggregation on floating point columns that can contain NaNs " +
              "will compute incorrect results. If it is known that there are no NaNs, set " +
              s" ${RapidsConf.HAS_NANS} to false.")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = GpuMax(child)
      }),
    expr[Min](
      "Min aggregate operator",
      ExprChecksImpl(
        ExprChecks.fullAgg(
          TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.orderable,
          Seq(ParamCheck("input",
            TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.orderable))
        ).asInstanceOf[ExprChecksImpl].contexts
          ++
          ExprChecks.windowOnly(
            TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL, TypeSig.orderable,
            Seq(ParamCheck("input",
              TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL, TypeSig.orderable))
          ).asInstanceOf[ExprChecksImpl].contexts
      ),
      (a, conf, p, r) => new AggExprMeta[Min](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val dataType = a.child.dataType
          if (conf.hasNans && (dataType == DoubleType || dataType == FloatType)) {
            willNotWorkOnGpu("Min aggregation on floating point columns that can contain NaNs " +
              "will compute incorrect results. If it is known that there are no NaNs, set " +
              s" ${RapidsConf.HAS_NANS} to false.")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = GpuMin(child)
      }),
    expr[Sum](
      "Sum aggregate operator",
      ExprChecksImpl(
        ExprChecks.fullAgg(
          TypeSig.LONG + TypeSig.DOUBLE, TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL,
          Seq(ParamCheck("input", TypeSig.integral + TypeSig.fp, TypeSig.numeric))
        ).asInstanceOf[ExprChecksImpl].contexts
          ++
          ExprChecks.windowOnly(
            TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL,
            TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL,
            Seq(ParamCheck("input", TypeSig.numeric, TypeSig.numeric))
          ).asInstanceOf[ExprChecksImpl].contexts
      ),
      (a, conf, p, r) => new AggExprMeta[Sum](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val dataType = a.child.dataType
          if (!conf.isFloatAggEnabled && (dataType == DoubleType || dataType == FloatType)) {
            willNotWorkOnGpu("the GPU will sum floating point values in" +
              " parallel and the result is not always identical each time. This can cause some" +
              " Spark queries to produce an incorrect answer if the value is computed more than" +
              " once as part of the same query.  To enable this anyways set" +
              s" ${RapidsConf.ENABLE_FLOAT_AGG} to true.")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = GpuSum(child, a.dataType)
      }),
    expr[Average](
      "Average aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.DOUBLE, TypeSig.DOUBLE + TypeSig.DECIMAL,
        Seq(ParamCheck("input", TypeSig.integral + TypeSig.fp, TypeSig.numeric))),
      (a, conf, p, r) => new AggExprMeta[Average](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val dataType = a.child.dataType
          if (!conf.isFloatAggEnabled && (dataType == DoubleType || dataType == FloatType)) {
            willNotWorkOnGpu("the GPU will sum floating point values in" +
              " parallel to compute an average and the result is not always identical each time." +
              " This can cause some Spark queries to produce an incorrect answer if the value is" +
              " computed more than once as part of the same query. To enable this anyways set" +
              s" ${RapidsConf.ENABLE_FLOAT_AGG} to true")
          }
        }

        override def convertToGpu(child: Expression): GpuExpression = GpuAverage(child)
      }),
    expr[First](
      "first aggregate operator",
      ExprChecks.aggNotWindow(TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all,
        Seq(ParamCheck("input", TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[First](a, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuFirst(childExprs.head.convertToGpu(), a.ignoreNulls)
      }),
    expr[Last](
      "last aggregate operator",
      ExprChecks.aggNotWindow(TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all,
        Seq(ParamCheck("input", TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[Last](a, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuLast(childExprs.head.convertToGpu(), a.ignoreNulls)
      }),
    expr[BRound](
      "Round an expression to d decimal places using HALF_EVEN rounding mode",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.numeric, TypeSig.numeric,
        ("value", TypeSig.numeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.numeric),
        ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      (a, conf, p, r) => new BinaryExprMeta[BRound](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.child.dataType match {
            case FloatType | DoubleType if !conf.isIncompatEnabled =>
              willNotWorkOnGpu("rounding floating point numbers may be slightly off " +
                  s"compared to Spark's result, to enable set ${RapidsConf.INCOMPATIBLE_OPS}")
            case _ => // NOOP
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuBRound(lhs, rhs)
      }),
    expr[Round](
      "Round an expression to d decimal places using HALF_UP rounding mode",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.numeric, TypeSig.numeric,
        ("value", TypeSig.numeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.numeric),
        ("scale", TypeSig.lit(TypeEnum.INT), TypeSig.lit(TypeEnum.INT))),
      (a, conf, p, r) => new BinaryExprMeta[Round](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.child.dataType match {
            case FloatType | DoubleType if !conf.isIncompatEnabled =>
              willNotWorkOnGpu("rounding floating point numbers may be slightly off " +
                  s"compared to Spark's result, to enable set ${RapidsConf.INCOMPATIBLE_OPS}")
            case _ => // NOOP
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuRound(lhs, rhs)
      }),
    expr[PythonUDF](
      "UDF run in an external python process. Does not actually run on the GPU, but " +
          "the transfer of data to/from it can be accelerated.",
      ExprChecks.fullAggAndProject(
        // Different types of Pandas UDF support different sets of output type. Please refer to
        //   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
        // for more details.
        // It is impossible to specify the exact type signature for each Pandas UDF type in a single
        // expression 'PythonUDF'.
        // So use the 'unionOfPandasUdfOut' to cover all types for Spark. The type signature of
        // plugin is also an union of all the types of Pandas UDF.
        (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested() + TypeSig.STRUCT,
        TypeSig.unionOfPandasUdfOut,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "param",
          (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[PythonUDF](a, conf, p, r) {
        override def replaceMessage: String = "not block GPU acceleration"
        override def noReplacementPossibleMessage(reasons: String): String =
          s"blocks running on GPU because $reasons"

        override def convertToGpu(): GpuExpression =
          GpuPythonUDF(a.name, a.func, a.dataType,
            childExprs.map(_.convertToGpu()),
            a.evalType, a.udfDeterministic, a.resultId)
        }),
    expr[Rand](
      "Generate a random column with i.i.d. uniformly distributed values in [0, 1)",
      ExprChecks.projectNotLambda(TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("seed",
          (TypeSig.INT + TypeSig.LONG).withAllLit(),
          (TypeSig.INT + TypeSig.LONG).withAllLit()))),
      (a, conf, p, r) => new UnaryExprMeta[Rand](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuRand(child)
      }),
    expr[SparkPartitionID] (
      "Returns the current partition id",
      ExprChecks.projectNotLambda(TypeSig.INT, TypeSig.INT),
      (a, conf, p, r) => new ExprMeta[SparkPartitionID](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuSparkPartitionID()
      }),
    expr[MonotonicallyIncreasingID] (
      "Returns monotonically increasing 64-bit integers",
      ExprChecks.projectNotLambda(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[MonotonicallyIncreasingID](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuMonotonicallyIncreasingID()
      }),
    expr[InputFileName] (
      "Returns the name of the file being read, or empty string if not available",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new ExprMeta[InputFileName](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileName()
      }),
    expr[InputFileBlockStart] (
      "Returns the start offset of the block being read, or -1 if not available",
      ExprChecks.projectNotLambda(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[InputFileBlockStart](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileBlockStart()
      }),
    expr[InputFileBlockLength] (
      "Returns the length of the block being read, or -1 if not available",
      ExprChecks.projectNotLambda(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[InputFileBlockLength](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileBlockLength()
      }),
    expr[Md5] (
      "MD5 hash operator",
      ExprChecks.unaryProjectNotLambda(TypeSig.STRING, TypeSig.STRING,
        TypeSig.BINARY, TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[Md5](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuMd5(child)
      }),
    expr[Upper](
      "String uppercase operator",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[Upper](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUpper(child)
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Lower](
      "String lowercase operator",
      ExprChecks.unaryProjectNotLambdaInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[Lower](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLower(child)
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[StringLPad](
      "Pad a string on the left",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("len", TypeSig.lit(TypeEnum.INT), TypeSig.INT),
          ParamCheck("pad", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new TernaryExprMeta[StringLPad](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          extractLit(in.pad).foreach { padLit =>
            if (padLit.value != null &&
                padLit.value.asInstanceOf[UTF8String].toString.length != 1) {
              willNotWorkOnGpu("only a single character is supported for pad")
            }
          }
        }
        override def convertToGpu(
            str: Expression,
            width: Expression,
            pad: Expression): GpuExpression =
          GpuStringLPad(str, width, pad)
      }),
    expr[StringRPad](
      "Pad a string on the right",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("len", TypeSig.lit(TypeEnum.INT), TypeSig.INT),
          ParamCheck("pad", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new TernaryExprMeta[StringRPad](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          extractLit(in.pad).foreach { padLit =>
            if (padLit.value != null &&
                padLit.value.asInstanceOf[UTF8String].toString.length != 1) {
              willNotWorkOnGpu("only a single character is supported for pad")
            }
          }
        }
        override def convertToGpu(
            str: Expression,
            width: Expression,
            pad: Expression): GpuExpression =
          GpuStringRPad(str, width, pad)
      }),
    expr[StringSplit](
       "Splits `str` around occurrences that match `regex`",
      ExprChecks.projectNotLambda(TypeSig.ARRAY.nested(TypeSig.STRING),
        TypeSig.ARRAY.nested(TypeSig.STRING),
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING)
              .withPsNote(TypeEnum.STRING, "very limited subset of regex supported"),
            TypeSig.STRING),
          ParamCheck("limit", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new GpuStringSplitMeta(in, conf, p, r)),
    expr[GetStructField](
      "Gets the named field of the struct",
      ExprChecks.unaryProjectNotLambda(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.NULL +
            TypeSig.DECIMAL).nested(),
        TypeSig.all,
        TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP + TypeSig.NULL + TypeSig.DECIMAL),
        TypeSig.STRUCT.nested(TypeSig.all)),
      (expr, conf, p, r) => new UnaryExprMeta[GetStructField](expr, conf, p, r) {
        override def convertToGpu(arr: Expression): GpuExpression =
          GpuGetStructField(arr, expr.ordinal, expr.name)
      }),
    expr[GetArrayItem](
      "Gets the field at `ordinal` in the Array",
      ExprChecks.binaryProjectNotLambda(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
            TypeSig.DECIMAL + TypeSig.MAP).nested(),
        TypeSig.all,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("ordinal", TypeSig.lit(TypeEnum.INT), TypeSig.INT)),
      (in, conf, p, r) => new GpuGetArrayItemMeta(in, conf, p, r)),
    expr[GetMapValue](
      "Gets Value from a Map based on a key",
      ExprChecks.binaryProjectNotLambda(TypeSig.STRING, TypeSig.all,
        ("map", TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.all)),
        ("key", TypeSig.lit(TypeEnum.STRING), TypeSig.all)),
      (in, conf, p, r) => new GpuGetMapValueMeta(in, conf, p, r)),
    expr[CreateNamedStruct](
      "Creates a struct with the given field names and values",
      CreateNamedStructCheck,
      (in, conf, p, r) => new ExprMeta[CreateNamedStruct](in, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuCreateNamedStruct(childExprs.map(_.convertToGpu()))
      }),
    expr[ArrayContains](
      "Returns a boolean if the array contains the passed in key",
      ExprChecks.binaryProjectNotLambda(
        TypeSig.BOOLEAN,
        TypeSig.BOOLEAN,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL),
          TypeSig.ARRAY.nested(TypeSig.all)),
        ("key", TypeSig.commonCudfTypes, TypeSig.all)),
      (in, conf, p, r) => new BinaryExprMeta[ArrayContains](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // do not support literal arrays as LHS
          if (extractLit(in.left).isDefined) {
            willNotWorkOnGpu("Literal arrays are not supported for array_contains")
          }

          val rhsVal = extractLit(in.right)
          val mightHaveNans = (in.right.dataType, rhsVal) match {
            case (FloatType, Some(f: Literal)) => f.value.asInstanceOf[Float].isNaN
            case (DoubleType, Some(d: Literal)) => d.value.asInstanceOf[Double].isNaN
            case (FloatType | DoubleType, None) => conf.hasNans // RHS is a column
            case _ => false
          }
          if (mightHaveNans) {
            willNotWorkOnGpu("Comparisons with NaN values are not supported and" +
              "will compute incorrect results. If it is known that there are no NaNs, set " +
              s" ${RapidsConf.HAS_NANS} to false.")
          }
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuArrayContains(lhs, rhs)
      }),
    expr[CreateArray](
      " Returns an array with the given elements",
      ExprChecks.projectNotLambda(
        TypeSig.ARRAY.nested(TypeSig.numeric + TypeSig.NULL + TypeSig.STRING +
            TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("arg",
          TypeSig.numeric + TypeSig.NULL + TypeSig.STRING +
              TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP,
          TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[CreateArray](in, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuCreateArray(childExprs.map(_.convertToGpu()), wrapped.useStringTypeWhenEmpty)
      }),
    expr[StringLocate](
      "Substring search operator",
      ExprChecks.projectNotLambda(TypeSig.INT, TypeSig.INT,
        Seq(ParamCheck("substr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("start", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new TernaryExprMeta[StringLocate](in, conf, p, r) {
        override def convertToGpu(
            val0: Expression,
            val1: Expression,
            val2: Expression): GpuExpression =
          GpuStringLocate(val0, val1, val2)
      }),
    expr[Substring](
      "Substring operator",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
          ParamCheck("pos", TypeSig.lit(TypeEnum.INT), TypeSig.INT),
          ParamCheck("len", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new TernaryExprMeta[Substring](in, conf, p, r) {
        override def convertToGpu(
            column: Expression,
            position: Expression,
            length: Expression): GpuExpression =
          GpuSubstring(column, position, length)
      }),
    expr[SubstringIndex](
      "substring_index operator",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("delim", TypeSig.lit(TypeEnum.STRING)
              .withPsNote(TypeEnum.STRING, "only a single character is allowed"), TypeSig.STRING),
          ParamCheck("count", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new SubstringIndexMeta(in, conf, p, r)),
    expr[StringReplace](
      "StringReplace operator",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("replace", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new TernaryExprMeta[StringReplace](in, conf, p, r) {
        override def convertToGpu(
            column: Expression,
            target: Expression,
            replace: Expression): GpuExpression =
          GpuStringReplace(column, target, replace)
      }),
    expr[StringTrim](
      "StringTrim operator",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new String2TrimExpressionMeta[StringTrim](in, conf, p, r) {
        override def convertToGpu(
            column: Expression,
            target: Option[Expression] = None): GpuExpression =
          GpuStringTrim(column, target)
      }),
    expr[StringTrimLeft](
      "StringTrimLeft operator",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) =>
        new String2TrimExpressionMeta[StringTrimLeft](in, conf, p, r) {
          override def convertToGpu(
            column: Expression,
            target: Option[Expression] = None): GpuExpression =
            GpuStringTrimLeft(column, target)
        }),
    expr[StringTrimRight](
      "StringTrimRight operator",
      ExprChecks.projectNotLambda(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) =>
        new String2TrimExpressionMeta[StringTrimRight](in, conf, p, r) {
          override def convertToGpu(
              column: Expression,
              target: Option[Expression] = None): GpuExpression =
            GpuStringTrimRight(column, target)
        }),
    expr[StartsWith](
      "Starts with",
      ExprChecks.binaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[StartsWith](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuStartsWith(lhs, rhs)
      }),
    expr[EndsWith](
      "Ends with",
      ExprChecks.binaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[EndsWith](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuEndsWith(lhs, rhs)
      }),
    expr[Concat](
      "String concatenate NO separator",
      ExprChecks.projectNotLambda(TypeSig.STRING,
        (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input", TypeSig.STRING,
          (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all)))),
      (a, conf, p, r) => new ComplexTypeMergingExprMeta[Concat](a, conf, p, r) {
        override def convertToGpu(child: Seq[Expression]): GpuExpression = GpuConcat(child)
      }),
    expr[Murmur3Hash] (
      "Murmur3 hash operator",
      ExprChecks.projectNotLambda(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[Murmur3Hash](a, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] = a.children
          .map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        def convertToGpu(): GpuExpression =
          GpuMurmur3Hash(childExprs.map(_.convertToGpu()), a.seed)
      }),
    expr[Contains](
      "Contains",
      ExprChecks.binaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[Contains](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuContains(lhs, rhs)
      }),
    expr[Like](
      "Like",
      ExprChecks.binaryProjectNotLambda(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[Like](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuLike(lhs, rhs, a.escapeChar)
      }),
    expr[Length](
      "String character length or binary byte length",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT,
        TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[Length](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuLength(child)
      }),
    expr[Size](
      "The size of an array or a map",
      ExprChecks.unaryProjectNotLambda(TypeSig.INT, TypeSig.INT,
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL
            + TypeSig.DECIMAL + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      (a, conf, p, r) => new UnaryExprMeta[Size](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuSize(child, a.legacySizeOfNull)
      }),
    expr[UnscaledValue](
      "Convert a Decimal to an unscaled long value for some aggregation optimizations",
      ExprChecks.unaryProject(TypeSig.LONG, TypeSig.LONG,
        TypeSig.DECIMAL, TypeSig.DECIMAL),
      (a, conf, p, r) => new UnaryExprMeta[UnscaledValue](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUnscaledValue(child)
      }),
    expr[MakeDecimal](
      "Create a Decimal from an unscaled long value for some aggregation optimizations",
      ExprChecks.unaryProject(TypeSig.DECIMAL, TypeSig.DECIMAL,
        TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new UnaryExprMeta[MakeDecimal](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression =
          GpuMakeDecimal(child, a.precision, a.scale, a.nullOnOverflow)
      }),
    expr[Explode](
      "Given an input array produces a sequence of rows for each value in the array.",
      ExprChecks.unaryProject(
        // Here is a walk-around representation, since multi-level nested type is not supported yet.
        // related issue: https://github.com/NVIDIA/spark-rapids/issues/1901
        TypeSig.ARRAY.nested(TypeSig.STRUCT
          + TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL + TypeSig.ARRAY),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.ARRAY.nested(
          TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL + TypeSig.ARRAY),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      (a, conf, p, r) => new GeneratorExprMeta[Explode](a, conf, p, r) {
        override val supportOuter: Boolean = true
        override def convertToGpu(): GpuExpression = GpuExplode(childExprs.head.convertToGpu())
      }),
    expr[PosExplode](
      "Given an input array produces a sequence of rows for each value in the array.",
      ExprChecks.unaryProject(
        // Here is a walk-around representation, since multi-level nested type is not supported yet.
        // related issue: https://github.com/NVIDIA/spark-rapids/issues/1901
        TypeSig.ARRAY.nested(TypeSig.STRUCT
          + TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL + TypeSig.ARRAY),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.ARRAY.nested(
          TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL + TypeSig.ARRAY),
        TypeSig.ARRAY.nested(
          TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL + TypeSig.ARRAY)),
      (a, conf, p, r) => new GeneratorExprMeta[PosExplode](a, conf, p, r) {
        override val supportOuter: Boolean = true
        override def convertToGpu(): GpuExpression = GpuPosExplode(childExprs.head.convertToGpu())
      }),
    expr[CollectList](
      "Collect a list of elements, now only supported by windowing.",
      // It should be 'fullAgg' eventually but now only support windowing,
      // so 'aggNotGroupByOrReduction'
      ExprChecks.aggNotGroupByOrReduction(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(ParamCheck("input",
          TypeSig.commonCudfTypes + TypeSig.DECIMAL +
            TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL),
          TypeSig.all))),
      (c, conf, p, r) => new ExprMeta[CollectList](c, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuCollectList(
          childExprs.head.convertToGpu(), c.mutableAggBufferOffset, c.inputAggBufferOffset)
      }),
    expr[GetJsonObject](
      "Extracts a json object from path",
      ExprChecks.projectOnly(
        TypeSig.STRING, TypeSig.STRING, Seq(ParamCheck("json", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("path", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (a, conf, p, r) => new BinaryExprMeta[GetJsonObject](a, conf, p, r) {
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
          GpuGetJsonObject(lhs, rhs)
      }
    ),
    expr[ScalarSubquery](
      "Subquery that will return only one row and one column",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL,
        Nil, None),
      (a, conf, p, r) => new ExprMeta[ScalarSubquery](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuScalarSubquery(a.plan, a.exprId)
      }
    ),
    GpuScalaUDF.exprMeta
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  // Shim expressions should be last to allow overrides with shim-specific versions
  val expressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
    commonExpressions ++ GpuHiveOverrides.exprs ++ ShimLoader.getSparkShims.getExprs ++
      TimeStamp.getExprs

  def wrapScan[INPUT <: Scan](
      scan: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): ScanMeta[INPUT] =
    scans.get(scan.getClass)
      .map(r => r.wrap(scan, conf, parent, r).asInstanceOf[ScanMeta[INPUT]])
      .getOrElse(new RuleNotFoundScanMeta(scan, conf, parent))

  val commonScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[CSVScan](
      "CSV parsing",
      (a, conf, p, r) => new ScanMeta[CSVScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = GpuCSVScan.tagSupport(this)

        override def convertToGpu(): Scan =
          GpuCSVScan(a.sparkSession,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf.maxReadBatchSizeRows,
            conf.maxReadBatchSizeBytes)
      })).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  val scans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] =
    commonScans ++ ShimLoader.getSparkShims.getScans

  def wrapPart[INPUT <: Partitioning](
      part: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): PartMeta[INPUT] =
    parts.get(part.getClass)
      .map(r => r.wrap(part, conf, parent, r).asInstanceOf[PartMeta[INPUT]])
      .getOrElse(new RuleNotFoundPartMeta(part, conf, parent))

  val parts : Map[Class[_ <: Partitioning], PartRule[_ <: Partitioning]] = Seq(
    part[HashPartitioning](
      "Hash based partitioning",
      // This needs to match what murmur3 supports.
      PartChecks(RepeatingParamCheck("hash_key",
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.STRUCT).nested(),
        TypeSig.all)),
      (hp, conf, p, r) => new PartMeta[HashPartitioning](hp, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          hp.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override def convertToGpu(): GpuPartitioning =
          GpuHashPartitioning(childExprs.map(_.convertToGpu()), hp.numPartitions)
      }),
    part[RangePartitioning](
      "Range partitioning",
      PartChecks(RepeatingParamCheck("order_key",
        pluginSupportedOrderableSig +
            TypeSig.psNote(TypeEnum.STRUCT, "Only supported for a single partition"),
        TypeSig.orderable)),
      (rp, conf, p, r) => new PartMeta[RangePartitioning](rp, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          rp.ordering.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override def convertToGpu(): GpuPartitioning = {
          if (rp.numPartitions > 1) {
            val gpuOrdering = childExprs.map(_.convertToGpu()).asInstanceOf[Seq[SortOrder]]
            GpuRangePartitioning(gpuOrdering, rp.numPartitions)
          } else {
            GpuSinglePartitioning
          }
        }
      }),
    part[RoundRobinPartitioning](
      "Round robin partitioning",
      PartChecks(),
      (rrp, conf, p, r) => new PartMeta[RoundRobinPartitioning](rrp, conf, p, r) {
        override def convertToGpu(): GpuPartitioning = {
          GpuRoundRobinPartitioning(rrp.numPartitions)
        }
      }),
    part[SinglePartition.type](
      "Single partitioning",
      PartChecks(),
      (sp, conf, p, r) => new PartMeta[SinglePartition.type](sp, conf, p, r) {
        override def convertToGpu(): GpuPartitioning = GpuSinglePartitioning
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Partitioning]), r)).toMap

  def wrapDataWriteCmds[INPUT <: DataWritingCommand](
      writeCmd: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): DataWritingCommandMeta[INPUT] =
    dataWriteCmds.get(writeCmd.getClass)
      .map(r => r.wrap(writeCmd, conf, parent, r).asInstanceOf[DataWritingCommandMeta[INPUT]])
      .getOrElse(new RuleNotFoundDataWritingCommandMeta(writeCmd, conf, parent))

  val dataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = Seq(
    dataWriteCmd[InsertIntoHadoopFsRelationCommand](
      "Write to Hadoop filesystem",
      (a, conf, p, r) => new InsertIntoHadoopFsRelationCommandMeta(a, conf, p, r)),
    dataWriteCmd[CreateDataSourceTableAsSelectCommand](
      "Create table with select command",
      (a, conf, p, r) => new CreateDataSourceTableAsSelectCommandMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap

  def wrapPlan[INPUT <: SparkPlan](
      plan: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): SparkPlanMeta[INPUT]  =
    execs.get(plan.getClass)
      .map(r => r.wrap(plan, conf, parent, r).asInstanceOf[SparkPlanMeta[INPUT]])
      .getOrElse(new RuleNotFoundSparkPlanMeta(plan, conf, parent))

  val commonExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    exec[GenerateExec] (
      "The backend for operations that generate more output rows than input rows like explode",
      ExecChecks(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.ARRAY.nested(
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY),
        TypeSig.all),
      (gen, conf, p, r) => new GpuGenerateExecSparkPlanMeta(gen, conf, p, r)),
    exec[ProjectExec](
      "The backend for most select, withColumn and dropColumn statements",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL).nested(),
        TypeSig.all),
      (proj, conf, p, r) => {
        new SparkPlanMeta[ProjectExec](proj, conf, p, r) {
          override def convertToGpu(): GpuExec = GpuProjectExec(
            // Force list to avoid recursive Java serialization of lazy list Seq implementation
            childExprs.map(_.convertToGpu()).toList,
            childPlans.head.convertIfNeeded()
          )
        }
      }),
    exec[RangeExec](
      "The backend for range operator",
      ExecChecks(TypeSig.LONG, TypeSig.LONG),
      (range, conf, p, r) => {
        new SparkPlanMeta[RangeExec](range, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuRangeExec(range.range, conf.gpuTargetBatchSizeBytes)
        }
      }),
    exec[BatchScanExec](
      "The backend for most file input",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.DECIMAL).nested(),
        TypeSig.all),
      (p, conf, parent, r) => new SparkPlanMeta[BatchScanExec](p, conf, parent, r) {
        override val childScans: scala.Seq[ScanMeta[_]] =
          Seq(GpuOverrides.wrapScan(p.scan, conf, Some(this)))

        override def convertToGpu(): GpuExec =
          GpuBatchScanExec(p.output, childScans.head.convertToGpu())
      }),
    exec[CoalesceExec](
      "The backend for the dataframe coalesce method",
      ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL, TypeSig.all),
      (coalesce, conf, parent, r) => new SparkPlanMeta[CoalesceExec](coalesce, conf, parent, r) {
        override def convertToGpu(): GpuExec =
          GpuCoalesceExec(coalesce.numPartitions, childPlans.head.convertIfNeeded())
      }),
    exec[DataWritingCommandExec](
      "Writing data",
      ExecChecks((TypeSig.commonCudfTypes +
        TypeSig.DECIMAL.withPsNote(TypeEnum.DECIMAL, "Only supported for Parquet")).nested(),
        TypeSig.all),
      (p, conf, parent, r) => new SparkPlanMeta[DataWritingCommandExec](p, conf, parent, r) {
        override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
          Seq(GpuOverrides.wrapDataWriteCmds(p.cmd, conf, Some(this)))

        override def convertToGpu(): GpuExec =
          GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(),
            childPlans.head.convertIfNeeded())
      }),
    exec[TakeOrderedAndProjectExec](
      "Take the first limit elements as defined by the sortOrder, and do projection if needed.",
      ExecChecks(pluginSupportedOrderableSig, TypeSig.all),
      (takeExec, conf, p, r) =>
        new SparkPlanMeta[TakeOrderedAndProjectExec](takeExec, conf, p, r) {
          val sortOrder: Seq[BaseExprMeta[SortOrder]] =
            takeExec.sortOrder.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          val projectList: Seq[BaseExprMeta[NamedExpression]] =
            takeExec.projectList.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] = sortOrder ++ projectList

          override def convertToGpu(): GpuExec = {
            // To avoid metrics confusion we split a single stage up into multiple parts
            val so = sortOrder.map(_.convertToGpu().asInstanceOf[SortOrder])
            GpuTopN(takeExec.limit,
              so,
              projectList.map(_.convertToGpu().asInstanceOf[NamedExpression]),
              ShimLoader.getSparkShims.getGpuShuffleExchangeExec(GpuSinglePartitioning,
                GpuTopN(takeExec.limit,
                  so,
                  takeExec.child.output,
                  childPlans.head.convertIfNeeded())))
          }
        }),
    exec[LocalLimitExec](
      "Per-partition limiting of results",
      ExecChecks(TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.NULL,
        TypeSig.all),
      (localLimitExec, conf, p, r) =>
        new SparkPlanMeta[LocalLimitExec](localLimitExec, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuLocalLimitExec(localLimitExec.limit, childPlans.head.convertIfNeeded())
        }),
    exec[GlobalLimitExec](
      "Limiting of results across partitions",
      ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
      (globalLimitExec, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimitExec, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(globalLimitExec.limit, childPlans.head.convertIfNeeded())
        }),
    exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      ExecChecks(pluginSupportedOrderableSig, TypeSig.all),
      (collectLimitExec, conf, p, r) => new GpuCollectLimitMeta(collectLimitExec, conf, p, r))
        .disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
          "of rows in a batch it could help by limiting the number of rows transferred from " +
          "GPU to CPU"),
    exec[FilterExec](
      "The backend for most filter statements",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.DECIMAL).nested(), TypeSig.all),
      (filter, conf, p, r) => new SparkPlanMeta[FilterExec](filter, conf, p, r) {
        override def convertToGpu(): GpuExec =
          GpuFilterExec(childExprs.head.convertToGpu(), childPlans.head.convertIfNeeded())
      }),
    exec[ShuffleExchangeExec](
      "The backend for most data being exchanged between processes",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
        TypeSig.STRUCT).nested()
          .withPsNote(TypeEnum.STRUCT, "Round-robin partitioning is not supported for nested " +
              s"structs if ${SQLConf.SORT_BEFORE_REPARTITION.key} is true")
          .withPsNote(TypeEnum.ARRAY, "Round-robin partitioning is not supported if " +
              s"${SQLConf.SORT_BEFORE_REPARTITION.key} is true")
          .withPsNote(TypeEnum.MAP, "Round-robin partitioning is not supported if " +
              s"${SQLConf.SORT_BEFORE_REPARTITION.key} is true"),
        TypeSig.all),
      (shuffle, conf, p, r) => new GpuShuffleMeta(shuffle, conf, p, r)),
    exec[UnionExec](
      "The backend for the union operator",
      ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL +
        TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL)
        .withPsNote(TypeEnum.STRUCT,
          "unionByName will not optionally impute nulls for missing struct fields  " +
          "when the column is a struct and there are non-overlapping fields"), TypeSig.all),
      (union, conf, p, r) => new SparkPlanMeta[UnionExec](union, conf, p, r) {
        override def convertToGpu(): GpuExec =
          GpuUnionExec(childPlans.map(_.convertIfNeeded()))
      }),
    exec[BroadcastExchangeExec](
      "The backend for broadcast exchange of data",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
          TypeSig.STRUCT).nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL)
        , TypeSig.all),
      (exchange, conf, p, r) => new GpuBroadcastMeta(exchange, conf, p, r)),
    exec[BroadcastNestedLoopJoinExec](
      "Implementation of join using brute force",
      ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
      (join, conf, p, r) => new GpuBroadcastNestedLoopJoinMeta(join, conf, p, r)),
    exec[CartesianProductExec](
      "Implementation of join using brute force",
      ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
      (join, conf, p, r) => new SparkPlanMeta[CartesianProductExec](join, conf, p, r) {
        val condition: Option[BaseExprMeta[_]] =
          join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] = condition.toSeq

        override def convertToGpu(): GpuExec = {
          val Seq(left, right) = childPlans.map(_.convertIfNeeded())
          GpuCartesianProductExec(
            left,
            right,
            condition.map(_.convertToGpu()),
            conf.gpuTargetBatchSizeBytes)
        }
      }),
    exec[HashAggregateExec](
      "The backend for hash based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.MAP + TypeSig.ARRAY)
          .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL),
        TypeSig.all),
      (agg, conf, p, r) => new GpuHashAggregateMeta(agg, conf, p, r)),
    exec[SortAggregateExec](
      "The backend for sort based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.MAP)
            .nested(TypeSig.STRING),
        TypeSig.all),
      (agg, conf, p, r) => new GpuSortAggregateMeta(agg, conf, p, r)),
    exec[SortExec](
      "The backend for the sort operator",
      // The SortOrder TypeSig will govern what types can actually be used as sorting key data type.
      // The types below are allowed as inputs and outputs.
      ExecChecks(pluginSupportedOrderableSig + (TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      (sort, conf, p, r) => new GpuSortMeta(sort, conf, p, r)),
    exec[ExpandExec](
      "The backend for the expand operator",
      ExecChecks(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL, TypeSig.all),
      (expand, conf, p, r) => new GpuExpandExecMeta(expand, conf, p, r)),
    exec[WindowExec](
      "Window-operator backend",
      ExecChecks(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL +
          TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL) +
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL + TypeSig.STRUCT),
        TypeSig.all),
      (windowOp, conf, p, r) =>
        new GpuWindowExecMeta(windowOp, conf, p, r)
    ),
    exec[CustomShuffleReaderExec](
      "A wrapper of shuffle query stage",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL + TypeSig.ARRAY +
        TypeSig.STRUCT).nested(), TypeSig.all),
      (exec, conf, p, r) =>
      new SparkPlanMeta[CustomShuffleReaderExec](exec, conf, p, r) {
        override def tagPlanForGpu(): Unit = {
          if (!exec.child.supportsColumnar) {
            willNotWorkOnGpu(
              "Unable to replace CustomShuffleReader due to child not being columnar")
          }
        }

        override def convertToGpu(): GpuExec = {
          GpuCustomShuffleReaderExec(childPlans.head.convertIfNeeded(),
            exec.partitionSpecs)
        }
      }),
    exec[FlatMapCoGroupsInPandasExec](
      "The backend for CoGrouped Aggregation Pandas UDF, it runs on CPU itself now but supports" +
        " scheduling GPU resources for the Python process when enabled",
      ExecChecks.hiddenHack(),
      (flatCoPy, conf, p, r) => new GpuFlatMapCoGroupsInPandasExecMeta(flatCoPy, conf, p, r))
        .disabledByDefault("Performance is not ideal now"),
    neverReplaceExec[AlterNamespaceSetPropertiesExec]("Namespace metadata operation"),
    neverReplaceExec[CreateNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[DescribeNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[DropNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[SetCatalogAndNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[ShowCurrentNamespaceExec]("Namespace metadata operation"),
    neverReplaceExec[ShowNamespacesExec]("Namespace metadata operation"),
    neverReplaceExec[ExecutedCommandExec]("Table metadata operation"),
    neverReplaceExec[AlterTableExec]("Table metadata operation"),
    neverReplaceExec[CreateTableExec]("Table metadata operation"),
    neverReplaceExec[DeleteFromTableExec]("Table metadata operation"),
    neverReplaceExec[DescribeTableExec]("Table metadata operation"),
    neverReplaceExec[DropTableExec]("Table metadata operation"),
    neverReplaceExec[AtomicReplaceTableExec]("Table metadata operation"),
    neverReplaceExec[RefreshTableExec]("Table metadata operation"),
    neverReplaceExec[RenameTableExec]("Table metadata operation"),
    neverReplaceExec[ReplaceTableExec]("Table metadata operation"),
    neverReplaceExec[ShowTablePropertiesExec]("Table metadata operation"),
    neverReplaceExec[ShowTablesExec]("Table metadata operation"),
    neverReplaceExec[AdaptiveSparkPlanExec]("Wrapper for adaptive query plan"),
    neverReplaceExec[BroadcastQueryStageExec]("Broadcast query stage"),
    neverReplaceExec[ShuffleQueryStageExec]("Shuffle query stage")
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
  val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    commonExecs ++ ShimLoader.getSparkShims.getExecs

  def getTimeParserPolicy: TimeParserPolicy = {
    val policy = SQLConf.get.getConfString(SQLConf.LEGACY_TIME_PARSER_POLICY.key, "EXCEPTION")
    policy match {
      case "LEGACY" => LegacyTimeParserPolicy
      case "EXCEPTION" => ExceptionTimeParserPolicy
      case "CORRECTED" => CorrectedTimeParserPolicy
    }
  }

}
/** Tag the initial plan when AQE is enabled */
case class GpuQueryStagePrepOverrides() extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan) :SparkPlan = {
    // Note that we disregard the GPU plan returned here and instead rely on side effects of
    // tagging the underlying SparkPlan.
    GpuOverrides().apply(plan)
    // return the original plan which is now modified as a side-effect of invoking GpuOverrides
    plan
  }
}

case class GpuOverrides() extends Rule[SparkPlan] with Logging {

  // Spark calls this method once for the whole plan when AQE is off. When AQE is on, it
  // gets called once for each query stage (where a query stage is an `Exchange`).
  override def apply(plan: SparkPlan) :SparkPlan = {
    val conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled) {
      val updatedPlan = if (plan.conf.adaptiveExecutionEnabled) {
        // AQE can cause Spark to inject undesired CPU shuffles into the plan because GPU and CPU
        // distribution expressions are not semantically equal.
        GpuOverrides.removeExtraneousShuffles(plan, conf)
      } else {
        plan
      }
      applyOverrides(updatedPlan, conf)
    } else {
      plan
    }
  }

  private def applyOverrides(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    val wrap = GpuOverrides.wrapPlan(plan, conf, None)
    wrap.tagForGpu()
    val reasonsToNotReplaceEntirePlan = wrap.getReasonsNotToReplaceEntirePlan
    val exp = conf.explain
    if (conf.allowDisableEntirePlan && reasonsToNotReplaceEntirePlan.nonEmpty) {
      if (!exp.equalsIgnoreCase("NONE")) {
        logWarning("Can't replace any part of this plan due to: " +
            s"${reasonsToNotReplaceEntirePlan.mkString(",")}")
      }
      plan
    } else {
      val optimizations = if (conf.optimizerEnabled) {
        // we need to run these rules both before and after CBO because the cost
        // is impacted by forcing operators onto CPU due to other rules that we have
        wrap.runAfterTagRules()
        val optimizer = try {
          Class.forName(conf.optimizerClassName).newInstance().asInstanceOf[Optimizer]
        } catch {
          case e: Exception =>
            throw new RuntimeException(s"Failed to create optimizer ${conf.optimizerClassName}", e)
        }
        optimizer.optimize(conf, wrap)
      } else {
        Seq.empty
      }
      wrap.runAfterTagRules()
      if (!exp.equalsIgnoreCase("NONE")) {
        wrap.tagForExplain()
        val explain = wrap.explain(exp.equalsIgnoreCase("ALL"))
        if (explain.nonEmpty) {
          logWarning(s"\n$explain")
          if (conf.optimizerExplain.equalsIgnoreCase("ALL") && optimizations.nonEmpty) {
            logWarning(s"Cost-based optimizations applied:\n${optimizations.mkString("\n")}")
          }
        }
      }
      val convertedPlan = wrap.convertIfNeeded()
      val sparkPlan = addSortsIfNeeded(convertedPlan, conf)
      GpuOverrides.listeners.foreach(_.optimizedPlan(wrap, sparkPlan, optimizations))
      sparkPlan
    }
  }

  private final class SortDataFromReplacementRule extends DataFromReplacementRule {
    override val operationName: String = "Exec"
    override def confKey = "spark.rapids.sql.exec.SortExec"

    override def getChecks: Option[TypeChecks[_]] = None
  }

  // copied from Spark EnsureRequirements but only does the ordering checks and
  // check to convert any SortExec added to GpuSortExec
  private def ensureOrdering(operator: SparkPlan, conf: RapidsConf): SparkPlan = {
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildOrderings.length == children.length)

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (GpuOverrides.orderingSatisfies(child.outputOrdering, requiredOrdering, conf)) {
        child
      } else {
        val sort = SortExec(requiredOrdering, global = false, child = child)
        // just specifically check Sort to see if we can change Sort to GPUSort
        val sortMeta = new GpuSortMeta(sort, conf, None, new SortDataFromReplacementRule)
        sortMeta.initReasons()
        sortMeta.tagPlanForGpu()
        if (sortMeta.canThisBeReplaced) {
          sortMeta.convertToGpu()
        } else {
          sort
        }
      }
    }
    operator.withNewChildren(children)
  }

  def addSortsIfNeeded(plan: SparkPlan, conf: RapidsConf): SparkPlan = {
    plan.transformUp {
      case operator: SparkPlan =>
        ensureOrdering(operator, conf)
    }
  }
}
