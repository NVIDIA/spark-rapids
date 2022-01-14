/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.RapidsConf.{SUPPRESS_PLANNING_FAILURE, TEST_CONF}
import com.nvidia.spark.rapids.shims.v2._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.python.{AggregateInPandasExec, ArrowEvalPythonExec, FlatMapGroupsInPandasExec, WindowInPandasExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.rapids.GpuHiveOverrides
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.rapids.execution._
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
abstract class ReplacementRule[INPUT <: BASE, BASE, WRAP_TYPE <: RapidsMeta[INPUT, BASE]](
    protected var doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _]],
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
      Option[RapidsMeta[_, _]],
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
      parent: Option[RapidsMeta[_, _]],
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
        Option[RapidsMeta[_, _]],
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
/*
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
*/
/**
 * Holds everything that is needed to replace a `Partitioning` with a GPU enabled version.
 */
class PartRule[INPUT <: Partitioning](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _]],
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
        Option[RapidsMeta[_, _]],
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
        Option[RapidsMeta[_, _]],
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
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends DataWritingCommandMeta[InsertIntoHadoopFsRelationCommand](cmd, conf, parent, rule) {

  // spark 2.3 doesn't have this so just code it here
  def sparkSessionActive: SparkSession = {
    SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.getOrElse(
      throw new IllegalStateException("No active or default Spark session found")))
  }

  override def tagSelfForGpu(): Unit = {
    if (cmd.bucketSpec.isDefined) {
      willNotWorkOnGpu("bucketing is not supported")
    }

    val spark = sparkSessionActive

    cmd.fileFormat match {
      case _: CSVFileFormat =>
        willNotWorkOnGpu("CSV output is not supported")
        None
      case _: JsonFileFormat =>
        willNotWorkOnGpu("JSON output is not supported")
        None
      case f if GpuOrcFileFormat.isSparkOrcFormat(f) =>
        GpuOrcFileFormat.tagGpuSupport(this, spark, cmd.options, cmd.query.schema)
        None
      case _: ParquetFileFormat =>
        GpuParquetFileFormat.tagGpuSupport(this, spark, cmd.options, cmd.query.schema)
        None
      case _: TextFileFormat =>
        willNotWorkOnGpu("text output is not supported")
        None
      case f =>
        willNotWorkOnGpu(s"unknown file format: ${f.getClass.getCanonicalName}")
        None
    }
  }
}


final class CreateDataSourceTableAsSelectCommandMeta(
    cmd: CreateDataSourceTableAsSelectCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends DataWritingCommandMeta[CreateDataSourceTableAsSelectCommand](cmd, conf, parent, rule) {

  private var origProvider: Class[_] = _

  // spark 2.3 doesn't have this so just code it here
  def sparkSessionActive: SparkSession = {
    SparkSession.getActiveSession.getOrElse(SparkSession.getDefaultSession.getOrElse(
      throw new IllegalStateException("No active or default Spark session found")))
  }

  override def tagSelfForGpu(): Unit = {
    if (cmd.table.bucketSpec.isDefined) {
      willNotWorkOnGpu("bucketing is not supported")
    }
    if (cmd.table.provider.isEmpty) {
      willNotWorkOnGpu("provider must be defined")
    }

    val spark = sparkSessionActive
    origProvider =
      GpuDataSource.lookupDataSource(cmd.table.provider.get, spark.sessionState.conf)
    // Note that the data source V2 always fallsback to the V1 currently.
    // If that changes then this will start failing because we don't have a mapping.
    origProvider.getConstructor().newInstance() match {
      case f: FileFormat if GpuOrcFileFormat.isSparkOrcFormat(f) =>
        GpuOrcFileFormat.tagGpuSupport(this, spark, cmd.table.storage.properties, cmd.query.schema)
        None
      case _: ParquetFileFormat =>
        GpuParquetFileFormat.tagGpuSupport(this, spark,
          cmd.table.storage.properties, cmd.query.schema)
        None
      case ds =>
        willNotWorkOnGpu(s"Data source class not supported: ${ds}")
        None
    }
  }

}

sealed abstract class Optimization


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

object GpuOverrides extends Logging {
  // Spark 2.x - don't pull in cudf so hardcode here
  val DECIMAL32_MAX_PRECISION = 9
  val DECIMAL64_MAX_PRECISION = 18
  val DECIMAL128_MAX_PRECISION = 38

  val FLOAT_DIFFERS_GROUP_INCOMPAT =
    "when enabling these, there may be extra groups produced for floating point grouping " +
    "keys (e.g. -0.0, and 0.0)"
  val CASE_MODIFICATION_INCOMPAT =
    "the Unicode version used by cuDF and the JVM may differ, resulting in some " +
    "corner-case characters not changing case correctly."
  val UTC_TIMEZONE_ID = ZoneId.of("UTC").normalized()
  // Based on https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
  private[this] lazy val regexList: Seq[String] = Seq("\\", "\u0000", "\\x", "\t", "\n", "\r",
    "\f", "\\a", "\\e", "\\cx", "[", "]", "^", "&", ".", "*", "\\d", "\\D", "\\h", "\\H", "\\s",
    "\\S", "\\v", "\\V", "\\w", "\\w", "\\p", "$", "\\b", "\\B", "\\A", "\\G", "\\Z", "\\z", "\\R",
    "?", "|", "(", ")", "{", "}", "\\k", "\\Q", "\\E", ":", "!", "<=", ">")

  /**
   * Provides a way to log an info message about how long an operation took in milliseconds.
   */
  def logDuration[T](shouldLog: Boolean, msg: Double => String)(block: => T): T = {
    val start = System.nanoTime()
    val ret = block
    val end = System.nanoTime()
    if (shouldLog) {
      val timeTaken = (end - start).toDouble / java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(1)
      logInfo(msg(timeTaken))
    }
    ret
  }

  private[this] val _gpuCommonTypes = TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64

  val pluginSupportedOrderableSig: TypeSig =
    _gpuCommonTypes + TypeSig.STRUCT.nested(_gpuCommonTypes)

  private[this] def isStructType(dataType: DataType) = dataType match {
    case StructType(_) => true
    case _ => false
  }

  // this listener mechanism is global and is intended for use by unit tests only
  private lazy val listeners: ListBuffer[GpuOverridesListener] =
    new ListBuffer[GpuOverridesListener]()

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

  def isSupportedStringReplacePattern(exp: Expression): Boolean = {
    extractLit(exp) match {
      case Some(Literal(null, _)) => false
      case Some(Literal(value: UTF8String, DataTypes.StringType)) =>
        val strLit = value.toString
        if (strLit.isEmpty) {
          false
        } else {
          // check for regex special characters, except for \u0000 which we can support
          !regexList.filterNot(_ == "\u0000").exists(pattern => strLit.contains(pattern))
        }
      case _ => false
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
      case TimestampType => TypeChecks.areTimestampsSupported(ZoneId.systemDefault())
      case StringType => true
      case dt: DecimalType if allowDecimal => dt.precision <= GpuOverrides.DECIMAL64_MAX_PRECISION
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

  def isOrContainsFloatingPoint(dataType: DataType): Boolean =
    TrampolineUtil.dataTypeExistsRecursively(dataType, dt => dt == FloatType || dt == DoubleType)

  def checkAndTagFloatAgg(dataType: DataType, conf: RapidsConf, meta: RapidsMeta[_,_]): Unit = {
    if (!conf.isFloatAggEnabled && isOrContainsFloatingPoint(dataType)) {
      meta.willNotWorkOnGpu("the GPU will aggregate floating point values in" +
          " parallel and the result is not always identical each time. This can cause" +
          " some Spark queries to produce an incorrect answer if the value is computed" +
          " more than once as part of the same query.  To enable this anyways set" +
          s" ${RapidsConf.ENABLE_FLOAT_AGG} to true.")
    }
  }

  def checkAndTagFloatNanAgg(
      op: String,
      dataType: DataType,
      conf: RapidsConf,
      meta: RapidsMeta[_,_]): Unit = {
    if (conf.hasNans && isOrContainsFloatingPoint(dataType)) {
      meta.willNotWorkOnGpu(s"$op aggregation on floating point columns that can contain NaNs " +
          "will compute incorrect results. If it is known that there are no NaNs, set " +
          s" ${RapidsConf.HAS_NANS} to false.")
    }
  }

  private val nanAggPsNote = "Input must not contain NaNs and" +
      s" ${RapidsConf.HAS_NANS} must be false."

  def expr[INPUT <: Expression](
      desc: String,
      pluginChecks: ExprChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _]], DataFromReplacementRule)
          => BaseExprMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ExprRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExprRule[INPUT](doWrap, desc, Some(pluginChecks), tag)
  }

  def part[INPUT <: Partitioning](
      desc: String,
      checks: PartChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _]], DataFromReplacementRule)
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
        p: Option[RapidsMeta[_, _]],
        cc: DataFromReplacementRule) =
      new DoNotReplaceOrWarnSparkPlanMeta[INPUT](exec, conf, p)
    new ExecRule[INPUT](doWrap, desc, None, tag).invisible()
  }

  def exec[INPUT <: SparkPlan](
      desc: String,
      pluginChecks: ExecChecks,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _]], DataFromReplacementRule)
          => SparkPlanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ExecRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExecRule[INPUT](doWrap, desc, Some(pluginChecks), tag)
  }

  def dataWriteCmd[INPUT <: DataWritingCommand](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _]], DataFromReplacementRule)
          => DataWritingCommandMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): DataWritingCommandRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new DataWritingCommandRule[INPUT](doWrap, desc, tag)
  }

  def wrapExpr[INPUT <: Expression](
      expr: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _]]): BaseExprMeta[INPUT] =
    expressions.get(expr.getClass)
      .map(r => r.wrap(expr, conf, parent, r).asInstanceOf[BaseExprMeta[INPUT]])
      .getOrElse(new RuleNotFoundExprMeta(expr, conf, parent))

  lazy val fileFormats: Map[FileFormatType, Map[FileFormatOp, FileFormatChecks]] = Map(
    (CsvFormatType, FileFormatChecks(
      cudfRead = TypeSig.commonCudfTypes,
      cudfWrite = TypeSig.none,
      sparkSig = TypeSig.cpuAtomics)),
    (ParquetFormatType, FileFormatChecks(
      cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
        TypeSig.ARRAY + TypeSig.MAP).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
          TypeSig.ARRAY + TypeSig.MAP).nested(),
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.UDT).nested())),
    (OrcFormatType, FileFormatChecks(
      cudfRead = (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.MAP).nested(),
      cudfWrite = (TypeSig.commonCudfTypes + TypeSig.ARRAY +
          // Note Map is not put into nested, now CUDF only support single level map
          TypeSig.STRUCT + TypeSig.DECIMAL_128).nested() + TypeSig.MAP,
      sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
          TypeSig.UDT).nested())))


  val commonExpressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    expr[Cast](
      "Convert a column of one type of data into another type",
      new CastChecks(),
      (cast, conf, p, r) => new CastExprMeta[Cast](cast, false, conf, p, r,
        doFloatToIntCheck = false, stringToAnsiDate = false)),
    expr[Average](
      "Average aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        Seq(ParamCheck("input",
          TypeSig.integral + TypeSig.fp + TypeSig.DECIMAL_128,
          TypeSig.cpuNumeric))),
      (a, conf, p, r) => new AggExprMeta[Average](a, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          // For Decimal Average the SUM adds a precision of 10 to avoid overflowing
          // then it divides by the count with an output scale that is 4 more than the input
          // scale. With how our divide works to match Spark, this means that we will need a
          // precision of 5 more. So 38 - 10 - 5 = 23
          val dataType = a.child.dataType
          dataType match {
            case dt: DecimalType =>
              if (dt.precision > 23) {
                if (conf.needDecimalGuarantees) {
                  willNotWorkOnGpu("GpuAverage cannot guarantee proper overflow checks for " +
                    s"a precision large than 23. The current precision is ${dt.precision}")
                } else {
                  logWarning("Decimal overflow guarantees disabled for " +
                    s"Average(${a.child.dataType}) produces $dt with an " +
                    s"intermediate precision of ${dt.precision + 15}")
                }
              }
            case _ => // NOOP
          }
          GpuOverrides.checkAndTagFloatAgg(dataType, conf, this)
        }

      }),
    expr[Abs](
      "Absolute value",
       ExprChecks.unaryProjectAndAstInputMatchesOutput(
          TypeSig.implicitCastsAstTypes, TypeSig.gpuNumeric,
          TypeSig.cpuNumeric),
      (a, conf, p, r) => new UnaryAstExprMeta[Abs](a, conf, p, r) {
      }),
    expr[RegExpReplace](
      "RegExpReplace support for string literal input patterns",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regex", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("rep", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (a, conf, p, r) => new GpuRegExpReplaceMeta(a, conf, p, r)).disabledByDefault(
      "the implementation is not 100% compatible. " +
        "See the compatibility guide for more information."),
    expr[TimeSub](
      "Subtracts interval from timestamp",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
        ("start", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("interval", TypeSig.lit(TypeEnum.CALENDAR)
          .withPsNote(TypeEnum.CALENDAR, "months not supported"), TypeSig.CALENDAR)),
      (timeSub, conf, p, r) => new BinaryExprMeta[TimeSub](timeSub, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          timeSub.interval match {
            case Literal(intvl: CalendarInterval, DataTypes.CalendarIntervalType) =>
              if (intvl.months != 0) {
                willNotWorkOnGpu("interval months isn't supported")
              }
            case _ =>
          }
          checkTimeZoneId(timeSub.timeZoneId)
        }
      }),
    expr[ScalaUDF](
      "User Defined Function, the UDF can choose to implement a RAPIDS accelerated interface " +
        "to get better performance.",
      ExprChecks.projectOnly(
        GpuUserDefinedFunction.udfTypeSig,
        TypeSig.all,
        repeatingParamCheck =
          Some(RepeatingParamCheck("param", GpuUserDefinedFunction.udfTypeSig, TypeSig.all))),
      (expr, conf, p, r) => new ScalaUDFMetaBase(expr, conf, p, r) {
      }),
    expr[Literal](
      "Holds a static value from the query",
      ExprChecks.projectAndAst(
        TypeSig.astTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.CALENDAR
            + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT)
            .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
                TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
        TypeSig.all),
      (lit, conf, p, r) => new LiteralExprMeta(lit, conf, p, r)),
    expr[Signum](
      "Returns -1.0, 0.0 or 1.0 as expr is negative, 0 or positive",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Signum](a, conf, p, r) {
      }),
    expr[Alias](
      "Gives a column a name",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT
            + TypeSig.DECIMAL_128).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryAstExprMeta[Alias](a, conf, p, r) {
      }),
    expr[AttributeReference](
      "References an input column",
      ExprChecks.projectAndAst(
        TypeSig.astTypes,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128).nested(),
        TypeSig.all),
      (att, conf, p, r) => new BaseExprMeta[AttributeReference](att, conf, p, r) {
        // This is the only NOOP operator.  It goes away when things are bound

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
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.DECIMAL_128,
        TypeSig.DECIMAL_128),
      (a, conf, p, r) => new UnaryExprMeta[PromotePrecision](a, conf, p, r) {
      }),
    expr[CheckOverflow](
      "CheckOverflow after arithmetic operations between DecimalType data",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.DECIMAL_128,
        TypeSig.DECIMAL_128),
      (a, conf, p, r) => new ExprMeta[CheckOverflow](a, conf, p, r) {
        private[this] def extractOrigParam(expr: BaseExprMeta[_]): BaseExprMeta[_] =
          expr.wrapped match {
            case lit: Literal if lit.dataType.isInstanceOf[DecimalType] =>
              val dt = lit.dataType.asInstanceOf[DecimalType]
              // Lets figure out if we can make the Literal value smaller
              val (newType, value) = lit.value match {
                case null =>
                  (DecimalType(0, 0), null)
                case dec: Decimal =>
                  val stripped = Decimal(dec.toJavaBigDecimal.stripTrailingZeros())
                  val p = stripped.precision
                  val s = stripped.scale
                  // allowNegativeScaleOfDecimalEnabled is not in 2.x assume its default false
                  val t = if (s < 0 && !false) {
                    // need to adjust to avoid errors about negative scale
                    DecimalType(p - s, 0)
                  } else {
                    DecimalType(p, s)
                  }
                  (t, stripped)
                case other =>
                  throw new IllegalArgumentException(s"Unexpected decimal literal value $other")
              }
              expr.asInstanceOf[LiteralExprMeta].withNewLiteral(Literal(value, newType))
            // We avoid unapply for Cast because it changes between versions of Spark
            case PromotePrecision(c: Cast) if c.dataType.isInstanceOf[DecimalType] =>
              val to = c.dataType.asInstanceOf[DecimalType]
              val fromType = DecimalUtil.optionallyAsDecimalType(c.child.dataType)
              fromType match {
                case Some(from) =>
                  val minScale = math.min(from.scale, to.scale)
                  val fromWhole = from.precision - from.scale
                  val toWhole = to.precision - to.scale
                  val minWhole = if (to.scale < from.scale) {
                    // If the scale is getting smaller in the worst case we need an
                    // extra whole part to handle rounding up.
                    math.min(fromWhole + 1, toWhole)
                  } else {
                    math.min(fromWhole, toWhole)
                  }
                  val newToType = DecimalType(minWhole + minScale, minScale)
                  if (newToType == from) {
                    // We can remove the cast totally
                    val castExpr = expr.childExprs.head
                    castExpr.childExprs.head
                  } else if (newToType == to) {
                    // The cast is already ideal
                    expr
                  } else {
                    val castExpr = expr.childExprs.head.asInstanceOf[CastExprMeta[_]]
                    castExpr.withToTypeOverride(newToType)
                  }
                case _ =>
                  expr
              }
            case _ => expr
          }
        private[this] lazy val binExpr = childExprs.head
        private[this] lazy val lhs = extractOrigParam(binExpr.childExprs.head)
        private[this] lazy val rhs = extractOrigParam(binExpr.childExprs(1))
        private[this] lazy val lhsDecimalType =
          DecimalUtil.asDecimalType(lhs.wrapped.asInstanceOf[Expression].dataType)
        private[this] lazy val rhsDecimalType =
          DecimalUtil.asDecimalType(rhs.wrapped.asInstanceOf[Expression].dataType)

        override def tagExprForGpu(): Unit = {
          a.child match {
            // Division and Multiplication of Decimal types is a little odd. Spark will cast the
            // inputs to a common wider value where the scale is the max of the two input scales,
            // and the precision is max of the two input non-scale portions + the new scale. Then it
            // will do the divide or multiply as a BigDecimal value but lie about the return type.
            // Finally here in CheckOverflow it will reset the scale and check the precision so that
            // Spark knows it fits in the final desired result.
            // Here we try to strip out the extra casts, etc to get to as close to the original
            // query as possible. This lets us then calculate what CUDF needs to get the correct
            // answer, which in some cases is a lot smaller.
            case _: Divide =>
              val intermediatePrecision =
                GpuDecimalDivide.nonRoundedIntermediateArgPrecision(lhsDecimalType,
                  rhsDecimalType, a.dataType)

              if (intermediatePrecision > GpuOverrides.DECIMAL128_MAX_PRECISION) {
                if (conf.needDecimalGuarantees) {
                  binExpr.willNotWorkOnGpu(s"the intermediate precision of " +
                      s"$intermediatePrecision that is required to guarantee no overflow issues " +
                      s"for this divide is too large to be supported on the GPU")
                } else {
                  logWarning("Decimal overflow guarantees disabled for " +
                      s"${lhs.dataType} / ${rhs.dataType} produces ${a.dataType} with an " +
                      s"intermediate precision of $intermediatePrecision")
                }
              }
            case _: Multiply =>
              val intermediatePrecision =
                GpuDecimalMultiply.nonRoundedIntermediatePrecision(lhsDecimalType,
                  rhsDecimalType, a.dataType)
              if (intermediatePrecision > GpuOverrides.DECIMAL128_MAX_PRECISION) {
                if (conf.needDecimalGuarantees) {
                  binExpr.willNotWorkOnGpu(s"the intermediate precision of " +
                      s"$intermediatePrecision that is required to guarantee no overflow issues " +
                      s"for this multiply is too large to be supported on the GPU")
                } else {
                  logWarning("Decimal overflow guarantees disabled for " +
                      s"${lhs.dataType} * ${rhs.dataType} produces ${a.dataType} with an " +
                      s"intermediate precision of $intermediatePrecision")
                }
              }
            case _ => // NOOP
          }
        }
      }),
    expr[ToDegrees](
      "Converts radians to degrees",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[ToDegrees](a, conf, p, r) {
      }),
    expr[ToRadians](
      "Converts degrees to radians",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[ToRadians](a, conf, p, r) {
      }),
    expr[WindowExpression](
      "Calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck("windowFunction",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all),
          ParamCheck("windowSpec",
            TypeSig.CALENDAR + TypeSig.NULL + TypeSig.integral + TypeSig.DECIMAL_64,
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
      }),
    expr[UnboundedPreceding.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      (unboundedPreceding, conf, p, r) =>
        new ExprMeta[UnboundedPreceding.type](unboundedPreceding, conf, p, r) {
        }),
    expr[UnboundedFollowing.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      ExprChecks.projectOnly(TypeSig.NULL, TypeSig.NULL),
      (unboundedFollowing, conf, p, r) =>
        new ExprMeta[UnboundedFollowing.type](unboundedFollowing, conf, p, r) {
        }),
    expr[RowNumber](
      "Window function that returns the index for the row within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT),
      (rowNumber, conf, p, r) => new ExprMeta[RowNumber](rowNumber, conf, p, r) {
      }),
    expr[Rank](
      "Window function that returns the rank value within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      (rank, conf, p, r) => new ExprMeta[Rank](rank, conf, p, r) {
      }),
    expr[DenseRank](
      "Window function that returns the dense rank value within the aggregation window",
      ExprChecks.windowOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck =
          Some(RepeatingParamCheck("ordering",
            TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
            TypeSig.all))),
      (denseRank, conf, p, r) => new ExprMeta[DenseRank](denseRank, conf, p, r) {
      }),
    expr[Lead](
      "Window function that returns N entries ahead of this one",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all)
        )
      ),
      (lead, conf, p, r) => new OffsetWindowFunctionMeta[Lead](lead, conf, p, r) {
      }),
    expr[Lag](
      "Window function that returns N entries behind this one",
      ExprChecks.windowOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        Seq(
          ParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 +
              TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all),
          ParamCheck("offset", TypeSig.INT, TypeSig.INT),
          ParamCheck("default",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
            TypeSig.all)
        )
      ),
      (lag, conf, p, r) => new OffsetWindowFunctionMeta[Lag](lag, conf, p, r) {
      }),
    expr[PreciseTimestampConversion](
      "Expression used internally to convert the TimestampType to Long and back without losing " +
          "precision, i.e. in microseconds. Used in time windowing",
      ExprChecks.unaryProject(
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG,
        TypeSig.TIMESTAMP + TypeSig.LONG),
      (a, conf, p, r) => new UnaryExprMeta[PreciseTimestampConversion](a, conf, p, r) {
      }),
    expr[UnaryMinus](
      "Negate a numeric value",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric,
        TypeSig.numericAndInterval),
      (a, conf, p, r) => new UnaryAstExprMeta[UnaryMinus](a, conf, p, r) {
        // val ansiEnabled = SQLConf.get.ansiEnabled
        val ansiEnabled = false

        override def tagSelfForAst(): Unit = {
          // Spark 2.x - ansi in not in 2.x
          /*
          if (ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
            willNotWorkInAst("AST unary minus does not support ANSI mode.")
          }

           */
        }
      }),
    expr[UnaryPositive](
      "A numeric value with a + in front of it",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes,
        TypeSig.gpuNumeric,
        TypeSig.numericAndInterval),
      (a, conf, p, r) => new UnaryAstExprMeta[UnaryPositive](a, conf, p, r) {
      }),
    expr[Year](
      "Returns the year from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Year](a, conf, p, r) {
      }),
    expr[Month](
      "Returns the month from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Month](a, conf, p, r) {
      }),
    expr[Quarter](
      "Returns the quarter of the year for date, in the range 1 to 4",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[Quarter](a, conf, p, r) {
      }),
    expr[DayOfMonth](
      "Returns the day of the month from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfMonth](a, conf, p, r) {
      }),
    expr[DayOfYear](
      "Returns the day of the year from a date or timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT, TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfYear](a, conf, p, r) {
      }),
    expr[Acos](
      "Inverse cosine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Acos](a, conf, p, r) {
      }),
    expr[Asin](
      "Inverse sine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Asin](a, conf, p, r) {
      }),
    expr[Sqrt](
      "Square root",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Sqrt](a, conf, p, r) {
      }),
    expr[Cbrt](
      "Cube root",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Cbrt](a, conf, p, r) {
      }),
    expr[Floor](
      "Floor of a number",
      ExprChecks.unaryProjectInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128),
      (a, conf, p, r) => new UnaryExprMeta[Floor](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.dataType match {
            case dt: DecimalType =>
              val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
              if (precision > GpuOverrides.DECIMAL128_MAX_PRECISION) {
                willNotWorkOnGpu(s"output precision $precision would require overflow " +
                    s"checks, which are not supported yet")
              }
            case _ => // NOOP
          }
        }

      }),
    expr[Ceil](
      "Ceiling of a number",
      ExprChecks.unaryProjectInputMatchesOutput(
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128,
        TypeSig.DOUBLE + TypeSig.LONG + TypeSig.DECIMAL_128),
      (a, conf, p, r) => new UnaryExprMeta[Ceil](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          a.dataType match {
            case dt: DecimalType =>
              val precision = GpuFloorCeil.unboundedOutputPrecision(dt)
              if (precision > GpuOverrides.DECIMAL128_MAX_PRECISION) {
                willNotWorkOnGpu(s"output precision $precision would require overflow " +
                    s"checks, which are not supported yet")
              }
            case _ => // NOOP
          }
        }

      }),
    expr[Not](
      "Boolean not operator",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes, TypeSig.BOOLEAN, TypeSig.BOOLEAN),
      (a, conf, p, r) => new UnaryAstExprMeta[Not](a, conf, p, r) {
      }),
    expr[IsNull](
      "Checks if a value is null",
      ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryExprMeta[IsNull](a, conf, p, r) {
      }),
    expr[IsNotNull](
      "Checks if a value is not null",
      ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.MAP + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.DECIMAL_128).nested(),
        TypeSig.all),
      (a, conf, p, r) => new UnaryExprMeta[IsNotNull](a, conf, p, r) {
      }),
    expr[IsNaN](
      "Checks if a value is NaN",
      ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        TypeSig.DOUBLE + TypeSig.FLOAT, TypeSig.DOUBLE + TypeSig.FLOAT),
      (a, conf, p, r) => new UnaryExprMeta[IsNaN](a, conf, p, r) {
      }),
    expr[Rint](
      "Rounds up a double value to the nearest double equal to an integer",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Rint](a, conf, p, r) {
      }),
    expr[BitwiseNot](
      "Returns the bitwise NOT of the operands",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral),
      (a, conf, p, r) => new UnaryAstExprMeta[BitwiseNot](a, conf, p, r) {
      }),
    expr[AtLeastNNonNulls](
      "Checks if number of non null/Nan values is greater than a given value",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[AtLeastNNonNulls](a, conf, p, r) {
      }),
    expr[DateAdd](
      "Returns the date that is num_days after start_date",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("startDate", TypeSig.DATE, TypeSig.DATE),
        ("days",
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE,
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE)),
      (a, conf, p, r) => new BinaryExprMeta[DateAdd](a, conf, p, r) {
      }),
    expr[DateSub](
      "Returns the date that is num_days before start_date",
      ExprChecks.binaryProject(TypeSig.DATE, TypeSig.DATE,
        ("startDate", TypeSig.DATE, TypeSig.DATE),
        ("days",
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE,
            TypeSig.INT + TypeSig.SHORT + TypeSig.BYTE)),
      (a, conf, p, r) => new BinaryExprMeta[DateSub](a, conf, p, r) {
      }),
    expr[NaNvl](
      "Evaluates to `left` iff left is not NaN, `right` otherwise",
      ExprChecks.binaryProject(TypeSig.fp, TypeSig.fp,
        ("lhs", TypeSig.fp, TypeSig.fp),
        ("rhs", TypeSig.fp, TypeSig.fp)),
      (a, conf, p, r) => new BinaryExprMeta[NaNvl](a, conf, p, r) {
      }),
    expr[ShiftLeft](
      "Bitwise shift left (<<)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftLeft](a, conf, p, r) {
      }),
    expr[ShiftRight](
      "Bitwise shift right (>>)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftRight](a, conf, p, r) {
      }),
    expr[ShiftRightUnsigned](
      "Bitwise unsigned shift right (>>>)",
      ExprChecks.binaryProject(TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG,
        ("value", TypeSig.INT + TypeSig.LONG, TypeSig.INT + TypeSig.LONG),
        ("amount", TypeSig.INT, TypeSig.INT)),
      (a, conf, p, r) => new BinaryExprMeta[ShiftRightUnsigned](a, conf, p, r) {
      }),
    expr[BitwiseAnd](
      "Returns the bitwise AND of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseAnd](a, conf, p, r) {
      }),
    expr[BitwiseOr](
      "Returns the bitwise OR of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseOr](a, conf, p, r) {
      }),
    expr[BitwiseXor](
      "Returns the bitwise XOR of the operands",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.integral, TypeSig.integral,
        ("lhs", TypeSig.integral, TypeSig.integral),
        ("rhs", TypeSig.integral, TypeSig.integral)),
      (a, conf, p, r) => new BinaryAstExprMeta[BitwiseXor](a, conf, p, r) {
      }),
    expr[Coalesce] (
      "Returns the first non-null argument if exists. Otherwise, null",
      ExprChecks.projectOnly(
        (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[Coalesce](a, conf, p, r) {
      }),
    expr[Least] (
      "Returns the least value of all parameters, skipping null values",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.orderable,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.orderable))),
      (a, conf, p, r) => new ExprMeta[Least](a, conf, p, r) {
      }),
    expr[Greatest] (
      "Returns the greatest value of all parameters, skipping null values",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.orderable,
        repeatingParamCheck = Some(RepeatingParamCheck("param",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.orderable))),
      (a, conf, p, r) => new ExprMeta[Greatest](a, conf, p, r) {
      }),
    expr[Atan](
      "Inverse tangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Atan](a, conf, p, r) {
      }),
    expr[Cos](
      "Cosine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Cos](a, conf, p, r) {
      }),
    expr[Exp](
      "Euler's number e raised to a power",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Exp](a, conf, p, r) {
      }),
    expr[Expm1](
      "Euler's number e raised to a power minus 1",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Expm1](a, conf, p, r) {
      }),
    expr[InitCap](
      "Returns str with the first letter of each word in uppercase. " +
      "All other letters are in lowercase",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[InitCap](a, conf, p, r) {
      }).incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Log](
      "Natural log",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log](a, conf, p, r) {
      }),
    expr[Log1p](
      "Natural log 1 + expr",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log1p](a, conf, p, r) {
      }),
    expr[Log2](
      "Log base 2",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log2](a, conf, p, r) {
      }),
    expr[Log10](
      "Log base 10",
      ExprChecks.mathUnary,
      (a, conf, p, r) => new UnaryExprMeta[Log10](a, conf, p, r) {
      }),
    expr[Logarithm](
      "Log variable base",
      ExprChecks.binaryProject(TypeSig.DOUBLE, TypeSig.DOUBLE,
        ("value", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("base", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      (a, conf, p, r) => new BinaryExprMeta[Logarithm](a, conf, p, r) {
      }),
    expr[Sin](
      "Sine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Sin](a, conf, p, r) {
      }),
    expr[Sinh](
      "Hyperbolic sine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Sinh](a, conf, p, r) {
      }),
    expr[Cosh](
      "Hyperbolic cosine",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Cosh](a, conf, p, r) {
      }),
    expr[Cot](
      "Cotangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Cot](a, conf, p, r) {
      }),
    expr[Tanh](
      "Hyperbolic tangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Tanh](a, conf, p, r) {
      }),
    expr[Tan](
      "Tangent",
      ExprChecks.mathUnaryWithAst,
      (a, conf, p, r) => new UnaryAstExprMeta[Tan](a, conf, p, r) {
      }),
    expr[KnownNotNull](
      "Tag an expression as known to not be null",
      ExprChecks.unaryProjectInputMatchesOutput(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.CALENDAR +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested(), TypeSig.all),
      (k, conf, p, r) => new UnaryExprMeta[KnownNotNull](k, conf, p, r) {
      }),
    expr[DateDiff](
      "Returns the number of days from startDate to endDate",
      ExprChecks.binaryProject(TypeSig.INT, TypeSig.INT,
        ("lhs", TypeSig.DATE, TypeSig.DATE),
        ("rhs", TypeSig.DATE, TypeSig.DATE)),
      (a, conf, p, r) => new BinaryExprMeta[DateDiff](a, conf, p, r) {
    }),
    expr[TimeAdd](
      "Adds interval to timestamp",
      ExprChecks.binaryProject(TypeSig.TIMESTAMP, TypeSig.TIMESTAMP,
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

    }),
    expr[DateFormatClass](
      "Converts timestamp to a value of string in the format specified by the date format",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("timestamp", TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
        ("strfmt", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[DateFormatClass](a, conf, p, r) {
        override def shouldFallbackOnAnsiTimestamp: Boolean = false

      }
    ),
    expr[ToUnixTimestamp](
      "Returns the UNIX timestamp of the given time",
      ExprChecks.binaryProject(TypeSig.LONG, TypeSig.LONG,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[ToUnixTimestamp](a, conf, p, r) {
        override def shouldFallbackOnAnsiTimestamp: Boolean = false
          // ShimLoader.getSparkShims.shouldFallbackOnAnsiTimestamp
      }),
    expr[UnixTimestamp](
      "Returns the UNIX timestamp of current or specified time",
      ExprChecks.binaryProject(TypeSig.LONG, TypeSig.LONG,
        ("timeExp",
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP,
            TypeSig.STRING + TypeSig.DATE + TypeSig.TIMESTAMP),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "A limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[UnixTimestamp](a, conf, p, r) {
        override def shouldFallbackOnAnsiTimestamp: Boolean = false
          // ShimLoader.getSparkShims.shouldFallbackOnAnsiTimestamp

      }),
    expr[Hour](
      "Returns the hour component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (hour, conf, p, r) => new UnaryExprMeta[Hour](hour, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          checkTimeZoneId(hour.timeZoneId)
        }

      }),
    expr[Minute](
      "Returns the minute component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (minute, conf, p, r) => new UnaryExprMeta[Minute](minute, conf, p, r) {
        override def tagExprForGpu(): Unit = {
         checkTimeZoneId(minute.timeZoneId)
        }

      }),
    expr[Second](
      "Returns the second component of the string/timestamp",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.TIMESTAMP, TypeSig.TIMESTAMP),
      (second, conf, p, r) => new UnaryExprMeta[Second](second, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          checkTimeZoneId(second.timeZoneId)
        }
      }),
    expr[WeekDay](
      "Returns the day of the week (0 = Monday...6=Sunday)",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[WeekDay](a, conf, p, r) {
      }),
    expr[DayOfWeek](
      "Returns the day of the week (1 = Sunday...7=Saturday)",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[DayOfWeek](a, conf, p, r) {
      }),
    expr[LastDay](
      "Returns the last day of the month which the date belongs to",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[LastDay](a, conf, p, r) {
      }),
    expr[FromUnixTime](
      "Get the string from a unix timestamp",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.STRING,
        ("sec", TypeSig.LONG, TypeSig.LONG),
        ("format", TypeSig.lit(TypeEnum.STRING)
            .withPsNote(TypeEnum.STRING, "Only a limited number of formats are supported"),
            TypeSig.STRING)),
      (a, conf, p, r) => new UnixTimeExprMeta[FromUnixTime](a, conf, p, r) {
        override def shouldFallbackOnAnsiTimestamp: Boolean = false

      }),
    expr[Pmod](
      "Pmod",
      ExprChecks.binaryProject(TypeSig.integral + TypeSig.fp, TypeSig.cpuNumeric,
        ("lhs", TypeSig.integral + TypeSig.fp, TypeSig.cpuNumeric),
        ("rhs", TypeSig.integral + TypeSig.fp, TypeSig.cpuNumeric)),
      (a, conf, p, r) => new BinaryExprMeta[Pmod](a, conf, p, r) {
      }),
    expr[Add](
      "Addition",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric, TypeSig.numericAndInterval,
        ("lhs", TypeSig.gpuNumeric,
            TypeSig.numericAndInterval),
        ("rhs", TypeSig.gpuNumeric,
            TypeSig.numericAndInterval)),
      (a, conf, p, r) => new BinaryAstExprMeta[Add](a, conf, p, r) {
        private val ansiEnabled = false

        override def tagSelfForAst(): Unit = {
        }

      }),
    expr[Subtract](
      "Subtraction",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric, TypeSig.numericAndInterval,
        ("lhs", TypeSig.gpuNumeric,
            TypeSig.numericAndInterval),
        ("rhs", TypeSig.gpuNumeric,
            TypeSig.numericAndInterval)),
      (a, conf, p, r) => new BinaryAstExprMeta[Subtract](a, conf, p, r) {
        private val ansiEnabled = false

        override def tagSelfForAst(): Unit = {
        }

      }),
    expr[Multiply](
      "Multiplication",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes,
        TypeSig.gpuNumeric + TypeSig.psNote(TypeEnum.DECIMAL,
          "Because of Spark's inner workings the full range of decimal precision " +
              "(even for 128-bit values) is not supported."),
        TypeSig.cpuNumeric,
        ("lhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
        ("rhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric)),
      (a, conf, p, r) => new BinaryAstExprMeta[Multiply](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
        }

      }),
    expr[And](
      "Logical AND",
      ExprChecks.binaryProjectAndAst(TypeSig.BOOLEAN, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
        ("rhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN)),
      (a, conf, p, r) => new BinaryExprMeta[And](a, conf, p, r) {
      }),
    expr[Or](
      "Logical OR",
      ExprChecks.binaryProjectAndAst(TypeSig.BOOLEAN, TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
        ("rhs", TypeSig.BOOLEAN, TypeSig.BOOLEAN)),
      (a, conf, p, r) => new BinaryExprMeta[Or](a, conf, p, r) {
      }),
    expr[EqualNullSafe](
      "Check if the values are equal including nulls <=>",
      ExprChecks.binaryProject(
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.comparable),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.comparable)),
      (a, conf, p, r) => new BinaryExprMeta[EqualNullSafe](a, conf, p, r) {
      }),
    expr[EqualTo](
      "Check if the values are equal",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.comparable),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.comparable)),
      (a, conf, p, r) => new BinaryAstExprMeta[EqualTo](a, conf, p, r) {
      }),
    expr[GreaterThan](
      "> operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.orderable),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.orderable)),
      (a, conf, p, r) => new BinaryAstExprMeta[GreaterThan](a, conf, p, r) {
      }),
    expr[GreaterThanOrEqual](
      ">= operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.orderable),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.orderable)),
      (a, conf, p, r) => new BinaryAstExprMeta[GreaterThanOrEqual](a, conf, p, r) {
      }),
    expr[In](
      "IN operator",
      ExprChecks.projectOnly(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        Seq(ParamCheck("value", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
          TypeSig.comparable)),
        Some(RepeatingParamCheck("list",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128).withAllLit(),
          TypeSig.comparable))),
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
      }),
    expr[InSet](
      "INSET operator",
      ExprChecks.unaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128, TypeSig.comparable),
      (in, conf, p, r) => new ExprMeta[InSet](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (in.hset.contains(null)) {
            willNotWorkOnGpu("nulls are not supported")
          }
        }
      }),
    expr[LessThan](
      "< operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.orderable),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.orderable)),
      (a, conf, p, r) => new BinaryAstExprMeta[LessThan](a, conf, p, r) {
      }),
    expr[LessThanOrEqual](
      "<= operator",
      ExprChecks.binaryProjectAndAst(
        TypeSig.comparisonAstTypes,
        TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("lhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.orderable),
        ("rhs", TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
            TypeSig.orderable)),
      (a, conf, p, r) => new BinaryAstExprMeta[LessThanOrEqual](a, conf, p, r) {
      }),
    expr[CaseWhen](
      "CASE WHEN expression",
      CaseWhenCheck,
      (a, conf, p, r) => new ExprMeta[CaseWhen](a, conf, p, r) {
      }),
    expr[If](
      "IF expression",
      ExprChecks.projectOnly(
        (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
            TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck("predicate", TypeSig.BOOLEAN, TypeSig.BOOLEAN),
          ParamCheck("trueValue",
            (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
                TypeSig.MAP).nested(),
            TypeSig.all),
          ParamCheck("falseValue",
            (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
                TypeSig.MAP).nested(),
            TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[If](a, conf, p, r) {
      }),
    expr[Pow](
      "lhs ^ rhs",
      ExprChecks.binaryProjectAndAst(
        TypeSig.implicitCastsAstTypes, TypeSig.DOUBLE, TypeSig.DOUBLE,
        ("lhs", TypeSig.DOUBLE, TypeSig.DOUBLE),
        ("rhs", TypeSig.DOUBLE, TypeSig.DOUBLE)),
      (a, conf, p, r) => new BinaryAstExprMeta[Pow](a, conf, p, r) {
      }),
    expr[Divide](
      "Division",
      ExprChecks.binaryProject(
        TypeSig.DOUBLE + TypeSig.DECIMAL_128 +
            TypeSig.psNote(TypeEnum.DECIMAL,
              "Because of Spark's inner workings the full range of decimal precision " +
                  "(even for 128-bit values) is not supported."),
        TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        ("lhs", TypeSig.DOUBLE + TypeSig.DECIMAL_128,
            TypeSig.DOUBLE + TypeSig.DECIMAL_128),
        ("rhs", TypeSig.DOUBLE + TypeSig.DECIMAL_128,
            TypeSig.DOUBLE + TypeSig.DECIMAL_128)),
      (a, conf, p, r) => new BinaryExprMeta[Divide](a, conf, p, r) {
      }),
    expr[Remainder](
      "Remainder or modulo",
      ExprChecks.binaryProject(
        TypeSig.integral + TypeSig.fp, TypeSig.cpuNumeric,
        ("lhs", TypeSig.integral + TypeSig.fp, TypeSig.cpuNumeric),
        ("rhs", TypeSig.integral + TypeSig.fp, TypeSig.cpuNumeric)),
      (a, conf, p, r) => new BinaryExprMeta[Remainder](a, conf, p, r) {
      }),
    expr[AggregateExpression](
      "Aggregate expression",
      ExprChecks.fullAgg(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck(
          "aggFunc",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
          TypeSig.all)),
        Some(RepeatingParamCheck("filter", TypeSig.BOOLEAN, TypeSig.BOOLEAN))),
      (a, conf, p, r) => new ExprMeta[AggregateExpression](a, conf, p, r) {
        // No filter parameter in 2.x
        private val childrenExprMeta: Seq[BaseExprMeta[Expression]] =
          a.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childExprs: Seq[BaseExprMeta[_]] =
          childrenExprMeta
      }),
    expr[SortOrder](
      "Sort order",
      ExprChecks.projectOnly(
        (pluginSupportedOrderableSig + TypeSig.DECIMAL_128 + TypeSig.STRUCT).nested(),
        TypeSig.orderable,
        Seq(ParamCheck(
          "input",
          (pluginSupportedOrderableSig + TypeSig.DECIMAL_128 + TypeSig.STRUCT).nested(),
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

      }),
    expr[PivotFirst](
      "PivotFirst operator",
      ExprChecks.reductionAndGroupByAgg(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64 +
          TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_64),
        TypeSig.all,
        Seq(ParamCheck(
          "pivotColumn",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64)
              .withPsNote(TypeEnum.DOUBLE, nanAggPsNote)
              .withPsNote(TypeEnum.FLOAT, nanAggPsNote),
          TypeSig.all),
          ParamCheck("valueColumn",
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64,
          TypeSig.all))),
      (pivot, conf, p, r) => new ImperativeAggExprMeta[PivotFirst](pivot, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          checkAndTagFloatNanAgg("Pivot", pivot.pivotColumn.dataType, conf, this)
          // If pivotColumnValues doesn't have distinct values, fall back to CPU
          if (pivot.pivotColumnValues.distinct.lengthCompare(pivot.pivotColumnValues.length) != 0) {
            willNotWorkOnGpu("PivotFirst does not work on the GPU when there are duplicate" +
                " pivot values provided")
          }
        }
      }),
    expr[Count](
      "Count aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.LONG, TypeSig.LONG,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "input", _gpuCommonTypes + TypeSig.DECIMAL_128 +
              TypeSig.STRUCT.nested(_gpuCommonTypes + TypeSig.DECIMAL_128),
          TypeSig.all))),
      (count, conf, p, r) => new AggExprMeta[Count](count, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          if (count.children.size > 1) {
            willNotWorkOnGpu("count of multiple columns not supported")
          }
        }
      }),
    expr[Max](
      "Max aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL, TypeSig.orderable,
        Seq(ParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL)
              .withPsNote(TypeEnum.DOUBLE, nanAggPsNote)
              .withPsNote(TypeEnum.FLOAT, nanAggPsNote),
          TypeSig.orderable))
      ),
      (max, conf, p, r) => new AggExprMeta[Max](max, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          val dataType = max.child.dataType
          checkAndTagFloatNanAgg("Max", dataType, conf, this)
        }
      }),
    expr[Min](
      "Min aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL, TypeSig.orderable,
        Seq(ParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL)
              .withPsNote(TypeEnum.DOUBLE, nanAggPsNote)
              .withPsNote(TypeEnum.FLOAT, nanAggPsNote),
          TypeSig.orderable))
      ),
      (a, conf, p, r) => new AggExprMeta[Min](a, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          val dataType = a.child.dataType
          checkAndTagFloatNanAgg("Min", dataType, conf, this)
        }
      }),
    expr[Sum](
      "Sum aggregate operator",
      ExprChecks.fullAgg(
        TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        TypeSig.LONG + TypeSig.DOUBLE + TypeSig.DECIMAL_128,
        Seq(ParamCheck("input", TypeSig.gpuNumeric, TypeSig.cpuNumeric))),
      (a, conf, p, r) => new AggExprMeta[Sum](a, conf, p, r) {
        override def tagAggForGpu(): Unit = {
          val inputDataType = a.child.dataType
          checkAndTagFloatAgg(inputDataType, conf, this)
        }

      }),
    expr[First](
      "first aggregate operator", {
        ExprChecks.aggNotWindow(
          (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
              TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
          TypeSig.all,
          Seq(ParamCheck("input",
            (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
                TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
            TypeSig.all))
        )
      },
      (a, conf, p, r) => new AggExprMeta[First](a, conf, p, r) {
      }),
    expr[Last](
      "last aggregate operator", {
        ExprChecks.aggNotWindow(
          (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
              TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
          TypeSig.all,
          Seq(ParamCheck("input",
            (TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
                TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128).nested(),
            TypeSig.all))
        )
      },
      (a, conf, p, r) => new AggExprMeta[Last](a, conf, p, r) {
      }),
    expr[BRound](
      "Round an expression to d decimal places using HALF_EVEN rounding mode",
      ExprChecks.binaryProject(
       TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("value", TypeSig.gpuNumeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.cpuNumeric),
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
      }),
    expr[Round](
      "Round an expression to d decimal places using HALF_UP rounding mode",
      ExprChecks.binaryProject(
        TypeSig.gpuNumeric, TypeSig.cpuNumeric,
        ("value", TypeSig.gpuNumeric +
            TypeSig.psNote(TypeEnum.FLOAT, "result may round slightly differently") +
            TypeSig.psNote(TypeEnum.DOUBLE, "result may round slightly differently"),
            TypeSig.cpuNumeric),
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
      }),
    expr[PythonUDF](
      "UDF run in an external python process. Does not actually run on the GPU, but " +
          "the transfer of data to/from it can be accelerated",
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
        }),
    expr[Rand](
      "Generate a random column with i.i.d. uniformly distributed values in [0, 1)",
      ExprChecks.projectOnly(TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("seed",
          (TypeSig.INT + TypeSig.LONG).withAllLit(),
          (TypeSig.INT + TypeSig.LONG).withAllLit()))),
      (a, conf, p, r) => new UnaryExprMeta[Rand](a, conf, p, r) {
      }),
    expr[SparkPartitionID] (
      "Returns the current partition id",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT),
      (a, conf, p, r) => new ExprMeta[SparkPartitionID](a, conf, p, r) {
      }),
    expr[MonotonicallyIncreasingID] (
      "Returns monotonically increasing 64-bit integers",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[MonotonicallyIncreasingID](a, conf, p, r) {
      }),
    expr[InputFileName] (
      "Returns the name of the file being read, or empty string if not available",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new ExprMeta[InputFileName](a, conf, p, r) {
      }),
    expr[InputFileBlockStart] (
      "Returns the start offset of the block being read, or -1 if not available",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[InputFileBlockStart](a, conf, p, r) {
      }),
    expr[InputFileBlockLength] (
      "Returns the length of the block being read, or -1 if not available",
      ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new ExprMeta[InputFileBlockLength](a, conf, p, r) {
      }),
    expr[Md5] (
      "MD5 hash operator",
      ExprChecks.unaryProject(TypeSig.STRING, TypeSig.STRING,
        TypeSig.BINARY, TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[Md5](a, conf, p, r) {
      }),
    expr[Upper](
      "String uppercase operator",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[Upper](a, conf, p, r) {
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Lower](
      "String lowercase operator",
      ExprChecks.unaryProjectInputMatchesOutput(TypeSig.STRING, TypeSig.STRING),
      (a, conf, p, r) => new UnaryExprMeta[Lower](a, conf, p, r) {
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[StringLPad](
      "Pad a string on the left",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
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
      }),
    expr[StringRPad](
      "Pad a string on the right",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
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
      }),
    expr[StringSplit](
       "Splits `str` around occurrences that match `regex`",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.STRING),
        TypeSig.ARRAY.nested(TypeSig.STRING),
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING)
              .withPsNote(TypeEnum.STRING, "very limited subset of regex supported"),
            TypeSig.STRING),
          ParamCheck("limit", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new GpuStringSplitMeta(in, conf, p, r)),
    expr[GetStructField](
      "Gets the named field of the struct",
      ExprChecks.unaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP + TypeSig.NULL +
            TypeSig.DECIMAL_128).nested(),
        TypeSig.all,
        TypeSig.STRUCT.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP + TypeSig.NULL + TypeSig.DECIMAL_128),
        TypeSig.STRUCT.nested(TypeSig.all)),
      (expr, conf, p, r) => new UnaryExprMeta[GetStructField](expr, conf, p, r) {
      }),
    expr[GetArrayItem](
      "Gets the field at `ordinal` in the Array",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.MAP).nested(),
        TypeSig.all,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("ordinal", TypeSig.lit(TypeEnum.INT), TypeSig.INT)),
      (in, conf, p, r) => new GpuGetArrayItemMeta(in, conf, p, r)),
    expr[GetMapValue](
      "Gets Value from a Map based on a key",
      ExprChecks.binaryProject(TypeSig.STRING, TypeSig.all,
        ("map", TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.all)),
        ("key", TypeSig.lit(TypeEnum.STRING), TypeSig.all)),
      (in, conf, p, r) => new GpuGetMapValueMeta(in, conf, p, r)),
    expr[ElementAt](
      "Returns element of array at given(1-based) index in value if column is array. " +
        "Returns value for the given key in value if column is map",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
          TypeSig.DECIMAL_128 + TypeSig.MAP).nested(), TypeSig.all,
        ("array/map", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
          TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP) +
          TypeSig.MAP.nested(TypeSig.STRING)
            .withPsNote(TypeEnum.MAP ,"If it's map, only string is supported."),
          TypeSig.ARRAY.nested(TypeSig.all) + TypeSig.MAP.nested(TypeSig.all)),
        ("index/key", (TypeSig.lit(TypeEnum.INT) + TypeSig.lit(TypeEnum.STRING))
          .withPsNote(TypeEnum.INT, "ints are only supported as array indexes, " +
            "not as maps keys")
          .withPsNote(TypeEnum.STRING, "strings are only supported as map keys, " +
            "not array indexes"),
          TypeSig.all)),
      (in, conf, p, r) => new BinaryExprMeta[ElementAt](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // To distinguish the supported nested type between Array and Map
          val checks = in.left.dataType match {
            case _: MapType =>
              // Match exactly with the checks for GetMapValue
              ExprChecks.binaryProject(TypeSig.STRING, TypeSig.all,
                ("map", TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.all)),
                ("key", TypeSig.lit(TypeEnum.STRING), TypeSig.all))
            case _: ArrayType =>
              // Match exactly with the checks for GetArrayItem
              ExprChecks.binaryProject(
                (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                  TypeSig.DECIMAL_128 + TypeSig.MAP).nested(),
                TypeSig.all,
                ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
                  TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP),
                  TypeSig.ARRAY.nested(TypeSig.all)),
                ("ordinal", TypeSig.lit(TypeEnum.INT), TypeSig.INT))
            case _ => throw new IllegalStateException("Only Array or Map is supported as input.")
          }
          checks.tag(this)
        }
      }),
    expr[MapKeys](
      "Returns an unordered array containing the keys of the map",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.ARRAY.nested(TypeSig.all - TypeSig.MAP), // Maps cannot have other maps as keys
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all)),
      (in, conf, p, r) => new UnaryExprMeta[MapKeys](in, conf, p, r) {
      }),
    expr[MapValues](
      "Returns an unordered array containing the values of the map",
      ExprChecks.unaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.MAP.nested(TypeSig.all)),
      (in, conf, p, r) => new UnaryExprMeta[MapValues](in, conf, p, r) {
      }),
    expr[ArrayMin](
      "Returns the minimum value in the array",
      ExprChecks.unaryProject(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
        TypeSig.orderable,
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL)
            .withPsNote(TypeEnum.DOUBLE, GpuOverrides.nanAggPsNote)
            .withPsNote(TypeEnum.FLOAT, GpuOverrides.nanAggPsNote),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      (in, conf, p, r) => new UnaryExprMeta[ArrayMin](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          GpuOverrides.checkAndTagFloatNanAgg("Min", in.dataType, conf, this)
        }
      }),
    expr[ArrayMax](
      "Returns the maximum value in the array",
      ExprChecks.unaryProject(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
        TypeSig.orderable,
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL)
            .withPsNote(TypeEnum.DOUBLE, GpuOverrides.nanAggPsNote)
            .withPsNote(TypeEnum.FLOAT, GpuOverrides.nanAggPsNote),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      (in, conf, p, r) => new UnaryExprMeta[ArrayMax](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          GpuOverrides.checkAndTagFloatNanAgg("Max", in.dataType, conf, this)
        }

      }),
    expr[CreateNamedStruct](
      "Creates a struct with the given field names and values",
      CreateNamedStructCheck,
      (in, conf, p, r) => new ExprMeta[CreateNamedStruct](in, conf, p, r) {
      }),
    expr[ArrayContains](
      "Returns a boolean if the array contains the passed in key",
      ExprChecks.binaryProject(
        TypeSig.BOOLEAN,
        TypeSig.BOOLEAN,
        ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL),
          TypeSig.ARRAY.nested(TypeSig.all)),
        ("key", TypeSig.commonCudfTypes
            .withPsNote(TypeEnum.DOUBLE, "NaN literals are not supported. Columnar input" +
                s" must not contain NaNs and ${RapidsConf.HAS_NANS} must be false.")
            .withPsNote(TypeEnum.FLOAT, "NaN literals are not supported. Columnar input" +
                s" must not contain NaNs and ${RapidsConf.HAS_NANS} must be false."),
            TypeSig.all)),
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
      }),
    expr[SortArray](
      "Returns a sorted array with the input array and the ascending / descending order",
      ExprChecks.binaryProject(
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
        TypeSig.ARRAY.nested(TypeSig.all),
        ("array", TypeSig.ARRAY.nested(
          TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
            TypeSig.ARRAY.nested(TypeSig.all)),
        ("ascendingOrder", TypeSig.lit(TypeEnum.BOOLEAN), TypeSig.lit(TypeEnum.BOOLEAN))),
      (sortExpression, conf, p, r) => new BinaryExprMeta[SortArray](sortExpression, conf, p, r) {
      }),
    expr[CreateArray](
      "Returns an array with the given elements",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.gpuNumeric +
          TypeSig.NULL + TypeSig.STRING + TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP +
          TypeSig.ARRAY + TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("arg",
           TypeSig.gpuNumeric + TypeSig.NULL + TypeSig.STRING +
              TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.STRUCT +
              TypeSig.ARRAY.nested(TypeSig.gpuNumeric + TypeSig.NULL + TypeSig.STRING +
                TypeSig.BOOLEAN + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.STRUCT +
                  TypeSig.ARRAY),
          TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[CreateArray](in, conf, p, r) {

        override def tagExprForGpu(): Unit = {
          wrapped.dataType match {
            case ArrayType(ArrayType(ArrayType(_, _), _), _) =>
              willNotWorkOnGpu("Only support to create array or array of array, Found: " +
                s"${wrapped.dataType}")
            case _ =>
          }
        }

      }),
    expr[LambdaFunction](
      "Holds a higher order SQL function",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck("function",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY +
              TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all)),
        Some(RepeatingParamCheck("arguments",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY +
              TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[LambdaFunction](in, conf, p, r) {
      }),
    expr[NamedLambdaVariable](
      "A parameter to a higher order SQL function",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all),
      (in, conf, p, r) => new ExprMeta[NamedLambdaVariable](in, conf, p, r) {
      }),
    expr[ArrayTransform](
      "Transform elements in an array using the transform function. This is similar to a `map` " +
          "in functional programming",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.commonCudfTypes +
        TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("function",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
            TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[ArrayTransform](in, conf, p, r) {
      }),
    expr[StringLocate](
      "Substring search operator",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
        Seq(ParamCheck("substr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("start", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new TernaryExprMeta[StringLocate](in, conf, p, r) {
      }),
    expr[Substring](
      "Substring operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
          ParamCheck("pos", TypeSig.lit(TypeEnum.INT), TypeSig.INT),
          ParamCheck("len", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new TernaryExprMeta[Substring](in, conf, p, r) {
      }),
    expr[SubstringIndex](
      "substring_index operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("delim", TypeSig.lit(TypeEnum.STRING)
              .withPsNote(TypeEnum.STRING, "only a single character is allowed"), TypeSig.STRING),
          ParamCheck("count", TypeSig.lit(TypeEnum.INT), TypeSig.INT))),
      (in, conf, p, r) => new SubstringIndexMeta(in, conf, p, r)),
    expr[StringRepeat](
      "StringRepeat operator that repeats the given strings with numbers of times " +
        "given by repeatTimes",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("input", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("repeatTimes", TypeSig.INT, TypeSig.INT))),
      (in, conf, p, r) => new BinaryExprMeta[StringRepeat](in, conf, p, r) {
      }),
    expr[StringReplace](
      "StringReplace operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("replace", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new TernaryExprMeta[StringReplace](in, conf, p, r) {
      }),
    expr[StringTrim](
      "StringTrim operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) => new String2TrimExpressionMeta[StringTrim](in, conf, p, r) {
      }),
    expr[StringTrimLeft](
      "StringTrimLeft operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) =>
        new String2TrimExpressionMeta[StringTrimLeft](in, conf, p, r) {
        }),
    expr[StringTrimRight](
      "StringTrimRight operator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("src", TypeSig.STRING, TypeSig.STRING)),
        // Should really be an OptionalParam
        Some(RepeatingParamCheck("trimStr", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (in, conf, p, r) =>
        new String2TrimExpressionMeta[StringTrimRight](in, conf, p, r) {
        }),
    expr[StartsWith](
      "Starts with",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[StartsWith](a, conf, p, r) {
      }),
    expr[EndsWith](
      "Ends with",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[EndsWith](a, conf, p, r) {
      }),
    expr[Concat](
      "List/String concatenate",
      ExprChecks.projectOnly((TypeSig.STRING + TypeSig.ARRAY).nested(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
        (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.STRING + TypeSig.ARRAY).nested(
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
          (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all)))),
      (a, conf, p, r) => new ComplexTypeMergingExprMeta[Concat](a, conf, p, r) {
      }),
    expr[ConcatWs](
      "Concatenates multiple input strings or array of strings into a single " +
        "string using a given separator",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.STRING + TypeSig.ARRAY).nested(TypeSig.STRING),
          (TypeSig.STRING + TypeSig.ARRAY).nested(TypeSig.STRING)))),
      (a, conf, p, r) => new ExprMeta[ConcatWs](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (a.children.size <= 1) {
            // If only a separator specified and its a column, Spark returns an empty
            // string for all entries unless they are null, then it returns null.
            // This seems like edge case so instead of handling on GPU just fallback.
            willNotWorkOnGpu("Only specifying separator column not supported on GPU")
          }
        }
      }),
    expr[Murmur3Hash] (
      "Murmur3 hash operator",
      ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64 + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[Murmur3Hash](a, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] = a.children
          .map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }),
    expr[Contains](
      "Contains",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[Contains](a, conf, p, r) {
      }),
    expr[Like](
      "Like",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("src", TypeSig.STRING, TypeSig.STRING),
        ("search", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new BinaryExprMeta[Like](a, conf, p, r) {
      }),
    expr[RLike](
      "RLike",
      ExprChecks.binaryProject(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
        ("str", TypeSig.STRING, TypeSig.STRING),
        ("regexp", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING)),
      (a, conf, p, r) => new GpuRLikeMeta(a, conf, p, r)).disabledByDefault(
      "the implementation is not 100% compatible. " +
        "See the compatibility guide for more information."),
    expr[RegExpExtract](
      "RegExpExtract",
      ExprChecks.projectOnly(TypeSig.STRING, TypeSig.STRING,
        Seq(ParamCheck("str", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("regexp", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING),
          ParamCheck("idx", TypeSig.lit(TypeEnum.INT),
            TypeSig.lit(TypeEnum.INT)))),
      (a, conf, p, r) => new GpuRegExpExtractMeta(a, conf, p, r))
      .disabledByDefault(
      "the implementation is not 100% compatible. " +
        "See the compatibility guide for more information."),
    expr[Length](
      "String character length or binary byte length",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.STRING, TypeSig.STRING + TypeSig.BINARY),
      (a, conf, p, r) => new UnaryExprMeta[Length](a, conf, p, r) {
      }),
    expr[Size](
      "The size of an array or a map",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL
            + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      (a, conf, p, r) => new UnaryExprMeta[Size](a, conf, p, r) {
      }),
    expr[UnscaledValue](
      "Convert a Decimal to an unscaled long value for some aggregation optimizations",
      ExprChecks.unaryProject(TypeSig.LONG, TypeSig.LONG,
        TypeSig.DECIMAL_64, TypeSig.DECIMAL_128),
      (a, conf, p, r) => new UnaryExprMeta[UnscaledValue](a, conf, p, r) {
        override val isFoldableNonLitAllowed: Boolean = true
      }),
    expr[MakeDecimal](
      "Create a Decimal from an unscaled long value for some aggregation optimizations",
      ExprChecks.unaryProject(TypeSig.DECIMAL_64, TypeSig.DECIMAL_128,
        TypeSig.LONG, TypeSig.LONG),
      (a, conf, p, r) => new UnaryExprMeta[MakeDecimal](a, conf, p, r) {
      }),
    expr[Explode](
      "Given an input array produces a sequence of rows for each value in the array",
      ExprChecks.unaryProject(
        // Here is a walk-around representation, since multi-level nested type is not supported yet.
        // related issue: https://github.com/NVIDIA/spark-rapids/issues/1901
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      (a, conf, p, r) => new GeneratorExprMeta[Explode](a, conf, p, r) {
        override val supportOuter: Boolean = true
      }),
    expr[PosExplode](
      "Given an input array produces a sequence of rows for each value in the array",
      ExprChecks.unaryProject(
        // Here is a walk-around representation, since multi-level nested type is not supported yet.
        // related issue: https://github.com/NVIDIA/spark-rapids/issues/1901
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.commonCudfTypes + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        (TypeSig.ARRAY + TypeSig.MAP).nested(TypeSig.all)),
      (a, conf, p, r) => new GeneratorExprMeta[PosExplode](a, conf, p, r) {
        override val supportOuter: Boolean = true
      }),
    expr[ReplicateRows](
      "Given an input row replicates the row N times",
      ExprChecks.projectOnly(
        // The plan is optimized to run HashAggregate on the rows to be replicated.
        // HashAggregateExec doesn't support grouping by 128-bit decimal value yet.
        // Issue to track decimal 128 support: https://github.com/NVIDIA/spark-rapids/issues/4410
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ReplicateRowsExprMeta[ReplicateRows](a, conf, p, r) {
     }),
    expr[StddevPop](
      "Aggregation computing population standard deviation",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      (a, conf, p, r) => new AggExprMeta[StddevPop](a, conf, p, r) {
      }),
    expr[StddevSamp](
      "Aggregation computing sample standard deviation",
      ExprChecks.aggNotReduction(
          TypeSig.DOUBLE, TypeSig.DOUBLE,
          Seq(ParamCheck("input", TypeSig.DOUBLE,
            TypeSig.DOUBLE))),
        (a, conf, p, r) => new AggExprMeta[StddevSamp](a, conf, p, r) {
        }),
    expr[VariancePop](
      "Aggregation computing population variance",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      (a, conf, p, r) => new AggExprMeta[VariancePop](a, conf, p, r) {
      }),
    expr[VarianceSamp](
      "Aggregation computing sample variance",
      ExprChecks.groupByOnly(
        TypeSig.DOUBLE, TypeSig.DOUBLE,
        Seq(ParamCheck("input", TypeSig.DOUBLE, TypeSig.DOUBLE))),
      (a, conf, p, r) => new AggExprMeta[VarianceSamp](a, conf, p, r) {
      }),
    expr[ApproximatePercentile](
      "Approximate percentile",
      ExprChecks.groupByOnly(
        // note that output can be single number or array depending on whether percentiles param
        // is a single number or an array
        TypeSig.gpuNumeric +
            TypeSig.ARRAY.nested(TypeSig.gpuNumeric),
        TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP + TypeSig.ARRAY.nested(
          TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP),
        Seq(
          ParamCheck("input",
            TypeSig.gpuNumeric,
            TypeSig.cpuNumeric + TypeSig.DATE + TypeSig.TIMESTAMP),
          ParamCheck("percentage",
            TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE),
            TypeSig.DOUBLE + TypeSig.ARRAY.nested(TypeSig.DOUBLE)),
          ParamCheck("accuracy", TypeSig.INT, TypeSig.INT))),
      (c, conf, p, r) => new TypedImperativeAggExprMeta[ApproximatePercentile](c, conf, p, r) {

        override def tagAggForGpu(): Unit = {
          // check if the percentile expression can be supported on GPU
          childExprs(1).wrapped match {
            case lit: Literal => lit.value match {
              case null =>
                willNotWorkOnGpu(
                  "approx_percentile on GPU only supports non-null literal percentiles")
              case a: ArrayData if a.numElements == 0 =>
                willNotWorkOnGpu(
                  "approx_percentile on GPU does not support empty percentiles arrays")
              case a: ArrayData if (0 until a.numElements).exists(a.isNullAt) =>
                willNotWorkOnGpu(
                  "approx_percentile on GPU does not support percentiles arrays containing nulls")
              case _ =>
                // this is fine
            }
            case _ =>
              willNotWorkOnGpu("approx_percentile on GPU only supports literal percentiles")
          }
        }
        override def aggBufferAttribute: AttributeReference = {
          // Spark's ApproxPercentile has an aggregation buffer named "buf" with type "BinaryType"
          // so we need to replace that here with the GPU aggregation buffer reference, which is
          // a t-digest type
          val aggBuffer = c.aggBufferAttributes.head
          aggBuffer.copy(dataType = CudfTDigest.dataType)(aggBuffer.exprId, aggBuffer.qualifier)
        }
      }).incompat("the GPU implementation of approx_percentile is not bit-for-bit " +
          s"compatible with Apache Spark. To enable it, set ${RapidsConf.INCOMPATIBLE_OPS}"),
    expr[GetJsonObject](
      "Extracts a json object from path",
      ExprChecks.projectOnly(
        TypeSig.STRING, TypeSig.STRING, Seq(ParamCheck("json", TypeSig.STRING, TypeSig.STRING),
          ParamCheck("path", TypeSig.lit(TypeEnum.STRING), TypeSig.STRING))),
      (a, conf, p, r) => new BinaryExprMeta[GetJsonObject](a, conf, p, r) {
      }
    ),
    expr[ScalarSubquery](
      "Subquery that will return only one row and one column",
      ExprChecks.projectOnly(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128,
        Nil, None),
      (a, conf, p, r) => new ExprMeta[ScalarSubquery](a, conf, p, r) {
      }),
    expr[CreateMap](
      desc = "Create a map",
      CreateMapCheck,
      (a, conf, p, r) => new ExprMeta[CreateMap](a, conf, p, r) {
      }),
    expr[Sequence](
      desc = "Sequence",
      ExprChecks.projectOnly(
        TypeSig.ARRAY.nested(TypeSig.integral), TypeSig.ARRAY.nested(TypeSig.integral +
          TypeSig.TIMESTAMP + TypeSig.DATE),
        Seq(ParamCheck("start", TypeSig.integral, TypeSig.integral + TypeSig.TIMESTAMP +
          TypeSig.DATE),
          ParamCheck("stop", TypeSig.integral, TypeSig.integral + TypeSig.TIMESTAMP +
            TypeSig.DATE)),
        Some(RepeatingParamCheck("step", TypeSig.integral, TypeSig.integral + TypeSig.CALENDAR))),
      (a, conf, p, r) => new GpuSequenceMeta(a, conf, p, r)
    )
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  // Shim expressions should be last to allow overrides with shim-specific versions
  val expressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] =
    commonExpressions ++ GpuHiveOverrides.exprs

  def wrapPart[INPUT <: Partitioning](
      part: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _]]): PartMeta[INPUT] =
    parts.get(part.getClass)
      .map(r => r.wrap(part, conf, parent, r).asInstanceOf[PartMeta[INPUT]])
      .getOrElse(new RuleNotFoundPartMeta(part, conf, parent))

  val parts : Map[Class[_ <: Partitioning], PartRule[_ <: Partitioning]] = Seq(
    part[HashPartitioning](
      "Hash based partitioning",
      // This needs to match what murmur3 supports.
      PartChecks(RepeatingParamCheck("hash_key",
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64 + TypeSig.STRUCT).nested(),
        TypeSig.all)),
      (hp, conf, p, r) => new PartMeta[HashPartitioning](hp, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          hp.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

      }),
    part[RangePartitioning](
      "Range partitioning",
      PartChecks(RepeatingParamCheck("order_key",
        (pluginSupportedOrderableSig + TypeSig.DECIMAL_128 + TypeSig.STRUCT).nested(),
        TypeSig.orderable)),
      (rp, conf, p, r) => new PartMeta[RangePartitioning](rp, conf, p, r) {
        override val childExprs: Seq[BaseExprMeta[_]] =
          rp.ordering.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

      }),
    part[RoundRobinPartitioning](
      "Round robin partitioning",
      PartChecks(),
      (rrp, conf, p, r) => new PartMeta[RoundRobinPartitioning](rrp, conf, p, r) {
      }),
    part[SinglePartition.type](
      "Single partitioning",
      PartChecks(),
      (sp, conf, p, r) => new PartMeta[SinglePartition.type](sp, conf, p, r) {
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Partitioning]), r)).toMap

  def wrapDataWriteCmds[INPUT <: DataWritingCommand](
      writeCmd: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _]]): DataWritingCommandMeta[INPUT] =
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
      parent: Option[RapidsMeta[_, _]]): SparkPlanMeta[INPUT]  =
    execs.get(plan.getClass)
      .map(r => r.wrap(plan, conf, parent, r).asInstanceOf[SparkPlanMeta[INPUT]])
      .getOrElse(new RuleNotFoundSparkPlanMeta(plan, conf, parent))

  val commonExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
     exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.DECIMAL_128).nested(), TypeSig.all),
        (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {

          // partition filters and data filters are not run on the GPU
          override val childExprs: Seq[ExprMeta[_]] = Seq.empty

          override def tagPlanForGpu(): Unit = {
            this.wrapped.relation.fileFormat match {
              case _: CSVFileFormat => GpuReadCSVFileFormat.tagSupport(this)
              case f if GpuOrcFileFormat.isSparkOrcFormat(f) =>
                GpuReadOrcFileFormat.tagSupport(this)
              case _: ParquetFileFormat => GpuReadParquetFileFormat.tagSupport(this)
              case f =>
                this.willNotWorkOnGpu(s"unsupported file format: ${f.getClass.getCanonicalName}")
            }
          }
        }),
     exec[ShuffledHashJoinExec](
        "Implementation of join using hashed shuffled data",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r)),
    exec[GenerateExec] (
      "The backend for operations that generate more output rows than input rows like explode",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all),
      (gen, conf, p, r) => new GpuGenerateExecSparkPlanMeta(gen, conf, p, r)),
    exec[ProjectExec](
      "The backend for most select, withColumn and dropColumn statements",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128).nested(),
        TypeSig.all),
      (proj, conf, p, r) => new GpuProjectExecMeta(proj, conf, p, r)),
    exec[RangeExec](
      "The backend for range operator",
      ExecChecks(TypeSig.LONG, TypeSig.LONG),
      (range, conf, p, r) => new SparkPlanMeta[RangeExec](range, conf, p, r) {
      }),
    exec[CoalesceExec](
      "The backend for the dataframe coalesce method",
      ExecChecks((_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT + TypeSig.ARRAY +
          TypeSig.MAP).nested(),
        TypeSig.all),
      (coalesce, conf, parent, r) => new SparkPlanMeta[CoalesceExec](coalesce, conf, parent, r) {
      }),
    exec[DataWritingCommandExec](
      "Writing data",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128.withPsNote(
          TypeEnum.DECIMAL, "128bit decimal only supported for Orc") +
          TypeSig.STRUCT.withPsNote(TypeEnum.STRUCT, "Only supported for Parquet") +
          TypeSig.MAP.withPsNote(TypeEnum.MAP, "Only supported for Parquet") +
          TypeSig.ARRAY.withPsNote(TypeEnum.ARRAY, "Only supported for Parquet")).nested(),
        TypeSig.all),
      (p, conf, parent, r) => new SparkPlanMeta[DataWritingCommandExec](p, conf, parent, r) {
        override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
          Seq(GpuOverrides.wrapDataWriteCmds(p.cmd, conf, Some(this)))

      }),
    exec[TakeOrderedAndProjectExec](
      "Take the first limit elements as defined by the sortOrder, and do projection if needed",
      // The SortOrder TypeSig will govern what types can actually be used as sorting key data type.
      // The types below are allowed as inputs and outputs.
      ExecChecks((pluginSupportedOrderableSig + TypeSig.DECIMAL_128 +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
      (takeExec, conf, p, r) =>
        new SparkPlanMeta[TakeOrderedAndProjectExec](takeExec, conf, p, r) {
          val sortOrder: Seq[BaseExprMeta[SortOrder]] =
            takeExec.sortOrder.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          val projectList: Seq[BaseExprMeta[NamedExpression]] =
            takeExec.projectList.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] = sortOrder ++ projectList

        }),
    exec[LocalLimitExec](
      "Per-partition limiting of results",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (localLimitExec, conf, p, r) =>
        new SparkPlanMeta[LocalLimitExec](localLimitExec, conf, p, r) {
        }),
    exec[GlobalLimitExec](
      "Limiting of results across partitions",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (globalLimitExec, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimitExec, conf, p, r) {
        }),
    exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (collectLimitExec, conf, p, r) =>
        new SparkPlanMeta[CollectLimitExec](collectLimitExec, conf, p, r) {
          override val childParts: scala.Seq[PartMeta[_]] =
            Seq(GpuOverrides.wrapPart(collectLimitExec.outputPartitioning, conf, Some(this)))})
        .disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
            "of rows in a batch it could help by limiting the number of rows transferred from " +
            "GPU to CPU"),
    exec[FilterExec](
      "The backend for most filter statements",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.DECIMAL_128).nested(), TypeSig.all),
      (filter, conf, p, r) => new SparkPlanMeta[FilterExec](filter, conf, p, r) {
      }),
    exec[ShuffleExchangeExec](
      "The backend for most data being exchanged between processes",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested()
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
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT).nested()
        .withPsNote(TypeEnum.STRUCT,
          "unionByName will not optionally impute nulls for missing struct fields " +
          "when the column is a struct and there are non-overlapping fields"), TypeSig.all),
      (union, conf, p, r) => new SparkPlanMeta[UnionExec](union, conf, p, r) {
      }),
    exec[BroadcastExchangeExec](
      "The backend for broadcast exchange of data",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(TypeSig.commonCudfTypes +
          TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT),
        TypeSig.all),
      (exchange, conf, p, r) => new GpuBroadcastMeta(exchange, conf, p, r)),
    exec[BroadcastHashJoinExec](
      "Implementation of join using broadcast data",
      JoinTypeChecks.equiJoinExecChecks,
      (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
    exec[BroadcastNestedLoopJoinExec](
      "Implementation of join using brute force. Full outer joins and joins where the " +
          "broadcast side matches the join side (e.g.: LeftOuter with left broadcast) are not " +
          "supported",
      JoinTypeChecks.nonEquiJoinChecks,
      (join, conf, p, r) => new GpuBroadcastNestedLoopJoinMeta(join, conf, p, r)),
    exec[CartesianProductExec](
      "Implementation of join using brute force",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT)
          .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
        TypeSig.all),
      (join, conf, p, r) => new SparkPlanMeta[CartesianProductExec](join, conf, p, r) {
        val condition: Option[BaseExprMeta[_]] =
          join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] = condition.toSeq

      }),
    exec[HashAggregateExec](
      "The backend for hash based aggregations",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.MAP + TypeSig.ARRAY + TypeSig.STRUCT)
            .nested()
            .withPsNote(TypeEnum.ARRAY, "not allowed for grouping expressions")
            .withPsNote(TypeEnum.MAP, "not allowed for grouping expressions")
            .withPsNote(TypeEnum.STRUCT,
              "not allowed for grouping expressions if containing Array or Map as child"),
        TypeSig.all),
      (agg, conf, p, r) => new GpuHashAggregateMeta(agg, conf, p, r)),
    exec[SortAggregateExec](
      "The backend for sort based aggregations",
      // SPARK 2.x we can't check for the TypedImperativeAggregate properly so
      // map/arrya/struct left off
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.MAP + TypeSig.BINARY)
            .nested()
            .withPsNote(TypeEnum.BINARY, "only allowed when aggregate buffers can be " +
              "converted between CPU and GPU")
            .withPsNote(TypeEnum.MAP, "not allowed for grouping expressions"),
        TypeSig.all),
      (agg, conf, p, r) => new GpuSortAggregateExecMeta(agg, conf, p, r)),
    // SPARK 2.x we can't check for the TypedImperativeAggregate properly so don't say we do the
    // ObjectHashAggregate
    exec[SortExec](
      "The backend for the sort operator",
      // The SortOrder TypeSig will govern what types can actually be used as sorting key data type.
      // The types below are allowed as inputs and outputs.
      ExecChecks((pluginSupportedOrderableSig + TypeSig.DECIMAL_128 + TypeSig.ARRAY +
          TypeSig.STRUCT +TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all),
      (sort, conf, p, r) => new GpuSortMeta(sort, conf, p, r)),
     exec[SortMergeJoinExec](
        "Sort merge join, replacing with shuffled hash join",
        JoinTypeChecks.equiJoinExecChecks,
        (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
    exec[ExpandExec](
      "The backend for the expand operator",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64 +
            TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (expand, conf, p, r) => new GpuExpandExecMeta(expand, conf, p, r)),
    exec[WindowExec](
      "Window-operator backend",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all,
        Map("partitionSpec" ->
            InputCheck(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64, TypeSig.all))),
      (windowOp, conf, p, r) =>
        new GpuWindowExecMeta(windowOp, conf, p, r)
    ),
    exec[SampleExec](
      "The backend for the sample operator",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
        TypeSig.ARRAY + TypeSig.DECIMAL_128).nested(), TypeSig.all),
      (sample, conf, p, r) => new SparkPlanMeta[SampleExec](sample, conf, p, r) {}
    ),
    exec[ArrowEvalPythonExec](
      "The backend of the Scalar Pandas UDFs. Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      (e, conf, p, r) =>
        new SparkPlanMeta[ArrowEvalPythonExec](e, conf, p, r) {
          val udfs: Seq[BaseExprMeta[PythonUDF]] =
            e.udfs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          val resultAttrs: Seq[BaseExprMeta[Attribute]] =
            e.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] = udfs ++ resultAttrs
          override def replaceMessage: String = "partially run on GPU"
          override def noReplacementPossibleMessage(reasons: String): String =
            s"cannot run even partially on the GPU because $reasons"
      }),
    exec[FlatMapGroupsInPandasExec](
      "The backend for Flat Map Groups Pandas UDF, Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (flatPy, conf, p, r) => new SparkPlanMeta[FlatMapGroupsInPandasExec](flatPy, conf, p, r) {
        override def replaceMessage: String = "partially run on GPU"
        override def noReplacementPossibleMessage(reasons: String): String =
          s"cannot run even partially on the GPU because $reasons"

        private val groupingAttrs: Seq[BaseExprMeta[Attribute]] =
          flatPy.groupingAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        private val udf: BaseExprMeta[PythonUDF] = GpuOverrides.wrapExpr(
          flatPy.func.asInstanceOf[PythonUDF], conf, Some(this))

        private val resultAttrs: Seq[BaseExprMeta[Attribute]] =
          flatPy.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] = groupingAttrs ++ resultAttrs :+ udf
      }),
    exec[WindowInPandasExec](
      "The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between" +
        " the Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled. For now it only supports row based window frame.",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested(TypeSig.commonCudfTypes),
        TypeSig.all),
      (winPy, conf, p, r) => new GpuWindowInPandasExecMetaBase(winPy, conf, p, r) {
        override val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
          winPy.windowExpression.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }).disabledByDefault("it only supports row based frame for now"),
    exec[AggregateInPandasExec](
      "The backend for an Aggregation Pandas UDF, this accelerates the data transfer between" +
        " the Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (aggPy, conf, p, r) => new GpuAggregateInPandasExecMeta(aggPy, conf, p, r)),
    // ShimLoader.getSparkShims.aqeShuffleReaderExec,
    // ShimLoader.getSparkShims.neverReplaceShowCurrentNamespaceCommand,
    neverReplaceExec[ExecutedCommandExec]("Table metadata operation")
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap

  lazy val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    commonExecs

  def getTimeParserPolicy: TimeParserPolicy = {
    // val key = SQLConf.LEGACY_TIME_PARSER_POLICY.key
    val key = "2xgone"
    val policy = SQLConf.get.getConfString(key, "EXCEPTION")
    policy match {
      case "LEGACY" => LegacyTimeParserPolicy
      case "EXCEPTION" => ExceptionTimeParserPolicy
      case "CORRECTED" => CorrectedTimeParserPolicy
    }
  }


  def wrapAndTagPlan(plan: SparkPlan, conf: RapidsConf): SparkPlanMeta[SparkPlan] = {
    val wrap = GpuOverrides.wrapPlan(plan, conf, None)
    wrap.tagForGpu()
    wrap
  }

  private def getOptimizations(wrap: SparkPlanMeta[SparkPlan],
      conf: RapidsConf): Seq[Optimization] = {
   Seq.empty
  }

  private final class SortDataFromReplacementRule extends DataFromReplacementRule {
    override val operationName: String = "Exec"
    override def confKey = "spark.rapids.sql.exec.SortExec"

    override def getChecks: Option[TypeChecks[_]] = None
  }

  // Only run the explain and don't actually convert or run on GPU.
  def explainPotentialGpuPlan(df: DataFrame, explain: String = "ALL"): String = {
    val plan = df.queryExecution.executedPlan
    val conf = new RapidsConf(plan.conf)
    val updatedPlan = prepareExplainOnly(plan)
    // Here we look for subqueries to pull out and do the explain separately on them.
    val subQueryExprs = getSubQueriesFromPlan(plan)
    val preparedSubPlans = subQueryExprs.map(_.plan).map(prepareExplainOnly(_))
    val subPlanExplains = preparedSubPlans.map(explainSinglePlan(_, conf, explain))
    val topPlanExplain = explainSinglePlan(updatedPlan, conf, explain)
    (subPlanExplains :+ topPlanExplain).mkString("\n")
  }

  private def explainSinglePlan(updatedPlan: SparkPlan, conf: RapidsConf,
      explain: String): String = {
    val wrap = wrapAndTagPlan(updatedPlan, conf)
    val reasonsToNotReplaceEntirePlan = wrap.getReasonsNotToReplaceEntirePlan
    if (conf.allowDisableEntirePlan && reasonsToNotReplaceEntirePlan.nonEmpty) {
      "Can't replace any part of this plan due to: " +
        s"${reasonsToNotReplaceEntirePlan.mkString(",")}"
    } else {
      wrap.runAfterTagRules()
      wrap.tagForExplain()
      val shouldExplainAll = explain.equalsIgnoreCase("ALL")
      wrap.explain(shouldExplainAll)
    }
  }

  private def getSubqueryExpressions(e: Expression): Seq[ExecSubqueryExpression] = {
    val childExprs = e.children.flatMap(getSubqueryExpressions(_))
    val res = e match {
      case sq: ExecSubqueryExpression => Seq(sq)
      case _ => Seq.empty
    }
    childExprs ++ res
  }

  private def getSubQueriesFromPlan(plan: SparkPlan): Seq[ExecSubqueryExpression] = {
    val childPlans = plan.children.flatMap(getSubQueriesFromPlan)
    val pSubs = plan.expressions.flatMap(getSubqueryExpressions)
    childPlans ++ pSubs
  }

  private def prepareExplainOnly(plan: SparkPlan): SparkPlan = {
    // Strip out things that would have been added after our GPU plugin would have
    // processed the plan.
    // AQE we look at the input plan so pretty much just like if AQE wasn't enabled.
    val planAfter = plan.transformUp {
      case ia: InputAdapter => prepareExplainOnly(ia.child)
      case ws: WholeStageCodegenExec => prepareExplainOnly(ws.child)
      // case c2r: ColumnarToRowExec => prepareExplainOnly(c2r.child)
      case re: ReusedExchangeExec => prepareExplainOnly(re.child)
      // case aqe: AdaptiveSparkPlanExec =>
      //   prepareExplainOnly(ShimLoader.getSparkShims.getAdaptiveInputPlan(aqe))
      case sub: SubqueryExec => prepareExplainOnly(sub.child)
    }
    planAfter
  }
}

object GpuUserDefinedFunction {
  // UDFs can support all types except UDT which does not have a clear columnar representation.
  val udfTypeSig: TypeSig = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
      TypeSig.BINARY + TypeSig.CALENDAR + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested()

}
