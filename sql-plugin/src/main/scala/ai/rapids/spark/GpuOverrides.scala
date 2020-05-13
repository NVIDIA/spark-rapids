/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import java.time.ZoneId

import ai.rapids.spark.DateUtils.TimestampFormatConversionException
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.command.{DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types._
import org.apache.spark.sql.{GpuInputFileBlockLength, GpuInputFileBlockStart, GpuInputFileName}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Base class for all ReplacementRules
 * @param doWrap wraps a part of the plan in a [[RapidsMeta]] for further processing.
 * @param incomDoc docs explaining if this rule produces an GPU version that is incompatible with
 *                    the CPU version in some way.
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
        ConfKeysAndIncompat) => WRAP_TYPE,
    protected var incomDoc: Option[String],
    protected var desc: String,
    final val tag: ClassTag[INPUT]) extends ConfKeysAndIncompat {

  override def incompatDoc: Option[String] = incomDoc

  /**
   * Mark this expression as incompatible with the original Spark version
   * @param str a description of how it is incompatible.
   * @return this for chaining.
   */
  final def incompat(str: String) : this.type = {
    incomDoc = Some(str)
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
      ConfKeysAndIncompat) => WRAP_TYPE): this.type = {
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

  def isIncompat: Boolean = incompatDoc.isDefined

  private var confKeyCache: String = null
  protected val confKeyPart: String

  override def confKey: String = {
    if (confKeyCache == null) {
      confKeyCache = "spark.rapids.sql." + confKeyPart + "." + tag.runtimeClass.getSimpleName
    }
    confKeyCache
  }

  private def isIncompatMsg(): Option[String] = if (incompatDoc.isDefined) {
    Some(s"This is not 100% compatible with the Spark version because ${incompatDoc.get}")
  } else {
    None
  }

  def confHelp(asTable: Boolean = false): Unit = {
    val incompatMsg = isIncompatMsg()
    if (asTable) {
      print(s"$confKey|$desc|${incompatMsg.isEmpty}|")
      if (incompatMsg.isDefined) {
        print(s"${incompatMsg.get}")
      } else {
        print("None")
      }
      println("|")
    } else {
      println(s"${confKey}:")
      println(s"\tEnable (true) or disable (false) the ${tag} ${operationName}.")
      println(s"\t${desc}")
      if (incompatMsg.isDefined) {
        println(s"\t${incompatMsg.get}")
      }
      println(s"\tdefault: ${incompatDoc.isEmpty}")
      println()
    }
  }

  final def wrap(
      op: BASE,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]],
      r: ConfKeysAndIncompat): WRAP_TYPE = {
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
        ConfKeysAndIncompat) => ExprMeta[INPUT],
    incompatDoc: Option[String],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Expression, ExprMeta[INPUT]](doWrap,
    incompatDoc, desc, tag) {

  override val confKeyPart = "expression"
  override val operationName = "Expression"
}

/**
 * Holds everything that is needed to replace a [[Scan]] with a GPU enabled version.
 */
class ScanRule[INPUT <: Scan](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => ScanMeta[INPUT],
    incompatDoc: Option[String],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Scan, ScanMeta[INPUT]](
    doWrap, incompatDoc, desc, tag) {

  override val confKeyPart: String = "input"
  override val operationName: String = "Input"
}

/**
 * Holds everything that is needed to replace a [[Partitioning]] with a GPU enabled version.
 */
class PartRule[INPUT <: Partitioning](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => PartMeta[INPUT],
    incompatDoc: Option[String],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Partitioning,
    PartMeta[INPUT]](doWrap, incompatDoc, desc, tag) {

  override val confKeyPart: String = "partitioning"
  override val operationName: String = "Partitioning"
}

/**
 * Holds everything that is needed to replace a [[SparkPlan]] with a GPU enabled version.
 */
class ExecRule[INPUT <: SparkPlan](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => SparkPlanMeta[INPUT],
    incompatDoc: Option[String],
    desc: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, SparkPlan, SparkPlanMeta[INPUT]](doWrap, incompatDoc, desc, tag) {

  override val confKeyPart: String = "exec"
  override val operationName: String = "Exec"
}

/**
 * Holds everything that is needed to replace a [[DataWritingCommand]] with a GPU enabled version.
 */
class DataWritingCommandRule[INPUT <: DataWritingCommand](
    doWrap: (
        INPUT,
        RapidsConf,
        Option[RapidsMeta[_, _, _]],
        ConfKeysAndIncompat) => DataWritingCommandMeta[INPUT],
    incompatDoc: Option[String],
    desc: String,
    tag: ClassTag[INPUT])
    extends ReplacementRule[INPUT, DataWritingCommand, DataWritingCommandMeta[INPUT]](
      doWrap, incompatDoc, desc, tag) {

  override val confKeyPart: String = "output"
  override val operationName: String = "Output"
}

final class InsertIntoHadoopFsRelationCommandMeta(
    cmd: InsertIntoHadoopFsRelationCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
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
      case _: OrcFileFormat =>
        GpuOrcFileFormat.tagGpuSupport(this, spark, cmd.options)
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

object GpuOverrides {
  val FLOAT_DIFFERS_GROUP_INCOMPAT =
    "when enabling these, there may be extra groups produced for floating point grouping " +
    "keys (e.g. -0.0, and 0.0)"
  val CASE_MODIFICATION_INCOMPAT =
    "in some cases unicode characters change byte width when changing the case. The GPU string " +
    "conversion does not support these characters. For a full list of unsupported characters " +
    "see https://github.com/rapidsai/cudf/issues/3132"
  private val UTC_TIMEZONE_ID = ZoneId.of("UTC").normalized()
  // Based on https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
  private[this] lazy val regexList: Seq[String] = Seq("\\", "\u0000", "\\x", "\t", "\n", "\r",
    "\f", "\\a", "\\e", "\\cx", "[", "]", "^", "&", ".", "*", "\\d", "\\D", "\\h", "\\H", "\\s",
    "\\S", "\\v", "\\V", "\\w", "\\w", "\\p", "$", "\\b", "\\B", "\\A", "\\G", "\\Z", "\\z", "\\R",
    "?", "|", "(", ")", "{", "}", "\\k", "\\Q", "\\E", ":", "!", "<=", ">")

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

  def isNullOrEmptyOrRegex(exp: Expression): Boolean = {
    val lit = extractLit(exp)
    if (!isOfType(lit, StringType)) {
      false
    } else {
      val value = lit.get.value
      if (value == null) return true
      val strLit = value.asInstanceOf[UTF8String].toString
      if (strLit.isEmpty) return true
      regexList.exists(pattern => strLit.contains(pattern))
    }
  }

  def areAllSupportedTypes(types: DataType*): Boolean = types.forall(isSupportedType)

  def isSupportedType(dataType: DataType): Boolean = dataType match {
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
      case _ => false
    }

  /**
   * Checks to see if any expressions are a String Literal
   */
  def isAnyStringLit(expressions: Seq[Expression]): Boolean =
    expressions.exists(isStringLit)

  def expr[INPUT <: Expression](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
          => ExprMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ExprRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExprRule[INPUT](doWrap, None, desc, tag)
  }

  def scan[INPUT <: Scan](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
          => ScanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ScanRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ScanRule[INPUT](doWrap, None, desc, tag)
  }

  def part[INPUT <: Partitioning](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
          => PartMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): PartRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new PartRule[INPUT](doWrap, None, desc, tag)
  }

  def exec[INPUT <: SparkPlan](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
          => SparkPlanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ExecRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExecRule[INPUT](doWrap, None, desc, tag)
  }

  def dataWriteCmd[INPUT <: DataWritingCommand](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat)
        => DataWritingCommandMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): DataWritingCommandRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new DataWritingCommandRule[INPUT](doWrap, None, desc, tag)
  }

  def wrapExpr[INPUT <: Expression](
      expr: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): ExprMeta[INPUT] =
    expressions.get(expr.getClass)
      .map(r => r.wrap(expr, conf, parent, r).asInstanceOf[ExprMeta[INPUT]])
      .getOrElse(new RuleNotFoundExprMeta(expr, conf, parent))

  val expressions: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    expr[Literal](
      "holds a static value from the query",
      (lit, conf, p, r) => new ExprMeta[Literal](lit, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuLiteral(lit.value, lit.dataType)

        // There are so many of these that we don't need to print them out.
        override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {}

        /**
         * We are overriding this method because currently we only support CalendarIntervalType
         * as a Literal
         */
        override def areAllSupportedTypes(types: DataType*): Boolean = types.forall {
            case CalendarIntervalType => true
            case x => isSupportedType(x)
          }

      }),
    expr[Signum](
      "Returns -1.0, 0.0 or 1.0 as expr is negative, 0 or positive",
      (a, conf, p, r) => new UnaryExprMeta[Signum](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuSignum(child)
      }),
    expr[Alias](
      "gives a column a name",
      (a, conf, p, r) => new UnaryExprMeta[Alias](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          GpuAlias(child, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      }),
    expr[AttributeReference](
      "references an input column",
      (att, conf, p, r) => new ExprMeta[AttributeReference](att, conf, p, r) {
        override def convertToGpu(): GpuExpression =
          GpuAttributeReference(att.name, att.dataType, att.nullable,
            att.metadata)(att.exprId, att.qualifier)

        // There are so many of these that we don't need to print them out.
        override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {}
      }),
    expr[Cast](
      "convert a column of one type of data into another type",
      (cast, conf, p, r) => new CastExprMeta[Cast](cast, SparkSession.active.sessionState.conf
        .ansiEnabled, conf, p, r)),
    expr[AnsiCast](
      "convert a column of one type of data into another type",
      (cast, conf, p, r) => new CastExprMeta[AnsiCast](cast, true, conf, p, r)),
    expr[ToDegrees](
      "Converts radians to degrees",
      (a, conf, p, r) => new UnaryExprMeta[ToDegrees](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuToDegrees = GpuToDegrees(child)
      }),
    expr[ToRadians](
      "Converts degrees to radians",
      (a, conf, p, r) => new UnaryExprMeta[ToRadians](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuToRadians = GpuToRadians(child)
      }),
    expr[WindowExpression](
      "calculates a return value for every input row of a table based on a group (or " +
        "\"window\") of rows",
      (windowExpression, conf, p, r) => new GpuWindowExpressionMeta(windowExpression, conf, p, r)),
    expr[SpecifiedWindowFrame](
      "specification of the width of the group (or \"frame\") of input rows " +
        "around which a window function is evaluated",
      (windowFrame, conf, p, r) => new GpuSpecifiedWindowFrameMeta(windowFrame, conf, p, r) ),
    expr[WindowSpecDefinition](
      "specification of a window function, indicating the partitioning-expression, the row " +
        "ordering, and the width of the window",
      (windowSpec, conf, p, r) => new GpuWindowSpecDefinitionMeta(windowSpec, conf, p, r)),
    expr[CurrentRow.type](
      "Special boundary for a window frame, indicating stopping at the current row",
      (currentRow, conf, p, r) => new ExprMeta[CurrentRow.type](currentRow, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(currentRow)

        // CURRENT ROW needs to support NullType.
        override def areAllSupportedTypes(types: DataType*): Boolean = types.forall {
          case _: NullType => true
          case anythingElse => isSupportedType(anythingElse)
        }
      }
    ),
    expr[UnboundedPreceding.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      (unboundedPreceding, conf, p, r) =>
        new ExprMeta[UnboundedPreceding.type](unboundedPreceding, conf, p, r) {
          override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(unboundedPreceding)

          // UnboundedPreceding needs to support NullType.
          override def areAllSupportedTypes(types: DataType*): Boolean = types.forall {
            case _: NullType => true
            case anythingElse => isSupportedType(anythingElse)
          }
        }
    ),
    expr[UnboundedFollowing.type](
      "Special boundary for a window frame, indicating all rows preceding the current row",
      (unboundedFollowing, conf, p, r) =>
        new ExprMeta[UnboundedFollowing.type](unboundedFollowing, conf, p, r) {
          override def convertToGpu(): GpuExpression = GpuSpecialFrameBoundary(unboundedFollowing)

          // UnboundedFollowing needs to support NullType.
          override def areAllSupportedTypes(types: DataType*): Boolean = types.forall {
            case _: NullType => true
            case anythingElse => isSupportedType(anythingElse)
          }
        }
    ),
    expr[RowNumber](
      "Window function that returns the index for the row within the aggregation window",
      (rowNumber, conf, p, r) => new ExprMeta[RowNumber](rowNumber, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuRowNumber()
      }
    ),
    expr[UnaryMinus](
      "negate a numeric value",
      (a, conf, p, r) => new UnaryExprMeta[UnaryMinus](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuUnaryMinus(child)
      }),
    expr[UnaryPositive](
      "a numeric value with a + in front of it",
      (a, conf, p, r) => new UnaryExprMeta[UnaryPositive](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuUnaryPositive(child)
      }),
    expr[Year](
      "get the year from a date or timestamp",
      (a, conf, p, r) => new UnaryExprMeta[Year](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuYear(child)
      }),
    expr[Month](
      "get the month from a date or timestamp",
      (a, conf, p, r) => new UnaryExprMeta[Month](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuMonth(child)
      }),
    expr[DayOfMonth](
      "get the day of the month from a date or timestamp",
      (a, conf, p, r) => new UnaryExprMeta[DayOfMonth](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuDayOfMonth(child)
      }),
    expr[Abs](
      "absolute value",
      (a, conf, p, r) => new UnaryExprMeta[Abs](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuAbs(child)
      }),
    expr[Acos](
      "inverse cosine",
      (a, conf, p, r) => new UnaryExprMeta[Acos](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuAcos(child)
      }),
    expr[Acosh](
      "inverse hyperbolic cosine",
      (a, conf, p, r) => new UnaryExprMeta[Acosh](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          if (conf.includeImprovedFloat) {
            GpuAcoshImproved(child)
          } else {
            GpuAcoshCompat(child)
          }
      }),
    expr[Asin](
      "inverse sine",
      (a, conf, p, r) => new UnaryExprMeta[Asin](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuAsin(child)
      }),
    expr[Asinh](
      "inverse hyperbolic sine",
      (a, conf, p, r) => new UnaryExprMeta[Asinh](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          if (conf.includeImprovedFloat) {
            GpuAsinhImproved(child)
          } else {
            GpuAsinhCompat(child)
          }
      }),
    expr[Sqrt](
      "square root",
      (a, conf, p, r) => new UnaryExprMeta[Sqrt](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuSqrt(child)
      }),
    expr[Cbrt](
      "cube root",
      (a, conf, p, r) => new UnaryExprMeta[Cbrt](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuCbrt(child)
      }),
    expr[Floor](
      "floor of a number",
      (a, conf, p, r) => new UnaryExprMeta[Floor](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuFloor(child)
      }),
    expr[Ceil](
      "ceiling of a number",
      (a, conf, p, r) => new UnaryExprMeta[Ceil](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuCeil(child)
      }),
    expr[Not](
      "boolean not operator",
      (a, conf, p, r) => new UnaryExprMeta[Not](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuNot(child)
      }),
    expr[IsNull](
      "checks if a value is null",
      (a, conf, p, r) => new UnaryExprMeta[IsNull](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuIsNull(child)
      }),
    expr[IsNotNull](
      "checks if a value is not null",
      (a, conf, p, r) => new UnaryExprMeta[IsNotNull](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuIsNotNull(child)
      }),
    expr[IsNaN](
      "checks if a value is NaN",
      (a, conf, p, r) => new UnaryExprMeta[IsNaN](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuIsNan(child)
      }),
    expr[Rint](
      "Rounds up a double value to the nearest double equal to an integer",
      (a, conf, p, r) => new UnaryExprMeta[Rint](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuRint(child)
      }),
    expr[BitwiseNot](
      "Returns the bitwise NOT of the operands",
      (a, conf, p, r) => new UnaryExprMeta[BitwiseNot](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = {
          GpuBitwiseNot(child)
        }
      }),
    expr[AtLeastNNonNulls](
      "checks if number of non null/Nan values is greater than a given value",
      (a, conf, p, r) => new ExprMeta[AtLeastNNonNulls](a, conf, p, r) {
        override val childExprs: Seq[ExprMeta[_]] = a.children
          .map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        def convertToGpu(): GpuExpression = {
          GpuAtLeastNNonNulls(a.n, childExprs.map(_.convertToGpu()))
        }
      }),
    expr[TimeSub](
      "Subtracts interval from timestamp",
      (a, conf, p, r) => new BinaryExprMeta[TimeSub](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (conf.dataContainsNegativeTimestamps) {
            willNotWorkOnGpu("Negative timestamps aren't supported i.e. timestamps prior to " +
              "Jan 1st 1970. To enable this anyways set " +
              s"${RapidsConf.DATA_CONTAINS_NEGATIVE_TIMESTAMPS} to false.")
          }
          a.interval match {
            case Literal(intvl: CalendarInterval, DataTypes.CalendarIntervalType) =>
              if (intvl.months != 0) {
                willNotWorkOnGpu("interval months isn't supported")
              }
            case _ =>
              willNotWorkOnGpu("only literals are supported for intervals")
          }
          if (ZoneId.of(a.timeZoneId.get).normalized() != UTC_TIMEZONE_ID) {
            willNotWorkOnGpu("Only UTC zone id is supported")
          }
        }

        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuTimeSub(lhs, rhs)
      }
    ),
    expr[NaNvl](
      "evaluates to `left` iff left is not NaN, `right` otherwise.",
      (a, conf, p, r) => new BinaryExprMeta[NaNvl](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuNaNvl(lhs, rhs)
      }
    ),
    expr[ShiftLeft](
      "Bitwise shift left (<<)",
      (a, conf, p, r) => new BinaryExprMeta[ShiftLeft](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuShiftLeft(lhs, rhs)
      }),
    expr[ShiftRight](
      "Bitwise shift right (>>)",
      (a, conf, p, r) => new BinaryExprMeta[ShiftRight](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuShiftRight(lhs, rhs)
      }),
    expr[ShiftRightUnsigned](
      "Bitwise unsigned shift right (>>>)",
      (a, conf, p, r) => new BinaryExprMeta[ShiftRightUnsigned](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuShiftRightUnsigned(lhs, rhs)
      }),
    expr[BitwiseAnd](
      "Returns the bitwise AND of the operands",
      (a, conf, p, r) => new BinaryExprMeta[BitwiseAnd](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuBitwiseAnd(lhs, rhs)
      }
    ),
    expr[BitwiseOr](
      "Returns the bitwise OR of the operands",
      (a, conf, p, r) => new BinaryExprMeta[BitwiseOr](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuBitwiseOr(lhs, rhs)
      }
    ),
    expr[BitwiseXor](
      "Returns the bitwise XOR of the operands",
      (a, conf, p, r) => new BinaryExprMeta[BitwiseXor](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuBitwiseXor(lhs, rhs)
      }
    ),
    expr[Coalesce] (
      "Returns the first non-null argument if exists. Otherwise, null.",
      (a, conf, p, r) => new ExprMeta[Coalesce](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuCoalesce(childExprs.map(_.convertToGpu()))
      }
    ),
    expr[Atan](
      "inverse tangent",
      (a, conf, p, r) => new UnaryExprMeta[Atan](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuAtan(child)
      }),
    expr[Atanh](
      "inverse hyperbolic tangent",
      (a, conf, p, r) => new UnaryExprMeta[Atanh](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuAtanh(child)
      }),
    expr[Cos](
      "cosine",
      (a, conf, p, r) => new UnaryExprMeta[Cos](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuCos(child)
      }),
    expr[Exp](
      "Euler's number e raised to a power",
      (a, conf, p, r) => new UnaryExprMeta[Exp](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuExp(child)
      }),
    expr[Expm1](
      "Euler's number e raised to a power minus 1",
      (a, conf, p, r) => new UnaryExprMeta[Expm1](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuExpm1(child)
      }),
    expr[InitCap]("Returns str with the first letter of each word in uppercase. " +
      "All other letters are in lowercase",
      (a, conf, p, r) => new UnaryExprMeta[InitCap](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuInitCap(child)
      }),
    expr[Log](
      "natural log",
      (a, conf, p, r) => new UnaryExprMeta[Log](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuLog(child)
      }),
    expr[Log1p](
      "natural log 1 + expr",
      (a, conf, p, r) => new UnaryExprMeta[Log1p](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          GpuLog(GpuAdd(child, GpuLiteral(1d, DataTypes.DoubleType)))
      }),
    expr[Log2](
      "log base 2",
      (a, conf, p, r) => new UnaryExprMeta[Log2](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          GpuLogarithm(child, GpuLiteral(2d, DataTypes.DoubleType))
      }),
    expr[Log10](
      "log base 10",
      (a, conf, p, r) => new UnaryExprMeta[Log10](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          GpuLogarithm(child, GpuLiteral(10d, DataTypes.DoubleType))
      }),
    expr[Logarithm](
      "log variable base",
      (a, conf, p, r) => new BinaryExprMeta[Logarithm](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression = {
          // the order of the parameters is transposed intentionally
          GpuLogarithm(rhs, lhs)
        }
      }),
    expr[Sin](
      "sine",
      (a, conf, p, r) => new UnaryExprMeta[Sin](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuSin(child)
      }),
    expr[Sinh](
      "hyperbolic sine",
      (a, conf, p, r) => new UnaryExprMeta[Sinh](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuSinh(child)
      }),
    expr[Cosh](
      "hyperbolic cosine",
      (a, conf, p, r) => new UnaryExprMeta[Cosh](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuCosh(child)
      }),
    expr[Cot](
      "Returns the cotangent",
      (a, conf, p, r) => new UnaryExprMeta[Cot](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuCot(child)
      }),
    expr[Tanh](
      "hyperbolic tangent",
      (a, conf, p, r) => new UnaryExprMeta[Tanh](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuTanh(child)
      }),
    expr[Tan](
      "tangent",
      (a, conf, p, r) => new UnaryExprMeta[Tan](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuTan(child)
      }),
    expr[NormalizeNaNAndZero](
      "normalize nan and zero",
      (a, conf, p, r) => new UnaryExprMeta[NormalizeNaNAndZero](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          GpuNormalizeNaNAndZero(child)
      })
      .incompat(FLOAT_DIFFERS_GROUP_INCOMPAT),
    expr[KnownFloatingPointNormalized](
      "tag to prevent redundant normalization",
      (a, conf, p, r) => new UnaryExprMeta[KnownFloatingPointNormalized](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          GpuKnownFloatingPointNormalized(child)
      })
      .incompat(FLOAT_DIFFERS_GROUP_INCOMPAT),
    expr[DateDiff]("datediff", (a, conf, p, r) =>
      new BinaryExprMeta[DateDiff](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression = {
          GpuDateDiff(lhs, rhs)
        }
    }),
    expr[UnixTimestamp](
      "convert a string date or timestamp to a unix timestamp",
      (a, conf, p, r) => new BinaryExprMeta[UnixTimestamp](a, conf, p, r) {
        var strfFormat: String = _
        override def tagExprForGpu(): Unit = {
          if (ZoneId.of(a.timeZoneId.get).normalized() != UTC_TIMEZONE_ID) {
            willNotWorkOnGpu("Only UTC zone id is supported")
          }
          // Date and Timestamp work too
          if (a.right.dataType == StringType) {
            try {
              val rightLit = extractStringLit(a.right)
              if (rightLit.isDefined) {
                strfFormat = DateUtils.toStrf(rightLit.get)
              } else {
                willNotWorkOnGpu("format has to be a string literal")
              }
            } catch {
              case x: TimestampFormatConversionException =>
                willNotWorkOnGpu(x.getMessage)
            }
          }
        }

        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression = {
          // passing the already converted strf string for a little optimization
          GpuUnixTimestamp(lhs, rhs, strfFormat)
        }
      })
      .incompat("Incorrectly formatted strings and bogus dates produce garbage data" +
        " instead of null"),
    expr[FromUnixTime](
      "get the String from a unix timestamp",
      (a, conf, p, r) => new BinaryExprMeta[FromUnixTime](a, conf, p, r) {
        var strfFormat: String = _
        override def tagExprForGpu(): Unit = {
          try {
            if (ZoneId.of(a.timeZoneId.get).normalized() != UTC_TIMEZONE_ID) {
              willNotWorkOnGpu("Only UTC zone id is supported")
            }
            val rightLit = extractStringLit(a.right)
            if (rightLit.isDefined) {
              strfFormat = DateUtils.toStrf(rightLit.get)
            } else {
              willNotWorkOnGpu("format has to be a string literal")
            }
          } catch {
            case x: TimestampFormatConversionException =>
              willNotWorkOnGpu(x.getMessage)
          }
        }

        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression = {
          // passing the already converted strf string for a little optimization
          GpuFromUnixTime(lhs, rhs, strfFormat)
        }
      }),
    expr[Pmod](
      "pmod",
      (a, conf, p, r) => new BinaryExprMeta[Pmod](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuPmod(lhs, rhs)
      }),
    expr[Add](
      "addition",
      (a, conf, p, r) => new BinaryExprMeta[Add](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuAdd(lhs, rhs)
      }),
    expr[Subtract](
      "subtraction",
      (a, conf, p, r) => new BinaryExprMeta[Subtract](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuSubtract(lhs, rhs)
      }),
    expr[Multiply](
      "multiplication",
      (a, conf, p, r) => new BinaryExprMeta[Multiply](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuMultiply(lhs, rhs)
      }),
    expr[And](
      "logical and",
      (a, conf, p, r) => new BinaryExprMeta[And](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuAnd(lhs, rhs)
      }),
    expr[Or](
      "logical or",
      (a, conf, p, r) => new BinaryExprMeta[Or](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuOr(lhs, rhs)
      }),
    expr[EqualTo](
      "check if the values are equal",
      (a, conf, p, r) => new BinaryExprMeta[EqualTo](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuEqualTo(lhs, rhs)
      }),
    expr[GreaterThan](
      "> operator",
      (a, conf, p, r) => new BinaryExprMeta[GreaterThan](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuGreaterThan(lhs, rhs)
      }),
    expr[GreaterThanOrEqual](
      ">= operator",
      (a, conf, p, r) => new BinaryExprMeta[GreaterThanOrEqual](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuGreaterThanOrEqual(lhs, rhs)
      }),
    expr[In](
      "IN operator",
      (in, conf, p, r) => new ExprMeta[In](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          val unaliased = in.list.map(extractLit)
          if (!unaliased.forall(_.isDefined)) {
            willNotWorkOnGpu("only literals are supported")
          }
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
      (in, conf, p, r) => new ExprMeta[InSet](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (in.hset.contains(null)) {
            willNotWorkOnGpu("nulls are not supported")
          }
          val literalTypes = in.hset.map(Literal(_).dataType).toSeq
          if (!areAllSupportedTypes(literalTypes:_*)) {
            val unsupported = literalTypes.filter(!areAllSupportedTypes(_)).mkString(", ")
            willNotWorkOnGpu(s"unsupported literal types: $unsupported")
          }
        }
        override def convertToGpu(): GpuExpression =
          GpuInSet(childExprs.head.convertToGpu(), in.hset.map(Literal(_)).toSeq)
      }),
    expr[LessThan](
      "< operator",
      (a, conf, p, r) => new BinaryExprMeta[LessThan](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuLessThan(lhs, rhs)
      }),
    expr[LessThanOrEqual](
      "<= operator",
      (a, conf, p, r) => new BinaryExprMeta[LessThanOrEqual](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuLessThanOrEqual(lhs, rhs)
      }),
    expr[CaseWhen](
      "CASE WHEN expression",
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
      (a, conf, p, r) => new BinaryExprMeta[Pow](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuPow(lhs, rhs)
      }),
    expr[Divide](
      "division",
      (a, conf, p, r) => new BinaryExprMeta[Divide](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuDivide(lhs, rhs)
      }),
    expr[IntegralDivide](
      "division with a integer result",
      (a, conf, p, r) => new BinaryExprMeta[IntegralDivide](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuIntegralDivide(lhs, rhs)
      }),
    expr[Remainder](
      "remainder or modulo",
      (a, conf, p, r) => new BinaryExprMeta[Remainder](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuRemainder(lhs, rhs)
      }),
    expr[AggregateExpression](
      "aggregate expression",
      (a, conf, p, r) => new ExprMeta[AggregateExpression](a, conf, p, r) {
        private val filter: Option[ExprMeta[_]] =
          a.filter.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        private val childrenExprMeta: Seq[ExprMeta[Expression]] =
          a.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override val childExprs: Seq[ExprMeta[_]] = if (filter.isDefined) {
          childrenExprMeta :+ filter.get
        } else {
          childrenExprMeta
        }
        override def convertToGpu(): GpuExpression =
          GpuAggregateExpression(childExprs.head.convertToGpu().asInstanceOf[GpuAggregateFunction],
            a.mode, a.isDistinct, filter.map(_.convertToGpu()) ,a.resultId)
      }),
    expr[SortOrder](
      "sort order",
      (a, conf, p, r) => new UnaryExprMeta[SortOrder](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression =
          GpuSortOrder(child, a.direction, a.nullOrdering, a.sameOrderExpressions, a.child)
      }),
    expr[Count](
      "count aggregate operator",
      (count, conf, p, r) => new ExprMeta[Count](count, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (count.children.size > 1) {
            willNotWorkOnGpu("count of multiple columns not supported")
          }
        }

        override def convertToGpu(): GpuExpression = GpuCount(childExprs.map(_.convertToGpu()))
      }),
    expr[Max](
      "max aggregate operator",
      (max, conf, p, r) => new AggExprMeta[Max](max, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuMax(child)
      }),
    expr[Min](
      "min aggregate operator",
      (a, conf, p, r) => new AggExprMeta[Min](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuMin(child)
      }),
    expr[First](
      "first aggregate operator",
      (a, conf, p, r) => new ExprMeta[First](a, conf, p, r) {
        val child: ExprMeta[_] = GpuOverrides.wrapExpr(a.child, conf, Some(this))
        val ignoreNulls: ExprMeta[_] = GpuOverrides.wrapExpr(a.ignoreNullsExpr, conf, Some(this))
        override val childExprs: Seq[ExprMeta[_]] = Seq(child, ignoreNulls)

        override def convertToGpu(): GpuExpression =
          GpuFirst(child.convertToGpu(), ignoreNulls.convertToGpu())
      }),
    expr[Last](
      "last aggregate operator",
      (a, conf, p, r) => new ExprMeta[Last](a, conf, p, r) {
        val child: ExprMeta[_] = GpuOverrides.wrapExpr(a.child, conf, Some(this))
        val ignoreNulls: ExprMeta[_] = GpuOverrides.wrapExpr(a.ignoreNullsExpr, conf, Some(this))
        override val childExprs: Seq[ExprMeta[_]] = Seq(child, ignoreNulls)

        override def convertToGpu(): GpuExpression =
          GpuLast(child.convertToGpu(), ignoreNulls.convertToGpu())
      }),
    expr[Sum](
      "sum aggregate operator",
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

        override def convertToGpu(child: GpuExpression): GpuExpression = GpuSum(child)
      }),
    expr[Average](
      "average aggregate operator",
      (a, conf, p, r) => new AggExprMeta[Average](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!conf.isFloatAggEnabled) {
            willNotWorkOnGpu("the GPU will sum floating point values in" +
              " parallel to compute an average and the result is not always identical each time." +
              " This can cause some Spark queries to produce an incorrect answer if the value is" +
              " computed more than once as part of the same query. To enable this anyways set" +
              s" ${RapidsConf.ENABLE_FLOAT_AGG} to true")
          }
        }

        override def convertToGpu(child: GpuExpression): GpuExpression = GpuAverage(child)
      }),
    expr[Rand](
      "Generate a random column with i.i.d. uniformly distributed values in [0, 1)",
      (a, conf, p, r) => new UnaryExprMeta[Rand](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuRand(child)
      }),
    expr[SparkPartitionID] (
      "Returns the current partition id.",
      (a, conf, p, r) => new ExprMeta[SparkPartitionID](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuSparkPartitionID()
      }
    ),
    expr[MonotonicallyIncreasingID] (
      "Returns monotonically increasing 64-bit integers.",
      (a, conf, p, r) => new ExprMeta[MonotonicallyIncreasingID](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuMonotonicallyIncreasingID()
      }
    ),
    expr[InputFileName] (
      "Returns the name of the file being read, or empty string if not available.",
      (a, conf, p, r) => new ExprMeta[InputFileName](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileName()
      }
    ),
    expr[InputFileBlockStart] (
      "Returns the start offset of the block being read, or -1 if not available.",
      (a, conf, p, r) => new ExprMeta[InputFileBlockStart](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileBlockStart()
      }
    ),
    expr[InputFileBlockLength] (
      "Returns the length of the block being read, or -1 if not available.",
      (a, conf, p, r) => new ExprMeta[InputFileBlockLength](a, conf, p, r) {
        override def convertToGpu(): GpuExpression = GpuInputFileBlockLength()
      }
    ),
    expr[Upper](
      "String uppercase operator",
      (a, conf, p, r) => new UnaryExprMeta[Upper](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuUpper(child)
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[Lower](
      "String lowercase operator",
      (a, conf, p, r) => new UnaryExprMeta[Lower](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuLower(child)
      })
      .incompat(CASE_MODIFICATION_INCOMPAT),
    expr[StringLocate](
      "Substring search operator",
      (in, conf, p, r) => new TernaryExprMeta[StringLocate](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!in.children(0).isInstanceOf[Literal] || !in.children(2).isInstanceOf[Literal]) {
            willNotWorkOnGpu("only literal search parameters supported")
          } else if (in.children(1).isInstanceOf[Literal]) {
            willNotWorkOnGpu("only operating on columns supported")
          }
        }
        override def convertToGpu(
            val0: GpuExpression,
            val1: GpuExpression,
            val2: GpuExpression): GpuExpression =
          GpuStringLocate(val0, val1, val2)
      }),
    expr[Substring](
      "Substring operator",
      (in, conf, p, r) => new TernaryExprMeta[Substring](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isLit(in.children(1)) || !isLit(in.children(2))) {
            willNotWorkOnGpu("only literal parameters supported for Substring position and " +
              "length parameters")
          }
        }

        override def convertToGpu(
            column: GpuExpression,
            position: GpuExpression,
            length: GpuExpression): GpuExpression =
          GpuSubstring(column, position, length)
      }),
    expr[SubstringIndex](
      "substring_index operator",
      (in, conf, p, r) => new SubstringIndexMeta(in, conf, p, r)),
    expr[StringReplace](
      "StringReplace operator",
      (in, conf, p, r) => new TernaryExprMeta[StringReplace](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(in.children(1)) || !isStringLit(in.children(2))) {
            willNotWorkOnGpu("only literal parameters supported for string literal target and " +
              "replace parameters")
          }
        }

        override def convertToGpu(
            column: GpuExpression,
            target: GpuExpression,
            replace: GpuExpression): GpuExpression =
          GpuStringReplace(column, target, replace)
      }),
    expr[StringTrim](
      "StringTrim operator",
      (in, conf, p, r) => new String2TrimExpressionMeta[StringTrim](in, in.trimStr, conf, p, r) {
        override def convertToGpu(
            column: GpuExpression,
            target: Option[GpuExpression] = None): GpuExpression =
          GpuStringTrim(column, target)
      }),
    expr[StringTrimLeft](
      "StringTrimLeft operator",
      (in, conf, p, r) => new String2TrimExpressionMeta[StringTrimLeft](in, in.trimStr, conf, p, r) {
        override def convertToGpu(
            column: GpuExpression,
            target: Option[GpuExpression] = None): GpuExpression =
          GpuStringTrimLeft(column, target)
      }),
    expr[StringTrimRight](
      "StringTrimRight operator",
      (in, conf, p, r) =>
        new String2TrimExpressionMeta[StringTrimRight](in, in.trimStr, conf, p, r) {
          override def convertToGpu(
              column: GpuExpression,
              target: Option[GpuExpression] = None): GpuExpression =
            GpuStringTrimRight(column, target)
        }
      ),
    expr[StartsWith](
      "Starts With",
      (a, conf, p, r) => new BinaryExprMeta[StartsWith](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(a.right)) {
            willNotWorkOnGpu("only literals are supported for startsWith")
          }
        }
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuStartsWith(lhs, rhs)
      }),
    expr[EndsWith](
      "Ends With",
      (a, conf, p, r) => new BinaryExprMeta[EndsWith](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(a.right)) {
            willNotWorkOnGpu("only literals are supported for endsWith")
          }
        }
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuEndsWith(lhs, rhs)
      }),
    expr[Concat](
      "String Concatenate NO separator",
      (a, conf, p, r) => new ComplexTypeMergingExprMeta[Concat](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {}
        override def convertToGpu(child: Seq[GpuExpression]): GpuExpression = GpuConcat(child)
      }),
    expr[Contains](
      "Contains",
      (a, conf, p, r) => new BinaryExprMeta[Contains](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(a.right)) {
            willNotWorkOnGpu("only literals are supported for Contains right hand side search" +
              " parameter")
          }
        }
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuContains(lhs, rhs)
      }),
    expr[Like](
      "Like",
      (a, conf, p, r) => new BinaryExprMeta[Like](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (!isStringLit(a.right)) {
            willNotWorkOnGpu("only literals are supported for Like right hand side search" +
              " parameter")
          }
        }
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuLike(lhs, rhs, a.escapeChar)
      }),
    expr[RegExpReplace](
      "RegExpReplace",
      (a, conf, p, r) => new TernaryExprMeta[RegExpReplace](a, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          if (isNullOrEmptyOrRegex(a.regexp)) {
            willNotWorkOnGpu(
              "Only non-null, non-empty String literals that are not regex patterns " +
                "are supported by RegExpReplace on the GPU")
          }
        }
        override def convertToGpu(lhs: GpuExpression, regexp: GpuExpression,
          rep: GpuExpression): GpuExpression = GpuStringReplace(lhs, regexp, rep)
      }),
    expr[Length](
      "String Character Length",
      (a, conf, p, r) => new UnaryExprMeta[Length](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuLength(child)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  def wrapScan[INPUT <: Scan](
      scan: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): ScanMeta[INPUT] =
    scans.get(scan.getClass)
      .map(r => r.wrap(scan, conf, parent, r).asInstanceOf[ScanMeta[INPUT]])
      .getOrElse(new RuleNotFoundScanMeta(scan, conf, parent))

  val scans : Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    scan[CSVScan](
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
      }),
    scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new ScanMeta[ParquetScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = GpuParquetScan.tagSupport(this)

        override def convertToGpu(): Scan =
          GpuParquetScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.pushedFilters,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf)
      }),
    scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new ScanMeta[OrcScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit =
          GpuOrcScan.tagSupport(this)

        override def convertToGpu(): Scan =
          GpuOrcScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.pushedFilters,
            a.partitionFilters,
            a.dataFilters,
            conf)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

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
      (hp, conf, p, r) => new PartMeta[HashPartitioning](hp, conf, p, r) {
        override val childExprs: Seq[ExprMeta[_]] =
          hp.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override def convertToGpu(): GpuPartitioning =
          GpuHashPartitioning(childExprs.map(_.convertToGpu()), hp.numPartitions)
      }),
    part[RangePartitioning]( "Range Partitioning",
      (rp, conf, p, r) => new PartMeta[RangePartitioning](rp, conf, p, r) {
        override val childExprs: Seq[ExprMeta[_]] =
          rp.ordering.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
        override def convertToGpu(): GpuPartitioning = {
          if (rp.numPartitions > 1) {
            GpuRangePartitioning(childExprs.map(_.convertToGpu())
              .asInstanceOf[Seq[GpuSortOrder]], rp.numPartitions, new GpuRangePartitioner)
          } else {
            GpuSinglePartitioning(childExprs.map(_.convertToGpu()))
          }
        }
      }),
    part[RoundRobinPartitioning]( "Round Robin Partitioning",
      (rrp, conf, p, r) => new PartMeta[RoundRobinPartitioning](rrp, conf, p, r) {
        override def convertToGpu(): GpuPartitioning = {
          GpuRoundRobinPartitioning(rrp.numPartitions)
        }
      }),
    part[SinglePartition.type]( "Single Partitioning",
      (sp, conf, p, r) => new PartMeta[SinglePartition.type](sp, conf, p, r) {
        override val childExprs: Seq[ExprMeta[_]] = Seq.empty[ExprMeta[_]]
        override def convertToGpu(): GpuPartitioning = {
          GpuSinglePartitioning(childExprs.map(_.convertToGpu()))
        }
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
      "Write to Hadoop FileSystem",
      (a, conf, p, r) => new InsertIntoHadoopFsRelationCommandMeta(a, conf, p, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap

  def wrapPlan[INPUT <: SparkPlan](
      plan: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): SparkPlanMeta[INPUT]  =
    execs.get(plan.getClass)
      .map(r => r.wrap(plan, conf, parent, r).asInstanceOf[SparkPlanMeta[INPUT]])
      .getOrElse(new RuleNotFoundSparkPlanMeta(plan, conf, parent))

  val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    exec[GenerateExec] (
      "The backend for operations that generate more output rows than input rows like explode.",
      (gen, conf, p, r) => new GpuGenerateExecSparkPlanMeta(gen, conf, p, r)),
    exec[ProjectExec](
      "The backend for most select, withColumn and dropColumn statements",
      (proj, conf, p, r) => {
        new SparkPlanMeta[ProjectExec](proj, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuProjectExec(childExprs.map(_.convertToGpu()), childPlans(0).convertIfNeeded())
        }
      }),
    exec[BatchScanExec](
      "The backend for most file input",
      (p, conf, parent, r) => new SparkPlanMeta[BatchScanExec](p, conf, parent, r) {
        override val childScans: scala.Seq[ScanMeta[_]] =
          Seq(GpuOverrides.wrapScan(p.scan, conf, Some(this)))

        override def convertToGpu(): GpuExec =
          GpuBatchScanExec(p.output, childScans(0).convertToGpu())
      }),
    exec[CoalesceExec](
      "The backend for the dataframe coalesce method",
      (coalesce, conf, parent, r) => new SparkPlanMeta[CoalesceExec](coalesce, conf, parent, r) {
        override def convertToGpu(): GpuExec =
          GpuCoalesceExec(coalesce.numPartitions, childPlans.head.convertIfNeeded())
      }),
    exec[DataWritingCommandExec](
      "Writing data",
      (p, conf, parent, r) => new SparkPlanMeta[DataWritingCommandExec](p, conf, parent, r) {
        override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
          Seq(GpuOverrides.wrapDataWriteCmds(p.cmd, conf, Some(this)))

        override def convertToGpu(): GpuExec =
          GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(),
            childPlans.head.convertIfNeeded())
      }),
    exec[FileSourceScanExec](
      "Reading data from files, often from Hive tables",
      (fsse, conf, p, r) => new SparkPlanMeta[FileSourceScanExec](fsse, conf, p, r) {
        // partition filters and data filters are not run on the GPU
        override val childExprs: Seq[ExprMeta[_]] = Seq.empty

        override def tagPlanForGpu(): Unit = GpuFileSourceScanExec.tagSupport(this)

        override def convertToGpu(): GpuExec = {
          val newRelation = HadoopFsRelation(
            wrapped.relation.location,
            wrapped.relation.partitionSchema,
            wrapped.relation.dataSchema,
            wrapped.relation.bucketSpec,
            GpuFileSourceScanExec.convertFileFormat(wrapped.relation.fileFormat),
            wrapped.relation.options)(wrapped.relation.sparkSession)
          GpuFileSourceScanExec(
            newRelation,
            wrapped.output,
            wrapped.requiredSchema,
            wrapped.partitionFilters,
            wrapped.optionalBucketSet,
            wrapped.dataFilters,
            wrapped.tableIdentifier)
        }
      }),
    exec[LocalLimitExec](
      "Per-partition limiting of results",
      (localLimitExec, conf, p, r) =>
        new SparkPlanMeta[LocalLimitExec](localLimitExec, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuLocalLimitExec(localLimitExec.limit, childPlans(0).convertIfNeeded())
        }),
    exec[GlobalLimitExec](
      "Limiting of results across partitions",
      (globalLimitExec, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimitExec, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(globalLimitExec.limit, childPlans(0).convertIfNeeded())
        }),
    exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      (collectLimitExec, conf, p, r) => new GpuCollectLimitMeta(collectLimitExec, conf, p, r)),
    exec[FilterExec](
      "The backend for most filter statements",
      (filter, conf, p, r) => new SparkPlanMeta[FilterExec](filter, conf, p, r) {
        override def convertToGpu(): GpuExec =
          GpuFilterExec(childExprs(0).convertToGpu(), childPlans(0).convertIfNeeded())
      }),
    exec[ShuffleExchangeExec](
      "The backend for most data being exchanged between processes",
      (shuffle, conf, p, r) => new GpuShuffleMeta(shuffle, conf, p, r)),
    exec[UnionExec](
      "The backend for the union operator",
      (union, conf, p, r) => new SparkPlanMeta[UnionExec](union, conf, p, r) {
        override def convertToGpu(): GpuExec =
          GpuUnionExec(childPlans.map(_.convertIfNeeded()))
      }),
    exec[BroadcastExchangeExec](
      "The backend for broadcast exchange of data",
      (exchange, conf, p, r) => new GpuBroadcastMeta(exchange, conf, p, r)),
    exec[BroadcastHashJoinExec](
      "Implementation of join using broadcast data",
      (join, conf, p, r) => new GpuBroadcastHashJoinMeta(join, conf, p, r)),
    exec[ShuffledHashJoinExec](
      "Implementation of join using hashed shuffled data",
      (join, conf, p, r) => new GpuShuffledHashJoinMeta(join, conf, p, r)),
    exec[SortMergeJoinExec](
      "Sort merge join, replacing with shuffled hash join",
      (join, conf, p, r) => new GpuSortMergeJoinMeta(join, conf, p, r)),
    exec[HashAggregateExec](
      "The backend for hash based aggregations",
      (agg, conf, p, r) => new GpuHashAggregateMeta(agg, conf, p, r)),
    exec[SortAggregateExec](
      "The backend for sort based aggregations",
      (agg, conf, p, r) => new GpuSortAggregateMeta(agg, conf, p, r)),
    exec[SortExec](
      "The backend for the sort operator",
      (sort, conf, p, r) => new GpuSortMeta(sort, conf, p, r)),
    exec[ExpandExec](
      "The backend for the expand operator",
      (expand, conf, p, r) => new GpuExpandExecMeta(expand, conf, p, r)),
    exec[WindowExec](
      "Window-operator backend",
      (windowOp, conf, p, r) =>
        new GpuWindowExecMeta(windowOp, conf, p, r)
    )
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
}

case class GpuOverrides() extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan) :SparkPlan = {
    val conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled) {
      val wrap = GpuOverrides.wrapPlan(plan, conf, None)
      wrap.tagForGpu()
      wrap.runAfterTagRules()
      val exp = conf.explain
      if (!exp.equalsIgnoreCase("NONE")) {
        logWarning(s"\n${wrap.explain(exp.equalsIgnoreCase("ALL"))}")
      }
      val convertedPlan = wrap.convertIfNeeded()
      addSortsIfNeeded(convertedPlan, conf)
    } else {
      plan
    }
  }

  private final class SortConfKeysAndIncompat extends ConfKeysAndIncompat {
    override val operationName: String = "Exec"
    override def confKey = "spark.rapids.sql.exec.SortExec"
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
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        val sort = SortExec(requiredOrdering, global = false, child = child)
        // just specifically check Sort to see if we can change Sort to GPUSort
        val sortMeta = new GpuSortMeta(sort, conf, None, new SortConfKeysAndIncompat)
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
