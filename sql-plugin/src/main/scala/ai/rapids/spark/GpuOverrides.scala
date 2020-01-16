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

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
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
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

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
    protected var doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => WRAP_TYPE,
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
  final def wrap(func: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => WRAP_TYPE): this.type = {
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

  final def wrap(op: BASE,
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
    doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => ExprMeta[INPUT],
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
    doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => ScanMeta[INPUT],
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
    doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => PartMeta[INPUT],
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
    doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => SparkPlanMeta[INPUT],
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
    doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => DataWritingCommandMeta[INPUT],
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
    if (cmd.partitionColumns.nonEmpty || cmd.bucketSpec.isDefined) {
      willNotWorkOnGpu("partitioning or bucketing is not supported")
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
        willNotWorkOnGpu("ORC output is not supported")
        None
      case _: ParquetFileFormat =>
        GpuParquetFileFormat.tagGpuSupport(this, spark, cmd.options)
      case _: TextFileFormat =>
        willNotWorkOnGpu("text output is not supported")
        None
      case f =>
        willNotWorkOnGpu(s"unknown file format: ${f.getClass.getCanonicalName}")
        None
    }
  }

  override def convertToGpu(): GpuDataWritingCommand = {
    val format = fileFormat.getOrElse(throw new IllegalStateException("fileFormat missing, tagSelfForGpu not called?"))

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
  val FLOAT_DIFFERS_INCOMPAT =
    "floating point results in some cases may differ with the JVM version by a small amount"
  val FLOAT_DIFFERS_GROUP_INCOMPAT =
    "when enabling these, there may be extra groups produced for floating point grouping keys (e.g. -0.0, and 0.0)"
  val CASE_MODIFICATION_INCOMPAT =
    "in some cases unicode characters change byte width when changing the case. The GPU string " +
    "conversion does not support these characters. For a full list of unsupported characters " +
    "see https://github.com/rapidsai/cudf/issues/3132"
  private val UTC_TIMEZONE_ID = ZoneId.of("UTC").normalized()

  @scala.annotation.tailrec
  def isStringLit(exp: Expression): Boolean = exp match {
    case Literal(_, StringType) => true
    case a: Alias => isStringLit(a.child)
    case _ => false
  }

  def areAllSupportedTypes(types: DataType*): Boolean = {
    types.forall {
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
  }

  /**
   * Checks to see if any expressions are a String Literal
   */
  def isAnyStringLit(expressions: Seq[Expression]): Boolean =
    expressions.exists(isStringLit)

  def tagNoStringChildren(expr: ExprMeta[_ <: Expression]): Unit =
    if (expr.wrapped.children.filter(_.dataType == StringType).nonEmpty) {
      expr.willNotWorkOnGpu(s"Strings are not supported for ${expr.wrapped.getClass.getSimpleName}")
    }

  def expr[INPUT <: Expression](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => ExprMeta[INPUT])
      (implicit tag: ClassTag[INPUT]): ExprRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExprRule[INPUT](doWrap, None, desc, tag)
  }

  def scan[INPUT <: Scan](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => ScanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ScanRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ScanRule[INPUT](doWrap, None, desc, tag)
  }

  def part[INPUT <: Partitioning](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => PartMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): PartRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new PartRule[INPUT](doWrap, None, desc, tag)
  }

  def exec[INPUT <: SparkPlan](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => SparkPlanMeta[INPUT])
    (implicit tag: ClassTag[INPUT]): ExecRule[INPUT] = {
    assert(desc != null)
    assert(doWrap != null)
    new ExecRule[INPUT](doWrap, None, desc, tag)
  }

  def dataWriteCmd[INPUT <: DataWritingCommand](
      desc: String,
      doWrap: (INPUT, RapidsConf, Option[RapidsMeta[_, _, _]], ConfKeysAndIncompat) => DataWritingCommandMeta[INPUT])
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
      (cast, conf, p, r) => new UnaryExprMeta[Cast](cast, conf, p, r) {
        override def tagExprForGpu(): Unit =
          if (!GpuCast.canCast(cast.child.dataType, cast.dataType)) {
            willNotWorkOnGpu(s"casting from ${cast.child.dataType} " +
              s"to ${cast.dataType} is not currently supported on the GPU")
          }

        override def convertToGpu(child: GpuExpression): GpuExpression =
          GpuCast(child, cast.dataType, cast.timeZoneId)
      }),
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
    expr[Asin](
      "inverse sine",
      (a, conf, p, r) => new UnaryExprMeta[Asin](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuAsin(child)
      }),
    expr[Sqrt](
      "square root",
      (a, conf, p, r) => new UnaryExprMeta[Sqrt](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuSqrt(child)
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
    expr[Atan](
      "inverse tangent",
      (a, conf, p, r) => new UnaryExprMeta[Atan](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuAtan(child)
      })
      .incompat(FLOAT_DIFFERS_INCOMPAT),
    expr[Cos](
      "cosine",
      (a, conf, p, r) => new UnaryExprMeta[Cos](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuCos(child)
      })
      .incompat(FLOAT_DIFFERS_INCOMPAT),
    expr[Exp](
      "Euler's number e raised to a power",
      (a, conf, p, r) => new UnaryExprMeta[Exp](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuExp(child)
      })
      .incompat(FLOAT_DIFFERS_INCOMPAT),
    expr[Log](
      "natural log",
      (a, conf, p, r) => new UnaryExprMeta[Log](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuLog(child)
      })
      .incompat(FLOAT_DIFFERS_INCOMPAT),
    expr[Sin](
      "sine",
      (a, conf, p, r) => new UnaryExprMeta[Sin](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuSin(child)
      })
      .incompat(FLOAT_DIFFERS_INCOMPAT),
    expr[Tan](
      "tangent",
      (a, conf, p, r) => new UnaryExprMeta[Tan](a, conf, p, r) {
        override def convertToGpu(child: GpuExpression): GpuExpression = GpuTan(child)
      })
      .incompat(FLOAT_DIFFERS_INCOMPAT),
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
    expr[Pow](
      "lhs ^ rhs",
      (a, conf, p, r) => new BinaryExprMeta[Pow](a, conf, p, r) {
        override def convertToGpu(lhs: GpuExpression, rhs: GpuExpression): GpuExpression =
          GpuPow(lhs, rhs)
      })
      .incompat(FLOAT_DIFFERS_INCOMPAT),
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
        override def convertToGpu(): GpuExpression =
          GpuAggregateExpression(childExprs(0).convertToGpu().asInstanceOf[GpuAggregateFunction],
            a.mode, a.isDistinct, a.resultId)
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
          if (!count.children.forall(_.isInstanceOf[Literal])) {
            willNotWorkOnGpu("only count(*) or count(1) supported")
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

        override def tagExprForGpu(): Unit = {
          if (a.ignoreNullsExpr.semanticEquals(Literal(false))) {
            willNotWorkOnGpu("including nulls is not supported, use first(col, true)")
          }
        }

        override def convertToGpu(): GpuExpression =
          GpuFirst(child.convertToGpu(), ignoreNulls.convertToGpu())
      }),
    expr[Last](
      "last aggregate operator",
      (a, conf, p, r) => new ExprMeta[Last](a, conf, p, r) {
        val child: ExprMeta[_] = GpuOverrides.wrapExpr(a.child, conf, Some(this))
        val ignoreNulls: ExprMeta[_] = GpuOverrides.wrapExpr(a.ignoreNullsExpr, conf, Some(this))
        override val childExprs: Seq[ExprMeta[_]] = Seq(child, ignoreNulls)

        override def tagExprForGpu(): Unit = {
          if (a.ignoreNullsExpr.semanticEquals(Literal(false))) {
            willNotWorkOnGpu("including nulls is not supported, use last(col, true)")
          }
        }

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
              " parallel and the result is not always identical each time. This can cause some Spark" +
              s" queries to produce an incorrect answer if the value is computed more than once" +
              s" as part of the same query.  To enable this anyways set" +
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
      .incompat(CASE_MODIFICATION_INCOMPAT)
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
            conf.maxReadBatchSize)
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
            conf)
      }),
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
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Partitioning]), r)).toMap

  def wrapDataWriteCmds[INPUT <: DataWritingCommand](
      writeCmd: INPUT,
      conf: RapidsConf,
      parent: Option[RapidsMeta[_, _, _]]): DataWritingCommandMeta[INPUT] =
    dataWriteCmds.get(writeCmd.getClass)
      .map(r => r.wrap(writeCmd, conf, parent, r).asInstanceOf[DataWritingCommandMeta[INPUT]])
      .getOrElse(new RuleNotFoundDataWritingCommandMeta(writeCmd, conf, parent))

  val dataWriteCmds: Map[Class[_ <: DataWritingCommand], DataWritingCommandRule[_ <: DataWritingCommand]] = Seq(
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
    exec[ProjectExec](
      "The backend for most select, withColumn and dropColumn statements",
      (proj, conf, p, r) => {
        new SparkPlanMeta[ProjectExec](proj, conf, p, r) {
          override def tagPlanForGpu(): Unit = {
            if (isAnyStringLit(wrapped.expressions)) {
              willNotWorkOnGpu("string literal values are not supported in a projection")
            }
          }

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
          GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(), childPlans.head.convertIfNeeded())
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
    exec[SortExec](
      "The backend for the sort operator",
      (sort, conf, p, r) => new GpuSortMeta(sort, conf, p, r))
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
      val childOrdering = child.outputOrdering
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
