/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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
import ai.rapids.cudf.{Cuda, Rmm, RmmAllocationMode}
import ai.rapids.spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.sources.v2.reader.Scan
import org.apache.spark.sql.types._
import org.apache.spark.{ExecutorPlugin, SparkEnv}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.rapids.{GpuAggregateExpression, GpuAggregateFunction, GpuAverage, GpuCount, GpuFirst, GpuLast, GpuMax, GpuMin, GpuSum}

trait GpuExec extends SparkPlan {
  override def supportsColumnar = true

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[GpuExec]
  }

  override def hashCode(): Int = super.hashCode()
}

trait GpuPartitioning extends Partitioning {
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[GpuPartitioning]
  }

  override def hashCode(): Int = super.hashCode()
}

class CannotReplaceException(str: String) extends RuntimeException(str) {

}

/**
 * Base class for replacement rules.
 */
abstract class ReplacementRule[INPUT, INPUT_BASE, OUTPUT](
    final val doConvert: (INPUT, GpuOverrides) => OUTPUT,
    final val doAssertIsAllowed: (INPUT, RapidsConf) => Unit,
    final val isIncompat: Boolean,
    final val incompatDoc: String,
    final val desc: String,
    final val context: String, // Only do the replacement if the contexts match
    final val tag: ClassTag[INPUT]) {

  private var confKeyCache: String = null
  def confKey: String = {
    if (confKeyCache == null) {
      confKeyCache = "spark.rapids.sql." + confKeyPart + "." + tag.runtimeClass.getSimpleName
    }
    confKeyCache
  }
  val confKeyPart: String
  val operationName: String

  def confHelp(): Unit = {
    println(s"${confKey}:")
    println(s"\tEnable (true) or disable (false) the ${tag} ${operationName}.")
    println(s"\t${desc}")
    if (isIncompat) {
      println(s"\tThis is not 100% compatible with the Spark version because ${incompatDoc}")
    }
    println(s"\tdefault: ${!isIncompat}")
    println()
  }

  final def assertIsAllowed(op: INPUT_BASE, conf: RapidsConf): Unit = {
    doAssertIsAllowed(op.asInstanceOf[INPUT], conf)
  }

  final def convert(op: INPUT_BASE, overrides: GpuOverrides): OUTPUT = {
    doConvert(op.asInstanceOf[INPUT], overrides)
  }
}

abstract class ReplacementRuleBuilder[INPUT, OUTPUT] {
  private def defaultAssert(exp: INPUT, conf: RapidsConf): Unit = {}

  protected var doAssertIsAllowed: (INPUT, RapidsConf) => Unit = defaultAssert
  protected var doConvert: (INPUT, GpuOverrides) => OUTPUT = null
  protected var isIncompat = false
  protected var incompatDoc: String = null
  protected var desc: String = null
  protected var context: String = null

  /**
   * Add the conversion function to the expression rule. This converts the original
   * INPUT into a OUTPUT
   * @param func the conversion code.
   * @return this for chaining
   */
  final def convert(func: (INPUT, GpuOverrides) => OUTPUT): this.type = {
    this.doConvert = func
    this
  }

  /**
   * Mark this expression as incompatible with the original Spark version
   * @param str a description of how it is incompatible.
   * @return this for chaining.
   */
  final def incompat(str: String) : this.type = {
    isIncompat = true
    incompatDoc = str
    this
  }

  /**
   * Set an additional assertion function that verifies conversion is allowed before
   * calling the conversion function.
   * @param func takes the original operator and checks if conversion is allowed.  If not
   *             a [[CannotReplaceException]] should be throw with a description of why
   *             it cannot be replaced.
   * @return this for chaining.
   */
  final def assertIsAllowed(func: (INPUT, RapidsConf) => Unit): this.type = {
    doAssertIsAllowed = func
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

  /**
   * Set the context that this operator is allowed to be replaced under.
   * @param str the context
   * @return this for chaining.
   */
  final def context(str: String): this.type = {
    this.context = str
    this
  }
}

/**
 * Holds everything that is needed to replace an Expression with a GPU enabled version.
 */
class ExprRule[INPUT <: Expression](
    doConvert: (INPUT, GpuOverrides) => GpuExpression,
    doAssertIsAllowed: (INPUT, RapidsConf) => Unit,
    isIncompat: Boolean,
    incompatDoc: String,
    desc: String,
    context: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Expression, GpuExpression](doConvert,
    doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag) {

  override val confKeyPart = "expression"
  override val operationName = "Expression"
}

/**
 * Builds an [[ExprRule]] from the given inputs.
 *
 * @param tag implicitly set value of the INPUT type
 * @tparam INPUT the type of INPUT [[Expression]] this rule will replace.
 */
class ExprRuleBuilder[INPUT <: Expression](implicit val tag: ClassTag[INPUT])
  extends ReplacementRuleBuilder[INPUT, GpuExpression] {

  /**
   * Set a simple conversion function for a [[UnaryExpression]].
   * @param func takes the already converted child of the expression and produces the
   *             converted expression.
   * @return this for chaining
   */
  final def unary(func: GpuExpression => GpuExpression): ExprRuleBuilder[INPUT] = {
    if (!classOf[UnaryExpression].isAssignableFrom(tag.runtimeClass)) {
      throw new IllegalStateException(s"unary called on a class that is not" +
        s" a UnaryExpression ${tag}")
    }
    convert((exp, overrides) => {
      val child = overrides.replaceWithGpuExpression(exp.asInstanceOf[UnaryExpression].child)
      func(child)
    })
  }

  final def unarySort(
      func: (GpuExpression, SortDirection, NullOrdering, Set[Expression]) => GpuExpression)
    : ExprRuleBuilder[INPUT] = {

    if (!classOf[UnaryExpression].isAssignableFrom(tag.runtimeClass)) {
      throw new IllegalStateException(s"unarySort called on a class that is not" +
        s" a UnaryExpression ${tag}")
    }
    convert((exp, overrides) => {
      val sortExp = exp.asInstanceOf[SortOrder]
      val child = overrides.replaceWithGpuExpression(exp.asInstanceOf[UnaryExpression].child)
      func(child, sortExp.direction, sortExp.nullOrdering, sortExp.sameOrderExpressions)
    })
  }

  /**
   * Set a conversion function for a [[UnaryExpression]] that needs access to the original operator.
   * @param func takes the original expression and the already converted child expression and
   *             produces the converted expression.
   * @return this for chaining
   */
  final def fullUnary(func: (INPUT, GpuExpression) => GpuExpression): ExprRuleBuilder[INPUT] = {
    if (!classOf[UnaryExpression].isAssignableFrom(tag.runtimeClass)) {
      throw new IllegalStateException(s"fullUnary called on a class that is not" +
        s" a UnaryExpression ${tag}")
    }
    convert((exp, overrides) => {
      val child = overrides.replaceWithGpuExpression(exp.asInstanceOf[UnaryExpression].child)
      func(exp, child)
    })
  }

  /**
   * Set a simple conversion function for a [[BinaryExpression]].
   * @param func takes the already converted children (left, right) of the expression and
   *             produces the converted expression.
   * @return this for chaining
   */
  final def binary(func: (GpuExpression, GpuExpression) => GpuExpression): ExprRuleBuilder[INPUT] = {
    if (!classOf[BinaryExpression].isAssignableFrom(tag.runtimeClass)) {
      throw new IllegalStateException(s"binary called on a class that is not" +
        s" a BinaryExpression ${tag}")
    }
    convert((exp, overrides) => {
      val bin = exp.asInstanceOf[BinaryExpression]
      val left = overrides.replaceWithGpuExpression(bin.left)
      val right = overrides.replaceWithGpuExpression(bin.right)
      func(left, right)
    })
  }

  /**
    *
    * Set a simple conversion function for a [[AggregateExpression]].
    * @param func takes the already converted aggregate function, the AggregateMode (Partial, Final, PartialMerge),
    *             whether it is distinct or not, its expression id (resultId)
    * @return this for chaining
    */
  final def aggregate(func: (GpuAggregateFunction, AggregateMode, Boolean, ExprId) => GpuExpression): ExprRuleBuilder[INPUT] = {
    if (!classOf[AggregateExpression].isAssignableFrom(tag.runtimeClass)) {
      throw new IllegalStateException(s"aggregate called on a class that is not" +
        s" a AggregateExpression ${tag}")
    }
    convert((exp, overrides) => {
      val aggExp = exp.asInstanceOf[AggregateExpression]
      if (aggExp.isDistinct) {
        throw new CannotReplaceException("distinct aggregates are not supported")
      }
      val aggFn = overrides.replaceWithGpuAggregate(aggExp.aggregateFunction)
      func(aggFn, aggExp.mode, aggExp.isDistinct, aggExp.resultId)
    })
  }

  /**
   * Build the final rule.
   * @return the rule along with the class it is replacing.
   */
  final def build(): (Class[_ <: Expression], ExprRule[_ <: Expression]) = {
    if (doConvert == null) {
      throw new IllegalStateException(s"Conversion function for ${tag} was not set")
    }
    (tag.runtimeClass.asSubclass(classOf[Expression]),
      new ExprRule[INPUT](doConvert, doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag))
  }
}


/**
 * Holds everything that is needed to replace a [[Scan]] with a GPU enabled version.
 */
class ScanRule[INPUT <: Scan](
    doConvert: (INPUT, GpuOverrides) => Scan,
    doAssertIsAllowed: (INPUT, RapidsConf) => Unit,
    isIncompat: Boolean,
    incompatDoc: String,
    desc: String,
    context: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Scan, Scan](doConvert,
    doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag) {

  override val confKeyPart: String = "input"
  override val operationName: String = "input"
}

/**
 * Builds an [[ScanRule]] from the given inputs.
 *
 * @param tag implicitly set value of the INPUT type
 * @tparam INPUT the type of INPUT [[Scan]] this rule will replace.
 */
class ScanRuleBuilder[INPUT <: Scan](implicit val tag: ClassTag[INPUT])
  extends ReplacementRuleBuilder[INPUT, Scan] {

  /**
   * Build the final rule.
   * @return the rule along with the class it is replacing.
   */
  final def build(): (Class[_ <: Scan], ScanRule[_ <: Scan]) = {
    if (doConvert == null) {
      throw new IllegalStateException(s"Conversion function for ${tag} was not set")
    }
    (tag.runtimeClass.asSubclass(classOf[Scan]),
      new ScanRule[INPUT](doConvert, doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag))
  }
}

/**
 * Holds everything that is needed to replace a [[Partitioning]] with a GPU enabled version.
 */
class PartRule[INPUT <: Partitioning](
    doConvert: (INPUT, GpuOverrides) => GpuPartitioning,
    doAssertIsAllowed: (INPUT, RapidsConf) => Unit,
    isIncompat: Boolean,
    incompatDoc: String,
    desc: String,
    context: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, Partitioning, GpuPartitioning](doConvert,
    doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag) {

  override val confKeyPart: String = "partitioning"
  override val operationName: String = "partitioning"
}

/**
 * Builds an [[PartRule]] from the given inputs.
 *
 * @param tag implicitly set value of the INPUT type
 * @tparam INPUT the type of INPUT [[Partitioning]] this rule will replace.
 */
class PartRuleBuilder[INPUT <: Partitioning](implicit val tag: ClassTag[INPUT])
  extends ReplacementRuleBuilder[INPUT, GpuPartitioning] {

  /**
   * Build the final rule.
   * @return the rule along with the class it is replacing.
   */
  final def build(): (Class[_ <: Partitioning], PartRule[_ <: Partitioning]) = {
    if (doConvert == null) {
      throw new IllegalStateException(s"Conversion function for ${tag} was not set")
    }
    (tag.runtimeClass.asSubclass(classOf[Partitioning]),
      new PartRule[INPUT](doConvert, doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag))
  }
}

/**
 * Holds everything that is needed to replace a [[SparkPlan]] with a GPU enabled version.
 */
class ExecRule[INPUT <: SparkPlan](
    doConvert: (INPUT, GpuOverrides) => GpuExec,
    doAssertIsAllowed: (INPUT, RapidsConf) => Unit,
    isIncompat: Boolean,
    incompatDoc: String,
    desc: String,
    context: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, SparkPlan, GpuExec](doConvert,
    doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag){

  override val confKeyPart: String = "exec"
  override val operationName: String = "operator"
}

/**
 * Builds an [[ExecRule]] from the given inputs.
 *
 * @param tag implicitly set value of the INPUT type
 * @tparam INPUT the type of INPUT [[SparkPlan]] this rule will replace.
 */
class ExecRuleBuilder[INPUT <: SparkPlan](implicit val tag: ClassTag[INPUT])
  extends ReplacementRuleBuilder[INPUT, GpuExec] {

  /**
   * Build the final rule.
   * @return the rule along with the class it is replacing.
   */
  final def build(): (Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]) = {
    if (doConvert == null) {
      throw new IllegalStateException(s"Conversion function for ${tag} was not set")
    }
    (tag.runtimeClass.asSubclass(classOf[SparkPlan]),
      new ExecRule[INPUT](doConvert, doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag))
  }
}

/**
  * Holds everything that is needed to replace a [[AggregateFunction]] with a GPU enabled version.
  */
class AggRule[INPUT <: AggregateFunction](
    doConvert: (INPUT, GpuOverrides) => GpuAggregateFunction,
    doAssertIsAllowed: (INPUT, RapidsConf) => Unit,
    isIncompat: Boolean,
    incompatDoc: String,
    desc: String,
    context: String,
    tag: ClassTag[INPUT])
  extends ReplacementRule[INPUT, AggregateFunction, GpuAggregateFunction](doConvert,
    doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag){

  override val confKeyPart: String = "agg"
  override val operationName: String = "aggregation"
}
/**
  * Builds an [[AggRule]] from the given inputs.
  *
  * @param tag implicitly set value of the INPUT type
  * @tparam INPUT the type of INPUT [[AggregateFunction]] this rule will replace.
  */
class AggRuleBuilder[INPUT <: AggregateFunction](implicit val tag: ClassTag[INPUT])
  extends ReplacementRuleBuilder[INPUT, GpuAggregateFunction] {

  /**
    * Build the final rule.
    * @return the rule along with the class it is replacing.
    */
  final def build(): (Class[_ <: AggregateFunction], AggRule[_ <: AggregateFunction]) = {
    if (doConvert == null) {
      throw new IllegalStateException(s"Conversion function for ${tag} was not set")
    }
    (tag.runtimeClass.asSubclass(classOf[AggregateFunction]),
      new AggRule[INPUT](doConvert, doAssertIsAllowed, isIncompat, incompatDoc, desc, context, tag))
  }
}

object GpuOverrides {
  val FLOAT_DIFFERS_INCOMPAT =
    "floating point results in some cases may differ with the JVM version by a small amount"
  val FLOAT_DIFFERS_GROUP_INCOMPAT =
    "when enabling these, there may be extra groups produced for floating point grouping keys (e.g. -0.0, and 0.0)"
  val DIVIDE_BY_ZERO_INCOMPAT = "divide by 0 does not result in null"
  private val UTC_TIMEZONE_ID = ZoneId.of("UTC").normalized()

  def isStringLit(exp: Expression): Boolean = exp match {
    case Literal(_, StringType) => true
    case a: Alias => isStringLit(a.child)
    case _ => false
  }

  /**
   * Checks to see if any expressions are a String Literal
   */
  def isAnyStringLit(expressions: Seq[Expression]): Boolean =
    expressions.filter(isStringLit).nonEmpty

  def expr[INPUT <: Expression](implicit tag: ClassTag[INPUT]): ExprRuleBuilder[INPUT] = {
    new ExprRuleBuilder[INPUT]()
  }

  def scan[INPUT <: Scan](implicit tag: ClassTag[INPUT]): ScanRuleBuilder[INPUT] = {
    new ScanRuleBuilder[INPUT]()
  }

  def part[INPUT <: Partitioning](implicit tag: ClassTag[INPUT]): PartRuleBuilder[INPUT] = {
    new PartRuleBuilder[INPUT]()
  }

  def exec[INPUT <: SparkPlan](implicit tag: ClassTag[INPUT]): ExecRuleBuilder[INPUT] = {
    new ExecRuleBuilder[INPUT]()
  }

  def agg[INPUT <: AggregateFunction](implicit tag: ClassTag[INPUT]): AggRuleBuilder[INPUT] = {
    new AggRuleBuilder[INPUT]()
  }

  val expressions : Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Map(
    expr[Literal]
      .convert((lit, overrides) => new GpuLiteral(lit.value, lit.dataType))
      .desc("holds a static value from the query")
      .build(),
    expr[Alias]
      .convert((a, overrides) =>
        new GpuAlias(overrides.replaceWithGpuExpression(a.child), a.name)(
          a.exprId, a.qualifier, a.explicitMetadata))
      .desc("gives a column a name")
      .build(),
    expr[AttributeReference]
      .convert((att, overrides) =>
        new GpuAttributeReference(att.name, att.dataType, att.nullable,
          att.metadata)(att.exprId, att.qualifier))
      .desc("references an input column")
      .build(),
    expr[Cast]
      .fullUnary((cast, child) => new GpuCast(child, cast.dataType, cast.timeZoneId))
      .assertIsAllowed((cast, conf) =>
        if (!GpuCast.canCast(cast.child.dataType, cast.dataType)) {
          throw new CannotReplaceException(s"casting from ${cast.child.dataType} " +
            s"to ${cast.dataType} is not currently supported on the GPU")
        })
      .desc("convert a column of one type of data into another type")
      .build(),
    expr[UnaryMinus]
      .unary(new GpuUnaryMinus(_))
      .desc("negate a numeric value")
      .build(),
    expr[UnaryPositive]
      .unary(new GpuUnaryPositive(_))
      .desc("a numeric value with a + in front of it")
      .build(),
    expr[Year]
      .unary(new GpuYear(_))
      .desc("get the year from a date or timestamp")
      .build(),
    expr[Month]
      .unary(new GpuMonth(_))
      .desc("get the month from a date or timestamp")
      .build(),
    expr[DayOfMonth]
      .unary(new GpuDayOfMonth(_))
      .desc("get the day of the month from a date or timestamp")
      .build(),
    expr[Abs]
      .unary(new GpuAbs(_))
      .desc("absolute value")
      .build(),
    expr[Acos]
      .unary(new GpuAcos(_))
      .desc("inverse cosine")
      .build(),
    expr[Asin]
      .unary(new GpuAsin(_))
      .desc("inverse sine")
      .build(),
    expr[Sqrt]
      .unary(new GpuSqrt(_))
      .desc("square root")
      .build(),
    expr[Floor]
      .unary(new GpuFloor(_))
      .desc("floor of a number")
      .build(),
    expr[Ceil]
      .unary(new GpuCeil(_))
      .desc("ceiling of a number")
      .build(),
    expr[Not]
      .unary(new GpuNot(_))
      .desc("boolean not operator")
      .build(),
    expr[IsNull]
      .unary(new GpuIsNull(_))
      .desc("checks if a value is null")
      .build(),
    expr[IsNotNull]
      .unary(new GpuIsNotNull(_))
      .desc("checks if a value is not null")
      .build(),
    expr[Atan]
      .unary(new GpuAtan(_))
      .desc("inverse tangent")
      .incompat(FLOAT_DIFFERS_INCOMPAT)
      .build(),
    expr[Cos]
      .unary(new GpuCos(_))
      .desc("cosine")
      .incompat(FLOAT_DIFFERS_INCOMPAT)
      .build(),
    expr[Exp]
      .unary(new GpuExp(_))
      .desc("Euler's number e raised to a power")
      .incompat(FLOAT_DIFFERS_INCOMPAT)
      .build(),
    expr[Log]
      .unary(new GpuLog(_))
      .desc("natural log")
      .incompat(FLOAT_DIFFERS_INCOMPAT)
      .build(),
    expr[Sin]
      .unary(new GpuSin(_))
      .desc("sine")
      .incompat(FLOAT_DIFFERS_INCOMPAT)
      .build(),
    expr[Tan]
      .unary(new GpuTan(_))
      .desc("tangent")
      .incompat(FLOAT_DIFFERS_INCOMPAT)
      .build(),
    expr[NormalizeNaNAndZero]
      .unary(new GpuNormalizeNaNAndZero(_))
      .desc("normalize nan and zero")
      .incompat(FLOAT_DIFFERS_GROUP_INCOMPAT)
      .build(),
    expr[KnownFloatingPointNormalized]
      .unary(new GpuKnownFloatingPointNormalized(_))
      .desc("tag to prevent redundant normalization")
      .incompat(FLOAT_DIFFERS_GROUP_INCOMPAT)
      .build(),
    expr[Add]
      .binary(new GpuAdd(_, _))
      .desc("addition")
      .build(),
    expr[Subtract]
      .binary(new GpuSubtract(_, _))
      .desc("subtraction")
      .build(),
    expr[Multiply]
      .binary(new GpuMultiply(_, _))
      .desc("multiplication")
      .build(),
    expr[And]
      .binary(new GpuAnd(_, _))
      .desc("logical and")
      .build(),
    expr[Or]
      .binary(new GpuOr(_, _))
      .desc("logical or")
      .build(),
    expr[EqualTo]
      .binary(new GpuEqualTo(_, _))
      .desc("check if the values are equal")
      .build(),
    expr[GreaterThan]
      .binary(new GpuGreaterThan(_, _))
      .desc("> operator")
      .build(),
    expr[GreaterThanOrEqual]
      .binary(new GpuGreaterThanOrEqual(_, _))
      .desc(">= operator")
      .build(),
    expr[LessThan]
      .binary(new GpuLessThan(_, _))
      .desc("< operator")
      .build(),
    expr[LessThanOrEqual]
      .binary(new GpuLessThanOrEqual(_, _))
      .desc("<= operator")
      .build(),
    expr[Pow]
      .binary(new GpuPow(_, _))
      .desc("lhs ^ rhs")
      .incompat(FLOAT_DIFFERS_INCOMPAT)
      .build(),
    expr[Divide]
      .binary(new GpuDivide(_, _))
      .desc("division")
      .incompat(DIVIDE_BY_ZERO_INCOMPAT)
      .build(),
    expr[IntegralDivide]
      .binary(new GpuIntegralDivide(_, _))
      .desc("division with a integer result")
      .incompat(DIVIDE_BY_ZERO_INCOMPAT)
      .build(),
    expr[Remainder]
      .binary(new GpuRemainder(_, _))
      .desc("remainder or modulo")
      .incompat(DIVIDE_BY_ZERO_INCOMPAT)
      .build(),
    expr[AggregateExpression]
      .aggregate(GpuAggregateExpression(_, _, _, _))
      .desc("aggregate expression")
      .build(),
    expr[SortOrder]
      .unarySort(new GpuSortOrder(_, _, _, _))
      .desc("sort order")
      .build()
  )

  val scans : Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Map(
    scan[CSVScan]
      .convert((scan, overrides) =>
        GpuCSVScan(scan.sparkSession,
          scan.fileIndex,
          scan.dataSchema,
          scan.readDataSchema,
          scan.readPartitionSchema,
          scan.options,
          overrides.conf.maxReadBatchSize))
      .desc("CSV parsing")
      .assertIsAllowed((scan, conf) => GpuCSVScan.assertCanSupport(scan))
      .build(),
    scan[ParquetScan]
      .convert((scan, overrides) =>
        GpuParquetScan(scan.sparkSession,
          scan.hadoopConf,
          scan.fileIndex,
          scan.dataSchema,
          scan.readDataSchema,
          scan.readPartitionSchema,
          scan.pushedFilters,
          scan.options,
          overrides.conf))
      .desc("Parquet parsing")
      .assertIsAllowed((scan, conf) => GpuParquetScan.assertCanSupport(scan, conf))
      .build(),
    scan[OrcScan]
      .convert((scan, overrides) =>
        GpuOrcScan(scan.sparkSession,
          scan.hadoopConf,
          scan.fileIndex,
          scan.dataSchema,
          scan.readDataSchema,
          scan.readPartitionSchema,
          scan.options,
          scan.pushedFilters,
          overrides.conf))
      .desc("ORC parsing")
      .assertIsAllowed((scan, conf) => GpuOrcScan.assertCanSupport(scan, conf))
      .build(),
  )

  val parts : Map[Class[_ <: Partitioning], PartRule[_ <: Partitioning]] = Map(
    part[HashPartitioning]
      .convert((hp, overrides) =>
        new GpuHashPartitioning(
          hp.expressions.map(overrides.replaceWithGpuExpression), hp.numPartitions))
      .assertIsAllowed((hp, conf) =>
        if (hp.expressions.map(_.dataType).contains(StringType)) {
          throw new CannotReplaceException("strings are not supported as the keys for hash partitioning.")
        })
      .desc("Hash based partitioning")
      .build()
  )

  private val GPU_BROADCAST_HASH_JOIN_CONTEXT = "GpuBroadcastHashJoinExec"

  val execs : Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Map(
    exec[ProjectExec]
      .convert((plan, overrides) =>
        GpuProjectExec(plan.projectList.map(overrides.replaceWithGpuExpression),
              overrides.replaceWithGpuPlan(plan.child)))
      .desc("The backend for most select, withColumn and dropColumn statements")
      .assertIsAllowed((proj, conf) =>
        if (isAnyStringLit(proj.expressions)) {
          throw new CannotReplaceException("string literal values are not supported in a projection")
        })
      .build(),
    exec[BatchScanExec]
      .convert((exec, overrides) =>
        GpuBatchScanExec(exec.output.map(
          (exec) => overrides.replaceWithGpuExpression(exec).asInstanceOf[AttributeReference]),
          overrides.replaceWithGpuScan(exec.scan)))
      .desc("The backend for most file input")
      .build(),
    exec[FilterExec]
      .convert((filter, overrides) =>
        GpuFilterExec(overrides.replaceWithGpuExpression(filter.condition),
          overrides.replaceWithGpuPlan(filter.child)))
      .desc("The backend for most filter statements")
      .build(),
    exec[ShuffleExchangeExec]
      .convert((shuffle, overrides) =>
        GpuShuffleExchangeExec(overrides.replaceWithGpuPartitioning(shuffle.outputPartitioning),
          overrides.replaceWithGpuPlan(shuffle.child), shuffle.canChangeNumPartitions))
      .desc("The backend for most data being exchanged between processes")
      .assertIsAllowed((shuffle, conf) => {
        if (shuffle.output.map(_.dataType).contains(StringType)) {
          throw new CannotReplaceException("Strings are not currently supported for shuffling on GPU")
        }
      })
      .build(),
    exec[UnionExec]
      .convert((union, overrides) =>
        GpuUnionExec(union.children.map(overrides.replaceWithGpuPlan(_))))
      .desc("The backend for the union operator")
      .build(),
    exec[BroadcastExchangeExec]
      .convert((exchange, overrides) => GpuBroadcastExchangeExec(exchange.mode, exchange.child))
      .desc("The backend for broadcast exchange of data")
      .assertIsAllowed((exec, conf) => {
        if (!TrampolineUtil.isHashedRelation(exec.mode)) {
          throw new CannotReplaceException("Broadcast exchange is only supported for HashedJoin")
        }
      })
      .context(GPU_BROADCAST_HASH_JOIN_CONTEXT)
      .build(),
    exec[BroadcastHashJoinExec]
      .convert((join, overrides) => {
        val left = overrides.replaceWithGpuPlan(join.left, GPU_BROADCAST_HASH_JOIN_CONTEXT)
        val right = overrides.replaceWithGpuPlan(join.right, GPU_BROADCAST_HASH_JOIN_CONTEXT)
        // The broadcast part of this must be a BroadcastExchangeExec
        val buildSide = join.buildSide match {
          case BuildLeft => left
          case BuildRight => right
        }
        if (!buildSide.isInstanceOf[GpuBroadcastExchangeExec]) {
          throw new CannotReplaceException("the broadcast must be on the GPU too")
        }
        GpuBroadcastHashJoinExec(
          join.leftKeys.map(overrides.replaceWithGpuExpression),
          join.rightKeys.map(overrides.replaceWithGpuExpression),
          join.joinType, join.buildSide,
          join.condition.map(overrides.replaceWithGpuExpression),
          left, right)
      })
      .desc("Implementation of join using broadcast data")
      .assertIsAllowed((exec, conf) => {
        GpuHashJoin.assertJoinTypeAllowed(exec.joinType)
        if (exec.output.map(_.dataType).contains(StringType)) {
          throw new CannotReplaceException("strings are not supported in a hash join.")
        } else if (exec.condition != None) {
          throw new CannotReplaceException("Conditional joins are not currently supported")
        }
      })
      .incompat("GPU required on the driver and joins with empty batches may fail")
      .build(),
    exec[ShuffledHashJoinExec]
      .convert((join, overrides) => {
        val left = overrides.replaceWithGpuPlan(join.left)
        val right = overrides.replaceWithGpuPlan(join.right)
        GpuShuffledHashJoinExec(
          join.leftKeys.map(overrides.replaceWithGpuExpression),
          join.rightKeys.map(overrides.replaceWithGpuExpression),
          join.joinType, join.buildSide,
          join.condition.map(overrides.replaceWithGpuExpression),
          left, right)
      })
      .desc("Implementation of join using hashed shuffled data")
      .assertIsAllowed((exec, conf) => {
        GpuHashJoin.assertJoinTypeAllowed(exec.joinType)
        if (exec.output.map(_.dataType).contains(StringType)) {
          throw new CannotReplaceException("strings are not supported in a hash join.")
        } else if (exec.condition != None) {
          throw new CannotReplaceException("Conditional joins are not currently supported")
        }
      })
      .incompat("joins with empty batches may fail")
      .build(),
    exec[HashAggregateExec]
      .convert((hashAgg, overrides) =>
        GpuHashAggregateExec(
          hashAgg.requiredChildDistributionExpressions.map(_.map(overrides.replaceWithGpuExpression)),
          hashAgg.groupingExpressions.map(overrides.replaceWithGpuExpression),
          hashAgg.aggregateExpressions.map(overrides.replaceWithGpuExpression).asInstanceOf[Seq[GpuAggregateExpression]],
          hashAgg.aggregateAttributes.map(overrides.replaceWithGpuExpression).asInstanceOf[Seq[GpuAttributeReference]],
          hashAgg.initialInputBufferOffset,
          hashAgg.resultExpressions.map(overrides.replaceWithGpuExpression).asInstanceOf[Seq[NamedExpression]],
          overrides.replaceWithGpuPlan(hashAgg.child)))
      .assertIsAllowed((hashAgg, conf) => {
        // The StringType check was added due to core dump in the latest branch while running the mortgage tests
        // in ~NVStrings.
        //
        // Note that we replace only if resultExpressions is non empty. This is due to a case
        // where HashAggregateExec nodes can be added with empty result expression (the MortgageTest exposes this).
        // The only choice should be to group by the grouping expressions, and use the grouping
        // key + the aggregate expressions as the result, but for now lets not handle it until
        // we can shed some light (in our tests the HashAggregateExec node without result expression appears
        // to go away after the plugin anyway)
        if (isAnyStringLit(hashAgg.groupingExpressions)) {
          throw new CannotReplaceException("string literal values are not supported in a hash aggregate")
        }
        val groupingExpressionTypes = hashAgg.groupingExpressions.map(_.dataType)
        val aggregateExpressionTypes = hashAgg.aggregateExpressions.map(_.dataType)
        if (groupingExpressionTypes.contains(StringType)) {
          throw new CannotReplaceException("strings are not supported as grouping keys for hash aggregation.")
        }
        if (conf.hasNans &&
          (groupingExpressionTypes.contains(FloatType) ||
            groupingExpressionTypes.contains(DoubleType))) {
          throw new CannotReplaceException("grouping expressions over floating point columns " +
            "that may contain -0.0 and NaN are disabled. You can bypass this by setting " +
            "spark.rapids.sql.hasNans=false")
        }
        if (hashAgg.resultExpressions.isEmpty) {
          throw new CannotReplaceException("result expressions is empty")
        }
        val hashAggMode = hashAgg.aggregateExpressions.map(_.mode).distinct
        val hashAggReplaceMode = conf.hashAggReplaceMode.toLowerCase
        if (!hashAggReplaceMode.equals("all")) {
          hashAggReplaceMode match {
            case "partial" => if (hashAggMode.contains(Final)) {
              // replacing only Partial hash aggregates, so a Final one should not replace
              throw new CannotReplaceException("Replacing Final hash aggregates disabled")
            }
            case "final" => if (hashAggMode.contains(Partial)) {
              // replacing only Final hash aggregates, so a Partial one should not replace
              throw new CannotReplaceException("Replacing Partial aggregates disabled")
            }
            case _ => {
              throw new IllegalArgumentException(s"The hash aggregate replacement mode ${hashAggReplaceMode} " +
                "is not valid. Valid options are: \"partial\", \"final\", or \"all\"")
            }
          }
        }
      })
      .desc("The backend for hash based aggregations")
      .build(),
    exec[SortExec]
      .convert((sort, overrides) =>
        GpuSortExec(sort.sortOrder.map(overrides.replaceWithGpuExpression).asInstanceOf[Seq[GpuSortOrder]],
          sort.global,
          overrides.replaceWithGpuPlan(sort.child)))
      .assertIsAllowed((sort, conf) => {
        // handle strings - https://gitlab-master.nvidia.com/nvspark/rapids-plugin-4-spark/issues/26
        if (isAnyStringLit(sort.sortOrder)) {
          throw new CannotReplaceException("string literal values are not supported in a sort")
        }
        val keyDataTypes = sort.sortOrder.map(_.dataType)
        if ((keyDataTypes.contains(FloatType) || keyDataTypes.contains(DoubleType)) && conf.hasNans) {
          throw new CannotReplaceException("floats/doubles are not supported in sort, due to " +
            "incompatibility with NaN. If you don't have any NaNs in your data you can set " +
            "spark.rapids.sql.hasNans=false to bypass this.")
        }
        if (keyDataTypes.contains(StringType) && !conf.allowIncompatUTF8Strings) {
          throw new CannotReplaceException("strings are not supported in sort if you have UTF-8 " +
            "characters in your strings. See spark.rapids.sql.allowIncompatUTF8Strings to allow " +
            "if you don't have UTF-8 characters in your strings.")
        }
        val nullOrderings = sort.sortOrder.map(o => o.nullOrdering)
        if (!nullOrderings.forall(_ == nullOrderings.head)) {
          // ERROR we can't handle this right now since only 1 parameter for areNullsSmallest
          // to Table.orderBy
          throw new CannotReplaceException(s"GPU cudf can't handle multiple null orderings!")
        }
        // note that dataframe.sort always sets this to true
        if (sort.global == true) {
          throw new CannotReplaceException(s"Don't support total ordering on GPU yet")
        }

      })
      .desc("The backend for the sort operator")
      .build()
  )

  val aggs: Map[Class[_ <: AggregateFunction], AggRule[_ <: AggregateFunction]] = Map(
    // declarative aggregates
    agg[Count]
      .assertIsAllowed((count, conf) =>
        if (!count.children.forall(_.isInstanceOf[Literal])) {
          throw new CannotReplaceException("only count(*) or count(1) supported")
        })
      .convert((count, overrides) =>
        GpuCount(count.children.map(overrides.replaceWithGpuExpression)))
      .desc("count aggregate operator")
      .build(),
    agg[Max]
      .convert((max, overrides) =>
        GpuMax(overrides.replaceWithGpuExpression(max.child)))
      .desc("max aggregate operator")
      .build(),
    agg[Min]
      .convert((min, overrides) =>
        GpuMin(overrides.replaceWithGpuExpression(min.child)))
      .desc("min aggregate operator")
      .build(),
    agg[First]
      .assertIsAllowed((first, conf) =>
        if (first.ignoreNullsExpr.semanticEquals(Literal(false))) {
          throw new CannotReplaceException("including nulls is not supported, use first(col, true)")
        })
      .convert((first, overrides) =>
        GpuFirst(overrides.replaceWithGpuExpression(first.child),
          ignoreNullsExpr = overrides.replaceWithGpuExpression(first.ignoreNullsExpr)))
      .desc("first aggregate operator")
      .build(),
    agg[Last]
      .assertIsAllowed((last, conf) =>
        if (last.ignoreNullsExpr.semanticEquals(Literal(false))) {
          throw new CannotReplaceException("including nulls is not supported, use last(col, true)")
        })
      .convert((last, overrides) =>
        GpuLast(overrides.replaceWithGpuExpression(last.child),
          ignoreNullsExpr = overrides.replaceWithGpuExpression(last.ignoreNullsExpr)))
      .desc("last aggregate operator")
      .build(),
    agg[Sum]
      .convert((sum, overrides) =>
        GpuSum(overrides.replaceWithGpuExpression(sum.child)))
      .assertIsAllowed((sum, conf) => {
        val dataType = sum.child.dataType
        if (!conf.allowFloatAgg && (dataType == DoubleType || dataType == FloatType)) {
          throw new CannotReplaceException("the GPU will sum floating point values in" +
            " parallel and the result is not always identical each time. This can cause some Spark" +
            s" queries to produce an incorrect answer if the value is computed more than once" +
            s" as part of the same query.  To enable this anyways set" +
            s" ${RapidsConf.ALLOW_FLOAT_AGG} to true.")
        }
      })
      .desc("sum aggregate operator")
      .build(),
    agg[Average]
      .convert((avg, overrides) =>
        GpuAverage(overrides.replaceWithGpuExpression(avg.child)))
      .assertIsAllowed((avg, conf) => {
        if (!conf.allowFloatAgg) {
          throw new CannotReplaceException("the GPU will sum floating point values in" +
            " parallel to compute an average and the result is not always identical each time." +
            " This can cause some Spark queries to produce an incorrect answer if the value is" +
            " computed more than once as part of the same query. To enable this anyways set" +
            s" ${RapidsConf.ALLOW_FLOAT_AGG} to true")
        }
      })
      .desc("average aggregate operator")
      .build(),
  )
}

/**
 * This is a hack and should be removed ASAP.  Because GpuShuffleExchangeExec is a subclass of
 * ShuffleExchangeExec which is a case class some odd things happen with equals that this overrides,
 * but we really should just not have anything inheret from a case class which means we will have to
 * pull in more code from spark proper. This is ShuffleExchangeExec that was rolled back.
 */
class RevertedShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    canChangeNumPartitions: Boolean) extends ShuffleExchangeExec(outputPartitioning,
  child,
  canChangeNumPartitions) {

  override def equals(o: Any): Boolean =
    super.equals(o) && o.isInstanceOf[RevertedShuffleExchangeExec]
}

case class GpuOverrides() extends Rule[SparkPlan] with Logging {
  var conf: RapidsConf = null

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

  def replaceWithGpuExpression(exp: Expression): GpuExpression = {
    val rule = GpuOverrides.expressions.getOrElse(exp.getClass,
      throw new CannotReplaceException(s"no GPU enabled version of expression" +
        s" ${exp.getClass.getName} ${exp} could be found"))

    if (!conf.isOperatorEnabled(rule.confKey, rule.isIncompat)) {
      if (rule.isIncompat && !conf.isIncompatEnabled) {
        throw new CannotReplaceException(s"the GPU version of expression ${exp.getClass.getName} ${exp}" +
          s" is not 100% compatible with the Spark version. ${rule.incompatDoc}. To enable this" +
          s" operator despite the incompatibilities please set the config" +
          s" ${rule.confKey} to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
          s" to enable all incompatible ops")
      } else {
        throw new CannotReplaceException(s"The expressions ${exp.getClass} ${exp} has been" +
          s" disabled. To enable it set ${rule.confKey} to true")
      }
    }

    rule.assertIsAllowed(exp, conf)

    if (!areAllSupportedTypes(exp.dataType)) {
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} produces an unsupported type ${exp.dataType}")
    }
    rule.convert(exp, this)
  }

  def replaceWithGpuScan(scan: Scan): Scan = {
    val rule = GpuOverrides.scans.getOrElse(scan.getClass,
      throw new CannotReplaceException(s"no GPU enabled version of input type" +
        s" ${scan.getClass.getName} could be found"))

    if (!conf.isOperatorEnabled(rule.confKey, rule.isIncompat)) {
      if (rule.isIncompat && !conf.isIncompatEnabled) {
        throw new CannotReplaceException(s"the GPU version of input type ${scan.getClass.getName}" +
          s" is not 100% compatible with the Spark version. ${rule.incompatDoc}. To enable this" +
          s" operator despite the incompatibilities please set the config" +
          s" ${rule.confKey} to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
          s" to enable all incompatible ops")
      } else {
        throw new CannotReplaceException(s"The input type ${scan.getClass} has been" +
          s" disabled. To enable it set ${rule.confKey} to true")
      }
    }

    rule.assertIsAllowed(scan, conf)

    rule.convert(scan, this)
  }

  def replaceWithGpuPartitioning(part: Partitioning): GpuPartitioning = {
    val rule = GpuOverrides.parts.getOrElse(part.getClass,
      throw new CannotReplaceException(s"no GPU enabled version of partitioning type" +
        s" ${part.getClass.getName} could be found"))

    if (!conf.isOperatorEnabled(rule.confKey, rule.isIncompat)) {
      if (rule.isIncompat && !conf.isIncompatEnabled) {
        throw new CannotReplaceException(s"the GPU version of partitioning type ${part.getClass.getName}" +
          s" is not 100% compatible with the Spark version. ${rule.incompatDoc}. To enable this" +
          s" operator despite the incompatibilities please set the config" +
          s" ${rule.confKey} to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
          s" to enable all incompatible ops")
      } else {
        throw new CannotReplaceException(s"The input type ${part.getClass} has been" +
          s" disabled. To enable it set ${rule.confKey} to true")
      }
    }

    rule.assertIsAllowed(part, conf)

    rule.convert(part, this)
  }

  def replaceWithGpuPlan(plan: SparkPlan, context: String = ""): SparkPlan =
    try {
      val rule = GpuOverrides.execs.getOrElse(plan.getClass,
        throw new CannotReplaceException(s"no GPU enabled version of operator" +
          s" ${plan.getClass.getName} could be found"))

      if (rule.context != null && !context.equals(rule.context)) {
        throw new CannotReplaceException(s"the operator ${plan.getClass} can only be replaced in" +
          s" the context of ${rule.context}")
      }

      if (!conf.isOperatorEnabled(rule.confKey, rule.isIncompat)) {
        if (rule.isIncompat && !conf.isIncompatEnabled) {
          throw new CannotReplaceException(s"the GPU version of ${plan.getClass.getName}" +
            s" is not 100% compatible with the Spark version. ${rule.incompatDoc}. To enable this" +
            s" operator despite the incompatibilities please set the config" +
            s" ${rule.confKey} to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
            s" to enable all incompatible ops")
        } else {
          throw new CannotReplaceException(s"the operator ${plan.getClass} has been" +
            s" disabled. To enable it set ${rule.confKey} to true")
        }
      }

      rule.assertIsAllowed(plan, conf)

      if (!areAllSupportedTypes(plan.output.map(_.dataType) :_*)) {
        val unsupported = plan.output.map(_.dataType).filter(!areAllSupportedTypes(_)).toSet
        throw new CannotReplaceException(s"unsupported data types in its output: ${unsupported}")
      }
      if (!areAllSupportedTypes(plan.children.flatMap(_.output.map(_.dataType)) :_*)) {
        val unsupported = plan.children.flatMap(_.output.map(_.dataType))
          .filter(!areAllSupportedTypes(_)).toSet
        throw new CannotReplaceException(s"unsupported data types in its input: ${unsupported}")
      }

      rule.convert(plan, this)
    } catch {
      case exp: CannotReplaceException =>
        if (conf.explain) {
          logWarning(s"${plan.getClass.getSimpleName} will not run on a GPU" +
            s" because ${exp.getMessage}")
        }
        plan.withNewChildren(plan.children.map(replaceWithGpuPlan(_)))
    }

  def replaceWithGpuAggregate(agg: AggregateFunction): GpuAggregateFunction = {
    val rule = GpuOverrides.aggs.getOrElse(agg.getClass,
      throw new CannotReplaceException(s"no GPU enabled version of aggregate type" +
        s" ${agg.getClass.getName} could be found"))

    if (!conf.isOperatorEnabled(rule.confKey, rule.isIncompat)) {
      if (rule.isIncompat && !conf.isIncompatEnabled) {
        throw new CannotReplaceException(s"the GPU version of aggregation type ${agg.getClass.getName}" +
          s" is not 100% compatible with the Spark version. ${rule.incompatDoc}. To enable this" +
          s" operator despite the incompatibilities please set the config" +
          s" ${rule.confKey} to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
          s" to enable all incompatible ops")
      } else {
        throw new CannotReplaceException(s"The input type ${agg.getClass} has been" +
          s" disabled. To enable it set ${rule.confKey} to true")
      }
    }

    rule.assertIsAllowed(agg, conf)

    rule.convert(agg, this)
  }

  def findShuffleExchanges(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case exchange: GpuShuffleExchangeExec => exchange :: Nil
    case exchange: ShuffleExchangeExec => exchange :: Nil
    case bkj: BroadcastHashJoinExec  => bkj.buildSide match {
      case BuildLeft => findShuffleExchanges(bkj.right)
      case BuildRight => findShuffleExchanges(bkj.left)
    }
    case other => other.children.flatMap(findShuffleExchanges)
  }

  def mapBackPartitioning(part: Partitioning): Partitioning = part match {
    case part: GpuHashPartitioning => HashPartitioning(part.expressions, part.numPartitions)
    case other => throw new IllegalStateException(s"${other.getClass} is not supported for fixing up exchange error")
  }

  def forceCpuShuffleExchange(plan: SparkPlan): SparkPlan = plan match {
    case exchange: GpuShuffleExchangeExec =>
      if (conf.explain) {
        logWarning(s"${plan.getClass.getSimpleName} will not run on a GPU" +
          s" because other exchanges that feed the same join are on the CPU and GPU hashing is" +
          s" not consistent with the CPU version")
      }
      new RevertedShuffleExchangeExec(mapBackPartitioning(exchange.outputPartitioning),
        exchange.child,
        exchange.canChangeNumPartitions)
    case exchange: ShuffleExchangeExec => exchange
    case bkj: BroadcastHashJoinExec => bkj.buildSide match {
      case BuildLeft =>
        bkj.withNewChildren(Seq(bkj.left, forceCpuShuffleExchange(bkj.right)))
      case BuildRight =>
        bkj.withNewChildren(Seq(forceCpuShuffleExchange(bkj.left), bkj.right))
    }
    case other =>
      other.withNewChildren(other.children.map(forceCpuShuffleExchange))
  }

  def needsShuffleFix(plan: SparkPlan): Boolean = {
    // Cannot rely on match to work properly because a GpuShuffleExchangeExec is a ShuffleExchangeExec
    val exchanges = findShuffleExchanges(plan)
    val onGpu = exchanges.map(e => e.isInstanceOf[GpuShuffleExchangeExec])
    val allOnGpu = onGpu.forall(identity)
    val allOnCpu = onGpu.forall(b => !b)
    (!allOnGpu) && (!allOnCpu)
  }

  def fixGlobalConsistency(plan: SparkPlan): SparkPlan = {
    val tmp = plan match {
      case p: ShuffledHashJoinExec if needsShuffleFix(p) =>
        p.withNewChildren(p.children.map(forceCpuShuffleExchange))
      case p: SortMergeJoinExec if needsShuffleFix(p) =>
        p.withNewChildren(p.children.map(forceCpuShuffleExchange))
      case other => other
    }
    tmp.withNewChildren(tmp.children.map(fixGlobalConsistency))
  }

  override def apply(plan: SparkPlan) :SparkPlan = {
    conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled) {
      val tmp = replaceWithGpuPlan(plan)
      fixGlobalConsistency(tmp)
    } else {
      plan
    }
  }
}

case class GpuTransitionOverrides() extends Rule[SparkPlan] {
  def optimizeGpuPlanTransitions(plan: SparkPlan): SparkPlan = plan match {
    case HostColumnarToGpu(r2c: RowToColumnarExec) =>
      GpuRowToColumnarExec(optimizeGpuPlanTransitions(r2c.child))
    case ColumnarToRowExec(bb: GpuBringBackToHost) =>
      GpuColumnarToRowExec(optimizeGpuPlanTransitions(bb.child))
    case p =>
      p.withNewChildren(p.children.map(optimizeGpuPlanTransitions))
  }

  /**
   * Inserts a transition to be running on the CPU columnar
   */
  private def insertColumnarFromGpu(plan: SparkPlan): SparkPlan = {
    if (plan.supportsColumnar && plan.isInstanceOf[GpuExec]) {
      GpuBringBackToHost(insertColumnarToGpu(plan))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarFromGpu))
    }
  }

  /**
   * Inserts a transition to be running on the GPU from CPU columnar
   */
  private def insertColumnarToGpu(plan: SparkPlan): SparkPlan = {
    if (plan.supportsColumnar && !plan.isInstanceOf[GpuExec]) {
      HostColumnarToGpu(insertColumnarFromGpu(plan))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarToGpu))
    }
  }

  private def getBaseNameFromClass(planClassStr: String): String = {
    val firstDotIndex = planClassStr.lastIndexOf(".")
    if (firstDotIndex != -1) planClassStr.substring(firstDotIndex + 1) else planClassStr
  }

  def assertIsOnTheGpu(exp: Expression, conf: RapidsConf): Unit = {
    if (!exp.isInstanceOf[GpuExpression] &&
      !conf.testingAllowedNonGpu.contains(getBaseNameFromClass(exp.getClass.toString))) {
      throw new IllegalArgumentException(s"The expression ${exp} is not columnar ${exp.getClass}")
    }
  }

  def assertIsOnTheGpu(plan: SparkPlan, conf: RapidsConf): Unit = {
    plan match {
      case lts: LocalTableScanExec =>
        if (!lts.expressions.forall(_.isInstanceOf[AttributeReference])) {
          throw new IllegalArgumentException("It looks like some operations were " +
            s"pushed down to LocalTableScanExec ${lts.expressions.mkString(",")}")
        }
      case _: GpuColumnarToRowExec => () // Ignored
      case _: ShuffleExchangeExec => () // Ignored for now
      case other =>
        if (!plan.supportsColumnar &&
          !conf.testingAllowedNonGpu.contains(getBaseNameFromClass(other.getClass.toString))) {
          throw new IllegalArgumentException(s"Part of the plan is not columnar ${plan.getClass}\n${plan}")
        }
        plan.expressions.foreach(assertIsOnTheGpu(_, conf))
    }
    plan.children.foreach(assertIsOnTheGpu(_, conf))
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    val conf = new RapidsConf(plan.conf)
    if (conf.isSqlEnabled) {
      val tmp = insertColumnarFromGpu(plan)
      val ret = optimizeGpuPlanTransitions(tmp)
      if (conf.isTestEnabled) {
        assertIsOnTheGpu(ret, conf)
      }
      ret
    } else {
      plan
    }
  }
}

case class ColumnarOverrideRules() extends ColumnarRule with Logging {
  val overrides = GpuOverrides()
  val overrideTransitions = GpuTransitionOverrides()

  override def preColumnarTransitions : Rule[SparkPlan] = overrides

  override def postColumnarTransitions: Rule[SparkPlan] = overrideTransitions
}

/**
  * Extension point to enable GPU processing.
  *
  * To run on a GPU set spark.sql.extensions to ai.rapids.spark.Plugin
  */
class Plugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logWarning("Installing extensions to enable rapids GPU SQL support." +
      s" To disable GPU support set `${RapidsConf.SQL_ENABLED}` to false")
    extensions.injectColumnar((session) => ColumnarOverrideRules())
  }
}

/**
 * Config to enable pooled GPU memory allocation which can improve performance.  This should be off
 * if you want to use operators that also use GPU memory like XGBoost or Tensorflow, as the pool
 * it allocates cannot be used by other tools.
 *
 * To enable this set spark.executor.plugins to ai.rapids.spark.GpuResourceManager
 */
class GpuResourceManager extends ExecutorPlugin with Logging {
  var loggingEnabled = false

  override def init(): Unit = synchronized {
    // We eventually will need a way to know which GPU to use/etc, but for now, we will just
    // go with the default GPU.
    if (!Rmm.isInitialized) {
      val env = SparkEnv.get
      val conf = new spark.RapidsConf(env.conf)
      loggingEnabled = conf.isMemDebugEnabled
      val info = Cuda.memGetInfo()
      val initialAllocation = info.free / 4
      logInfo(s"Initializing RMM ${initialAllocation / 1024 / 1024.0} MB")
      try {
        Rmm.initialize(RmmAllocationMode.POOL, loggingEnabled, initialAllocation)
      } catch {
        case e: Exception => logError("Could not initialize RMM", e)
      }
    }
  }

  override def shutdown(): Unit = {
    if (loggingEnabled) {
      logWarning(s"RMM LOG\n${Rmm.getLog}")
    }
  }
}
