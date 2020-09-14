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

package org.apache.spark.sql.rapids

import ai.rapids.cudf
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExprId, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, BooleanType, DataType, DoubleType, LongType, NumericType, StructType}

trait GpuAggregateFunction extends GpuExpression {
  // using the child reference, define the shape of the vectors sent to
  // the update/merge expressions
  val inputProjection: Seq[Expression]

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false

  /** The schema of the aggregation buffer. */
  def aggBufferSchema: StructType

  /** Attributes of fields in aggBufferSchema. */
  def aggBufferAttributes: Seq[AttributeReference]

  /**
   * Attributes of fields in input aggregation buffers (immutable aggregation buffers that are
   * merged with mutable aggregation buffers in the merge() function or merge expressions).
   * These attributes are created automatically by cloning the [[aggBufferAttributes]].
   */
  def inputAggBufferAttributes: Seq[AttributeReference]

  /**
   * Result of the aggregate function when the input is empty. This is currently only used for the
   * proper rewriting of distinct aggregate functions.
   */
  def defaultResult: Option[GpuLiteral] = None

  // TODO: Do we need toAggregateExpression methods?

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  /** String representation used in explain plans. */
  def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + flatArguments.mkString(start, ", ", ")")
  }
}

case class WrappedAggFunction(aggregateFunction: GpuAggregateFunction, filter: Expression)
    extends GpuDeclarativeAggregate {
  override val inputProjection: Seq[GpuExpression] = {
    val caseWhenExpressions = aggregateFunction.inputProjection.map { ip =>
      // special case average with null result from the filter as expected values should be
      // (0.0,0) for (sum, count)
      val initialValue: Expression =
        aggregateFunction match {
          case _ : GpuAverage => ip.dataType match {
            case doubleType: DoubleType => GpuLiteral(0D, doubleType)
            case _ : LongType => GpuLiteral(0L, LongType)
          }
          case _ => GpuLiteral(null, ip.dataType)
        }
      val filterConditional = GpuCaseWhen(Seq((filter, ip)))
      GpuCaseWhen(Seq((GpuIsNotNull(filterConditional), filterConditional)), Some(initialValue))
    }
    caseWhenExpressions
  }
  /** Attributes of fields in aggBufferSchema. */
  override def aggBufferAttributes: Seq[AttributeReference] =
    aggregateFunction.aggBufferAttributes
  override def nullable: Boolean = aggregateFunction.nullable

  override def dataType: DataType = aggregateFunction.dataType

  override def children: Seq[Expression] = Seq(aggregateFunction, filter)

  override val initialValues: Seq[GpuExpression] =
    aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].initialValues
  override val updateExpressions: Seq[Expression] =
    aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].updateExpressions
  override val mergeExpressions: Seq[GpuExpression] =
    aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].mergeExpressions
  override val evaluateExpression: Expression =
    aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].evaluateExpression
}

case class GpuAggregateExpression(origAggregateFunction: GpuAggregateFunction,
                                  mode: AggregateMode,
                                  isDistinct: Boolean,
                                  filter: Option[Expression],
                                  resultId: ExprId)
  extends GpuExpression with GpuUnevaluable {

  val aggregateFunction = if (filter.isDefined) {
    WrappedAggFunction(origAggregateFunction, filter.get)
  } else {
    origAggregateFunction
  }

  //
  // Overrides form AggregateExpression
  //

  // We compute the same thing regardless of our final result.
  override lazy val canonicalized: Expression = {
    val normalizedAggFunc = mode match {
      // For PartialMerge or Final mode, the input to the `aggregateFunction` is aggregate buffers,
      // and the actual children of `aggregateFunction` is not used, here we normalize the expr id.
      case PartialMerge | Final => aggregateFunction.transform {
        case a: AttributeReference => a.withExprId(ExprId(0))
      }
      case Partial | Complete => aggregateFunction
    }

    GpuAggregateExpression(
      normalizedAggFunc.canonicalized.asInstanceOf[GpuAggregateFunction],
      mode,
      isDistinct,
      filter.map(_.canonicalized),
      ExprId(0))
  }

  override def nullable: Boolean = aggregateFunction.nullable
  override def dataType: DataType = aggregateFunction.dataType
  override def children: Seq[Expression] = aggregateFunction +: filter.toSeq

  @transient
  override lazy val references: AttributeSet = {
    mode match {
      case Partial | Complete => aggregateFunction.references
      case PartialMerge | Final => AttributeSet(aggregateFunction.aggBufferAttributes)
    }
  }

  override def toString: String = {
    val prefix = mode match {
      case Partial => "partial_"
      case PartialMerge => "merge_"
      case Final | Complete => ""
    }
    prefix + origAggregateFunction.toAggString(isDistinct)
  }

  override def sql: String = aggregateFunction.sql(isDistinct)
}

abstract case class CudfAggregate(ref: Expression) extends GpuUnevaluable {
  // we use this to get the ordinal of the bound reference, s.t. we can ask cudf to perform
  // the aggregate on that column
  def getOrdinal(ref: Expression): Int = ref.asInstanceOf[GpuBoundReference].ordinal
  val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar
  val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar
  val updateAggregate: cudf.Aggregate
  val mergeAggregate: cudf.Aggregate

  def dataType: DataType = ref.dataType
  def nullable: Boolean = ref.nullable
  def children: Seq[Expression] = ref :: Nil
}

class CudfCount(ref: Expression) extends CudfAggregate(ref) {
  // includeNulls set to false in count aggregate to exclude nulls while calculating count(column)
  val includeNulls = false
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => cudf.Scalar.fromLong(col.getRowCount - col.getNullCount)
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override lazy val updateAggregate: cudf.Aggregate =
    cudf.Table.count(getOrdinal(ref), includeNulls)
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
  override def toString(): String = "CudfCount"
}

class CudfSum(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
  override def toString(): String = "CudfSum"
}

class CudfMax(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.max(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.max(getOrdinal(ref))
  override def toString(): String = "CudfMax"
}

class CudfMin(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.min(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.min(getOrdinal(ref))
  override def toString(): String = "CudfMin"
}

abstract class CudfFirstLastBase(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) =>
      throw new UnsupportedOperationException("first/last reduction not supported on GPU")
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) =>
      throw new UnsupportedOperationException("first/last reduction not supported on GPU")
}

class CudfFirstIncludeNulls(ref: Expression) extends CudfFirstLastBase(ref) {
  val includeNulls = true
  override lazy val updateAggregate: cudf.Aggregate =
    cudf.Table.first(getOrdinal(ref), includeNulls)
  override lazy val mergeAggregate: cudf.Aggregate =
    cudf.Table.first(getOrdinal(ref), includeNulls)
}

class CudfFirstExcludeNulls(ref: Expression) extends CudfFirstLastBase(ref) {
  val includeNulls = false
  override lazy val updateAggregate: cudf.Aggregate =
    cudf.Table.first(getOrdinal(ref), includeNulls)
  override lazy val mergeAggregate: cudf.Aggregate =
    cudf.Table.first(getOrdinal(ref), includeNulls)
}

class CudfLastIncludeNulls(ref: Expression) extends CudfFirstLastBase(ref) {
  val includeNulls = true
  override lazy val updateAggregate: cudf.Aggregate =
    cudf.Table.last(getOrdinal(ref), includeNulls)
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.last(getOrdinal(ref), includeNulls)
}

class CudfLastExcludeNulls(ref: Expression) extends CudfFirstLastBase(ref) {
  val includeNulls = false
  override lazy val updateAggregate: cudf.Aggregate =
    cudf.Table.last(getOrdinal(ref), includeNulls)
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.last(getOrdinal(ref), includeNulls)
}

abstract class GpuDeclarativeAggregate extends GpuAggregateFunction with GpuUnevaluable {
  // these are values that spark calls initial because it uses
  // them to initialize the aggregation buffer, and returns them in case
  // of an empty aggregate when there are no expressions,
  // here we copy them but with the gpu equivalent
  val initialValues: Seq[GpuExpression]

  // update: first half of the aggregation (count = count)
  val updateExpressions: Seq[Expression]

  // merge: second half of the aggregation (count = sum). Also use to merge multiple batches.
  val mergeExpressions: Seq[GpuExpression]

  // mostly likely a pass through (count => sum we merged above).
  // average has a more interesting expression to compute the division of sum/count
  val evaluateExpression: Expression

  /** An expression-based aggregate's bufferSchema is derived from bufferAttributes. */
  final override def aggBufferSchema: StructType = null //not used in GPU version

  final lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())
}

case class GpuMin(child: Expression) extends GpuDeclarativeAggregate {
  private lazy val cudfMin = AttributeReference("min", child.dataType)()

  override lazy val inputProjection: Seq[Expression] = Seq(child)
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMin(cudfMin))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMin(cudfMin))
  override lazy val evaluateExpression: Expression = cudfMin

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfMin :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(null, child.dataType))

  // Copied from Min
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: Nil
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function gpu min")
}

case class GpuMax(child: Expression) extends GpuDeclarativeAggregate {
  private lazy val cudfMax = AttributeReference("max", child.dataType)()

  override lazy val inputProjection: Seq[Expression] = Seq(child)
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax))
  override lazy val evaluateExpression: Expression = cudfMax

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfMax :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(null, child.dataType))

  // Copied from Max
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: Nil
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function gpu max")
}

case class GpuSum(child: Expression)
  extends GpuDeclarativeAggregate with ImplicitCastInputTypes {
  private lazy val resultType = child.dataType match {
    case _: DoubleType => DoubleType
    case _ => LongType
  }

  private lazy val cudfSum = AttributeReference("sum", resultType)()

  override lazy val inputProjection: Seq[Expression] = Seq(child)
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum))
  override lazy val evaluateExpression: Expression = cudfSum

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfSum :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(null, resultType))

  // Copied from Sum
  override def nullable: Boolean = true
  override def dataType: DataType = resultType
  override def children: Seq[Expression] = child :: Nil
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function gpu sum")
}

case class GpuCount(children: Seq[Expression]) extends GpuDeclarativeAggregate {
  // counts are Long
  private lazy val cudfCount = AttributeReference("count", LongType)()

  override lazy val inputProjection: Seq[Expression] = Seq(children.head)
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfCount(cudfCount))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfCount))
  override lazy val evaluateExpression: Expression = cudfCount

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfCount :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(0L, LongType))

  // Copied from Count
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
}

case class GpuAverage(child: Expression) extends GpuDeclarativeAggregate {
  // averages are either Decimal or Double. We don't support decimal yet, so making this double.
  private lazy val cudfSum = AttributeReference("sum", DoubleType)()
  private lazy val cudfCount = AttributeReference("count", LongType)()

  private def toDoubleLit(v: Any): GpuLiteral = {
    val litVal = v match {
      case null => null
      case l: Long => l.toDouble
      case i: Int => i.toDouble
      case f: Float => f.toDouble
      case s: Short => s.toDouble
      case b: Byte => b.toDouble
      case d: Double => d
      case _ => throw new IllegalArgumentException("function average requires numeric type")
    }
    GpuLiteral(litVal, DoubleType)
  }

  override lazy val inputProjection: Seq[GpuExpression] = Seq(
    child match {
      case literal: GpuLiteral => toDoubleLit(literal.value)
      case _ => GpuCoalesce(Seq(GpuCast(child, DoubleType), GpuLiteral(0D, DoubleType)))
    },
    child match {
      case literal : GpuLiteral => GpuLiteral(if (literal.value != null) 1L else 0L, LongType)
      case _ =>
        // takes any column and turns it into 1 for non null, and 0 for null
        // a sum of this == the count
        GpuCast(GpuIsNotNull(child), LongType)
    })
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum),
    new CudfSum(cudfCount))
  // The count input projection will need to be collected as a sum (of counts) instead of
  // counts (of counts) as the GpuIsNotNull o/p is casted to count=0 for null and 1 otherwise, and
  // the total count can be correctly evaluated only by summing them. eg. avg(col(null, 27))
  // should be 27, with count column projection as (0, 1) and total count for dividing the
  // average = (0 + 1) and not 2 which is the rowcount of the projected column.
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum),
    new CudfSum(cudfCount))
  override lazy val evaluateExpression: GpuExpression = GpuDivide(
    GpuCast(cudfSum, DoubleType),
    GpuCast(cudfCount, DoubleType))

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    GpuLiteral(0.0, DoubleType),
    GpuLiteral(0L, LongType))

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfSum :: cudfCount :: Nil

  // Copied from Average
  override def prettyName: String = "gpuavg"
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType // we don't support Decimal
  override def children: Seq[Expression] = child :: Nil
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function gpu average")
}

/*
 * First/Last are "good enough" for the hash aggregate, and should only be used when we
 * want to collapse to a grouped key. The hash aggregate doesn't make guarantees on the
 * ordering of how batches are processed, so this is as good as picking any old function
 * (we picked max)
 *
 * These functions have an extra field they expect to be around in the aggregation buffer.
 * So this adds a "max" of that, and currently sends it to the GPU. The CPU version uses it
 * to check if the value was set (if we don't ignore nulls, valueSet is true, that's what we do
 * here).
 */
abstract class GpuFirstBase(child: Expression)
  extends GpuDeclarativeAggregate with ImplicitCastInputTypes with Serializable {

  val ignoreNulls: Boolean

  private lazy val cudfFirst = AttributeReference("first", child.dataType)()
  private lazy val valueSet = AttributeReference("valueSet", BooleanType)()

  override lazy val inputProjection: Seq[Expression] =
    Seq(child, GpuLiteral(ignoreNulls, BooleanType))

  private lazy val commonExpressions: Seq[CudfAggregate] = if (ignoreNulls) {
    Seq(new CudfFirstExcludeNulls(cudfFirst), new CudfFirstExcludeNulls(valueSet))
  } else {
    Seq(new CudfFirstIncludeNulls(cudfFirst), new CudfFirstIncludeNulls(valueSet))
  }

  override lazy val updateExpressions: Seq[GpuExpression] = commonExpressions
  override lazy val mergeExpressions: Seq[GpuExpression] = commonExpressions
  override lazy val evaluateExpression: Expression = cudfFirst

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfFirst :: valueSet :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    GpuLiteral(null, child.dataType),
    GpuLiteral(false, BooleanType))

  // Copied from First
  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  // First is not a deterministic function.
  override lazy val deterministic: Boolean = false
  override def toString: String = s"gpufirst($child)${if (ignoreNulls) " ignore nulls"}"
}

abstract class GpuLastBase(child: Expression)
  extends GpuDeclarativeAggregate with ImplicitCastInputTypes with Serializable {

  val ignoreNulls: Boolean

  private lazy val cudfLast = AttributeReference("last", child.dataType)()
  private lazy val valueSet = AttributeReference("valueSet", BooleanType)()

  override lazy val inputProjection: Seq[Expression] =
    Seq(child, GpuLiteral(!ignoreNulls, BooleanType))

  private lazy val commonExpressions: Seq[CudfAggregate] = if (ignoreNulls) {
    Seq(new CudfLastExcludeNulls(cudfLast), new CudfLastExcludeNulls(valueSet))
  } else {
    Seq(new CudfLastIncludeNulls(cudfLast), new CudfLastIncludeNulls(valueSet))
  }

  override lazy val updateExpressions: Seq[GpuExpression] = commonExpressions
  override lazy val mergeExpressions: Seq[GpuExpression] = commonExpressions
  override lazy val evaluateExpression: Expression = cudfLast

  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfLast :: valueSet :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    GpuLiteral(null, child.dataType),
    GpuLiteral(false, BooleanType))

  // Copied from Last
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  // Last is not a deterministic function.
  override lazy val deterministic: Boolean = false
  override def toString: String = s"gpulast($child)${if (ignoreNulls) " ignore nulls"}"
}
