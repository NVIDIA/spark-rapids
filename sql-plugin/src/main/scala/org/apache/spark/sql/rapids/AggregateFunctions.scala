package org.apache.spark.sql.rapids

import ai.rapids.cudf
import ai.rapids.spark._
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, AggregateMode, Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, ExprId, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, BooleanType, DataType, DoubleType, LongType, NumericType, StructType}

// AggregateFunction is not a case class
trait GpuAggregateFunction extends AggregateFunction with GpuUnevaluable {
  // using the child reference, define the shape of the vectors sent to
  // the update/merge expressions
  val inputProjection: Seq[GpuExpression]

  // returns the attribute references associated with this function
  // given a mode of aggregation
  override val aggBufferAttributes: Seq[AttributeReference]
}

case class GpuAggregateExpression(aggregateFunction: GpuAggregateFunction,
                                  mode: AggregateMode,
                                  isDistinct: Boolean,
                                  resultId: ExprId)
  extends GpuExpression with GpuUnevaluable {
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[GpuAggregateExpression]
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
      ExprId(0))
  }

  override def nullable: Boolean = aggregateFunction.nullable
  override def dataType: DataType = aggregateFunction.dataType
  override def children: Seq[Expression] = aggregateFunction.children

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
    prefix + aggregateFunction.toAggString(isDistinct)
  }

  override def sql: String = aggregateFunction.sql(isDistinct)
}

abstract case class CudfAggregate(ref: GpuExpression) extends GpuUnevaluable {
  // we use this to get the ordinal of the bound reference, s.t. we can ask cudf to perform
  // the aggregate on that column
  def getOrdinal(ref: GpuExpression): Int = ref.asInstanceOf[GpuBoundReference].ordinal
  val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar
  val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar
  val updateAggregate: cudf.Aggregate
  val mergeAggregate: cudf.Aggregate

  def dataType: DataType = ref.dataType
  def nullable: Boolean = ref.nullable
  def children: Seq[Expression] = ref :: Nil
}

class CudfCount(ref: GpuExpression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => cudf.Scalar.fromLong(col.getRowCount)
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.count(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
}

class CudfSum(ref: GpuExpression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
}

class CudfMax(ref: GpuExpression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.max(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.max(getOrdinal(ref))
}

class CudfMin(ref: GpuExpression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.min(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.min(getOrdinal(ref))
}

abstract class GpuDeclarativeAggregate extends GpuAggregateFunction with GpuUnevaluable {
  // these are values that spark calls initial because it uses
  // them to initialize the aggregation buffer, and returns them in case
  // of an empty aggregate when there are no expressions,
  // here we copy them but with the gpu equivalent
  val initialValues: Seq[GpuExpression]

  // update: first half of the aggregation (count = count)
  val updateExpressions: Seq[GpuExpression]

  // merge: second half of the aggregation (count = sum). Also use to merge multiple batches.
  val mergeExpressions: Seq[GpuExpression]

  // mostly likely a pass through (count => sum we merged above).
  // average has a more interesting expression to compute the division of sum/count
  val evaluateExpression: GpuExpression

  /** An expression-based aggregate's bufferSchema is derived from bufferAttributes. */
  final override def aggBufferSchema: StructType = null //not used in GPU version

  final lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())
}

// TODO: for all constructors below s/Expression/GpuExpression, once we get the rest
// of the expressions fixed
// TODO2: remove asInstanceOf[Seq[GpuExpression]] from the inputProjections

case class GpuMin(child: Expression) extends GpuDeclarativeAggregate {
  private lazy val cudfMin = new GpuAttributeReference("cudf_min", child.dataType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child.asInstanceOf[GpuExpression])
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMin(cudfMin))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMin(cudfMin))
  override lazy val evaluateExpression: GpuExpression = cudfMin

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfMin :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(new GpuLiteral(null, child.dataType))

  // Copied from Min
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: Nil
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function gpu min")
}

case class GpuMax(child: Expression) extends GpuDeclarativeAggregate {
  private lazy val cudfMax = new GpuAttributeReference("cudf_max", child.dataType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child.asInstanceOf[GpuExpression])
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax))
  override lazy val evaluateExpression: GpuExpression = cudfMax

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfMax :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(new GpuLiteral(null, child.dataType))

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

  private lazy val cudfSum = new GpuAttributeReference("cudf_sum", resultType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child.asInstanceOf[GpuExpression])
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum))
  override lazy val evaluateExpression: GpuExpression = cudfSum

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfSum :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(new GpuLiteral(null, resultType))

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
  private lazy val cudfCount = new GpuAttributeReference("cudf_count", LongType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(children.head.asInstanceOf[GpuExpression])
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfCount(cudfCount))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfCount))
  override lazy val evaluateExpression: GpuExpression = cudfCount

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfCount :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(new GpuLiteral(0L, LongType))

  // Copied from Count
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
}

case class GpuAverage(child: Expression) extends GpuDeclarativeAggregate {
  // averages are either Decimal or Double. We don't support decimal yet, so making this double.
  private lazy val cudfSum = new GpuAttributeReference("cudf_sum", DoubleType)()
  private lazy val cudfCount = new GpuAttributeReference("cudf_count", LongType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child,
    if (child.isInstanceOf[GpuLiteral]) {
      child
    } else {
      // takes any column and turns it into 1 for non null, and 0 for null
      // a sum of this == the count
      new GpuCast(new GpuIsNotNull(child), LongType)
    }).asInstanceOf[Seq[GpuExpression]]
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum), new CudfSum(cudfCount))
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum), new CudfSum(cudfCount))
  override lazy val evaluateExpression: GpuExpression = new GpuDivide(
    new GpuCast(cudfSum, DoubleType),
    new GpuCast(cudfCount, DoubleType))

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    new GpuLiteral(null, DoubleType),
    new GpuLiteral(null, LongType))

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfSum :: cudfCount :: Nil

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
 * to check if the value was set (if we don't ignore nulls, valueSet is true, that's what we do here).
 */
case class GpuFirst(child: Expression, ignoreNullsExpr: Expression)
  extends GpuDeclarativeAggregate with ImplicitCastInputTypes {
  private lazy val cudfMax = new GpuAttributeReference("cudf_max", child.dataType)()
  private lazy val valueSet = new GpuAttributeReference("valueSet", BooleanType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child, new GpuNot(ignoreNullsExpr)).asInstanceOf[Seq[GpuExpression]]
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax), new CudfMax(valueSet))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax), new CudfMax(valueSet))
  override lazy val evaluateExpression: GpuExpression = cudfMax

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfMax :: valueSet :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    new GpuLiteral(null, child.dataType),
    new GpuLiteral(false, BooleanType))

  // Copied from First
  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: ignoreNullsExpr :: Nil
  // First is not a deterministic function.
  override lazy val deterministic: Boolean = false
  private def ignoreNulls: Boolean = ignoreNullsExpr.eval().asInstanceOf[Boolean]
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ignoreNullsExpr.foldable) {
      TypeCheckFailure(
        s"The second argument of GpuFirst must be a boolean literal, but got: ${ignoreNullsExpr.sql}")
    } else {
      TypeCheckSuccess
    }
  }
  override def toString: String = s"gpufirst($child)${if (ignoreNulls) " ignore nulls"}"
}

case class GpuLast(child: Expression, ignoreNullsExpr: Expression)
  extends GpuDeclarativeAggregate with ImplicitCastInputTypes {
  private lazy val cudfMax = new GpuAttributeReference("cudf_max", child.dataType)()
  private lazy val valueSet = new GpuAttributeReference("valueSet", BooleanType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child, new GpuNot(ignoreNullsExpr)).asInstanceOf[Seq[GpuExpression]]
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax), new CudfMax(valueSet))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax), new CudfMax(valueSet))
  override lazy val evaluateExpression: GpuExpression = cudfMax

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfMax :: valueSet :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    new GpuLiteral(null, child.dataType),
    new GpuLiteral(false, BooleanType))

  // Copied from Last
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: ignoreNullsExpr :: Nil
  // Last is not a deterministic function.
  override lazy val deterministic: Boolean = false
  private def ignoreNulls: Boolean = ignoreNullsExpr.eval().asInstanceOf[Boolean]
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ignoreNullsExpr.foldable) {
      TypeCheckFailure(
        s"The second argument of GpuLast must be a boolean literal, but got: ${ignoreNullsExpr.sql}")
    } else {
      TypeCheckSuccess
    }
  }
  override def toString: String = s"gpulast($child)${if (ignoreNulls) " ignore nulls"}"
}