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

package org.apache.spark.sql.rapids

import ai.rapids.cudf
import ai.rapids.cudf.{BinaryOp, ColumnVector, DType, GroupByAggregation, GroupByScanAggregation, NullPolicy, ReductionAggregation, ReplacePolicy, RollingAggregation, RollingAggregationOnColumn, ScanAggregation}
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExprId, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

trait GpuAggregateFunction extends GpuExpression with GpuUnevaluable {
  // using the child reference, define the shape of the vectors sent to
  // the update/merge expressions
  val inputProjection: Seq[Expression]

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false

  /** The schema of the aggregation buffer. */
  def aggBufferSchema: StructType = null //not used in GPU version

  /** Attributes of fields in aggBufferSchema. */
  def aggBufferAttributes: Seq[AttributeReference]

  /**
   * Result of the aggregate function when the input is empty. This is currently only used for the
   * proper rewriting of distinct aggregate functions.
   */
  def defaultResult: Option[GpuLiteral] = None

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  /** String representation used in explain plans. */
  def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + flatArguments.mkString(start, ", ", ")")
  }

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

  // Attributes of fields in input aggregation buffers (immutable aggregation buffers that are
  // merged with mutable aggregation buffers in the merge() function or merge expressions).
  // These attributes are created automatically by cloning the [[aggBufferAttributes]].
  final lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())
}

case class WrappedAggFunction(aggregateFunction: GpuAggregateFunction, filter: Expression)
    extends GpuAggregateFunction {
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
    aggregateFunction.initialValues
  override val updateExpressions: Seq[Expression] =
    aggregateFunction.updateExpressions
  override val mergeExpressions: Seq[GpuExpression] =
    aggregateFunction.mergeExpressions
  override val evaluateExpression: Expression =
    aggregateFunction.evaluateExpression
}

case class GpuAggregateExpression(origAggregateFunction: GpuAggregateFunction,
                                  mode: AggregateMode,
                                  isDistinct: Boolean,
                                  filter: Option[Expression],
                                  resultId: ExprId)
  extends GpuExpression with GpuUnevaluable {

  val aggregateFunction: GpuAggregateFunction = if (filter.isDefined) {
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
  val updateAggregate: GroupByAggregation
  val mergeAggregate: GroupByAggregation

  def dataType: DataType = ref.dataType
  def nullable: Boolean = ref.nullable
  def children: Seq[Expression] = ref :: Nil
}

class CudfCount(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => cudf.Scalar.fromLong(col.getRowCount - col.getNullCount)
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override lazy val updateAggregate: GroupByAggregation =
    GroupByAggregation.count(NullPolicy.EXCLUDE)
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.sum()
  override def toString(): String = "CudfCount"
}

class CudfSum(ref: Expression) extends CudfAggregate(ref) {
  // Up to 3.1.1, analyzed plan widened the input column type before applying
  // aggregation. Thus even though we did not explicitly pass the output column type
  // we did not run into integer overflow issues:
  //
  // == Analyzed Logical Plan ==
  // sum(shorts): bigint
  // Aggregate [sum(cast(shorts#77 as bigint)) AS sum(shorts)#94L]
  //
  // In Spark's main branch (3.2.0-SNAPSHOT as of this comment), analyzed logical plan
  // no longer applies the cast to the input column such that the output column type has to
  // be passed explicitly into aggregation
  //
  // == Analyzed Logical Plan ==
  // sum(shorts): bigint
  // Aggregate [sum(shorts#33) AS sum(shorts)#50L]
  //
  @transient val rapidsSumType: DType = GpuColumnVector.getNonNestedRapidsType(ref.dataType)

  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum(rapidsSumType)

  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar = updateReductionAggregate

  override lazy val updateAggregate: GroupByAggregation = GroupByAggregation.sum()
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.sum()
  override def toString(): String = "CudfSum"
}

class CudfMax(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override lazy val updateAggregate: GroupByAggregation = GroupByAggregation.max()
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.max()
  override def toString(): String = "CudfMax"
}

class CudfMin(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override lazy val updateAggregate: GroupByAggregation = GroupByAggregation.min()
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.min()
  override def toString(): String = "CudfMin"
}

class CudfCollectList(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CollectList is not yet supported in reduction")
  override lazy val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CollectList is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregation = GroupByAggregation.collectList()
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.mergeLists()
  override def toString(): String = "CudfCollectList"
  override def dataType: DataType = ArrayType(ref.dataType, containsNull = false)
  override def nullable: Boolean = false
}

class CudfMergeLists(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("MergeLists is not yet supported in reduction")
  override lazy val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("MergeLists is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregation = GroupByAggregation.mergeLists()
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.mergeLists()
  override def toString(): String = "CudfMergeLists"
}

class CudfCollectSet(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CollectSet is not yet supported in reduction")
  override lazy val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CollectSet is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregation = GroupByAggregation.collectSet()
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.mergeSets()
  override def toString(): String = "CudfCollectSet"
  override def dataType: DataType = ArrayType(ref.dataType, containsNull = false)
  override def nullable: Boolean = false
}

class CudfMergeSets(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMergeSets is not yet supported in reduction")
  override lazy val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMergeSets is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregation = GroupByAggregation.mergeSets()
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.mergeSets()
  override def toString(): String = "CudfMergeSets"
}

class CudfM2(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("M2 aggregation is not yet supported in reduction")
  override lazy val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("M2 aggregation is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregation = GroupByAggregation.M2()
  override lazy val mergeAggregate: GroupByAggregation =
    throw new UnsupportedOperationException("Called mergeAggregate on CudfM2 aggregation " +
      "(which only supports updateAggregate operation)")
  override def toString(): String = "CudfM2"


  // TODO: remove comment below
  // dataType must be double, and nullable of the output must be the same as the input ref

  override def dataType: DataType = DoubleType
}

class CudfMergeM2s(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMergeM2s is not yet supported in reduction")
  override lazy val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMergeM2s is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregation =
    throw new UnsupportedOperationException("Called updateAggregate on CudfMergeM2s aggregation " +
      "(which only supports mergeAggregate operation)")
  override lazy val mergeAggregate: GroupByAggregation = GroupByAggregation.mergeM2()
  override def toString(): String = "CudfMergeM2s"

  // TODO: remove comment below
  // dataType must be a structs column, and nullable of the output must be false
  // the struct field `mean` and `m2` can be nullable (but are not always nullable)
  // so I'm no sure if set `nullable = true` is correct here.

  override def dataType: DataType = StructType(Array(
    StructField("count", IntegerType, nullable = false),
    StructField("mean", DoubleType, nullable = true),
    StructField("m2", DoubleType, nullable = true)))
  override def nullable: Boolean = false
}

abstract class CudfFirstLastBase(ref: Expression) extends CudfAggregate(ref) {
  val includeNulls: NullPolicy
  val offset: Int

  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.reduce(ReductionAggregation.nth(offset, includeNulls))
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.reduce(ReductionAggregation.nth(offset, includeNulls))
  override lazy val updateAggregate: GroupByAggregation =
    GroupByAggregation.nth(offset, includeNulls)
  override lazy val mergeAggregate: GroupByAggregation =
    GroupByAggregation.nth(offset, includeNulls)
}

class CudfFirstIncludeNulls(ref: Expression) extends CudfFirstLastBase(ref) {
  override val includeNulls: NullPolicy = NullPolicy.INCLUDE
  override val offset: Int = 0
}

class CudfFirstExcludeNulls(ref: Expression) extends CudfFirstLastBase(ref) {
  override val includeNulls: NullPolicy = NullPolicy.EXCLUDE
  override val offset: Int = 0
}

class CudfLastIncludeNulls(ref: Expression) extends CudfFirstLastBase(ref) {
  override val includeNulls: NullPolicy = NullPolicy.INCLUDE
  override val offset: Int = -1
}

class CudfLastExcludeNulls(ref: Expression) extends CudfFirstLastBase(ref) {
  override val includeNulls: NullPolicy = NullPolicy.EXCLUDE
  override val offset: Int = -1
}

case class GpuMin(child: Expression) extends GpuAggregateFunction
    with GpuBatchedRunningWindowWithFixer
    with GpuAggregateWindowFunction
    with GpuRunningWindowFunction {
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

  // GENERAL WINDOW FUNCTION
  override lazy val windowInputProjection: Seq[Expression] = inputProjection
  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.min().onColumn(inputs.head._2)

  // RUNNING WINDOW
  override def newFixer(): BatchedRunningWindowFixer =
    new BatchedRunningWindowBinaryFixer(BinaryOp.NULL_MIN, "min")

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    inputProjection

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.min(), Some(ReplacePolicy.PRECEDING)))

  override def isGroupByScanSupported: Boolean = child.dataType match {
    case StringType | TimestampType | DateType => false
    case _ => true
  }

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] = inputProjection
  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.min(), Some(ReplacePolicy.PRECEDING)))

  override def isScanSupported: Boolean  = child.dataType match {
    case TimestampType | DateType => false
    case _ => true
  }

}

case class GpuMax(child: Expression) extends GpuAggregateFunction
    with GpuBatchedRunningWindowWithFixer
    with GpuAggregateWindowFunction
    with GpuRunningWindowFunction {
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

  // GENERAL WINDOW FUNCTION
  override lazy val windowInputProjection: Seq[Expression] = inputProjection
  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.max().onColumn(inputs.head._2)

  // RUNNING WINDOW
  override def newFixer(): BatchedRunningWindowFixer =
    new BatchedRunningWindowBinaryFixer(BinaryOp.NULL_MAX, "max")

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    inputProjection

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.max(), Some(ReplacePolicy.PRECEDING)))

  override def isGroupByScanSupported: Boolean = child.dataType match {
    case StringType | TimestampType | DateType => false
    case _ => true
  }

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] = inputProjection
  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.max(), Some(ReplacePolicy.PRECEDING)))

  override def isScanSupported: Boolean = child.dataType match {
    case TimestampType | DateType => false
    case _ => true
  }
}

case class GpuSum(child: Expression, resultType: DataType)
  extends GpuAggregateFunction with ImplicitCastInputTypes
      with GpuBatchedRunningWindowWithFixer
      with GpuAggregateWindowFunction
      with GpuRunningWindowFunction {

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

  // GENERAL WINDOW FUNCTION
  override lazy val windowInputProjection: Seq[Expression] = inputProjection
  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.sum().onColumn(inputs.head._2)

  // RUNNING WINDOW
  override def newFixer(): BatchedRunningWindowFixer =
    new SumBinaryFixer()

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    inputProjection
  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.sum(), Some(ReplacePolicy.PRECEDING)))

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] = inputProjection
  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.sum(), Some(ReplacePolicy.PRECEDING)))
}

/*
 * GpuPivotFirst is an aggregate function used in the second phase of a two phase pivot to do the
 * required rearrangement of values into pivoted form.
 *
 * For example on an input of
 * type | A | B
 * -----+--+--
 *   b | x | 1
 *   a | x | 2
 *   b | y | 3
 *
 * with type=groupbyKey, pivotColumn=A, valueColumn=B, and pivotColumnValues=[x,y]
 *
 * updateExpressions - In the partial_pivot stage, new columns are created based on
 * pivotColumnValues one for each of the aggregation. Last aggregation on these columns grouped by
 * `type` and convert into an array( as per Spark's expectation). Last aggregation(excluding nulls)
 * works here as there would be atmost one entry in new columns when grouped by `type`.
 * After CudfLastExcludeNulls, the intermediate result would be
 *
 * type | x | y
 * -----+---+--
 *   b | 1 | 3
 *   a | 2 | null
 *
 *
 * mergeExpressions - this is the final pivot aggregation after shuffle. We do another `Last`
 * aggregation to merge the results. In this example all the data was combined in the
 * partial_pivot hash aggregation. So it didn't do anything in this stage.
 *
 * The final result would be:
 *
 * type | x | y
 * -----+---+--
 *   b | 1 | 3
 *   a | 2 | null
 *
 * @param pivotColumn column that determines which output position to put valueColumn in.
 * @param valueColumn the column that is being rearranged.
 * @param pivotColumnValues the list of pivotColumn values in the order of desired output. Values
 *                          not listed here will be ignored.
 */
case class GpuPivotFirst(
  pivotColumn: Expression,
  valueColumn: Expression,
  pivotColumnValues: Seq[Any]) extends GpuAggregateFunction {

  val valueDataType = valueColumn.dataType

  override val dataType: DataType = valueDataType
  override val nullable: Boolean = false

  val pivotColAttr = pivotColumnValues.map(pivotColumnValue => {
    // If `pivotColumnValue` is null, then create an AttributeReference for null column.
    if (pivotColumnValue == null) {
      AttributeReference(GpuLiteral(null, valueDataType).toString, valueDataType)()
    } else {
      AttributeReference(pivotColumnValue.toString, valueDataType)()
    }
  })

  override lazy val inputProjection: Seq[Expression] = {
    val expr = pivotColumnValues.map(pivotColumnValue => {
      if (pivotColumnValue == null) {
        GpuIf(GpuIsNull(pivotColumn), valueColumn, GpuLiteral(null, valueDataType))
      } else {
        GpuIf(GpuEqualTo(pivotColumn, GpuLiteral(pivotColumnValue, pivotColumn.dataType)),
          valueColumn, GpuLiteral(null, valueDataType))
      }
    })
    expr
  }

  override lazy val updateExpressions: Seq[GpuExpression] = {
    pivotColAttr.map(pivotColumnValue => new CudfLastExcludeNulls(pivotColumnValue))
  }

  override lazy val mergeExpressions: Seq[GpuExpression] = {
    pivotColAttr.map(pivotColumnValue => new CudfLastExcludeNulls(pivotColumnValue))
  }

  override lazy val evaluateExpression: Expression = {
    GpuCreateArray(pivotColAttr, false)
  }
  override lazy val aggBufferAttributes: Seq[AttributeReference] = pivotColAttr

  override lazy val initialValues: Seq[GpuLiteral] =
    Seq.fill(pivotColumnValues.length)(GpuLiteral(null, valueDataType))

  override def children: Seq[Expression] = pivotColumn :: valueColumn :: Nil
}

case class GpuCount(children: Seq[Expression]) extends GpuAggregateFunction
    with GpuBatchedRunningWindowWithFixer
    with GpuAggregateWindowFunction
    with GpuRunningWindowFunction {
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

  // GENERAL WINDOW FUNCTION
  // countDistinct is not supported for window functions in spark right now.
  // we could support it by doing an `Aggregation.nunique(false)`
  override lazy val windowInputProjection: Seq[Expression] = inputProjection
  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.count(NullPolicy.EXCLUDE).onColumn(inputs.head._2)

  // RUNNING WINDOW
  override def newFixer(): BatchedRunningWindowFixer =
    new BatchedRunningWindowBinaryFixer(BinaryOp.ADD, "count")

  // Scan and group by scan do not support COUNT with nulls excluded.
  // one of them does not even support count at all, so we are going to SUM
  // ones and zeros based off of the validity
  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] = {
    // There can be only one child according to requirements for count right now
    require(children.length == 1)
    val child = children.head
    if (child.nullable) {
      Seq(GpuIf(GpuIsNull(child), GpuLiteral(0, IntegerType), GpuLiteral(1, IntegerType)))
    } else {
      Seq(GpuLiteral(1, IntegerType))
    }
  }

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.sum(), None))

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    groupByScanInputProjection(isRunningBatched)

  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.sum(), None))
}

case class GpuAverage(child: Expression) extends GpuAggregateFunction
    with GpuAggregateWindowFunction {
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

  // NOTE: this sets `failOnErrorOverride=false` in `GpuDivide` to force it not to throw
  // divide-by-zero exceptions, even when ansi mode is enabled in Spark.
  // This is to conform with Spark's behavior in the Average aggregate function.
  override lazy val evaluateExpression: GpuExpression = GpuDivide(
    GpuCast(cudfSum, DoubleType),
    GpuCast(cudfCount, DoubleType), failOnErrorOverride = false)

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

  override val windowInputProjection: Seq[Expression] = Seq(children.head)
  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.mean().onColumn(inputs.head._2)
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
case class GpuFirst(child: Expression, ignoreNulls: Boolean)
  extends GpuAggregateFunction with ImplicitCastInputTypes with Serializable {

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
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  // First is not a deterministic function.
  override lazy val deterministic: Boolean = false
  override def toString: String = s"gpufirst($child)${if (ignoreNulls) " ignore nulls"}"

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }
}

case class GpuLast(child: Expression, ignoreNulls: Boolean)
  extends GpuAggregateFunction with ImplicitCastInputTypes with Serializable {

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
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  // Last is not a deterministic function.
  override lazy val deterministic: Boolean = false
  override def toString: String = s"gpulast($child)${if (ignoreNulls) " ignore nulls"}"

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }
}

trait GpuCollectBase extends GpuAggregateFunction with GpuAggregateWindowFunction {

  def child: Expression

  // Collect operations are non-deterministic since their results depend on the
  // actual order of input rows.
  override lazy val deterministic: Boolean = false

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(child.dataType, containsNull = false)

  override def children: Seq[Expression] = child :: Nil

  // WINDOW FUNCTION
  override val windowInputProjection: Seq[Expression] = Seq(child)

  // Make them lazy to avoid being initialized when creating a GpuCollectOp.
  override lazy val initialValues: Seq[GpuExpression] = throw new UnsupportedOperationException

  override val inputProjection: Seq[Expression] = Seq(child)

  // Unlike other GpuAggregateFunction, GpuCollectFunction will change the type of input data in
  // update stage (childType => Array[childType]). And the input type of merge expression is not
  // same as update expression. Meanwhile, they still share the same ordinal in terms of cuDF
  // table.
  // Therefore, we create two separate buffers for update and merge. And they are pointed to
  // the same ordinal since they share the same exprId.
  protected final lazy val inputBuf: AttributeReference =
  AttributeReference("inputBuf", child.dataType)()

  protected final lazy val outputBuf: AttributeReference =
    inputBuf.copy("outputBuf", dataType)(inputBuf.exprId, inputBuf.qualifier)
}

/**
 * Collects and returns a list of non-unique elements.
 *
 * The two 'offset' parameters are not used by GPU version, but are here for the compatibility
 * with the CPU version and automated checks.
 */
case class GpuCollectList(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends GpuCollectBase {

  override lazy val updateExpressions: Seq[GpuExpression] = new CudfCollectList(inputBuf) :: Nil

  override lazy val mergeExpressions: Seq[GpuExpression] = new CudfMergeLists(outputBuf) :: Nil

  override lazy val evaluateExpression: Expression = outputBuf

  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil

  override def prettyName: String = "collect_list"

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.collectList().onColumn(inputs.head._2)
}

/**
 * Collects and returns a set of unique elements.
 *
 * The two 'offset' parameters are not used by GPU version, but are here for the compatibility
 * with the CPU version and automated checks.
 */
case class GpuCollectSet(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends GpuCollectBase {

  override lazy val updateExpressions: Seq[GpuExpression] = new CudfCollectSet(inputBuf) :: Nil

  override lazy val mergeExpressions: Seq[GpuExpression] = new CudfMergeSets(outputBuf) :: Nil

  override lazy val evaluateExpression: Expression = outputBuf

  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil

  override def prettyName: String = "collect_set"

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.collectSet().onColumn(inputs.head._2)
}

/**
 * TODO
 */
abstract class GpuM2(child: Expression)
  extends GpuAggregateFunction with ImplicitCastInputTypes {

  // TODO: remove comment below
  // dataType must be a structs column, and nullable of the output must be false
  // the struct field `mean` and `m2` can be nullable (but are not always nullable)
  // so I'm no sure if set `nullable = true` is correct here.

  override def dataType: DataType = StructType(Array(
    StructField("count", IntegerType, nullable = false),
    StructField("mean", DoubleType, nullable = true),
    StructField("m2", DoubleType, nullable = true)))
  override def nullable: Boolean = false
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  protected lazy val outputBuffer: AttributeReference = assembleOutputBuffer
    // AttributeReference("bufferCountMeanM2", dataType, nullable = false)()
  protected lazy val bufferCount: AttributeReference =
    AttributeReference("bufferCount", IntegerType, nullable = false)()
  protected lazy val bufferSum: AttributeReference =
    AttributeReference("bufferSum", DoubleType, nullable = true)()
  protected lazy val bufferM2: AttributeReference =
    AttributeReference("bufferM2", DoubleType, nullable = true)()

  private def assembleOutputBuffer: AttributeReference = {
    // Firstly we need to compute mean from count and sum
    // TODO: withResource
//    val mean = GpuDivide(bufferSum, GpuCast(bufferCount, DoubleType), failOnErrorOverride = false)

    // Assemble a struct column from the columns count, mean, and M2

    AttributeReference("bufferCountMeanM2", dataType, nullable = false)()
  }

  //  assembleOutputBuffer

  override lazy val aggBufferAttributes: Seq[AttributeReference] = outputBuffer :: Nil
  override lazy val inputProjection: Seq[Expression] = Seq(child)

  // TODO: Remove comment below
  // initializeValues should not be called, as we don't need to intialize values
  // override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(0.0, BooleanType))
  override lazy val initialValues: Seq[GpuLiteral] = throw new UnsupportedOperationException

  // For local update, we need to compute all 3 aggregates: count, sum, and M2.
  override lazy val updateExpressions: Seq[Expression] =
    new CudfCount(bufferCount) :: new CudfSum(bufferSum) :: new CudfM2(bufferM2) :: Nil

  override lazy val mergeExpressions: Seq[GpuExpression] = new CudfMergeM2s(outputBuffer) :: Nil
}

case class GpuStddevPop(child: Expression) extends GpuM2(child) {
  override lazy val evaluateExpression: GpuExpression =
  {
     // Compute stddev_pop from M2s.
    GpuDivide(bufferSum, GpuCast(bufferCount, DoubleType), failOnErrorOverride = false)
  }

  override def children: Seq[Expression] = Seq(child)
  override def prettyName: String = "stddev_pop"
}
//
//case class GpuStddevSamp(child: Expression,
//                        mutableAggBufferOffset: Int = 0,
//                        inputAggBufferOffset: Int = 0) extends GpuAggregateFunction {
//
//  override lazy val updateExpressions: Seq[GpuExpression] = new CudfCollectSet(inputBuf) :: Nil
//
//  override lazy val mergeExpressions: Seq[GpuExpression] = new CudfMergeSets(outputBuf) :: Nil
//
//  override lazy val evaluateExpression: Expression = outputBuf
//
//  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil
//
//  override def prettyName: String = "stddev_pop"
//}
//
//case class GpuVariancePop(child: Expression,
//                        mutableAggBufferOffset: Int = 0,
//                        inputAggBufferOffset: Int = 0) extends GpuAggregateFunction {
//
//  override lazy val updateExpressions: Seq[GpuExpression] = new CudfCollectSet(inputBuf) :: Nil
//
//  override lazy val mergeExpressions: Seq[GpuExpression] = new CudfMergeSets(outputBuf) :: Nil
//
//  override lazy val evaluateExpression: Expression = outputBuf
//
//  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil
//
//  override def prettyName: String = "stddev_pop"
//}
//
//case class GpuVarianceSamp(child: Expression,
//                        mutableAggBufferOffset: Int = 0,
//                        inputAggBufferOffset: Int = 0) extends GpuAggregateFunction {
//
//  override lazy val updateExpressions: Seq[GpuExpression] = new CudfCollectSet(inputBuf) :: Nil
//
//  override lazy val mergeExpressions: Seq[GpuExpression] = new CudfMergeSets(outputBuf) :: Nil
//
//  override lazy val evaluateExpression: Expression = outputBuf
//
//  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil
//
//  override def prettyName: String = "stddev_pop"
//}
