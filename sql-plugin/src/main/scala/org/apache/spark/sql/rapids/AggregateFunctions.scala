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
import ai.rapids.cudf.{BinaryOp, ColumnVector, DType, GroupByAggregation, GroupByAggregationOnColumn, GroupByScanAggregation, NullPolicy, ReductionAggregation, ReplacePolicy, RollingAggregation, RollingAggregationOnColumn, ScanAggregation}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.v2._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExprId, ImplicitCastInputTypes, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, TypeUtils}
import org.apache.spark.sql.types._

trait GpuAggregateFunction extends GpuExpression
    with ShimExpression
    with GpuUnevaluable {
  // using the child reference, define the shape of the vectors sent to
  // the update/merge expressions
  val inputProjection: Seq[Expression]

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false

  /** The schema of the aggregation buffer. */
  def aggBufferSchema: StructType = null //not used in GPU version

  /** Attributes of fields in aggBufferSchema. */
  def aggBufferAttributes: Seq[AttributeReference]

  /** This is the shape of merge aggregates, to which the postMerge binds to */
  def mergeBufferAttributes: Seq[AttributeReference] = aggBufferAttributes

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
  val initialValues: Seq[Expression]

  // update: first half of the aggregation (count = count)
  val updateExpressions: Seq[Expression]

  // expression to use to modify pre and post a cuDF update aggregate
  // preUpdate: modify an incoming batch to match the shape/type cuDF expects
  // postUpdate and postUpdateAttr: take the output of a cuDF update aggregate and return
  //   what spark expects
  lazy val preUpdate: Seq[Expression] = aggBufferAttributes
  lazy val postUpdate: Seq[Expression] = aggBufferAttributes
  lazy val postUpdateAttr: Seq[AttributeReference] = aggBufferAttributes

  // merge: second half of the aggregation (count = sum). Also use to merge multiple batches.
  val mergeExpressions: Seq[Expression]

  // expression to use to modify pre and post a cudf merge aggregate
  // preMerge: modify a partial batch to match the input required by a merge aggregate
  // postMerge and postMergeAttr: used to put the result of the merge aggregate, in Spark terms.
  lazy val preMerge: Seq[Expression] = aggBufferAttributes
  lazy val postMerge: Seq[Expression] = aggBufferAttributes
  lazy val postMergeAttr: Seq[AttributeReference] = aggBufferAttributes

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
  override val inputProjection: Seq[Expression] = {
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

  override val initialValues: Seq[Expression] =
    aggregateFunction.initialValues
  override val updateExpressions: Seq[Expression] =
    aggregateFunction.updateExpressions
  override val mergeExpressions: Seq[Expression] =
    aggregateFunction.mergeExpressions
  override val evaluateExpression: Expression =
    aggregateFunction.evaluateExpression

  override lazy val preUpdate: Seq[Expression] = aggregateFunction.preUpdate
  override lazy val postUpdate: Seq[Expression] = aggregateFunction.postUpdate
  override lazy val postUpdateAttr: Seq[AttributeReference] = aggregateFunction.postUpdateAttr

  override lazy val preMerge: Seq[Expression] = aggregateFunction.preMerge
  override lazy val postMerge: Seq[Expression] = aggregateFunction.postMerge
  override lazy val postMergeAttr: Seq[AttributeReference] = aggregateFunction.postMergeAttr
}

case class GpuAggregateExpression(origAggregateFunction: GpuAggregateFunction,
                                  mode: AggregateMode,
                                  isDistinct: Boolean,
                                  filter: Option[Expression],
                                  resultId: ExprId)
  extends GpuExpression
      with ShimExpression
      with GpuUnevaluable {

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
      // For Partial, PartialMerge, or Final mode, the input to the `aggregateFunction` is
      // aggregate buffers, and the actual children of `aggregateFunction` is not used,
      // here we normalize the expr id.
      case Partial | PartialMerge | Final => aggregateFunction.transform {
        case a: AttributeReference => a.withExprId(ExprId(0))
      }
      case Complete => aggregateFunction
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

abstract case class CudfAggregate(ref: Expression) extends GpuUnevaluable with ShimExpression {
  // we use this to get the ordinal of the bound reference, s.t. we can ask cudf to perform
  // the aggregate on that column
  protected def getOrdinal(ref: Expression): Int =
    ref.asInstanceOf[GpuBoundReference].ordinal
  lazy val updateReductionAggregate: Seq[GpuColumnVector] => cudf.Scalar =
    (cvs: Seq[GpuColumnVector]) => {
      updateReductionAggregateInternal(cvs(getOrdinal(ref)).getBase)
    }

  lazy val mergeReductionAggregate: Seq[GpuColumnVector]=> cudf.Scalar =
    (cvs: Seq[GpuColumnVector]) => {
      mergeReductionAggregateInternal(cvs(getOrdinal(ref)).getBase)
    }

  val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar
  val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar
  val updateAggregate: GroupByAggregationOnColumn
  val mergeAggregate: GroupByAggregationOnColumn

  def dataType: DataType = ref.dataType
  def updateDataType: DataType = dataType
  def nullable: Boolean = ref.nullable
  def children: Seq[Expression] = Seq(ref)
}

class CudfCount(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => cudf.Scalar.fromLong(col.getRowCount - col.getNullCount)
  override val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.count(NullPolicy.EXCLUDE)
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.sum()
      .onColumn(getOrdinal(ref))

  // the partial count outputs an int
  override def updateDataType: DataType = IntegerType
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
  @transient val rapidsSumType: DType = GpuColumnVector.getNonNestedRapidsType(dataType)

  override val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum(rapidsSumType)

  override val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    updateReductionAggregateInternal

  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.sum()
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.sum()
      .onColumn(getOrdinal(ref))
  override def toString(): String = "CudfSum"

}

class CudfMax(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.max()
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.max()
      .onColumn(getOrdinal(ref))
  override def toString(): String = "CudfMax"
}

class CudfMin(ref: Expression) extends CudfAggregate(ref) {
  override val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.min()
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.min()
      .onColumn(getOrdinal(ref))
  override def toString(): String = "CudfMin"
}

class CudfCollectList(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CollectList is not yet supported in reduction")
  override lazy val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CollectList is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.collectList()
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mergeLists()
      .onColumn(getOrdinal(ref))
  override def toString(): String = "CudfCollectList"
  override def dataType: DataType = ArrayType(ref.dataType, containsNull = false)
  override def nullable: Boolean = false
}

class CudfMergeLists(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("MergeLists is not yet supported in reduction")
  override lazy val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("MergeLists is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mergeLists()
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mergeLists()
      .onColumn(getOrdinal(ref))
  override def toString(): String = "CudfMergeLists"
  override def nullable: Boolean = false
}

class CudfCollectSet(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CollectSet is not yet supported in reduction")
  override lazy val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CollectSet is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.collectSet()
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mergeSets()
      .onColumn(getOrdinal(ref))
  override def toString(): String = "CudfCollectSet"
  override def dataType: DataType = ArrayType(ref.dataType, containsNull = false)
  override def nullable: Boolean = false
}

class CudfMergeSets(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMergeSets is not yet supported in reduction")
  override lazy val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMergeSets is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mergeSets()
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn=
    GroupByAggregation.mergeSets()
      .onColumn(getOrdinal(ref))
  override def toString(): String = "CudfMergeSets"
  override def dataType: DataType = ref.dataType
  override def nullable: Boolean = false
}

abstract class CudfFirstLastBase(ref: Expression) extends CudfAggregate(ref) {
  val includeNulls: NullPolicy
  val offset: Int

  override val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.reduce(ReductionAggregation.nth(offset, includeNulls))
  override val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.reduce(ReductionAggregation.nth(offset, includeNulls))
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.nth(offset, includeNulls)
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.nth(offset, includeNulls)
      .onColumn(getOrdinal(ref))
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

/**
 * This class is only used by the M2 class aggregates, do not confuse this with GpuAverage.
 * In the future, this aggregate class should be removed and the mean values should be
 * generated in the output of libcudf's M2 aggregate.
 */
class CudfMean(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMean is not supported in reduction")
  override lazy val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMean is not supported in reduction")

  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mean().onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mean().onColumn(getOrdinal(ref))

  override def toString(): String = "CudfMeanForM2"
}

class CudfM2(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfM2 aggregation is not supported in reduction")
  override lazy val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfM2 aggregation is not supported in reduction")

  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.M2().onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mergeM2().onColumn(getOrdinal(ref))

  override def toString(): String = "CudfM2"
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = false
}

class CudfMergeM2(ref: Expression) extends CudfAggregate(ref) {
  override lazy val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMergeM2 aggregation is not supported in reduction")
  override lazy val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("CudfMergeM2 aggregation is not supported in reduction")

  override lazy val updateAggregate: GroupByAggregationOnColumn =
    throw new UnsupportedOperationException("CudfMergeM2 only supports mergeAggregate")
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mergeM2().onColumn(getOrdinal(ref))

  override def toString(): String = "CudfMergeM2"
  override def dataType: DataType =
    StructType(
      StructField("n", IntegerType, nullable = false) ::
        StructField("avg", DoubleType, nullable = true) ::
        StructField("m2", DoubleType, nullable = true) :: Nil)
  override def nullable: Boolean = false
}

case class GpuMin(child: Expression) extends GpuAggregateFunction
    with GpuBatchedRunningWindowWithFixer
    with GpuAggregateWindowFunction
    with GpuRunningWindowFunction {
  private lazy val cudfMin = AttributeReference("min", child.dataType)()

  override lazy val inputProjection: Seq[Expression] = Seq(child)
  override lazy val updateExpressions: Seq[Expression] = Seq(new CudfMin(cudfMin))
  override lazy val mergeExpressions: Seq[Expression] = Seq(new CudfMin(cudfMin))
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
  override lazy val updateExpressions: Seq[Expression] = Seq(new CudfMax(cudfMax))
  override lazy val mergeExpressions: Seq[Expression] = Seq(new CudfMax(cudfMax))
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
  override lazy val updateExpressions: Seq[Expression] = Seq(new CudfSum(cudfSum))
  // we need to cast to `resultType` here, since Spark is not widening types
  // as done before Spark 3.2.0. See CudfSum for more info.
  override lazy val preUpdate: Seq[Expression] = Seq(GpuCast(cudfSum, resultType))
  override lazy val mergeExpressions: Seq[Expression] = Seq(new CudfSum(cudfSum))
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
  // Spark 3.2.0+ stopped casting the input data to the output type before the sum operation
  // This fixes that.
  override lazy val windowInputProjection: Seq[Expression] = {
    if (child.dataType != resultType) {
      Seq(GpuCast(child, resultType))
    } else {
      Seq(child)
    }
  }

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.sum().onColumn(inputs.head._2)

  // RUNNING WINDOW
  override def newFixer(): BatchedRunningWindowFixer =
    new SumBinaryFixer()

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    windowInputProjection

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.sum(), Some(ReplacePolicy.PRECEDING)))

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    windowInputProjection

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
 * type | x |
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

  override lazy val updateExpressions: Seq[Expression] = {
    pivotColAttr.map(pivotColumnValue => new CudfLastExcludeNulls(pivotColumnValue))
  }

  override lazy val mergeExpressions: Seq[Expression] = {
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
  // counts are GpuToCpuBufferTransitionLong
  private lazy val cudfCount = AttributeReference("count", LongType)()

  override lazy val inputProjection: Seq[Expression] = Seq(children.head)
  override lazy val updateExpressions: Seq[Expression] = Seq(new CudfCount(cudfCount))

  override lazy val postUpdate: Seq[Expression] = Seq(GpuCast(cudfCount, dataType))
  override lazy val postUpdateAttr: Seq[AttributeReference] = Seq(cudfCount)

  override lazy val mergeExpressions: Seq[Expression] = Seq(new CudfSum(cudfCount))
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

  override lazy val inputProjection: Seq[Expression] = Seq(
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
  override lazy val mergeExpressions: Seq[Expression] = Seq(new CudfSum(cudfSum),
    new CudfSum(cudfCount))
  // The count input projection will need to be collected as a sum (of counts) instead of
  // counts (of counts) as the GpuIsNotNull o/p is casted to count=0 for null and 1 otherwise, and
  // the total count can be correctly evaluated only by summing them. eg. avg(col(null, 27))
  // should be 27, with count column projection as (0, 1) and total count for dividing the
  // average = (0 + 1) and not 2 which is the rowcount of the projected column.
  override lazy val updateExpressions: Seq[Expression] = Seq(new CudfSum(cudfSum),
    new CudfSum(cudfCount))

  // NOTE: this sets `failOnErrorOverride=false` in `GpuDivide` to force it not to throw
  // divide-by-zero exceptions, even when ansi mode is enabled in Spark.
  // This is to conform with Spark's behavior in the Average aggregate function.
  override lazy val evaluateExpression: Expression = GpuDivide(
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

  override lazy val updateExpressions: Seq[Expression] = commonExpressions
  override lazy val mergeExpressions: Seq[Expression] = commonExpressions
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

  override lazy val updateExpressions: Seq[Expression] = commonExpressions
  override lazy val mergeExpressions: Seq[Expression] = commonExpressions
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
  override lazy val initialValues: Seq[Expression] = throw new UnsupportedOperationException

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

  override lazy val updateExpressions: Seq[Expression] = new CudfCollectList(inputBuf) :: Nil

  override lazy val mergeExpressions: Seq[Expression] = new CudfMergeLists(outputBuf) :: Nil

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

  override lazy val updateExpressions: Seq[Expression] = new CudfCollectSet(inputBuf) :: Nil

  override lazy val mergeExpressions: Seq[Expression] = new CudfMergeSets(outputBuf) :: Nil

  override lazy val evaluateExpression: Expression = outputBuf

  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil

  override def prettyName: String = "collect_set"

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.collectSet().onColumn(inputs.head._2)
}

trait CpuToGpuAggregateBufferConverter {
  def createExpression(child: Expression): CpuToGpuBufferTransition
}

trait GpuToCpuAggregateBufferConverter {
  def createExpression(child: Expression): GpuToCpuBufferTransition
}

trait CpuToGpuBufferTransition extends ShimUnaryExpression with CodegenFallback

trait GpuToCpuBufferTransition extends ShimUnaryExpression with CodegenFallback {
  override def dataType: DataType = BinaryType
}

class CpuToGpuCollectBufferConverter(
    elementType: DataType) extends CpuToGpuAggregateBufferConverter {
  def createExpression(child: Expression): CpuToGpuBufferTransition = {
    CpuToGpuCollectBufferTransition(child, elementType)
  }
}

case class CpuToGpuCollectBufferTransition(
    override val child: Expression,
    private val elementType: DataType) extends CpuToGpuBufferTransition {

  private lazy val row = new UnsafeRow(1)

  override def dataType: DataType = ArrayType(elementType, containsNull = false)

  override protected def nullSafeEval(input: Any): ArrayData = {
    // Converts binary buffer into UnSafeArrayData, according to the deserialize method of Collect.
    // The input binary buffer is the binary view of a UnsafeRow, which only contains single field
    // with ArrayType of elementType. Since array of elements exactly matches the GPU format, we
    // don't need to do any conversion in memory level. Instead, we simply bind the binary data to
    // a reused UnsafeRow. Then, fetch the only field as ArrayData.
    val bytes = input.asInstanceOf[Array[Byte]]
    row.pointTo(bytes, bytes.length)
    row.getArray(0).copy()
  }
}

class GpuToCpuCollectBufferConverter extends GpuToCpuAggregateBufferConverter {
  def createExpression(child: Expression): GpuToCpuBufferTransition = {
    GpuToCpuCollectBufferTransition(child)
  }
}

case class GpuToCpuCollectBufferTransition(
    override val child: Expression) extends GpuToCpuBufferTransition {

  private lazy val projection = UnsafeProjection.create(Array(child.dataType))

  override protected def nullSafeEval(input: Any): Array[Byte] = {
    // Converts UnSafeArrayData into binary buffer, according to the serialize method of Collect.
    // The binary buffer is the binary view of a UnsafeRow, which only contains single field
    // with ArrayType of elementType. As Collect.serialize, we create an UnsafeProjection to
    // transform ArrayData to binary view of the single field UnsafeRow. Unlike Collect.serialize,
    // we don't have to build ArrayData from on-heap array, since the input is already formatted
    // in ArrayData(UnsafeArrayData).
    val arrayData = input.asInstanceOf[ArrayData]
    projection.apply(InternalRow.apply(arrayData)).getBytes
  }
}

/**
 * Base class for overriding standard deviation and variance aggregations.
 * This is also a GPU-based implementation of 'CentralMomentAgg' aggregation class in Spark with
 * the fixed 'momentOrder' variable set to '2'.
 */
abstract class GpuM2(child: Expression, nullOnDivideByZero: Boolean)
  extends GpuAggregateFunction with ImplicitCastInputTypes with Serializable {

  override def children: Seq[Expression] = Seq(child)
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  protected def divideByZeroEvalResult: Expression =
    ShimLoader.getSparkShims.getCentralMomentDivideByZeroEvalResult(nullOnDivideByZero)

  override lazy val inputProjection: Seq[Expression] = Seq(child, child, child)
  override lazy val initialValues: Seq[GpuLiteral] =
    Seq(GpuLiteral(0.0), GpuLiteral(0.0), GpuLiteral(0.0))

  // Buffers for the update stage.
  protected lazy val bufferN: AttributeReference =
    AttributeReference("n", DoubleType, nullable = false)()
  protected lazy val bufferAvg: AttributeReference =
    AttributeReference("avg", DoubleType, nullable = false)()
  protected lazy val bufferM2: AttributeReference =
    AttributeReference("m2", DoubleType, nullable = false)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    bufferN :: bufferAvg :: bufferM2 :: Nil

  // For local update, we need to compute all 3 aggregates: n, avg, m2.
  override lazy val updateExpressions: Seq[Expression] =
    new CudfCount(bufferN) :: new CudfMean(bufferAvg) :: new CudfM2(bufferM2) :: Nil

  // We copy the `bufferN` attribute and stomp on the type as Integer here, because we only
  // have its values are of Integer type. However,we want to output `DoubleType` to match
  // with Spark so we need to cast it to `DoubleType`.
  //
  // In the future, when we make CudfM2 aggregate outputs all the buffers at once,
  // we need to make sure that bufferN is a LongType.
  //
  // Note that avg and m2 output from libcudf's M2 aggregate are nullable while Spark's
  // corresponding buffers require them to be non-nullable.
  // As such, we need to convert those nulls into Double(0.0) in the postUpdate step.
  // This will not affect the outcome of the merge step.
  override lazy val postUpdate: Seq[Expression] = {
    val bufferCountAsInt = bufferN.copy(dataType = IntegerType)(bufferN.exprId, bufferN.qualifier)
    val bufferAvgNoNulls = GpuCoalesce(Seq(bufferAvg, GpuLiteral(0.0, DoubleType)))
    val bufferM2NoNulls  = GpuCoalesce(Seq(bufferM2, GpuLiteral(0.0, DoubleType)))
    GpuCast(bufferCountAsInt, DoubleType) :: bufferAvgNoNulls :: bufferM2NoNulls :: Nil
  }

  // Before merging we have 3 columns and we need to combine them into a structs column.
  // This is because we are going to do the merging using libcudf's native MERGE_M2 aggregate,
  // which only accepts one column in the input.
  //
  // We cast `n` to be an Integer, as that's what MERGE_M2 expects. Note that Spark keeps
  // `n` as Double thus we also need to cast `n` back to Double after merging.
  // In the future, we need to rewrite CudfMergeM2 such that it accepts `n` in Double type and
  // also output `n` in Double type.
  override lazy val preMerge: Seq[Expression] = {
    val childrenWithNames =
      GpuLiteral("n", StringType) :: GpuCast(bufferN, IntegerType) ::
        GpuLiteral("avg", StringType) :: bufferAvg ::
        GpuLiteral("m2", StringType) :: bufferM2 :: Nil
    GpuCreateNamedStruct(childrenWithNames) :: Nil
  }

  def mergeM2DataType: DataType =
    StructType(
      StructField("n", IntegerType, nullable = false) ::
        StructField("avg", DoubleType, nullable = false) ::
        StructField("m2", DoubleType, nullable = false) :: Nil)

  override def mergeBufferAttributes: Seq[AttributeReference] = postMergeAttr

  private val m2Struct = AttributeReference("m2struct", mergeM2DataType, nullable = false)()
  override lazy val mergeExpressions: Seq[Expression] = new CudfMergeM2(m2Struct) :: Nil

  // The result of merging step is a structs column thus we create this attribute to bind it.
  override lazy val postMergeAttr: Seq[AttributeReference] = Seq(m2Struct)

  // The postMerge step needs to extract 3 columns (n, avg, m2) from the structs column
  // output from the merge step. Note that the first one is casted to Double to match with Spark.
  //
  // In the future, when rewriting CudfMergeM2, we will need to ouput it in Double type.
  //
  override lazy val postMerge: Seq[Expression] = Seq(
    GpuCast(GpuGetStructField(m2Struct, 0), DoubleType),
    GpuCast(GpuGetStructField(m2Struct, 1), DoubleType),
    GpuCast(GpuGetStructField(m2Struct, 2), DoubleType))
}

// TODO: Shim for Spark >=3.1.0:
//  `nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate`
case class GpuStddevPop(child: Expression, nullOnDivideByZero: Boolean = true)
  extends GpuM2(child, nullOnDivideByZero) {

  override lazy val evaluateExpression: Expression = {
    // stddev_pop = sqrt(m2 / n).
    val stddevPop = GpuSqrt(GpuDivide(bufferM2, bufferN, failOnErrorOverride = false))

    // Set nulls for the rows where n == 0.
    GpuIf(GpuEqualTo(bufferN, GpuLiteral(0.0)), GpuLiteral(null, DoubleType), stddevPop)
  }

  override def prettyName: String = "stddev_pop"
}

// TODO: Shim for Spark >=3.1.0:
//  `nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate`
case class GpuStddevSamp(child: Expression, nullOnDivideByZero: Boolean = true)
  extends GpuM2(child, nullOnDivideByZero) {

  override lazy val evaluateExpression: Expression = {
    // stddev_samp = sqrt(m2 / (n - 1.0)).
    val stddevSamp =
      GpuSqrt(GpuDivide(bufferM2, GpuSubtract(bufferN, GpuLiteral(1.0), failOnError = false),
        failOnErrorOverride = false))

    // Set nulls for the rows where n == 0, and set nulls (or NaN) for the rows where n == 1.
    GpuIf(GpuEqualTo(bufferN, GpuLiteral(1.0)), divideByZeroEvalResult,
      GpuIf(GpuEqualTo(bufferN, GpuLiteral(0.0)), GpuLiteral(null, DoubleType), stddevSamp))
  }

  override def prettyName: String = "stddev_samp"
}

// TODO: Shim for Spark >=3.1.0:
//  `nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate`
case class GpuVariancePop(child: Expression, nullOnDivideByZero: Boolean = true)
  extends GpuM2(child, nullOnDivideByZero) {

  override lazy val evaluateExpression: Expression = {
    // var_pop = m2 / n.
    val varPop = GpuDivide(bufferM2, bufferN, failOnErrorOverride = false)

    // Set nulls for the rows where n == 0.
    GpuIf(GpuEqualTo(bufferN, GpuLiteral(0.0)), GpuLiteral(null, DoubleType), varPop)
  }

  override def prettyName: String = "var_pop"
}

// TODO: Shim for Spark >=3.1.0:
//  `nullOnDivideByZero: Boolean = !SQLConf.get.legacyStatisticalAggregate`
case class GpuVarianceSamp(child: Expression, nullOnDivideByZero: Boolean = true)
  extends GpuM2(child, nullOnDivideByZero) {

  override lazy val evaluateExpression: Expression = {
    // var_samp = m2 / (n - 1.0).
    val varSamp = GpuDivide(bufferM2, GpuSubtract(bufferN, GpuLiteral(1.0), failOnError = false),
      failOnErrorOverride = false)

    // Set nulls for the rows where n == 0, and set nulls (or NaN) for the rows where n == 1.
    GpuIf(GpuEqualTo(bufferN, GpuLiteral(1.0)), divideByZeroEvalResult,
      GpuIf(GpuEqualTo(bufferN, GpuLiteral(0.0)), GpuLiteral(null, DoubleType), varSamp))
  }

  override def prettyName: String = "var_samp"
}
