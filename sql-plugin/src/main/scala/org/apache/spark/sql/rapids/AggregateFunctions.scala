/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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
import ai.rapids.cudf.{Aggregation128Utils, BinaryOp, ColumnVector, DType, GroupByAggregation, GroupByScanAggregation, NaNEquality, NullEquality, NullPolicy, ReductionAggregation, ReplacePolicy, RollingAggregation, RollingAggregationOnColumn, Scalar, ScanAggregation}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.{GpuDeterministicFirstLastCollectShim, ShimExpression, ShimUnaryExpression}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExprId, ImplicitCastInputTypes, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Trait that all aggregate functions implement.
 *
 * Aggregates start with some input from the child plan or from another aggregate
 * (or from itself if the aggregate is merging several batches).
 *
 * In general terms an aggregate function can be in one of two modes of operation:
 * update or merge. Either the function is aggregating raw input, or it is merging
 * previously aggregated data. Normally, Spark breaks up the processing of the aggregate
 * in two exec nodes (a partial aggregate and a final), and the are separated by a
 * shuffle boundary. That is not true for all aggregates, especially when looking at
 * other flavors of Spark. What doesn't change is the core function of updating or
 * merging. Note that an aggregate can merge right after an update is
 * performed, as we have cases where input batches are update-aggregated and then
 * a bigger batch is built by merging together those pre-aggregated inputs.
 *
 * Aggregates have an interface to Spark and that is defined by `aggBufferAttributes`.
 * This collection of attributes must match the Spark equivalent of the aggregate,
 * so that if half of the aggregate (update or merge) executes on the CPU, we can
 * be compatible. The GpuAggregateFunction adds special steps to ensure that it can
 * produce (and consume) batches in the shape of `aggBufferAttributes`.
 *
 * The general transitions that are implemented in the aggregate function are as
 * follows:
 *
 * 1) `inputProjection` -> `updateAggregates`: `inputProjection` creates a sequence of
 *    values that are operated on by the `updateAggregates`. The length of `inputProjection`
 *    must be the same as `updateAggregates`, and `updateAggregates` (cuDF aggregates) should
 *    be able to work with the product of the `inputProjection` (i.e. types are compatible)
 *
 * 2) `updateAggregates` -> `postUpdate`: after the cuDF update aggregate, a post process step
 *    can (optionally) be performed. The `postUpdate` takes the output of `updateAggregate`
 *    that must match the order of columns and types as specified in `aggBufferAttributes`.
 *
 * 3) `postUpdate` -> `preMerge`: preMerge prepares batches before going into the `mergeAggregate`.
 *    The `preMerge` step binds to `aggBufferAttributes`, so it can be used to transform Spark
 *    compatible batch to a batch that the cuDF merge aggregate expects. Its input has the
 *    same shape as that produced by `postUpdate`.
 *
 * 4) `mergeAggregates`->`postMerge`: postMerge optionally transforms the output of the cuDF merge
 *    aggregate in two situations:
 *      1 - The step is used to match the `aggBufferAttributes` references for partial
 *           aggregates where each partially aggregated batch is getting merged with
 *           `AggHelper(merge=true)`
 *      2 - In a final aggregate where the merged batches are transformed to what
 *          `evaluateExpression` expects. For simple aggregates like sum or count,
 *          `evaluateExpression` is just `aggBufferAttributes`, but for more complex
 *          aggregates, it is an expression (see GpuAverage and GpuM2 subclasses) that
 *          relies on the merge step producing a columns in the shape of `aggBufferAttributes`.
 */
trait GpuAggregateFunction extends GpuExpression
    with ShimExpression
    with GpuUnevaluable {

  def filteredInputProjection(filter: Expression): Seq[Expression] =
    inputProjection.map { ip =>
      GpuIf(filter, ip, GpuLiteral(null, ip.dataType))
    }

  /**
   * These are values that spark calls initial because it uses
   * them to initialize the aggregation buffer, and returns them in case
   * of an empty aggregate when there are no expressions.
   *
   * In our case they are only used in a very specific case:
   * the empty input reduction case. In this case we don't have input
   * to reduce, but we do have reduction functions, so each reduction function's
   * `initialValues` is invoked to populate a single row of output.
   **/
  val initialValues: Seq[Expression]

  /**
   * Using the child reference, define the shape of input batches sent to
   * the update expressions
   * @note this can be thought of as "pre" update: as update consumes its
   *       output in order
   */
  val inputProjection: Seq[Expression]

  /**
   * update: first half of the aggregation
   * The sequence of `CudfAggregate` must match the shape of `inputProjections`,
   * and care must be taken to ensure that each cuDF aggregate is able to work
   * with the corresponding inputProjection (i.e. inputProjection[i] is the input
   * to updateAggregates[i]).
   */
  val updateAggregates: Seq[CudfAggregate]

  /**
   * This is the last step in the update phase. It can optionally modify the result of the
   * cuDF update aggregates, or be a pass-through.
   * postUpdateAttr: matches the order (and types) of `updateAggregates`
   * postUpdate: binds to `postUpdateAttr` and defines an expression that results
   *             in what Spark expects from the update.
   *             By default this is `postUpdateAttr`, as it should match the
   *             shape of the Spark agg buffer leaving cuDF, but in the
   *             M2 and Count cases we overwrite it, because the cuDF shape isn't
   *             what Spark expects.
   */
  final lazy val postUpdateAttr: Seq[AttributeReference] = updateAggregates.map(_.attr)
  lazy val postUpdate: Seq[Expression] = postUpdateAttr

  /**
   * This step is the first step into the merge phase. It can optionally modify the result of
   * the postUpdate before it goes into the cuDF merge aggregation.
   * preMerge: modify a partial batch to match the input required by a merge aggregate
   *
   * This always binds to `aggBufferAttributes` as that is the inbound schema
   * for this aggregate from Spark. If it is set to `aggBufferAttributes` by default
   * so the bind behaves like a pass through in most cases.
   */
  lazy val preMerge: Seq[Expression] = aggBufferAttributes

  /**
   * merge: second half of the aggregation. Also used to merge multiple batches in the
   * update or merge stages. These cuDF aggregates consume the output of `preMerge`.
   * The sequence of `CudfAggregate` must match the shape of `aggBufferAttributes`,
   * and care must be taken to ensure that each cuDF aggregate is able to work
   * with the corresponding input (i.e. aggBufferAttributes[i] is the input
   * to mergeAggregates[i]). If a transformation is required, `preMerge` can be used
   * to mutate the batches before they arrive at `mergeAggregates`.
   */
  val mergeAggregates: Seq[CudfAggregate]

  /**
   * This is the last aggregation step, which optionally changes the result of the
   * `mergeAggregate`.
   * postMergeAttr: matches the order (and types) of `mergeAggregates`
   * postMerge: binds to `postMergeAttr` and defines an expression that results
   *            in what Spark expects from the merge. We set this to `postMergeAttr`
   *            by default, for the pass through case (like in `postUpdate`). GpuM2
   *            is the exception, where `postMerge` mutates the result of the
   *            `mergeAggregates` to output what Spark expects.
   */
  final lazy val postMergeAttr: Seq[AttributeReference] = mergeAggregates.map(_.attr)
  lazy val postMerge: Seq[Expression] = postMergeAttr

  /**
   * This takes the output of `postMerge` computes the final result of the aggregation.
   * @note `evaluateExpression` is bound to `aggBufferAttributes`, so the references used in
   *       `evaluateExpression` must also be used in `aggBufferAttributes`.
   */
  val evaluateExpression: Expression

  /**
   * This is the contract with the outside world. It describes what the output of postUpdate should
   * look like, and what the input to preMerge looks like. It also describes what the output of
   * postMerge must look like.
   */
  def aggBufferAttributes: Seq[AttributeReference]

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  /** String representation used in explain plans. */
  def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + flatArguments.mkString(start, ", ", ")")
  }

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false
}

case class WrappedAggFunction(aggregateFunction: GpuAggregateFunction, filter: Expression)
    extends GpuAggregateFunction {
  override val inputProjection: Seq[Expression] = aggregateFunction.filteredInputProjection(filter)

  /** Attributes of fields in aggBufferSchema. */
  override def aggBufferAttributes: Seq[AttributeReference] =
    aggregateFunction.aggBufferAttributes
  override def nullable: Boolean = aggregateFunction.nullable

  override def dataType: DataType = aggregateFunction.dataType

  override def children: Seq[Expression] = Seq(aggregateFunction, filter)

  override val initialValues: Seq[Expression] = aggregateFunction.initialValues
  override lazy val updateAggregates: Seq[CudfAggregate] = aggregateFunction.updateAggregates
  override lazy val mergeAggregates: Seq[CudfAggregate] = aggregateFunction.mergeAggregates
  override val evaluateExpression: Expression = aggregateFunction.evaluateExpression

  override lazy val postUpdate: Seq[Expression] = aggregateFunction.postUpdate

  override lazy val preMerge: Seq[Expression] = aggregateFunction.preMerge
  override lazy val postMerge: Seq[Expression] = aggregateFunction.postMerge
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

trait CudfAggregate {
  // we use this to get the ordinal of the bound reference, s.t. we can ask cudf to perform
  // the aggregate on that column
  val reductionAggregate: cudf.ColumnVector => cudf.Scalar
  val groupByAggregate: GroupByAggregation
  def dataType: DataType
  val name: String
  override def toString: String = name

  // the purpose of this attribute is for catalyst expressions that need to
  // refer to the output of a cuDF aggregate (`CudfAggregate`) in `postUpdate` or `postMerge`.
  final lazy val attr: AttributeReference = AttributeReference(name, dataType)()
}

class CudfCount(override val dataType: DataType) extends CudfAggregate {
  override val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => cudf.Scalar.fromInt((col.getRowCount - col.getNullCount).toInt)
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.count(NullPolicy.EXCLUDE)
  override val name: String = "CudfCount"
}

class CudfSum(override val dataType: DataType) extends CudfAggregate with Arm {
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

  override val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum(rapidsSumType)

  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.sum()

  override val name: String = "CudfSum"
}

class CudfMax(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.max()
  override val name: String = "CudfMax"
}

/**
 * Check if there is a `true` value in a boolean column.
 * The CUDF any aggregation does not work for reductions or group by aggregations
 * so we use Max as a workaround for this.
 */
object CudfAny {
  def apply(): CudfAggregate = new CudfMax(BooleanType)
}

class CudfMin(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.min()
  override val name: String = "CudfMin"
}

/**
 * Check if all values in a boolean column are trues.
 * The CUDF all aggregation does not work for reductions or group by aggregations
 * so we use Min as a workaround for this.
 */
object CudfAll {
  def apply(): CudfAggregate = new CudfMin(BooleanType)
}

class CudfCollectList(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.reduce(ReductionAggregation.collectList(), DType.LIST)
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.collectList()
  override val name: String = "CudfCollectList"
}

class CudfMergeLists(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.reduce(ReductionAggregation.mergeLists(), DType.LIST)
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.mergeLists()
  override val name: String = "CudfMergeLists"
}

class CudfCollectSet(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => {
      val collectSet = ReductionAggregation.collectSet(
        NullPolicy.EXCLUDE, NullEquality.EQUAL, NaNEquality.UNEQUAL)
      col.reduce(collectSet, DType.LIST)
    }
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.collectSet(NullPolicy.EXCLUDE, NullEquality.EQUAL, NaNEquality.UNEQUAL)
  override val name: String = "CudfCollectSet"
}

class CudfMergeSets(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => {
      val mergeSets = ReductionAggregation.mergeSets(NullEquality.EQUAL, NaNEquality.UNEQUAL)
      col.reduce(mergeSets, DType.LIST)
    }
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.mergeSets(NullEquality.EQUAL, NaNEquality.UNEQUAL)
  override val name: String = "CudfMergeSets"
}

class CudfNthLikeAggregate(opName: String, override val dataType: DataType, offset: Int,
    includeNulls: NullPolicy) extends CudfAggregate {

  override val name = includeNulls match {
    case NullPolicy.INCLUDE => opName + "IncludeNulls"
    case NullPolicy.EXCLUDE => opName + "ExcludeNulls"
  }

  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.reduce(ReductionAggregation.nth(offset, includeNulls))

  override lazy val groupByAggregate: GroupByAggregation = {
    GroupByAggregation.nth(offset, includeNulls)
  }
}

object CudfNthLikeAggregate {
  def newFirstExcludeNulls(dataType: DataType): CudfAggregate =
    new CudfNthLikeAggregate("CudfFirst", dataType, 0, NullPolicy.EXCLUDE)

  def newFirstIncludeNulls(dataType: DataType): CudfAggregate =
    new CudfNthLikeAggregate("CudfFirst", dataType, 0, NullPolicy.INCLUDE)

  def newLastExcludeNulls(dataType: DataType): CudfAggregate =
    new CudfNthLikeAggregate("CudfLast", dataType, -1, NullPolicy.EXCLUDE)

  def newLastIncludeNulls(dataType: DataType): CudfAggregate =
    new CudfNthLikeAggregate("CudfLast", dataType, -1, NullPolicy.INCLUDE)
}

/**
 * This class is only used by the M2 class aggregates, do not confuse this with GpuAverage.
 * In the future, this aggregate class should be removed and the mean values should be
 * generated in the output of libcudf's M2 aggregate.
 */
class CudfMean(override val dataType: DataType) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar = _ =>
    throw new UnsupportedOperationException("CudfMean is not supported in reduction")

  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.mean()

  override val name: String = "CudfMeanForM2"
}

class CudfM2 extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar = _ =>
    throw new UnsupportedOperationException("CudfM2 aggregation is not supported in reduction")

  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.M2()

  override val name: String = "CudfM2"
  override def dataType: DataType = DoubleType
}

class CudfMergeM2 extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar = _ =>
    throw new UnsupportedOperationException("CudfMergeM2 aggregation is not supported in reduction")

  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.mergeM2()

  override val name: String = "CudfMergeM2"
  override val dataType: DataType =
    StructType(
      StructField("n", IntegerType, nullable = false) ::
        StructField("avg", DoubleType, nullable = true) ::
        StructField("m2", DoubleType, nullable = true) :: Nil)
}

object GpuMin{
  def apply(child: Expression): GpuMin = child.dataType match {
    case FloatType | DoubleType => GpuFloatMin(child)
    case _ => GpuBasicMin(child)
  }
}

abstract class GpuMin(child: Expression) extends GpuAggregateFunction
    with GpuBatchedRunningWindowWithFixer
    with GpuAggregateWindowFunction
    with GpuRunningWindowFunction
    with Serializable {
  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(null, child.dataType))
  override lazy val inputProjection: Seq[Expression] = Seq(child)
  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(new CudfMin(child.dataType))
  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(new CudfMin(child.dataType))

  private lazy val cudfMin = AttributeReference("min", child.dataType)()
  override lazy val evaluateExpression: Expression = cudfMin
  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfMin :: Nil

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

/** Min aggregation without `Nan` handling */
case class GpuBasicMin(child: Expression) extends GpuMin(child)

/** GpuMin for FloatType and DoubleType to handle `Nan`s.
 *
 * In Spark, `Nan` is the max float value, however in cuDF, the calculation
 * involving `Nan` is undefined.
 * We design a workaround method here to match the Spark's behaviour.
 * The high level idea is:
 *   if the column contains only `Nan`s or `null`s
 *   then
       if the column contains `Nan`
 *     then return `Nan`
 *     else return null
 *   else
 *     replace all `Nan`s with nulls;
 *     use cuDF kernel to find the min value
 */
case class GpuFloatMin(child: Expression) extends GpuMin(child)
  with GpuReplaceWindowFunction {

  override val dataType: DataType = child.dataType match {
    case FloatType | DoubleType => child.dataType
    case t => throw new IllegalStateException(s"child type $t is not FloatType or DoubleType")
  }

  protected val nan: Any = child.dataType match {
    case FloatType => Float.NaN
    case DoubleType => Double.NaN
    case t => throw new IllegalStateException(s"child type $t is not FloatType or DoubleType")
  }

  protected lazy val updateAllNansOrNulls = CudfAll()
  protected lazy val updateHasNan = CudfAny()
  protected lazy val updateMinVal = new CudfMin(dataType)

  protected lazy val mergeAllNansOrNulls = CudfAll()
  protected lazy val mergeHasNan = CudfAny()
  protected lazy val mergeMinVal = new CudfMin(dataType)

  // Project 3 columns:
  // 1. A boolean column indicating whether the values in `child` are `Nan`s or `null`s
  // 2. A boolean column indicating whether the values in `child` are `Nan`s
  // 3. Replace all `Nan`s in the `child` with `null`s
  override lazy val inputProjection: Seq[Expression] = Seq(
    GpuOr(GpuIsNan(child), GpuIsNull(child)),
    GpuIsNan(child),
    // We must eliminate all Nans before calling the cuDF min kernel.
    // As this expression is only used when `allNansOrNulls` = false,
    // and `Nan` is the max value in Spark, the elimination will
    // not affect the final result.
    GpuNansToNulls(child)
  )
  // 1. Check if all values in the `child` are `Nan`s or `null`s
  // 2. Check if `child` contains `Nan`
  // 3. Calculate the min value on `child` with all `Nan`s has been replaced.
  override lazy val updateAggregates: Seq[CudfAggregate] =
    Seq(updateAllNansOrNulls, updateHasNan, updateMinVal)

  // If the column only contains `Nan`s or `null`s
  // Then
  //   if the column contains `Nan`
  //   then return `Nan`
  //   else return `null`
  // Else return the min value
  override lazy val postUpdate: Seq[Expression] = Seq(
    GpuIf(
      updateAllNansOrNulls.attr,
      GpuIf(
        updateHasNan.attr, GpuLiteral(nan, dataType), GpuLiteral(null, dataType)
      ),
      updateMinVal.attr
    )
  )

  // Same logic as the `inputProjection` stage.
  override lazy val preMerge: Seq[Expression] = Seq (
    GpuOr(GpuIsNan(evaluateExpression), GpuIsNull(evaluateExpression)),
    GpuIsNan(evaluateExpression),
    GpuNansToNulls(evaluateExpression)
  )

  // Same logic as the `updateAggregates` stage.
  override lazy val mergeAggregates: Seq[CudfAggregate] =
    Seq(mergeAllNansOrNulls, mergeHasNan, mergeMinVal)

  // Same logic as the `postUpdate` stage.
  override lazy val postMerge: Seq[Expression] = Seq(
    GpuIf(
      mergeAllNansOrNulls.attr,
      GpuIf(
        mergeHasNan.attr, GpuLiteral(nan, dataType), GpuLiteral(null, dataType)
      ),
      mergeMinVal.attr
    )
  )

  // We should always override the windowing expression to handle `Nan`.
  override def shouldReplaceWindow(spec: GpuWindowSpecDefinition): Boolean = true

  override def windowReplacement(spec: GpuWindowSpecDefinition): Expression = {
    // The `GpuBasicMin` here has the same functionality as `CudfAll`,
    // as `true > false` in cuDF.
    val allNansOrNull = GpuWindowExpression(
      GpuBasicMin(GpuOr(GpuIsNan(child), GpuIsNull(child))), spec
    )
    val hasNan = GpuWindowExpression(GpuBasicMax(GpuIsNan(child)), spec)
    // We use `GpuBasicMin` but not `GpuMin` to avoid self recursion.
    val min = GpuWindowExpression(GpuBasicMin(GpuNansToNulls(child)), spec)
    GpuIf(
      allNansOrNull,
      GpuIf(hasNan, GpuLiteral(nan, dataType), GpuLiteral(null, dataType)),
      min
    )
  }
}

object GpuMax {
  def apply(child: Expression): GpuMax = {
    child.dataType match {
      case FloatType | DoubleType => GpuFloatMax(child)
      case _ => GpuBasicMax(child)
    }
  }
}

abstract class GpuMax(child: Expression) extends GpuAggregateFunction
    with GpuBatchedRunningWindowWithFixer
    with GpuAggregateWindowFunction
    with GpuRunningWindowFunction
    with Serializable {
  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(null, child.dataType))
  override lazy val inputProjection: Seq[Expression] = Seq(child)
  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(new CudfMax(dataType))
  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(new CudfMax(dataType))

  private lazy val cudfMax = AttributeReference("max", child.dataType)()
  override lazy val evaluateExpression: Expression = cudfMax
  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfMax :: Nil

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

/** Max aggregation without `Nan` handling */
case class GpuBasicMax(child: Expression) extends GpuMax(child)

/** Max aggregation for FloatType and DoubleType to handle `Nan`s.
 *
 * In Spark, `Nan` is the max float value, however in cuDF, the calculation
 * involving `Nan` is undefined.
 * We design a workaround method here to match the Spark's behaviour.
 * The high level idea is that, in the projection stage, we create another
 * column `isNan`. If any value in this column is true, return `Nan`,
 * Else, return what `GpuBasicMax` returns.
 */
case class GpuFloatMax(child: Expression) extends GpuMax(child)
    with GpuReplaceWindowFunction{

  override val dataType: DataType = child.dataType match {
    case FloatType | DoubleType => child.dataType
    case t => throw new IllegalStateException(s"child type $t is not FloatType or DoubleType")
  }
  protected val nan: Any = child.dataType match {
      case FloatType => Float.NaN
      case DoubleType => Double.NaN
      case t => throw new IllegalStateException(s"child type $t is not FloatType or DoubleType")
  }

  protected lazy val updateIsNan = CudfAny()
  protected lazy val updateMaxVal = new CudfMax(dataType)
  protected lazy val mergeIsNan = CudfAny()
  protected lazy val mergeMaxVal = new CudfMax(dataType)

  // Project 2 columns. The first one is the target column, second one is a
  // Boolean column indicating whether the values in the target column are` Nan`s.
  override lazy val inputProjection: Seq[Expression] = Seq(child, GpuIsNan(child))
  // Execute the `CudfMax` on the target column. At the same time,
  // execute the `CudfAny` on the `isNan` column.
  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(updateMaxVal, updateIsNan)
  // If there is `Nan` value in the target column, return `Nan`
  // else return what the `CudfMax` returns
  override lazy val postUpdate: Seq[Expression] =
    Seq(
      GpuIf(updateIsNan.attr, GpuLiteral(nan, dataType), updateMaxVal.attr)
    )

  // Same logic as the `inputProjection` stage.
  override lazy val preMerge: Seq[Expression] =
    Seq(evaluateExpression, GpuIsNan(evaluateExpression))
  // Same logic as the `updateAggregates` stage.
  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(mergeMaxVal,  mergeIsNan)
  // Same logic as the `postUpdate` stage.
  override lazy val postMerge: Seq[Expression] =
    Seq(
      GpuIf(mergeIsNan.attr, GpuLiteral(nan, dataType), mergeMaxVal.attr)
    )

  // We should always override the windowing expression to handle `Nan`.
  override def shouldReplaceWindow(spec: GpuWindowSpecDefinition): Boolean =  true

  override def windowReplacement(spec: GpuWindowSpecDefinition): Expression = {
    // The `GpuBasicMax` here has the same functionality as `CudfAny`,
    // as `true > false` in cuDF.
    val isNan = GpuWindowExpression(GpuBasicMax(GpuIsNan(child)), spec)
    // We use `GpuBasicMax` but not `GpuMax` to avoid self recursion.
    val max = GpuWindowExpression(GpuBasicMax(child), spec)
    GpuIf(isNan, GpuLiteral(nan, dataType), max)
  }
}

/**
 * Extracts a 32-bit chunk from a 128-bit value
 * @param data expression producing 128-bit values
 * @param chunkIdx index of chunk to extract (0-3)
 * @param replaceNullsWithZero whether to replace nulls with zero
 */
case class GpuExtractChunk32(
    data: Expression,
    chunkIdx: Int,
    replaceNullsWithZero: Boolean) extends GpuExpression with ShimExpression {
  override def nullable: Boolean = true

  override def dataType: DataType = if (chunkIdx < 3) GpuUnsignedIntegerType else IntegerType

  override def sql: String = data.sql

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuProjectExec.projectSingle(batch, data)) { dataCol =>
      val dtype = if (chunkIdx < 3) DType.UINT32 else DType.INT32
      val chunkCol = Aggregation128Utils.extractInt32Chunk(dataCol.getBase, dtype, chunkIdx)
      val replacedCol = if (replaceNullsWithZero) {
        withResource(chunkCol) { chunkCol =>
          val zero = dtype match {
            case DType.INT32 => Scalar.fromInt(0)
            case DType.UINT32 => Scalar.fromUnsignedInt(0)
          }
          withResource(zero) { zero =>
            chunkCol.replaceNulls(zero)
          }
        }
      } else {
        chunkCol
      }
      GpuColumnVector.from(replacedCol, dataType)
    }
  }

  override def children: Seq[Expression] = Seq(data)
}

/**
 * Reassembles a 128-bit value from four separate 64-bit sum results
 * @param chunkAttrs attributes for the four 64-bit sum chunks ordered from least significant to
 *                   most significant
 * @param dataType   output type of the reconstructed 128-bit value
 * @param nullOnOverflow whether to produce null on overflows
 */
case class GpuAssembleSumChunks(
    chunkAttrs: Seq[AttributeReference],
    dataType: DecimalType,
    nullOnOverflow: Boolean) extends GpuExpression with ShimExpression {

  override def nullable: Boolean = true

  override def columnarEval(batch: ColumnarBatch): Any = {
    val cudfType = DecimalUtil.createCudfDecimal(dataType)
    val assembledTable = withResource(GpuProjectExec.project(batch, chunkAttrs)) { dataCol =>
      withResource(GpuColumnVector.from(dataCol)) { chunkTable =>
        Aggregation128Utils.combineInt64SumChunks(chunkTable, cudfType)
      }
    }
    withResource(assembledTable) { assembledTable =>
      assert(assembledTable.getNumberOfColumns == 2)
      val hasOverflowed = assembledTable.getColumn(0)
      val decimalData = assembledTable.getColumn(1)
      assert(hasOverflowed.getType == DType.BOOL8)
      assert(decimalData.getType.getTypeId == DType.DTypeEnum.DECIMAL128)
      withResource(Scalar.fromNull(cudfType)) { nullScalar =>
        GpuColumnVector.from(hasOverflowed.ifElse(nullScalar, decimalData), dataType)
      }
    }
  }

  override def children: Seq[Expression] = chunkAttrs
}


/**
 * All decimal processing in Spark has overflow detection as a part of it. Either it replaces
 * the value with a null in non-ANSI mode, or it throws an exception in ANSI mode. Spark will also
 * do the processing for larger values as `Decimal` values which are based on `BigDecimal` and have
 * unbounded precision. So in most cases it is impossible to overflow/underflow so much that an
 * incorrect value is returned. Spark will just use more and more memory to hold the value and
 * then check for overflow at some point when the result needs to be turned back into a 128-bit
 * value.
 *
 * We cannot do the same thing. Instead we take three strategies to detect overflow.
 *
 * 1. For decimal values with a precision of 8 or under we follow Spark and do the SUM
 *    on the unscaled value as a long, and then bit-cast the result back to a Decimal value.
 *    this means that we can SUM `174,467,442,481` maximum or minimum decimal values with a
 *    precision of 8 before overflow can no longer be detected. It is much higher for decimal
 *    values with a smaller precision.
 * 2. For decimal values with a precision from 9 to 20 inclusive we sum them as 128-bit values.
 *    this is very similar to what we do in the first strategy. The main differences are that we
 *    use a 128-bit value when doing the sum, and we check for overflow after processing each batch.
 *    In the case of group-by and reduction that happens after the update stage and also after each
 *    merge stage. This gives us enough room that we can always detect overflow when summing a
 *    single batch. Even on a merge where we could be doing the aggregation on a batch that has
 *    all max output values in it.
 * 3. For values from 21 to 28 inclusive we have enough room to not check for overflow on teh update
 *    aggregation, but for the merge aggregation we need to do some extra checks. This is done by
 *    taking the digits above 28 and sum them separately. We then check to see if they would have
 *    overflowed the original limits. This lets us detect overflow in cases where the original
 *    value would have wrapped around. The reason this works is because we have a hard limit on the
 *    maximum number of values in a single batch being processed. `Int.MaxValue`, or about 2.2
 *    billion values. So we use a precision on the higher values that is large enough to handle
 *    2.2 billion values and still detect overflow. This equates to a precision of about 10 more
 *    than is needed to hold the higher digits. This effectively gives us unlimited overflow
 *    detection.
 * 4. For anything larger than precision 28 we do the same overflow detection for strategy 3, but
 *    also do it on the update aggregation. This lets us fully detect overflows in any stage of
 *    an aggregation.
 *
 * Note that for Window operations either there is no merge stage or it only has a single value
 * being merged into a batch instead of an entire batch being merged together. This lets us handle
 * the overflow detection with what is built into GpuAdd.
 */
object GpuDecimalSumOverflow {
  /**
   * The increase in precision for the output of a SUM from the input. This is hard coded by
   * Spark so we just have it here. This means that for most types without being limited to
   * a precision of 38 you get 10-billion+ values before an overflow would even be possible.
   */
  val sumPrecisionIncrease: Int = 10

  /**
   * Generally we want a guarantee that is at least 10x larger than the original overflow.
   */
  val extraGuaranteePrecision: Int = 1

  /**
   * The precision above which we need extra overflow checks while doing an update. This is because
   * anything above this precision could in theory overflow beyond detection within a single input
   * batch.
   */
  val updateCutoffPrecision: Int = 28
}

/**
 * This is equivalent to what Spark does after a sum to check for overflow
 * `
 * If(isEmpty, Literal.create(null, resultType),
 *    CheckOverflowInSum(sum, d, !SQLConf.get.ansiEnabled))`
 *
 * But we are renaming it to avoid confusion with the overflow detection we do as a part of sum
 * itself that takes the place of the overflow checking that happens with add.
 */
case class GpuCheckOverflowAfterSum(
    data: Expression,
    isEmpty: Expression,
    dataType: DecimalType,
    nullOnOverflow: Boolean) extends GpuExpression with ShimExpression {

  override def nullable: Boolean = true

  override def toString: String = s"CheckOverflowInSum($data, $isEmpty, $dataType, $nullOnOverflow)"

  override def sql: String = data.sql

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuProjectExec.projectSingle(batch, data)) { dataCol =>
      val dataBase = dataCol.getBase
      withResource(GpuProjectExec.projectSingle(batch, isEmpty)) { isEmptyCol =>
        val isEmptyBase = isEmptyCol.getBase
        if (!nullOnOverflow) {
          // ANSI mode
          val problem = withResource(dataBase.isNull) { isNull =>
            withResource(isEmptyBase.not()) { notEmpty =>
              isNull.and(notEmpty)
            }
          }
          withResource(problem) { problem =>
            withResource(problem.any()) { anyProblem =>
              if (anyProblem.isValid && anyProblem.getBoolean) {
                throw new ArithmeticException("Overflow in sum of decimals.")
              }
            }
          }
          // No problems fall through...
        }
        withResource(GpuScalar.from(null, dataType)) { nullScale =>
          GpuColumnVector.from(isEmptyBase.ifElse(nullScale, dataBase), dataType)
        }
      }
    }
  }

  override def children: Seq[Expression] = Seq(data, isEmpty)
}

/**
 * This extracts the highest digits from a Decimal value as a part of doing a SUM.
 */
case class GpuDecimalSumHighDigits(
    input: Expression,
    originalInputType: DecimalType) extends GpuExpression with ShimExpression {

  override def nullable: Boolean = input.nullable

  override def toString: String = s"GpuDecimalSumHighDigits($input)"

  override def sql: String = input.sql

  override val dataType: DecimalType = DecimalType(originalInputType.precision +
      GpuDecimalSumOverflow.sumPrecisionIncrease + GpuDecimalSumOverflow.extraGuaranteePrecision -
      GpuDecimalSumOverflow.updateCutoffPrecision, 0)
  // Marking these as lazy because they are not serializable
  private lazy val outputDType = GpuColumnVector.getNonNestedRapidsType(dataType)
  private lazy val intermediateDType =
    DType.create(DType.DTypeEnum.DECIMAL128, outputDType.getScale)

  private lazy val divisionFactor: Decimal =
    Decimal(math.pow(10, GpuDecimalSumOverflow.updateCutoffPrecision))
  private val divisionType = DecimalType(38, 0)

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuProjectExec.projectSingle(batch, input)) { inputCol =>
      val inputBase = inputCol.getBase
      // We don't have direct access to 128 bit ints so we use a decimal with a scale of 0
      // as a stand in.
      val bitCastInputType = DType.create(DType.DTypeEnum.DECIMAL128, 0)
      val divided = withResource(inputBase.bitCastTo(bitCastInputType)) { bitCastInput =>
        withResource(GpuScalar.from(divisionFactor, divisionType)) { divisor =>
          bitCastInput.div(divisor, intermediateDType)
        }
      }
      val ret = withResource(divided) { divided =>
        if (divided.getType.equals(outputDType)) {
          divided.incRefCount()
        } else {
          divided.castTo(outputDType)
        }
      }
      GpuColumnVector.from(ret, dataType)
    }
  }

  override def children: Seq[Expression] = Seq(input)
}

object GpuSum {
  def apply(
      child: Expression,
      resultType: DataType,
      failOnErrorOverride: Boolean = SQLConf.get.ansiEnabled,
      forceWindowSumToNotBeReplaced: Boolean = false): GpuSum = {
    resultType match {
      case dt: DecimalType =>
        if (dt.precision > Decimal.MAX_LONG_DIGITS) {
          GpuDecimal128Sum(child, dt, failOnErrorOverride, forceWindowSumToNotBeReplaced)
        } else {
          GpuBasicDecimalSum(child, dt, failOnErrorOverride)
        }
      case _ => GpuBasicSum(child, resultType, failOnErrorOverride)
    }
  }
}

abstract class GpuSum(
    child: Expression,
    resultType: DataType,
    failOnErrorOverride: Boolean)
    extends GpuAggregateFunction
        with ImplicitCastInputTypes
        with GpuBatchedRunningWindowWithFixer
        with GpuAggregateWindowFunction
        with GpuRunningWindowFunction
        with Serializable {
  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(null, resultType))

  // we need to cast to `resultType` here, since Spark is not widening types
  // as done before Spark 3.2.0. See CudfSum for more info.
  override lazy val inputProjection: Seq[Expression] = Seq(GpuCast(child, resultType))

  protected lazy val updateSum: CudfAggregate = new CudfSum(resultType)

  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(updateSum)

  // output of GpuSum
  protected lazy val sum: AttributeReference = AttributeReference("sum", resultType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = sum :: Nil

  protected lazy val mergeSum: CudfAggregate = new CudfSum(resultType)

  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(mergeSum)

  override lazy val evaluateExpression: Expression = sum

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

  override def windowOutput(result: ColumnVector): ColumnVector = result.incRefCount()

  // RUNNING WINDOW
  override def newFixer(): BatchedRunningWindowFixer =
    new SumBinaryFixer(resultType, failOnErrorOverride)

  override def groupByScanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    windowInputProjection

  override def groupByScanAggregation(
      isRunningBatched: Boolean): Seq[AggAndReplace[GroupByScanAggregation]] =
    Seq(AggAndReplace(GroupByScanAggregation.sum(), Some(ReplacePolicy.PRECEDING)))

  override def scanInputProjection(isRunningBatched: Boolean): Seq[Expression] =
    windowInputProjection

  override def scanAggregation(isRunningBatched: Boolean): Seq[AggAndReplace[ScanAggregation]] =
    Seq(AggAndReplace(ScanAggregation.sum(), Some(ReplacePolicy.PRECEDING)))

  override def scanCombine(isRunningBatched: Boolean, cols: Seq[ColumnVector]): ColumnVector = {
    cols.head.incRefCount()
  }
}

/** Sum aggregation for non-decimal types */
case class GpuBasicSum(
    child: Expression,
    resultType: DataType,
    failOnErrorOverride: Boolean)
    extends GpuSum(child, resultType, failOnErrorOverride)

abstract class GpuDecimalSum(
    child: Expression,
    dt: DecimalType,
    failOnErrorOverride: Boolean)
    extends GpuSum(child, dt, failOnErrorOverride) {
  private lazy val zeroDec = GpuLiteral(Decimal(0, dt.precision, dt.scale), dt)

  override lazy val initialValues: Seq[GpuLiteral] = {
    Seq(zeroDec, GpuLiteral(true, BooleanType))
  }

  // we need to cast to `resultType` here, since Spark is not widening types
  // as done before Spark 3.2.0. See CudfSum for more info.
  override lazy val inputProjection: Seq[Expression] = {
    // Spark tracks null columns through a second column isEmpty for decimal. So null values
    // are replaced with 0, and a separate boolean column for isNull is added
    Seq(GpuIf(GpuIsNull(child), zeroDec, GpuCast(child, dt)), GpuIsNull(child))
  }

  protected lazy val updateIsEmpty: CudfAggregate = new CudfMin(BooleanType)

  override lazy val updateAggregates: Seq[CudfAggregate] = {
    Seq(updateSum, updateIsEmpty)
  }

  // Used for Decimal overflow detection
  protected lazy val isEmpty: AttributeReference = AttributeReference("isEmpty", BooleanType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = {
    Seq(sum, isEmpty)
  }

  override lazy val preMerge: Seq[Expression] = {
    Seq(sum, isEmpty, GpuIsNull(sum))
  }

  protected lazy val mergeIsEmpty: CudfAggregate = new CudfMin(BooleanType)
  protected lazy val mergeIsOverflow: CudfAggregate = new CudfMax(BooleanType)

  // To be able to do decimal overflow detection, we need a CudfSum that does **not** ignore nulls.
  // Cudf does not have such an aggregation, so for merge we have to work around that similar to
  // what happens with isEmpty
  override lazy val mergeAggregates: Seq[CudfAggregate] = {
    Seq(mergeSum, mergeIsEmpty, mergeIsOverflow)
  }

  override lazy val postMerge: Seq[Expression] = {
    Seq(
      GpuIf(mergeIsOverflow.attr, GpuLiteral.create(null, dt), mergeSum.attr),
      mergeIsEmpty.attr)
  }

  override lazy val evaluateExpression: Expression = {
    GpuCheckOverflowAfterSum(sum, isEmpty, dt, !failOnErrorOverride)
  }

  override def windowOutput(result: ColumnVector): ColumnVector = {
    // Check for overflow
    GpuCast.checkNFixDecimalBounds(result, dt, failOnErrorOverride)
  }

  override def scanCombine(isRunningBatched: Boolean, cols: Seq[ColumnVector]): ColumnVector = {
    // We do bounds checks if we are not going to use the running fixer and it is decimal
    // The fixer will do the bounds checks for us on the actual final values.
    if (!isRunningBatched) {
      // Check for overflow
      GpuCast.checkNFixDecimalBounds(cols.head, dt, failOnErrorOverride)
    } else {
      super.scanCombine(isRunningBatched, cols)
    }
  }
}

/** Sum aggregations for decimals up to and including DECIMAL64 */
case class GpuBasicDecimalSum(
    child: Expression,
    dt: DecimalType,
    failOnErrorOverride: Boolean)
    extends GpuDecimalSum(child, dt, failOnErrorOverride)

/**
 * Sum aggregations for DECIMAL128.
 *
 * The sum aggregation is performed by splitting the original 128-bit values into 32-bit "chunks"
 * and summing those. The chunking accomplishes two things. First, it helps avoid cudf resorting
 * to a much slower aggregation since currently DECIMAL128 sums are only implemented for
 * sort-based aggregations. Second, chunking allows detection of overflows.
 *
 * The chunked approach to sum aggregation works as follows. The 128-bit value is split into its
 * four 32-bit chunks, with the most significant chunk being an INT32 and the remaining three
 * chunks being UINT32. When these are sum aggregated, cudf will implicitly upscale the accumulated
 * result to a 64-bit value. Since cudf only allows up to 2**31 rows to be aggregated at a time,
 * the "extra" upper 32-bits of the upscaled 64-bit accumulation values will be enough to hold the
 * worst-case "carry" bits from summing each 32-bit chunk.
 *
 * After the cudf aggregation has completed, the four 64-bit chunks are reassembled into a 128-bit
 * value. The lowest 32-bits of the least significant 64-bit chunk are used directly as the lowest
 * 32-bits of the final value, and the remaining 32-bits are added to the next most significant
 * 64-bit chunk. The lowest 32-bits of that chunk then become the next 32-bits of the 128-bit value
 * and the remaining 32-bits are added to the next 64-bit chunk, and so on. Finally after the
 * 128-bit value is constructed, the remaining "carry" bits of the most significant chunk after
 * reconstruction are checked against the sign bit of the 128-bit result to see if there was an
 * overflow.
 */
case class GpuDecimal128Sum(
    child: Expression,
    dt: DecimalType,
    failOnErrorOverride: Boolean,
    forceWindowSumToNotBeReplaced: Boolean)
    extends GpuDecimalSum(child, dt, failOnErrorOverride) with GpuReplaceWindowFunction {
  private lazy val childIsDecimal: Boolean =
    child.dataType.isInstanceOf[DecimalType]

  private lazy val childDecimalType: DecimalType =
    child.dataType.asInstanceOf[DecimalType]

  private lazy val needsDec128UpdateOverflowChecks: Boolean =
    childIsDecimal &&
        childDecimalType.precision > GpuDecimalSumOverflow.updateCutoffPrecision

  // For some operations we need to sum the higher digits in addition to the regular value so
  // we can detect overflow. This is the type of the higher digits SUM value.
  private lazy val higherDigitsCheckType: DecimalType = {
    DecimalType(dt.precision - GpuDecimalSumOverflow.updateCutoffPrecision, 0)
  }

  override lazy val inputProjection: Seq[Expression] = {
    val chunks = (0 until 4).map {
      GpuExtractChunk32(GpuCast(child, dt), _, replaceNullsWithZero = true)
    }
    // Spark tracks null columns through a second column isEmpty for decimal. So null values
    // are replaced with 0, and a separate boolean column for isNull is added
    chunks :+ GpuIsNull(child)
  }

  private lazy val updateSumChunks = (0 until 4).map(_ => new CudfSum(LongType))

  override lazy val updateAggregates: Seq[CudfAggregate] = updateSumChunks :+ updateIsEmpty

  override lazy val postUpdate: Seq[Expression] = {
    Seq(
      GpuAssembleSumChunks(updateSumChunks.map(_.attr), dt, !failOnErrorOverride),
      updateIsEmpty.attr)
  }

  override lazy val preMerge: Seq[Expression] = {
    val chunks = (0 until 4).map {
      GpuExtractChunk32(sum, _, replaceNullsWithZero = false)
    }
    // Spark tracks null columns through a second column isEmpty for decimal. So null values
    // are replaced with 0, and a separate boolean column for isNull is added
    chunks ++ Seq(isEmpty, GpuIsNull(sum))
  }

  private lazy val mergeSumChunks = (0 until 4).map(_ => new CudfSum(LongType))

  // To be able to do decimal overflow detection, we need a CudfSum that does **not** ignore nulls.
  // Cudf does not have such an aggregation, so for merge we have to work around that similar to
  // what happens with isEmpty
  override lazy val mergeAggregates: Seq[CudfAggregate] = {
    mergeSumChunks ++ Seq(mergeIsEmpty, mergeIsOverflow)
  }

  override lazy val postMerge: Seq[Expression] = {
    val assembleExpr = GpuAssembleSumChunks(mergeSumChunks.map(_.attr), dt, !failOnErrorOverride)
    Seq(
      GpuIf(mergeIsOverflow.attr, GpuLiteral.create(null, dt), assembleExpr),
      mergeIsEmpty.attr)
  }

  // Replacement Window Function
  override def shouldReplaceWindow(spec: GpuWindowSpecDefinition): Boolean = {
    // We only will replace this if we think an update will fail. In the cases where we can
    // handle a window function larger than a single batch, we already have merge overflow
    // detection enabled.
    !forceWindowSumToNotBeReplaced && needsDec128UpdateOverflowChecks
  }

  override def windowReplacement(spec: GpuWindowSpecDefinition): Expression = {
    // We need extra overflow checks for some larger decimal type. To do these checks we
    // extract the higher digits and SUM them separately to see if they would overflow.
    // If they do we know that the regular SUM also overflowed. If not we know that we can rely on
    // the existing overflow code to detect it.
    val regularSum = GpuWindowExpression(
      GpuSum(child, dt, failOnErrorOverride = failOnErrorOverride,
        forceWindowSumToNotBeReplaced = true),
      spec)
    val highOrderDigitsSum = GpuWindowExpression(
      GpuSum(
        GpuDecimalSumHighDigits(GpuCast(child, dt), child.dataType.asInstanceOf[DecimalType]),
        higherDigitsCheckType,
        failOnErrorOverride = failOnErrorOverride),
      spec)
    GpuIf(GpuIsNull(highOrderDigitsSum), GpuLiteral(null, dt), regularSum)
  }
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

  private val valueDataType = valueColumn.dataType

  override lazy val initialValues: Seq[GpuLiteral] =
    Seq.fill(pivotColumnValues.length)(GpuLiteral(null, valueDataType))

  override lazy val inputProjection: Seq[Expression] = {
    val expr = pivotColumnValues.map(pivotColumnValue => {
      if (pivotColumnValue == null) {
        GpuIf(GpuIsNull(pivotColumn), valueColumn, GpuLiteral(null, valueDataType))
      } else {
        // Need to use an equal to comparison that is != when both values are NaN to be consistent
        // with Spark's inconsistency with regards to PivotFirst
        GpuIf(GpuEqualToNoNans(pivotColumn, GpuLiteral(pivotColumnValue, pivotColumn.dataType)),
          valueColumn, GpuLiteral(null, valueDataType))
      }
    })
    expr
  }

  private lazy val pivotColAttr = pivotColumnValues.map(pivotColumnValue => {
    // If `pivotColumnValue` is null, then create an AttributeReference for null column.
    if (pivotColumnValue == null) {
      AttributeReference(GpuLiteral(null, valueDataType).toString, valueDataType)()
    } else {
      AttributeReference(pivotColumnValue.toString, valueDataType)()
    }
  })

  override lazy val updateAggregates: Seq[CudfAggregate] =
    pivotColAttr.map(c => CudfNthLikeAggregate.newLastExcludeNulls(c.dataType))

  override lazy val mergeAggregates: Seq[CudfAggregate] =
    pivotColAttr.map(c => CudfNthLikeAggregate.newLastExcludeNulls(c.dataType))

  override lazy val evaluateExpression: Expression =
    GpuCreateArray(pivotColAttr, false)

  override lazy val aggBufferAttributes: Seq[AttributeReference] = pivotColAttr

  override val dataType: DataType = valueDataType
  override val nullable: Boolean = false
  override def children: Seq[Expression] = pivotColumn :: valueColumn :: Nil
}

case class GpuCount(children: Seq[Expression],
    failOnError: Boolean = SQLConf.get.ansiEnabled)
    extends GpuAggregateFunction
    with GpuBatchedRunningWindowWithFixer
    with GpuUnboundToUnboundWindowWithFixer
    with GpuAggregateWindowFunction
    with GpuRunningWindowFunction {
  override lazy val initialValues: Seq[GpuLiteral] = Seq(GpuLiteral(0L, LongType))

  // inputAttr
  override lazy val inputProjection: Seq[Expression] = Seq(children.head)

  private lazy val cudfCountUpdate = new CudfCount(IntegerType)

  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(cudfCountUpdate)

  // Integer->Long before we are done with the update aggregate
  override lazy val postUpdate: Seq[Expression] = Seq(GpuCast(cudfCountUpdate.attr, dataType))

  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(new CudfSum(dataType))

  // This is the spark API
  private lazy val count = AttributeReference("count", dataType)()
  override lazy val evaluateExpression: Expression = count
  override lazy val aggBufferAttributes: Seq[AttributeReference] = count :: Nil

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

  override def windowOutput(result: ColumnVector): ColumnVector = {
    // The output needs to be a long
    result.castTo(DType.INT64)
  }

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

  override def scanCombine(isRunningBatched: Boolean, cols: Seq[ColumnVector]): ColumnVector =
    cols.head.castTo(DType.INT64)

  override def newUnboundedToUnboundedFixer: BatchedUnboundedToUnboundedWindowFixer =
    new CountUnboundedToUnboundedFixer(failOnError)
}

object GpuAverage {
  def apply(child: Expression): GpuAverage = {
    child.dataType match {
      case DecimalType.Fixed(p, s) =>
        val sumDataType = DecimalType.bounded(p + 10, s)
        if (sumDataType.precision > Decimal.MAX_LONG_DIGITS) {
          GpuDecimal128Average(child, sumDataType)
        } else {
          GpuBasicDecimalAverage(child, sumDataType)
        }
      case _ =>
        GpuBasicAverage(child, DoubleType)
    }
  }
}

abstract class GpuAverage(child: Expression, sumDataType: DataType) extends GpuAggregateFunction
    with GpuReplaceWindowFunction with Serializable {

  override lazy val inputProjection: Seq[Expression] = {
    // Replace the nulls with 0s in the SUM column because Spark does not protect against
    // nulls in the merge phase. It does this to be able to detect overflow errors in
    // decimal aggregations.  The null gets inserted back in with evaluateExpression where
    // a divide by 0 gets replaced with a null.
    val castedForSum = GpuCoalesce(Seq(
      GpuCast(child, sumDataType),
      GpuLiteral.default(sumDataType)))
    val forCount = GpuCast(GpuIsNotNull(child), LongType)
    Seq(castedForSum, forCount)
  }

  override def filteredInputProjection(filter: Expression): Seq[Expression] = {
    inputProjection.map(e => GpuIf(filter, e, GpuLiteral.default(e.dataType)))
  }

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    GpuLiteral.default(sumDataType),
    GpuLiteral(0L, LongType))

  protected lazy val updateSum = new CudfSum(sumDataType)
  protected lazy val updateCount = new CudfSum(LongType)

  // The count input projection will need to be collected as a sum (of counts) instead of
  // counts (of counts) as the GpuIsNotNull o/p is casted to count=0 for null and 1 otherwise, and
  // the total count can be correctly evaluated only by summing them. eg. avg(col(null, 27))
  // should be 27, with count column projection as (0, 1) and total count for dividing the
  // average = (0 + 1) and not 2 which is the rowcount of the projected column.
  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(updateSum, updateCount)

  protected lazy val sum: AttributeReference = AttributeReference("sum", sumDataType)()
  protected lazy val count: AttributeReference = AttributeReference("count", LongType)()
  override lazy val aggBufferAttributes: Seq[AttributeReference] = sum :: count :: Nil

  protected lazy val mergeSum = new CudfSum(sumDataType)
  protected lazy val mergeCount = new CudfSum(LongType)

  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(mergeSum, mergeCount)

  // NOTE: this sets `failOnErrorOverride=false` in `GpuDivide` to force it not to throw
  // divide-by-zero exceptions, even when ansi mode is enabled in Spark.
  // This is to conform with Spark's behavior in the Average aggregate function.
  override lazy val evaluateExpression: Expression =
      GpuDivide(sum, GpuCast(count, DoubleType), failOnErrorOverride = false)

  // Window
  // Replace average with SUM/COUNT. This lets us run average in running window mode without
  // recreating everything that would have to go into doing the SUM and the COUNT here.
  override def windowReplacement(spec: GpuWindowSpecDefinition): Expression = {
    val count = GpuWindowExpression(GpuCount(Seq(child)), spec)
    val sum = GpuWindowExpression(
      GpuSum(GpuCast(child, dataType), dataType, failOnErrorOverride = false), spec)
    GpuDivide(sum, GpuCast(count, dataType), failOnErrorOverride = false)
  }

  // Copied from Average
  override def prettyName: String = "avg"
  override def children: Seq[Expression] = child :: Nil

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function gpu average")

  override def nullable: Boolean = true

  override val dataType: DataType = DoubleType
}

case class GpuBasicAverage(child: Expression, dt: DataType) extends GpuAverage(child, dt)

abstract class GpuDecimalAverage(child: Expression, sumDataType: DecimalType)
    extends GpuAverage(child, sumDataType) {
  override lazy val postUpdate: Seq[Expression] =
      Seq(GpuCheckOverflow(updateSum.attr, sumDataType, nullOnOverflow = true), updateCount.attr)

  // To be able to do decimal overflow detection, we need a CudfSum that does **not** ignore nulls.
  // Cudf does not have such an aggregation, so for merge we have to work around that with an extra
  // isOverflow column.  We only do this for Decimal because that is the only one that can have a
  // null inserted as a part of overflow checks. Spark does this for all overflow columns.
  override lazy val preMerge: Seq[Expression] = Seq(sum, count, GpuIsNull(sum))

  protected lazy val mergeIsOverflow = new CudfMax(BooleanType)

  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(mergeSum, mergeCount, mergeIsOverflow)

  override lazy val postMerge: Seq[Expression] = Seq(
    GpuCheckOverflow(
      GpuIf(mergeIsOverflow.attr, GpuLiteral.create(null, sumDataType), mergeSum.attr),
          sumDataType, nullOnOverflow = true),
    mergeCount.attr)

  // This is here to be bug for bug compatible with Spark. They round in the divide and then cast
  // the result to the final value. This loses some data in many cases and we need to be able to
  // match that. This bug appears to have been fixed in Spark 3.4.0.
  lazy val intermediateSparkDivideType = GpuDecimalDivide.calcOrigSparkOutputType(sumDataType,
    DecimalType.LongDecimal)

  // NOTE: this sets `failOnErrorOverride=false` in `GpuDivide` to force it not to throw
  // divide-by-zero exceptions, even when ansi mode is enabled in Spark.
  // This is to conform with Spark's behavior in the Average aggregate function.
  override lazy val evaluateExpression: Expression = {
    GpuCast(
      GpuDecimalDivide(sum, count, intermediateSparkDivideType, failOnError = false),
      dataType)
  }

  // Window
  // Replace average with SUM/COUNT. This lets us run average in running window mode without
  // recreating everything that would have to go into doing the SUM and the COUNT here.
  override def windowReplacement(spec: GpuWindowSpecDefinition): Expression = {
    val count = GpuWindowExpression(GpuCount(Seq(child)), spec)
    val sum = GpuWindowExpression(GpuSum(child, sumDataType, failOnErrorOverride = false), spec)
    GpuCast(
      GpuDecimalDivide(sum, count, intermediateSparkDivideType, failOnError = false),
      dataType)
  }

  override val dataType: DecimalType = child.dataType match {
    case DecimalType.Fixed(p, s) => DecimalType.bounded(p + 4, s + 4)
    case t => throw new IllegalStateException(s"child type $t is not DecimalType")
  }
}

case class GpuBasicDecimalAverage(child: Expression, dt: DecimalType)
    extends GpuDecimalAverage(child, dt)

/**
 * Average aggregations for DECIMAL128.
 *
 * To avoid the significantly slower sort-based aggregations in cudf for DECIMAL128 columns,
 * the incoming DECIMAL128 values are split into four 32-bit chunks which are summed separately
 * into 64-bit intermediate results and then recombined into a 128-bit result with overflow
 * checking. See GpuDecimal128Sum for more details.
 */
case class GpuDecimal128Average(child: Expression, dt: DecimalType)
    extends GpuDecimalAverage(child, dt) {
  override lazy val inputProjection: Seq[Expression] = {
    // Replace the nulls with 0s in the SUM column because Spark does not protect against
    // nulls in the merge phase. It does this to be able to detect overflow errors in
    // decimal aggregations.  The null gets inserted back in with evaluateExpression where
    // a divide by 0 gets replaced with a null.
    val chunks = (0 until 4).map { chunkIdx =>
      val extract = GpuExtractChunk32(GpuCast(child, dt), chunkIdx, replaceNullsWithZero = false)
      GpuCoalesce(Seq(extract, GpuLiteral.default(extract.dataType)))
    }
    val forCount = GpuCast(GpuIsNotNull(child), LongType)
    chunks :+ forCount
  }

  private lazy val updateSumChunks = (0 until 4).map(_ => new CudfSum(LongType))

  override lazy val updateAggregates: Seq[CudfAggregate] = updateSumChunks :+ updateCount

  override lazy val postUpdate: Seq[Expression] = {
    val assembleExpr = GpuAssembleSumChunks(updateSumChunks.map(_.attr), dt, nullOnOverflow = true)
    Seq(GpuCheckOverflow(assembleExpr, dt, nullOnOverflow = true), updateCount.attr)
  }

  // To be able to do decimal overflow detection, we need a CudfSum that does **not** ignore nulls.
  // Cudf does not have such an aggregation, so for merge we have to work around that with an extra
  // isOverflow column.  We only do this for Decimal because that is the only one that can have a
  // null inserted as a part of overflow checks. Spark does this for all overflow columns.
  override lazy val preMerge: Seq[Expression] = {
    val chunks = (0 until 4).map(GpuExtractChunk32(sum, _, replaceNullsWithZero = false))
    chunks ++ Seq(count, GpuIsNull(sum))
  }

  private lazy val mergeSumChunks = (0 until 4).map(_ => new CudfSum(LongType))

  override lazy val mergeAggregates: Seq[CudfAggregate] =
    mergeSumChunks ++ Seq(mergeCount, mergeIsOverflow)

  override lazy val postMerge: Seq[Expression] = {
    val assembleExpr = GpuAssembleSumChunks(mergeSumChunks.map(_.attr), dt, nullOnOverflow = true)
    Seq(
      GpuCheckOverflow(GpuIf(mergeIsOverflow.attr,
        GpuLiteral.create(null, dt),
        assembleExpr), dt, nullOnOverflow = true),
      mergeCount.attr)
  }
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
  extends GpuAggregateFunction
  with GpuAggregateWindowFunction
  with GpuDeterministicFirstLastCollectShim
  with ImplicitCastInputTypes
  with Serializable {

  private lazy val cudfFirst = AttributeReference("first", child.dataType)()
  private lazy val valueSet = AttributeReference("valueSet", BooleanType)()

  override lazy val inputProjection: Seq[Expression] =
    Seq(child, GpuLiteral(ignoreNulls, BooleanType))

  private lazy val commonExpressions: Seq[CudfAggregate] = if (ignoreNulls) {
    Seq(CudfNthLikeAggregate.newFirstExcludeNulls(cudfFirst.dataType),
      CudfNthLikeAggregate.newFirstExcludeNulls(valueSet.dataType))
  } else {
    Seq(CudfNthLikeAggregate.newFirstIncludeNulls(cudfFirst.dataType),
      CudfNthLikeAggregate.newFirstIncludeNulls(valueSet.dataType))
  }

  // Expected input data type.
  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    GpuLiteral(null, child.dataType),
    GpuLiteral(false, BooleanType))
  override lazy val updateAggregates: Seq[CudfAggregate] = commonExpressions
  override lazy val mergeAggregates: Seq[CudfAggregate] = commonExpressions
  override lazy val evaluateExpression: Expression = cudfFirst
  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfFirst :: valueSet :: Nil

  // Copied from First
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def toString: String = s"gpufirst($child)${if (ignoreNulls) " ignore nulls"}"

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }

  // GENERAL WINDOW FUNCTION
  override lazy val windowInputProjection: Seq[Expression] = inputProjection
  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.nth(0, if (ignoreNulls) NullPolicy.EXCLUDE else NullPolicy.INCLUDE)
        .onColumn(inputs.head._2)

}

case class GpuLast(child: Expression, ignoreNulls: Boolean)
  extends GpuAggregateFunction
  with GpuAggregateWindowFunction
  with GpuDeterministicFirstLastCollectShim
  with ImplicitCastInputTypes
  with Serializable {

  private lazy val cudfLast = AttributeReference("last", child.dataType)()
  private lazy val valueSet = AttributeReference("valueSet", BooleanType)()

  override lazy val inputProjection: Seq[Expression] =
    Seq(child, GpuLiteral(!ignoreNulls, BooleanType))

  private lazy val commonExpressions: Seq[CudfAggregate] = if (ignoreNulls) {
    Seq(CudfNthLikeAggregate.newLastExcludeNulls(cudfLast.dataType),
      CudfNthLikeAggregate.newLastExcludeNulls(valueSet.dataType))
  } else {
    Seq(CudfNthLikeAggregate.newLastIncludeNulls(cudfLast.dataType),
      CudfNthLikeAggregate.newLastIncludeNulls(valueSet.dataType))
  }

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    GpuLiteral(null, child.dataType),
    GpuLiteral(false, BooleanType))
  override lazy val updateAggregates: Seq[CudfAggregate] = commonExpressions
  override lazy val mergeAggregates: Seq[CudfAggregate] = commonExpressions
  override lazy val evaluateExpression: Expression = cudfLast
  override lazy val aggBufferAttributes: Seq[AttributeReference] = cudfLast :: valueSet :: Nil

  // Copied from Last
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def toString: String = s"gpulast($child)${if (ignoreNulls) " ignore nulls"}"

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }

  // GENERAL WINDOW FUNCTION
  override lazy val windowInputProjection: Seq[Expression] = inputProjection
  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.nth(-1, if (ignoreNulls) NullPolicy.EXCLUDE else NullPolicy.INCLUDE)
        .onColumn(inputs.head._2)
}

case class GpuNthValue(child: Expression, offset: Expression, ignoreNulls: Boolean)
  extends GpuAggregateWindowFunction
        with ImplicitCastInputTypes
        with Serializable {

  // offset is foldable, get value as Spark does
  private lazy val offsetVal = offset.eval().asInstanceOf[Int]

  // Copied from First
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def toString: String = s"gpu_nth_value($child, $offset)" +
      s"${if (ignoreNulls) " ignore nulls"}"

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }

  // GENERAL WINDOW FUNCTION
  override lazy val windowInputProjection: Seq[Expression] =
    Seq(child)

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.nth(offsetVal - 1,
      if (ignoreNulls) NullPolicy.EXCLUDE else NullPolicy.INCLUDE)
        .onColumn(inputs.head._2)
}

trait GpuCollectBase
  extends GpuAggregateFunction
  with GpuDeterministicFirstLastCollectShim
  with GpuAggregateWindowFunction {

  def child: Expression

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(child.dataType, containsNull = false)

  override def children: Seq[Expression] = child :: Nil

  // WINDOW FUNCTION
  override val windowInputProjection: Seq[Expression] = Seq(child)

  override val initialValues: Seq[Expression] = {
    Seq(GpuLiteral.create(new GenericArrayData(Array.empty[Any]), dataType))
  }

  override val inputProjection: Seq[Expression] = Seq(child)

  protected final lazy val outputBuf: AttributeReference =
    AttributeReference("inputBuf", dataType)()
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

  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(new CudfCollectList(dataType))
  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(new CudfMergeLists(dataType))
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

  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(new CudfCollectSet(dataType))
  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(new CudfMergeSets(dataType))
  override lazy val evaluateExpression: Expression = outputBuf
  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil

  override def prettyName: String = "collect_set"

  override def windowAggregation(
      inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn =
    RollingAggregation.collectSet(NullPolicy.EXCLUDE, NullEquality.EQUAL,
        NaNEquality.UNEQUAL).onColumn(inputs.head._2)
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
    GpuLiteral(if (nullOnDivideByZero) null else Double.NaN, DoubleType)

  override lazy val initialValues: Seq[GpuLiteral] =
    Seq(GpuLiteral(0.0), GpuLiteral(0.0), GpuLiteral(0.0))

  override lazy val inputProjection: Seq[Expression] = Seq(child, child, child)

  // cudf aggregates
  lazy val cudfCountN: CudfAggregate = new CudfCount(IntegerType)
  lazy val cudfMean: CudfMean = new CudfMean(DoubleType)
  lazy val cudfM2: CudfM2 = new CudfM2

  // For local update, we need to compute all 3 aggregates: n, avg, m2.
  override lazy val updateAggregates: Seq[CudfAggregate] = Seq(cudfCountN, cudfMean, cudfM2)

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
    val bufferAvgNoNulls = GpuCoalesce(Seq(cudfMean.attr, GpuLiteral(0.0, DoubleType)))
    val bufferM2NoNulls = GpuCoalesce(Seq(cudfM2.attr, GpuLiteral(0.0, DoubleType)))
    GpuCast(cudfCountN.attr, DoubleType) :: bufferAvgNoNulls :: bufferM2NoNulls :: Nil
  }

  protected lazy val bufferN: AttributeReference =
    AttributeReference("n", DoubleType, nullable = false)()
  protected lazy val bufferAvg: AttributeReference =
    AttributeReference("avg", DoubleType, nullable = false)()
  protected lazy val bufferM2: AttributeReference =
    AttributeReference("m2", DoubleType, nullable = false)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    bufferN :: bufferAvg :: bufferM2 :: Nil

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

  private lazy val mergeM2 = new CudfMergeM2
  override lazy val mergeAggregates: Seq[CudfAggregate] = Seq(mergeM2)

  // The postMerge step needs to extract 3 columns (n, avg, m2) from the structs column
  // output from the merge step. Note that the first one is casted to Double to match with Spark.
  //
  // In the future, when rewriting CudfMergeM2, we will need to output it in Double type.
  override lazy val postMerge: Seq[Expression] = Seq(
    GpuCast(GpuGetStructField(mergeM2.attr, 0), DoubleType),
    GpuCoalesce(Seq(GpuCast(GpuGetStructField(mergeM2.attr, 1), DoubleType),
      GpuLiteral(0.0, DoubleType))),
    GpuCoalesce(Seq(GpuCast(GpuGetStructField(mergeM2.attr, 2), DoubleType),
      GpuLiteral(0.0, DoubleType))))
}

case class GpuStddevPop(child: Expression, nullOnDivideByZero: Boolean)
  extends GpuM2(child, nullOnDivideByZero) {

  override lazy val evaluateExpression: Expression = {
    // stddev_pop = sqrt(m2 / n).
    val stddevPop = GpuSqrt(GpuDivide(bufferM2, bufferN, failOnErrorOverride = false))

    // Set nulls for the rows where n == 0.
    GpuIf(GpuEqualTo(bufferN, GpuLiteral(0.0)), GpuLiteral(null, DoubleType), stddevPop)
  }

  override def prettyName: String = "stddev_pop"
}

case class WindowStddevSamp(
    child: Expression,
    nullOnDivideByZero: Boolean)
    extends GpuAggregateWindowFunction {

  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = true

  /**
   * Using child references, define the shape of the vectors sent to the window operations
   */
  override val windowInputProjection: Seq[Expression] = Seq(child)

  override def windowAggregation(inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn = {
    RollingAggregation.standardDeviation().onColumn(inputs.head._2)
  }
}

case class GpuStddevSamp(child: Expression, nullOnDivideByZero: Boolean)
    extends GpuM2(child, nullOnDivideByZero) with GpuReplaceWindowFunction {

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

  override def windowReplacement(spec: GpuWindowSpecDefinition): Expression = {
    // calculate n
    val count = GpuCast(GpuWindowExpression(GpuCount(Seq(child)), spec), DoubleType)
    val stddev = GpuWindowExpression(WindowStddevSamp(child, nullOnDivideByZero), spec)
    // if (n == 0.0)
    GpuIf(GpuEqualTo(count, GpuLiteral(0.0)),
      // return null
      GpuLiteral(null, DoubleType),
      // else if (n == 1.0)
      GpuIf(GpuEqualTo(count, GpuLiteral(1.0)),
        // return divideByZeroEval
        divideByZeroEvalResult,
        // else return stddev
        stddev))
  }
}

case class GpuVariancePop(child: Expression, nullOnDivideByZero: Boolean)
  extends GpuM2(child, nullOnDivideByZero) {

  override lazy val evaluateExpression: Expression = {
    // var_pop = m2 / n.
    val varPop = GpuDivide(bufferM2, bufferN, failOnErrorOverride = false)

    // Set nulls for the rows where n == 0.
    GpuIf(GpuEqualTo(bufferN, GpuLiteral(0.0)), GpuLiteral(null, DoubleType), varPop)
  }

  override def prettyName: String = "var_pop"
}

case class GpuVarianceSamp(child: Expression, nullOnDivideByZero: Boolean)
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
