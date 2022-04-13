/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import scala.collection.mutable

import ai.rapids.cudf
import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, MapType, Metadata}
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * A named lambda variable. In Spark on the CPU this includes an AtomicReference to the value that
 * is updated each time a lambda function is called. On the GPU we have to bind this and turn it
 * into a GpuBoundReference for a modified input batch. In the future this should also work with AST
 * when cudf supports that type of operation.
 */
case class GpuNamedLambdaVariable(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    exprId: ExprId = NamedExpression.newExprId)
    extends GpuLeafExpression
        with NamedExpression
        with GpuUnevaluable {

  override def qualifier: Seq[String] = Seq.empty

  override def newInstance(): NamedExpression =
    copy(exprId = NamedExpression.newExprId)

  override def toAttribute: Attribute = {
    AttributeReference(name, dataType, nullable, Metadata.empty)(exprId, Seq.empty)
  }

  override def toString: String = s"lambda $name#${exprId.id}$typeSuffix"

  override def simpleString(maxFields: Int): String = {
    s"lambda $name#${exprId.id}: ${dataType.simpleString(maxFields)}"
  }
}

/**
 * A lambda function and its arguments on the GPU. This is mostly just a wrapper around the
 * function expression, but it holds references to the arguments passed into it.
 */
case class GpuLambdaFunction(
    function: Expression,
    arguments: Seq[NamedExpression],
    hidden: Boolean = false)
    extends GpuExpression with ShimExpression {

  override def children: Seq[Expression] = function +: arguments
  override def dataType: DataType = function.dataType
  override def nullable: Boolean = function.nullable

  override def columnarEval(batch: ColumnarBatch): Any =
    function.asInstanceOf[GpuExpression].columnarEval(batch)
}

/**
 * A higher order function takes one or more (lambda) functions and applies these to some objects.
 * The function produces a number of variables which can be consumed by some lambda function.
 */
trait GpuHigherOrderFunction extends GpuExpression with ShimExpression {

  override def nullable: Boolean = arguments.exists(_.nullable)

  override def children: Seq[Expression] = arguments ++ functions

  /**
   * Arguments of the higher ordered function.
   */
  def arguments: Seq[Expression]

  /**
   * Functions applied by the higher order function.
   */
  def functions: Seq[Expression]
}

/**
 * Trait for functions having as input one argument and one function.
 */
trait GpuSimpleHigherOrderFunction extends GpuHigherOrderFunction with GpuBind {

  def argument: Expression

  override def arguments: Seq[Expression] = argument :: Nil

  def function: Expression

  protected val lambdaFunction: GpuLambdaFunction = function.asInstanceOf[GpuLambdaFunction]

  override def functions: Seq[Expression] = function :: Nil

  /**
   * Do the core work of binding this and its lambda function.
   * @param input the input attributes
   * @return the bound child GpuLambdaFunction, the bound argument, and project expressions for
   *         everything except the lambda function's arguments, because how you get those is
   *         often dependent on the type of processing you are doing.
   */
  protected def bindLambdaFunc(input: AttributeSeq): (GpuLambdaFunction, GpuExpression,
      Seq[GpuExpression]) = {
    // Bind the argument parameter, but it can also be a lambda variable...
    val boundArg = GpuBindReferences.bindRefInternal[Expression, GpuExpression](argument, input, {
      case lr: GpuNamedLambdaVariable if input.indexOf(lr.exprId) >= 0 =>
        val ordinal = input.indexOf(lr.exprId)
        GpuBoundReference(ordinal, lr.dataType, input(ordinal).nullable)(lr.exprId, lr.name)
    })

    // `function` is a lambda function. In CPU Spark a lambda function's parameters are wrapping
    // AtomicReference values and the parent expression sets the values before they are processed.
    // That does not work for us. When processing a lambda function we pass in a modified
    // columnar batch, which includes the arguments to that lambda function. To make this work
    // we have to bind the GpuNamedLambdaVariable to a GpuBoundReference and also handle the
    // binding of AttributeReference to GpuBoundReference based on the attributes in the new batch
    // that will be passed to the lambda function. This get especially tricky when dealing with
    // nested lambda functions. So to make that work we first have to find all of the
    // GpuNamedLambdaVariable instances that are provided by lambda expressions below us in the
    // expression tree

    val namedVariablesProvidedByChildren = mutable.HashSet[ExprId]()
    // We purposely include the arguments to the lambda function just below us because
    // we will add them in as a special case later on.
    lambdaFunction.foreach {
      case childLambda: GpuLambdaFunction =>
        namedVariablesProvidedByChildren ++= childLambda.arguments.map(_.exprId)
      case _ => // ignored
    }
    // With this information we can now find all of the AttributeReference and
    // GpuNamedLambdaVariable instances below us so we know what columns in `input` we have
    // to pass on. This is a performance and memory optimization because we are going to explode
    // the columns that are used below us, which can end up using a lot of memory
    val usedReferences = new mutable.HashMap[ExprId, Attribute]()
    function.foreach {
      case att: AttributeReference => usedReferences(att.exprId) = att
      case namedLambda: GpuNamedLambdaVariable =>
        if (!namedVariablesProvidedByChildren.contains(namedLambda.exprId)) {
          usedReferences(namedLambda.exprId) = namedLambda.toAttribute
        } // else it is provided by something else so ignore it
      case _ => // ignored
    }
    val references = usedReferences.toSeq.sortBy(_._1.id)

    // The format of the columnar batch passed to `lambdaFunction` will be
    // `references ++ lambdaFunction.arguments` We are going to take the references
    // and turn them into bound references from `input` so the bound version of this operator
    // knows how to create the `references` part of the batch that is passed down.

    val boundIntermediate = references.map {
      case (_, att) => GpuBindReferences.bindGpuReference(att, input)
    }

    // Now get the full set of attributes that we will pass to `lambdaFunction` so any nested
    // higher order functions know how to bind their arguments, and also so we can build a
    // mapping to know how to replace expressions

    val argsAndReferences = references ++ lambdaFunction.arguments.map { expr =>
      (expr.exprId, expr)
    }

    val argsAndRefsAtters = argsAndReferences.map {
      case (_, named: NamedExpression) => named.toAttribute
    }

    val replacementMap = argsAndReferences.zipWithIndex.map {
      case ((exprId, expr), ordinal) =>
        (exprId, GpuBoundReference(ordinal, expr.dataType, expr.nullable)(exprId, expr.name))
    }.toMap

    // Now we actually bind all of the attribute references and GpuNamedLambdaVariables
    // with the appropriate replacements.

    val childFunction = GpuBindReferences.transformNoRecursionOnReplacement(lambdaFunction) {
      case bind: GpuBind =>
        bind.bind(argsAndRefsAtters)
      case a: AttributeReference =>
        replacementMap(a.exprId)
      case lr: GpuNamedLambdaVariable if replacementMap.contains(lr.exprId) =>
        replacementMap(lr.exprId)
    }
    val boundFunc =
      GpuLambdaFunction(childFunction, lambdaFunction.arguments, lambdaFunction.hidden)

    (boundFunc, boundArg, boundIntermediate)
  }
}


trait GpuArrayTransformBase extends GpuSimpleHigherOrderFunction {
  def isBound: Boolean
  def  boundIntermediate: Seq[GpuExpression]

  protected lazy val inputToLambda: Seq[DataType] = {
    assert(isBound)
    boundIntermediate.map(_.dataType) ++ lambdaFunction.arguments.map(_.dataType)
  }

  private[this] def makeElementProjectBatch(
      inputBatch: ColumnarBatch,
      listColumn: cudf.ColumnVector): ColumnarBatch = {
    assert(listColumn.getType.equals(DType.LIST))
    assert(isBound, "Trying to execute an un-bound transform expression")

    if (function.asInstanceOf[GpuLambdaFunction].arguments.length >= 2) {
      // Need to do an explodePosition
      val boundProject = boundIntermediate :+ argument
      val explodedTable = withResource(GpuProjectExec.project(inputBatch, boundProject)) {
        projectedBatch =>
          withResource(GpuColumnVector.from(projectedBatch)) { projectedTable =>
            projectedTable.explodePosition(boundIntermediate.length)
          }
      }
      val reorderedTable = withResource(explodedTable) { explodedTable =>
        // The column order is wrong after an explodePosition. It is
        // [other_columns*, position, entry]
        // but we want
        // [other_columns*, entry, position]
        // So we have to remap it
        val cols = new Array[cudf.ColumnVector](explodedTable.getNumberOfColumns)
        val numOtherColumns = explodedTable.getNumberOfColumns - 2
        (0 until numOtherColumns).foreach { index =>
          cols(index) = explodedTable.getColumn(index)
        }
        cols(numOtherColumns) = explodedTable.getColumn(numOtherColumns + 1)
        cols(numOtherColumns + 1) = explodedTable.getColumn(numOtherColumns)

        new cudf.Table(cols: _*)
      }
      withResource(reorderedTable) { reorderedTable =>
        GpuColumnVector.from(reorderedTable, inputToLambda.toArray)
      }
    } else {
      // Need to do an explode
      val boundProject = boundIntermediate :+ argument
      val explodedTable = withResource(GpuProjectExec.project(inputBatch, boundProject)) {
        projectedBatch =>
          withResource(GpuColumnVector.from(projectedBatch)) { projectedTable =>
            projectedTable.explode(boundIntermediate.length)
          }
      }
      withResource(explodedTable) { explodedTable =>
        GpuColumnVector.from(explodedTable, inputToLambda.toArray)
      }
    }
  }

  /*
   * Post-process the column view of the array after applying the function parameter
   */
  protected def transformListColumnView(
    lambdaTransformedCV: cudf.ColumnView): GpuColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuExpressionsUtils.columnarEvalToColumn(argument, batch)) { arg =>
      val dataCol = withResource(
        makeElementProjectBatch(batch, arg.getBase)) { cb =>
        GpuExpressionsUtils.columnarEvalToColumn(function, cb)
      }
      withResource(dataCol) { _ =>
        val cv = GpuListUtils.replaceListDataColumnAsView(arg.getBase, dataCol.getBase)
        withResource(cv)(transformListColumnView(_))
      }
    }
  }
}


case class GpuArrayTransform(
  argument: Expression,
  function: Expression,
  isBound: Boolean = false,
  boundIntermediate: Seq[GpuExpression] = Seq.empty) extends GpuArrayTransformBase {

  override def dataType: ArrayType = ArrayType(function.dataType, function.nullable)

  override def prettyName: String = "transform"

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)

    GpuArrayTransform(boundArg, boundFunc, isBound = true, boundIntermediate)
  }

  override protected def transformListColumnView(
    lambdaTransformedCV: cudf.ColumnView): GpuColumnVector = {
    GpuColumnVector.from(lambdaTransformedCV.copyToColumnVector(), dataType)
  }
}


case class GpuArrayExists(
    argument: Expression,
    function: Expression,
    followThreeValuedLogic: Boolean,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty) extends GpuArrayTransformBase {

  override def dataType: DataType = BooleanType

  override def prettyName: String = "exists"

  override def nullable: Boolean = super.nullable || function.nullable

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)
    GpuArrayExists(boundArg, boundFunc,  followThreeValuedLogic,isBound = true, boundIntermediate)
  }

  private def imputeFalseForEmptyArrays(
    transformedCV: cudf.ColumnView,
    result: cudf.ColumnView
  ): GpuColumnVector = {
    withResource(cudf.Scalar.fromBool(false)) { falseScalar =>
      withResource(cudf.Scalar.fromInt(0)) { zeroScalar =>
        withResource(transformedCV.countElements()) { elementCounts =>
          withResource(elementCounts.equalTo(zeroScalar)) { isEmptyList =>
            GpuColumnVector.from(isEmptyList.ifElse(falseScalar, result), dataType)
          }
        }
      }
    }
  }

  private def existsReduce(columnView: cudf.ColumnView, nullPolicy: cudf.NullPolicy) = {
    columnView.listReduce(
      cudf.SegmentedReductionAggregation.any(),
      nullPolicy,
      DType.BOOL8)
  }

  /*
   * The difference between legacyExists and EXCLUDE nulls reduction
   * is that the list without valid values (all nulls) should produce false
   * which is equivalent to replacing nulls with false after lambda prior
   * to aggregation
   */
  private def legacyExists(cv: cudf.ColumnView): cudf.ColumnView = {
    withResource(cudf.Scalar.fromBool(false)) { falseScalar =>
      withResource(cv.getChildColumnView(0)) { childView =>
        withResource(childView.replaceNulls(falseScalar)) { noNullsChildView =>
          withResource(cv.replaceListChild(noNullsChildView)) { reduceInput =>
            existsReduce(reduceInput, cudf.NullPolicy.EXCLUDE)
          }
        }
      }
    }
  }

  /*
   * 3VL is true if EXCLUDE nulls reduce is true
   * 3VL is false if INCLUDE nulls reduce is false
   * 3VL is null if
   *    EXCLUDE null reduce is false and
   *    INCLUDE nulls reduce is null
   */
  private def threeValueExists(cv: cudf.ColumnView): cudf.ColumnView = {
    withResource(existsReduce(cv, cudf.NullPolicy.EXCLUDE)) { existsNullsExcludedCV =>
      withResource(existsReduce(cv, cudf.NullPolicy.INCLUDE)) { existsNullsIncludedCV =>
        existsNullsExcludedCV.ifElse(existsNullsExcludedCV, existsNullsIncludedCV)
      }
    }
  }

  private def exists(cv: cudf.ColumnView) = {
    if (followThreeValuedLogic) {
      threeValueExists(cv)
    } else {
      legacyExists(cv)
    }
  }

  override protected def transformListColumnView(
    lambdaTransformedCV: cudf.ColumnView
  ): GpuColumnVector = {
    withResource(exists(lambdaTransformedCV)) { existsCV =>
      // exists is false for empty arrays
      // post process empty arrays until cudf allows specifying
      // the initial value for a list reduction (i.e. similar to Scala fold)
      // https://github.com/rapidsai/cudf/issues/10455
      imputeFalseForEmptyArrays(lambdaTransformedCV, existsCV)
    }
  }

}

trait GpuMapSimpleHigherOrderFunction extends GpuSimpleHigherOrderFunction with GpuBind {

  protected def isBound: Boolean
  protected def boundIntermediate: Seq[GpuExpression]

  protected lazy val inputToLambda: Seq[DataType] = {
    assert(isBound)
    boundIntermediate.map(_.dataType) ++ lambdaFunction.arguments.map(_.dataType)
  }

  protected def makeElementProjectBatch(
      inputBatch: ColumnarBatch,
      listColumn: cudf.ColumnVector): ColumnarBatch = {
    assert(listColumn.getType.equals(DType.LIST))
    assert(isBound, "Trying to execute an un-bound transform value expression")

    // Need to do an explode followed by pulling out the key/value columns
    val boundProject = boundIntermediate :+ argument
    val explodedTable = withResource(GpuProjectExec.project(inputBatch, boundProject)) {
      projectedBatch =>
        withResource(GpuColumnVector.from(projectedBatch)) { projectedTable =>
          projectedTable.explode(boundIntermediate.length)
        }
    }
    val moddedTable = withResource(explodedTable) { explodedTable =>
      // The last column is a struct column with key/values pairs in it. We need to pull them
      // out into stand alone columns
      val cols = new Array[cudf.ColumnVector](explodedTable.getNumberOfColumns + 1)
      val numOtherColumns = explodedTable.getNumberOfColumns - 1
      (0 until numOtherColumns).foreach { index =>
        cols(index) = explodedTable.getColumn(index)
      }
      val keyValuePairColumn = explodedTable.getColumn(numOtherColumns)
      val keyCol = withResource(
        keyValuePairColumn.getChildColumnView(GpuMapUtils.KEY_INDEX)) { keyView =>
        keyView.copyToColumnVector()
      }
      withResource(keyCol) { keyCol =>
        val valCol = withResource(
          keyValuePairColumn.getChildColumnView(GpuMapUtils.VALUE_INDEX)) { valueView =>
          valueView.copyToColumnVector()
        }

        withResource(valCol) { valCol =>
          cols(numOtherColumns) = keyCol
          cols(numOtherColumns + 1) = valCol
          new cudf.Table(cols: _*)
        }
      }
    }
    withResource(moddedTable) { moddedTable =>
      GpuColumnVector.from(moddedTable, inputToLambda.toArray)
    }
  }
}

case class GpuTransformKeys(
    argument: Expression,
    function: Expression,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty)
    extends GpuMapSimpleHigherOrderFunction {

  @transient lazy val MapType(keyType, valueType, valueContainsNull) = argument.dataType

  override def dataType: DataType = MapType(function.dataType, valueType, valueContainsNull)

  override def prettyName: String = "transform_keys"

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)

    GpuTransformKeys(boundArg, boundFunc, isBound = true, boundIntermediate)
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuExpressionsUtils.columnarEvalToColumn(argument, batch)) { arg =>
      val newKeysCol = withResource(
        makeElementProjectBatch(batch, arg.getBase)) { cb =>
        GpuExpressionsUtils.columnarEvalToColumn(function, cb)
      }
      withResource(newKeysCol) { newKeysCol =>
        withResource(GpuMapUtils.replaceExplodedKeyAsView(arg.getBase, newKeysCol.getBase)) {
          updatedMapView =>
            GpuMapUtils.assertNoNullKeys(updatedMapView)
            GpuMapUtils.assertNoDuplicateKeys(updatedMapView)
            GpuColumnVector.from(updatedMapView.copyToColumnVector(), dataType)
        }
      }
    }
  }
}

case class GpuTransformValues(
    argument: Expression,
    function: Expression,
    isBound: Boolean = false,
    boundIntermediate: Seq[GpuExpression] = Seq.empty)
    extends GpuMapSimpleHigherOrderFunction {

  @transient lazy val MapType(keyType, valueType, valueContainsNull) = argument.dataType

  override def dataType: DataType = MapType(keyType, function.dataType, function.nullable)

  override def prettyName: String = "transform_values"

  override def bind(input: AttributeSeq): GpuExpression = {
    val (boundFunc, boundArg, boundIntermediate) = bindLambdaFunc(input)

    GpuTransformValues(boundArg, boundFunc, isBound = true, boundIntermediate)
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuExpressionsUtils.columnarEvalToColumn(argument, batch)) { arg =>
      val newValueCol = withResource(
        makeElementProjectBatch(batch, arg.getBase)) { cb =>
        GpuExpressionsUtils.columnarEvalToColumn(function, cb)
      }
      withResource(newValueCol) { newValueCol =>
        withResource(GpuMapUtils.replaceExplodedValueAsView(arg.getBase, newValueCol.getBase)) {
          updatedMapView =>
            GpuColumnVector.from(updatedMapView.copyToColumnVector(), dataType)
        }
      }
    }
  }
}