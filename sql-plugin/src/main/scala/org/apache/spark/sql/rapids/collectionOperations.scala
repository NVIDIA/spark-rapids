/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import java.util.Optional

import ai.rapids.cudf
import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, Scalar, SegmentedReductionAggregation, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.ArrayIndexUtils.firstIndexAndNumElementUnchecked
import com.nvidia.spark.rapids.BoolUtils.isAllValidTrue
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{GetSequenceSize, ShimExpression}

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ElementAt, ExpectsInputTypes, Expression, ImplicitCastInputTypes, NamedExpression, NullIntolerant, RowOrdering, Sequence, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

case class GpuConcat(children: Seq[Expression]) extends GpuComplexTypeMergingExpression {

  @transient override lazy val dataType: DataType = {
    if (children.isEmpty) {
      StringType
    } else {
      super.dataType
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val res = dataType match {
      // in Spark concat() will be considered as an empty string here
      case dt if children.isEmpty => GpuScalar("", dt)
      // For single column concat, we pass the result of child node to avoid extra cuDF call.
      case _ if children.length == 1 => children.head.columnarEval(batch)
      case StringType => stringConcat(batch)
      case ArrayType(_, _) => listConcat(batch)
      case _ => throw new IllegalArgumentException(s"unsupported dataType $dataType")
    }
    GpuExpressionsUtils.resolveColumnVector(res, batch.numRows())
  }

  private def stringConcat(batch: ColumnarBatch): GpuColumnVector = {
    withResource(children.safeMap(_.columnarEval(batch).getBase())) {cols =>
      // run string concatenate
      GpuColumnVector.from(
        cudf.ColumnVector.stringConcatenate(cols.toArray[ColumnView]), StringType)
    }
  }

  private def listConcat(batch: ColumnarBatch): GpuColumnVector = {
    withResource(children.safeMap(_.columnarEval(batch).getBase())) {cols =>
      // run list concatenate
      GpuColumnVector.from(cudf.ColumnVector.listConcatenateByRow(cols: _*), dataType)
    }
  }
}

case class GpuMapConcat(children: Seq[Expression]) extends GpuComplexTypeMergingExpression {

  override lazy val hasSideEffects: Boolean =
    GpuCreateMap.exceptionOnDupKeys || super.hasSideEffects

  @transient override lazy val dataType: MapType = {
    if (children.isEmpty) {
      MapType(StringType, StringType)
    } else {
      super.dataType.asInstanceOf[MapType]
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
    (dataType, children.length) match {
      // Explicitly return null for empty concat as Spark, since cuDF doesn't support empty concat.
      case (dt, 0) => GpuColumnVector.fromNull(batch.numRows(), dt)
      // For single column concat, we pass the result of child node to avoid extra cuDF call.
      case (_, 1) => children.head.columnarEval(batch)
      case (_, _) => {
        withResource(children.safeMap(_.columnarEval(batch).getBase())) {cols =>
          withResource(cudf.ColumnVector.listConcatenateByRow(cols: _*)) {structs =>
            GpuCreateMap.createMapFromKeysValuesAsStructs(dataType, structs)
          }
        }
      }
    }
}

object GpuElementAtMeta {
  /**
   * Construct the expression rule for `ElementAt`.
   * @param nullOnInvalidAccessToMap
   *  Returns `null` or throws an exception on invalid access to map column.
   *  For Spark 3.4+ and DB11.3+, this argument is `true`,
   *  and for other Spark versions, it is `false`.
   */
  def elementAtRule(nullOnInvalidAccessToMap: Boolean): ExprRule[_ <: Expression] = {
    GpuOverrides.expr[ElementAt](
      "Returns element of array at given(1-based) index in value if column is array. " +
        "Returns value for the given key in value if column is map.",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
          TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all,
        ("array/map", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
          TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY) +
          TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
            TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY)
            .withPsNote(TypeEnum.MAP, "If it's map, only primitive key types are supported."),
          TypeSig.ARRAY.nested(TypeSig.all) + TypeSig.MAP.nested(TypeSig.all)),
        ("index/key", (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128)
          .withPsNote(
            Seq(TypeEnum.BOOLEAN, TypeEnum.BYTE, TypeEnum.SHORT, TypeEnum.LONG,
              TypeEnum.FLOAT, TypeEnum.DOUBLE, TypeEnum.DATE, TypeEnum.TIMESTAMP,
              TypeEnum.STRING, TypeEnum.DECIMAL), "Unsupported as array index."),
          TypeSig.all)),
      (in, conf, p, r) => new BinaryExprMeta[ElementAt](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // To distinguish the supported nested type between Array and Map
          val checks = in.left.dataType match {
            case _: MapType =>
              // Match exactly with the checks for GetMapValue
              ExprChecks.binaryProject(
                (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                  TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
                TypeSig.all,
                ("map",
                  TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
                    TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY),
                  TypeSig.MAP.nested(TypeSig.all)),
                ("key", TypeSig.commonCudfTypes + TypeSig.DECIMAL_128, TypeSig.all))
            case _: ArrayType =>
              // Match exactly with the checks for GetArrayItem
              ExprChecks.binaryProject(
                (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                  TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
                TypeSig.all,
                ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
                  TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP +
                  TypeSig.BINARY),
                  TypeSig.ARRAY.nested(TypeSig.all)),
                ("ordinal", TypeSig.INT, TypeSig.INT))
            case _ => throw new IllegalStateException("Only Array or Map is supported as input.")
          }
          checks.tag(this)
        }
        override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
          val failOnError = if (nullOnInvalidAccessToMap) {
            in.failOnError && lhs.dataType.isInstanceOf[ArrayType]
          } else {
            in.failOnError
          }
          GpuElementAt(lhs, rhs, failOnError)
        }
      })
  }
}

case class GpuElementAt(left: Expression, right: Expression, failOnError: Boolean)
  extends GpuBinaryExpression with ExpectsInputTypes {

  override def hasSideEffects: Boolean = super.hasSideEffects || failOnError

  override lazy val dataType: DataType = left.dataType match {
    case ArrayType(elementType, _) => elementType
    case MapType(_, valueType, _) => valueType
  }

  override def inputTypes: Seq[AbstractDataType] = {
    (left.dataType, right.dataType) match {
      case (arr: ArrayType, e2: IntegralType) if e2 != LongType =>
        Seq(arr, IntegerType)
      case (MapType(keyType, valueType, hasNull), e2) =>
        TypeCoercion.findTightestCommonType(keyType, e2) match {
          case Some(dt) => Seq(MapType(dt, valueType, hasNull), dt)
          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (_: ArrayType, e2) if e2 != IntegerType =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${ArrayType.simpleString} followed by a ${IntegerType.simpleString}, but it's " +
          s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
      case (MapType(e1, _, _), e2) if !e2.sameType(e1) =>
        TypeCheckResult.TypeCheckFailure(s"Input to function $prettyName should have " +
          s"been ${MapType.simpleString} followed by a value of same key type, but it's " +
          s"[${left.dataType.catalogString}, ${right.dataType.catalogString}].")
      case (e1, _) if !e1.isInstanceOf[MapType] && !e1.isInstanceOf[ArrayType] =>
        TypeCheckResult.TypeCheckFailure(s"The first argument to function $prettyName should " +
          s"have been ${ArrayType.simpleString} or ${MapType.simpleString} type, but its " +
          s"${left.dataType.catalogString} type.")
      case _ => TypeCheckResult.TypeCheckSuccess
    }
  }

  // Eventually we need something more full featured like
  // GetArrayItemUtil.computeNullabilityFromArray
  override def nullable: Boolean = true

  @transient
  private lazy val doElementAtV: (ColumnVector, ColumnVector) => cudf.ColumnVector =
    left.dataType match {
      case _: ArrayType =>
        (array, indices) => {
          if (failOnError) {
            // Check if any index is out of bound only when ansi mode is enabled. Nulls in either
            // array or indices are skipped during this computation. Then no exception will be
            // raised if no valid entry (An entry is valid when both the array row and its index
            // are not null), the same with what Spark does.
            withResource(array.countElements()) { numElements =>
              val hasLargerIndices = withResource(indices.abs()) { absIndices =>
                absIndices.greaterThan(numElements)
              }
              withResource(hasLargerIndices) { _ =>
                if (BoolUtils.isAnyValidTrue(hasLargerIndices)) {
                  val (index, numElem) = firstIndexAndNumElementUnchecked(hasLargerIndices,
                    indices, numElements)
                  throw RapidsErrorUtils.invalidArrayIndexError(index, numElem, true)
                }
              }
            }
          } // end of "if (failOnError)"

          // convert to zero-based indices for positive values
          val indicesCol = withResource(Scalar.fromInt(0)) { zeroS =>
            // No exception should be raised if no valid entry (An entry is valid when both
            // the array row and its index are not null), the same with what Spark does.
            val hasValidEntryCV = indices.mergeAndSetValidity(BinaryOp.BITWISE_AND,
              indices, array)
            withResource(hasValidEntryCV) { _ =>
              if (hasValidEntryCV.contains(zeroS)) {
                throw RapidsErrorUtils.sqlArrayIndexNotStartAtOneError()
              }
            }
            val zeroBasedIndices = withResource(Scalar.fromInt(1)) { oneS =>
              indices.sub(oneS, indices.getType)
            }
            withResource(zeroBasedIndices) { _ =>
              withResource(indices.greaterThan(zeroS)) { hasPositiveIndices =>
                hasPositiveIndices.ifElse(zeroBasedIndices, indices)
              }
            }
          }
          withResource(indicesCol) { _ =>
            array.extractListElement(indicesCol)
          }
        }
      case _: MapType =>
        (map, indices) => {
          if (failOnError) {
            GpuMapUtils.getMapValueOrThrow(map, indices, right.dataType, origin)
          }
          else {
            map.getMapValue(indices)
          }
        }
    }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): cudf.ColumnVector =
    doElementAtV(lhs.getBase, rhs.getBase)

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): cudf.ColumnVector =
    withResource(ColumnVector.fromScalar(lhs.getBase, rhs.getRowCount.toInt)) { expandedCV =>
      doElementAtV(expandedCV, rhs.getBase)
    }

  @transient
  private lazy val doElementAtS: (ColumnView, GpuScalar) => cudf.ColumnVector =
    left.dataType match {
      case _: ArrayType =>
        (array, indexS) => {
          if (!indexS.isValid || array.getRowCount == array.getNullCount) {
            // Return nulls when index is null or all the array rows are null,
            // the same with what Spark does.
            GpuColumnVector.columnVectorFromNull(array.getRowCount.toInt, dataType)
          } else {
            // The index is valid, and array column contains at least one non-null row.
            val index = indexS.getValue.asInstanceOf[Int]
            if (failOnError) {
              // Check if index is out of bound only when ansi mode is enabled, since cuDF
              // returns nulls if index is out of bound, the same with what Spark does when
              // ansi mode is disabled.
              withResource(array.countElements()) { numElementsCV =>
                withResource(numElementsCV.min) { minScalar =>
                  val minNumElements = minScalar.getInt
                  if (math.abs(index) > minNumElements) {
                    throw RapidsErrorUtils.invalidArrayIndexError(index, minNumElements, true)
                  }
                }
              }
            }
            // convert to zero-based index if it is positive
            val idx = if (index == 0) {
              throw RapidsErrorUtils.sqlArrayIndexNotStartAtOneError()
            } else if (index > 0) {
              index - 1
            } else {
              index
            }
            array.extractListElement(idx)
          }
        }
      case MapType(keyType, _, _) =>
        (map, keyS) => {
          val key = keyS.getBase
          if (failOnError) {
            withResource(map.getMapKeyExistence(key)){ keyExistenceColumn =>
              withResource(keyExistenceColumn.all()) { exist =>
                if (!exist.isValid || exist.getBoolean) {
                  map.getMapValue(key)
                } else {
                  throw RapidsErrorUtils.mapKeyNotExistError(keyS.getValue.toString, keyType,
                    origin)
                }
              }
            }
          } else {
            map.getMapValue(key)
          }
        }
    }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): cudf.ColumnVector =
    doElementAtS(lhs.getBase, rhs)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): cudf.ColumnVector =
    withResource(ColumnVector.fromScalar(lhs.getBase, numRows)) { expandedCV =>
      doElementAtS(expandedCV, rhs)
    }

  override def prettyName: String = "element_at"
}

case class GpuSize(child: Expression, legacySizeOfNull: Boolean)
    extends GpuUnaryExpression {

  require(child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType],
    s"The size function doesn't support the operand type ${child.dataType}")

  override def dataType: DataType = IntegerType
  override def nullable: Boolean = if (legacySizeOfNull) false else super.nullable

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {

    // Compute sizes of cuDF.ListType to get sizes of each ArrayData or MapData, considering
    // MapData is represented as List of Struct in terms of cuDF.
    withResource(input.getBase.countElements()) { collectionSize =>
      if (legacySizeOfNull) {
        withResource(Scalar.fromInt(-1)) { nullScalar =>
          withResource(input.getBase.isNull) { inputIsNull =>
            inputIsNull.ifElse(nullScalar, collectionSize)
          }
        }
      } else {
        collectionSize.incRefCount()
      }
    }
  }
}

case class GpuReverse(child: Expression) extends GpuUnaryExpression {
  require(child.dataType.isInstanceOf[StringType] || child.dataType.isInstanceOf[ArrayType],
    s"The reverse function doesn't support the operand type ${child.dataType}")

  override def dataType: DataType = child.dataType

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.getBase.reverseStringsOrLists()
  }
}

case class GpuMapKeys(child: Expression)
    extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = ArrayType(child.dataType.asInstanceOf[MapType].keyType)

  override def prettyName: String = "map_keys"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(GpuMapUtils.getKeysAsListView(input.getBase)) { retView =>
      retView.copyToColumnVector()
    }
  }
}

case class GpuMapValues(child: Expression)
    extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  override def dataType: DataType = {
    val mt = child.dataType.asInstanceOf[MapType]
    ArrayType(mt.valueType, containsNull = mt.valueContainsNull)
  }

  override def prettyName: String = "map_values"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(GpuMapUtils.getValuesAsListView(input.getBase)) { retView =>
      retView.copyToColumnVector()
    }
  }
}

case class GpuMapEntries(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(MapType)

  @transient private lazy val childDataType: MapType = child.dataType.asInstanceOf[MapType]

  override def dataType: DataType = {
    ArrayType(
      StructType(
        StructField("key", childDataType.keyType, false) ::
            StructField("value", childDataType.valueType, childDataType.valueContainsNull) ::
            Nil),
      false)
  }

  override def prettyName: String = "map_entries"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    // Internally the format for a list of key/value structs is the same, so just
    // return the same thing, and let Spark think it is a different type.
    input.getBase.incRefCount()
  }
}

case class GpuSortArray(base: Expression, ascendingOrder: Expression)
    extends GpuBinaryExpressionArgsAnyScalar with ExpectsInputTypes {

  override def left: Expression = base

  override def right: Expression = ascendingOrder

  override def dataType: DataType = base.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) =>
      ascendingOrder match {
        // replace Literal with GpuLiteral here
        case GpuLiteral(_: Boolean, BooleanType) =>
          TypeCheckResult.TypeCheckSuccess
        case order =>
          TypeCheckResult.TypeCheckFailure(
            s"Sort order in second argument requires a boolean literal, but found $order")
      }
    case ArrayType(dt, _) =>
      val dtSimple = dt.catalogString
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type $dtSimple which is not orderable")
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }


  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): cudf.ColumnVector = {
    val isDescending = isDescendingOrder(rhs)
    lhs.getBase.listSortRows(isDescending, true)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): cudf.ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { cv =>
      doColumnar(cv, rhs)
    }
  }

  private def isDescendingOrder(scalar: GpuScalar): Boolean = scalar.getValue match {
    case ascending: Boolean => !ascending
    case invalidValue => throw new IllegalArgumentException(s"invalid value $invalidValue")
  }
}

object GpuArrayMin {
  def apply(child: Expression): GpuArrayMin = {
    child.dataType match {
      case ArrayType(FloatType | DoubleType, _) => GpuFloatArrayMin(child)
      case ArrayType(_, _) => GpuBasicArrayMin(child)
      case _ => throw new IllegalStateException(s"array_min accepts only arrays.")
    }
  }
}

abstract class GpuArrayMin(child: Expression) extends GpuUnaryExpression
  with ImplicitCastInputTypes
  with Serializable {

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_min"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector =
    input.getBase.listReduce(SegmentedReductionAggregation.min())
}

/** ArrayMin without `Nan` handling */
case class GpuBasicArrayMin(child: Expression) extends GpuArrayMin(child)

/** ArrayMin for FloatType and DoubleType to handle `Nan`s.
 *
 * In Spark, `Nan` is the max float value, however in cuDF, the calculation
 * involving `Nan` is undefined.
 * We design a workaround method here to match the Spark's behaviour.
 * The high level idea is:
 *   if one list contains only `Nan`s or `null`s
 *   then
       if the list contains `Nan`
 *     then return `Nan`
 *     else return null
 *   else
 *     replace all `Nan`s with nulls;
 *     use cuDF kernel to find the min value
 */
case class GpuFloatArrayMin(child: Expression) extends GpuArrayMin(child) {
    @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(FloatType, _) => FloatType
    case ArrayType(DoubleType, _) => DoubleType
    case _ => throw new IllegalStateException(
      s"GpuFloatArrayMin accepts only float array and double array."
    )
  }

  protected def getNanScalar: Scalar = dataType match {
    case FloatType => Scalar.fromFloat(Float.NaN)
    case DoubleType => Scalar.fromDouble(Double.NaN)
    case t => throw new IllegalStateException(s"dataType $t is not FloatType or DoubleType")
  }

  protected def getNullScalar: Scalar = dataType match {
    case FloatType => Scalar.fromNull(DType.FLOAT32)
    case DoubleType => Scalar.fromNull(DType.FLOAT64)
    case t => throw new IllegalStateException(s"dataType $t is not FloatType or DoubleType")
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    val listAll = SegmentedReductionAggregation.all()
    val listAny = SegmentedReductionAggregation.max()
    val base = input.getBase()

    withResource(base.getChildColumnView(0)) { child =>
      withResource(child.isNan()){ childIsNan =>
        // if all values in each list are nans or nulls
        val allNanOrNull = {
          val childIsNanOrNull = withResource(child.isNull()) {_.or(childIsNan)}
          withResource(childIsNanOrNull) { newChild =>
            withResource(base.replaceListChild(newChild)) {
              _.listReduce(listAll)
            }
          }
        }
        withResource(allNanOrNull){ allNanOrNull =>
          // return nan if the list contains nan, else return null
          val trueOption = {
            val anyNan = withResource(base.replaceListChild(childIsNan)) {
              _.listReduce(listAny)
            }
            withResource(anyNan) { anyNan =>
              withResource(getNanScalar) { nanScalar =>
                withResource(getNullScalar) { nullScalar =>
                  anyNan.ifElse(nanScalar, nullScalar)
                }
              }
            }
          }
          withResource(trueOption){ trueOption =>
            // replace all nans to nulls, and then find the min value.
            val falseOption = withResource(child.nansToNulls()) { nanToNullChild =>
              withResource(base.replaceListChild(nanToNullChild)) { nanToNullList =>
                nanToNullList.listReduce(SegmentedReductionAggregation.min())
              }
            }
            // if a list contains values other than nan or null
            // return `trueOption`, else return `falseOption`.
            withResource(falseOption){ falseOption =>
              allNanOrNull.ifElse(trueOption, falseOption)
            }
          }
        }
      }
    }
  }
}

object GpuArrayMax {
  def apply(child: Expression): GpuArrayMax = {
    child.dataType match {
      case ArrayType(FloatType | DoubleType, _) => GpuFloatArrayMax(child)
      case ArrayType(_, _) => GpuBasicArrayMax(child)
      case _ => throw new IllegalStateException(s"array_max accepts only arrays.")
    }
  }
}

abstract class GpuArrayMax(child: Expression) extends GpuUnaryExpression
  with ImplicitCastInputTypes
  with Serializable{

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType)

  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(dt, _) => dt
    case _ => throw new IllegalStateException(s"$prettyName accepts only arrays.")
  }

  override def prettyName: String = "array_max"

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector =
    input.getBase.listReduce(SegmentedReductionAggregation.max())
}

/** ArrayMax without `NaN` handling */
case class GpuBasicArrayMax(child: Expression) extends GpuArrayMax(child)

/** ArrayMax for FloatType and DoubleType to handle `Nan`s.
 *
 * In Spark, `Nan` is the max float value, however in cuDF, the calculation
 * involving `Nan` is undefined.
 * We design a workaround method here to match the Spark's behaviour.
 * The high level idea is that, we firstly check if each list contains `Nan`.
 * If it is, the max value is `Nan`, else we use the cuDF kernel to
 * calculate the max value.
 */
case class GpuFloatArrayMax(child: Expression) extends GpuArrayMax(child){
  @transient override lazy val dataType: DataType = child.dataType match {
    case ArrayType(FloatType, _) => FloatType
    case ArrayType(DoubleType, _) => DoubleType
    case _ => throw new IllegalStateException(
      s"GpuFloatArrayMax accepts only float array and double array."
    )
  }

  protected def getNanSalar: Scalar = dataType match {
    case FloatType => Scalar.fromFloat(Float.NaN)
    case DoubleType => Scalar.fromDouble(Double.NaN)
    case t => throw new IllegalStateException(s"dataType $t is not FloatType or DoubleType")
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    withResource(getNanSalar){nan =>
      withResource(input.getBase().listContains(nan)){hasNan =>
        withResource(input.getBase().listReduce(SegmentedReductionAggregation.max())) {max =>
          hasNan.ifElse(nan, max)
        }
      }
    }
  }
}

case class GpuArrayRepeat(left: Expression, right: Expression) extends GpuBinaryExpression {

  override def dataType: DataType = ArrayType(left.dataType, left.nullable)

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    // The primary issue of array_repeat is to workaround the null and negative count.
    // Spark returns a null (list) when encountering a null count, and an
    // empty list when encountering a negative count.
    // cudf does not handle these cases properly.

    // Step 1. replace invalid counts
    //  null -> 0
    //  negative values -> 0
    val refinedCount = withResource(GpuScalar.from(0, DataTypes.IntegerType)) { zero =>
      withResource(rhs.getBase.replaceNulls(zero)) { notNull =>
        withResource(notNull.lessThan(zero)) { lessThanZero =>
          lessThanZero.ifElse(zero, notNull)
        }
      }
    }
    // Step 2. perform cuDF repeat
    val repeated = closeOnExcept(refinedCount) { cnt =>
      withResource(new Table(lhs.getBase)) { table =>
        table.repeat(cnt).getColumn(0)
      }
    }
    // Step 3. generate list offsets from refined counts
    val offsets = closeOnExcept(repeated) { _ =>
      withResource(refinedCount) { cnt =>
        cnt.generateListOffsets()
      }
    }
    // Step 4. make the result list column with offsets and child column
    val list = withResource(offsets) { offsets =>
      withResource(repeated) { repeated =>
        repeated.makeListFromOffsets(lhs.getRowCount, offsets)
      }
    }
    // Step 5. merge the validity of count column to the result
    withResource(list) { list =>
      list.mergeAndSetValidity(BinaryOp.BITWISE_AND, rhs.getBase)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt, lhs.dataType)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val numRows = lhs.getRowCount.toInt
    if (!rhs.isValid) {
      GpuColumnVector.fromNull(numRows, dataType).getBase
    } else {
      val count = rhs.getValue.asInstanceOf[Int] max 0

      val offsets = withResource(GpuScalar.from(count, IntegerType)) { cntScalar =>
        withResource(GpuColumnVector.from(cntScalar, numRows, rhs.dataType)) { cnt =>
          cnt.getBase.generateListOffsets()
        }
      }

      withResource(offsets) { offsets =>
        withResource(new Table(lhs.getBase)) { table =>
          withResource(table.repeat(count).getColumn(0)) { repeated =>
            repeated.makeListFromOffsets(lhs.getRowCount, offsets)
          }
        }
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    if (!rhs.isValid) {
      GpuColumnVector.fromNull(numRows, dataType).getBase
    } else {
      withResource(GpuColumnVector.from(lhs, numRows, lhs.dataType)) { left =>
        doColumnar(left, rhs)
      }
    }
  }
}

case class GpuArraysZip(children: Seq[Expression]) extends GpuExpression with ShimExpression
  with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.length)(ArrayType)

  @transient override lazy val dataType: DataType = {
    val fields = children.zip(arrayElementTypes).zipWithIndex.map {
      case ((expr: NamedExpression, elementType), _) =>
        StructField(expr.name, elementType, nullable = true)
      case ((_, elementType), idx) =>
        StructField(idx.toString, elementType, nullable = true)
    }
    ArrayType(StructType(fields), containsNull = false)
  }

  override def nullable: Boolean = children.exists(_.nullable)

  @transient private lazy val arrayElementTypes =
    children.map(_.dataType.asInstanceOf[ArrayType].elementType)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector  = {
    val res = if (children.isEmpty) {
      GpuScalar(new GenericArrayData(Array.empty[Any]), dataType)
    } else {
      // Prepare input columns
      val inputs = children.safeMap { expr =>
        expr.columnarEval(batch).getBase
      }

      val cleanedInputs = withResource(inputs) { inputs =>
        normalizeNulls(inputs)
      }

      val padded = withResource(cleanedInputs) { cleanedInputs =>
        padArraysToMaxLength(cleanedInputs)
      }

      withResource(padded) { _ =>
        closeOnExcept(zipArrays(padded)) { ret =>
          GpuColumnVector.from(ret, dataType)
        }
      }
    }
    GpuExpressionsUtils.resolveColumnVector(res, batch.numRows())
  }

  /**
   * Segmented gather in CUDF produces a NULL output for a NULL input. But we need to produce
   * child columns that we can put together in a struct. This requires them all to have the
   * same length. To make this work we need to make sure all of the inputs have nulls in the
   * same places at the top level. That way when we gather things we get the same output
   * size for all of the children of the input columns. The result of this will have the same
   * validity for all top level columns, but possibly different offsets.
   */
  private def normalizeNulls(inputs: Seq[cudf.ColumnVector]): Seq[ColumnVector] = {
    // First let's figure out if there are any nulls at all, because if there are not we don't
    // need to do anything.
    if (inputs.exists(_.hasNulls)) {
      var nullOutput = inputs.head.isNull
      try {
        inputs.drop(1).foreach { cv =>
          val combinedIsNull = withResource(cv.isNull) { tmpIsNull =>
            tmpIsNull.or(nullOutput)
          }
          closeOnExcept(combinedIsNull) { _ =>
            nullOutput.close()
            nullOutput = combinedIsNull
          }
        }

        // input1: [[A, B, C], [D, E], [F], [G]]
        // input2: [[a, b], [c, d, e], null, [f, g]]
        // combinedIsNull, false, false, true, false

        // output1: [[A, B, C], [D, E], null, [G]]
        // output2: [[a, b], [c, d, e], null, [f, g]]

        inputs.zip(children).safeMap { case (cv, child) =>
          withResource(GpuScalar.from(null, child.dataType)) { nullArray =>
            nullOutput.ifElse(nullArray, cv)
          }
        }
      } finally {
        nullOutput.close()
      }
    } else {
      inputs.map(_.incRefCount())
    }
  }


  private def computeMaxArraySize(inputs: Seq[ColumnVector]): ColumnVector = {
    // Compute array sizes of input arrays
    val arraySizes = inputs.safeMap(_.countElements())

    // Pick max array size of each row.
    // Replace with zero if there exists null among input values.
    // [1, 3, 5, null, 4]
    // [2, 4, 3,    8, 0]   => [4, 4, 5, 0, 7]
    // [4, 2, 3,   10, 7]
    val arraySizeList = withResource(arraySizes) { sizes =>
      ColumnVector.makeList(sizes: _*)
    }
    val maxArraySizeWithNull = withResource(arraySizeList) { list =>
      list.listReduce(SegmentedReductionAggregation.max())
    }
    withResource(maxArraySizeWithNull) { max =>
      withResource(GpuScalar.from(0, IntegerType)) { zero =>
        max.replaceNulls(zero)
      }
    }
  }

  private def generateSeqIndices(maxArraySize: ColumnVector): ColumnVector = {
    withResource(GpuScalar.from(0, IntegerType)) { s =>
      withResource(ColumnVector.fromScalar(s, maxArraySize.getRowCount.toInt)) { zero =>
        ColumnVector.sequence(zero, maxArraySize)
      }
    }
  }

  /**
   * Do a segmented gather on the inputs so that they are padded with nulls to make sure each LIST
   * on the same row, at the top level, has the same length. The columns returned should have the
   * same offsets and the same validity. This assumes that the validity on the inputs all match.
   */
  private def padArraysToMaxLength(inputs: Seq[ColumnVector]): Seq[ColumnVector] = {
    // Compute max size of input arrays for each row, this is to know how we need to pad things.
    //
    // input1: [[A, B, C], [D, E], null, [G]]
    // input2: [[a, b], [c, d, e], null, [f, g]]
    // max array size: [3, 3, 0, 2]
    val seqIndices = withResource(computeMaxArraySize(inputs)) { maxArraySize =>
      // Generate sequence indices for gathering children of input arrays
      //
      // [3, 3, 0, 2] => [[0, 1, 2], [0, 1, 2], [], [0, 1]]
      generateSeqIndices(maxArraySize)
    }

    // Perform segment gather on input columns with indices covering each element
    //
    // input1: [[A, B, C], [D, E], null, [G]]
    // input2: [[a, b], [c, d, e], null, [f, g]]
    // indices: [[0, 1, 2], [0, 1, 2], [], [0, 1]]
    // output1: [[A, B, C], [D, E, null], null, [G, null]]
    // output2: [[a, b, null], [c, d, e], null, [f, g]]
    withResource(seqIndices) { _ =>
      inputs.safeMap { cv =>
        cv.segmentedGather(seqIndices)
      }
    }
  }

  /**
   * This turns LIST[X], LIST[Y], ... into a LIST[ STRUCT[X, Y, ...] ] but requires that
   * the input LIST columns all have the same validity and offsets.
   */
  private def zipArrays(padded: Seq[ColumnVector]): ColumnVector = {
    // Get the data column from the children, without any offsets
    withResource(padded.safeMap(_.getChildColumnView(0))) { children =>
      // Put them into a struct column view
      withResource(ColumnView.makeStructView(children: _*)) { structView =>
        // Make the struct a list using the input's offsets and validity
        // in the cheapest way possible.
        val proto = padded.head
        withResource(proto.getValid) { valid =>
          withResource(proto.getOffsets) { offsets =>
            withResource(new ColumnView(DType.LIST, proto.getRowCount,
              java.util.Optional.of[java.lang.Long](proto.getNullCount),
              valid, offsets, Array(structView))) { retView =>
              // Finally copy the result out to a ColumnVector so we can return it
              retView.copyToColumnVector()
            }
          }
        }
      }
    }
  }
}

// Base class for GpuArrayExcept, GpuArrayUnion, GpuArrayIntersect
trait GpuArrayBinaryLike extends GpuComplexTypeMergingExpression with NullIntolerant {
  val left: Expression
  val right: Expression

  @transient override final lazy val children: Seq[Expression] = IndexedSeq(left, right)

  def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector
  def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector
  def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector
  def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(left.columnarEvalAny(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEvalAny(batch)) { rhs =>
        (lhs, rhs) match {
          case (l: GpuColumnVector, r: GpuColumnVector) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuScalar, r: GpuColumnVector) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuColumnVector, r: GpuScalar) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuScalar, r: GpuScalar) =>
            GpuColumnVector.from(doColumnar(batch.numRows(), l, r), dataType)
          case (l, r) =>
            throw new UnsupportedOperationException(s"Unsupported data '($l: " +
              s"${l.getClass}, $r: ${r.getClass})' for GPU binary expression.")
        }
      }
    }
  }
}

case class GpuArrayExcept(left: Expression, right: Expression)
    extends GpuArrayBinaryLike with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
    case (ArrayType(ldt, _), ArrayType(rdt, _)) =>
      if (ldt.sameType(rdt)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"Array_intersect requires both array params to have the same subType: $ldt != $rdt")
      }
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    ColumnView.listsDifferenceDistinct(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt, lhs.dataType)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt, rhs.dataType)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, lhs.dataType)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows, rhs.dataType)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuArrayIntersect(left: Expression, right: Expression)
    extends GpuArrayBinaryLike with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
    case (ArrayType(ldt, _), ArrayType(rdt, _)) =>
      if (ldt.sameType(rdt)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"Array_intersect requires both array params to have the same subType: $ldt != $rdt")
      }
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    ColumnView.listsIntersectDistinct(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt, lhs.dataType)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt, rhs.dataType)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, lhs.dataType)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows, rhs.dataType)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuArrayUnion(left: Expression, right: Expression)
    extends GpuArrayBinaryLike with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
    case (ArrayType(ldt, _), ArrayType(rdt, _)) =>
      if (ldt.sameType(rdt)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"Array_union requires both array params to have the same subType: $ldt != $rdt")
      }
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    ColumnView.listsUnionDistinct(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt, lhs.dataType)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt, rhs.dataType)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, lhs.dataType)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows, rhs.dataType)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuArraysOverlap(left: Expression, right: Expression)
    extends GpuBinaryExpression with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, ArrayType)

  override def checkInputDataTypes(): TypeCheckResult =
    (left.dataType, right.dataType) match {
    case (ArrayType(ldt, _), ArrayType(rdt, _)) =>
      if (ldt.sameType(rdt)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"Array_union requires both array params to have the same subType: $ldt != $rdt")
      }
    case dt =>
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input, but found $dt")
  }

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    ColumnView.listsHaveOverlap(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt, lhs.dataType)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(rhs, lhs.getRowCount.toInt, rhs.dataType)) { right =>
      doColumnar(lhs, right)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, lhs.dataType)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows, rhs.dataType)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuArrayRemove(left: Expression, right: Expression) extends GpuBinaryExpression {

  override def dataType: DataType = left.dataType

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    // Handle special case for null entries in rhs, replace corresponding rows in lhs with null
    //
    // lhs: [[1, 2, null], [1, 2, 2], [2, 3]]
    // rhs: [1, null, 2]
    // lhsWithNull: [[1, 2, null], null, [2, 3]]
    val lhsWithNull = withResource(rhs.getBase.isNull) { rhsIsNull =>
      withResource(GpuScalar.from(null, dataType)) { nullList =>
        rhsIsNull.ifElse(nullList, lhs.getBase)
      }
    }
    // Repeat entries in rhs N times as N is number of elements in corresponding row in lhsWithNull
    //
    // lhsWithNull: [[1, 2, null], null, [2, 3]]
    // rhs: [1, null, 2]
    // repeatedRhs: [1, 1, 1, 2, 2]
    val repeatedRhs = explodeRhs(rhs.getBase, lhsWithNull.countElements)
    withResource(lhsWithNull) { lhsWithNull =>
      // Construct boolean mask where true values correspond to entries to keep
      //
      // lhsWithNull: [[1, 2, null], null, [2, 3]]
      // repeatedRhs: [1, 1, 1, 2, 2]
      // boolMask: [[F, T, T], null, [F, T]]
      val boolMask = constructBooleanMask(lhsWithNull.getChildColumnView(0), repeatedRhs,
                                          lhsWithNull.getListOffsetsView, lhs.getRowCount)
      withResource(boolMask) { boolMask =>
        lhsWithNull.applyBooleanMask(boolMask)
      }
    }
  }

  private def explodeRhs(rhs: ColumnVector, counts: ColumnVector): ColumnVector = {
    withResource(counts) { counts =>
      withResource(GpuScalar.from(0, DataTypes.IntegerType)) { zero =>
        withResource(counts.replaceNulls(zero)) { noNullCounts =>
          withResource(new Table(rhs)) { table =>
            table.repeat(noNullCounts).getColumn(0)
          }
        }
      }
    }
  }

  private def constructBooleanMask(lhs: ColumnView, rhs: ColumnView,
                                   offSets: ColumnView, rowCount: Long): ColumnVector = {
    withResource(lhs) { lhs =>
      withResource(rhs) { rhs =>
        val boolMaskNoNans = lhs.equalToNullAware(rhs)
        val boolMaskWithNans = if (lhs.getType == DType.FLOAT32 || lhs.getType == DType.FLOAT64) {
            // Compare NaN values for arrays with float or double type
            withResource(booleanMaskNansOnly(lhs, rhs)) { boolMaskNansOnly =>
              withResource(boolMaskNoNans) { boolMaskNoNans =>
                boolMaskNoNans.or(boolMaskNansOnly)
              }
            }
          } else {
            boolMaskNoNans
          }
        withResource(boolMaskWithNans) { boolMaskWithNans =>
          withResource(boolMaskWithNans.not) { boolMaskToKeep =>
            withResource(offSets) { offSets =>
              boolMaskToKeep.makeListFromOffsets(rowCount, offSets)
            }
          }
        }
      }
    }
  }

  private def booleanMaskNansOnly(lhs: ColumnView, rhs: ColumnView): ColumnVector = {
    withResource(lhs.isNan) { lhsIsNan =>
      withResource(rhs.isNan) { rhsIsNan =>
        lhsIsNan.and(rhsIsNan)
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, rhs.getRowCount.toInt, lhs.dataType)) { left =>
      doColumnar(left, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val lhsBase = lhs.getBase
    // Construct boolean mask where true values correspond to elements to keep
    val boolMask = withResource(lhsBase.getListOffsetsView) { offSets =>
      withResource(lhsBase.getChildColumnView(0)){ lhsFlatten =>
        withResource(lhsFlatten.equalToNullAware(rhs.getBase)) { boolMaskToRemove =>
          withResource(boolMaskToRemove.not) { boolMaskToKeep =>
            boolMaskToKeep.makeListFromOffsets(lhs.getRowCount, offSets)
          }
        }
      }
    }
    withResource(boolMask) { boolMask =>
      lhsBase.applyBooleanMask(boolMask)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, lhs.dataType)) { left =>
      withResource(GpuColumnVector.from(rhs, numRows, rhs.dataType)) { right =>
        doColumnar(left, right)
      }
    }
  }
}

case class GpuFlattenArray(child: Expression) extends GpuUnaryExpression with NullIntolerant {
  private def childDataType: ArrayType = child.dataType.asInstanceOf[ArrayType]
  override def nullable: Boolean = child.nullable || childDataType.containsNull
  override def dataType: DataType = childDataType.elementType
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.getBase.flattenLists
  }
}

class GpuSequenceMeta(
    expr: Sequence,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[Sequence](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    //  We have to fall back to the CPU if the timeZoneId is not UTC when
    //  we are processing date/timestamp.
    //  Date/Timestamp are not enabled right now so this is probably fine.
  }

  override def convertToGpu(): GpuExpression = {
    val (startExpr, stopExpr, stepOpt) = if (expr.stepOpt.isDefined) {
        val Seq(start, stop, step) = childExprs.map(_.convertToGpu())
        (start, stop, Some(step))
      } else {
        val Seq(start, stop) = childExprs.map(_.convertToGpu())
        (start, stop, None)
      }
    GpuSequence(startExpr, stopExpr, stepOpt, expr.timeZoneId)
  }
}

object GpuSequenceUtil {

  def checkSequenceInputs(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): Unit = {
    // Keep the same requirement with Spark:
    // (step > 0 && start <= stop) || (step < 0 && start >= stop) || (step == 0 && start == stop)
    withResource(Scalar.fromByte(0.toByte)) { zero =>
      // The check should ignore each row (Row(start, stop, step)) that contains at least
      // one null element according to Spark's code. Thanks to the cudf binary ops, who ignore
      // nulls already, skipping nulls can be done without any additional process.
      //
      // Because the filtered table (e.g. upTbl) in each rule check excludes the rows that the
      // step is null. Next a null row will be produced when comparing start or stop when any
      // of them is null, and the nulls are skipped in the final assertion 'isAllValidTrue'.
      withResource(new Table(start, stop)) { startStopTable =>
        // (step > 0 && start <= stop)
        val upTbl = withResource(step.greaterThan(zero)) { positiveStep =>
          startStopTable.filter(positiveStep)
        }
        val allUp = withResource(upTbl) { _ =>
          upTbl.getColumn(0).lessOrEqualTo(upTbl.getColumn(1))
        }
        withResource(allUp) { _ =>
          require(isAllValidTrue(allUp), "Illegal sequence boundaries: step > 0 but start > stop")
        }

        // (step < 0 && start >= stop)
        val downTbl = withResource(step.lessThan(zero)) { negativeStep =>
          startStopTable.filter(negativeStep)
        }
        val allDown = withResource(downTbl) { _ =>
          downTbl.getColumn(0).greaterOrEqualTo(downTbl.getColumn(1))
        }
        withResource(allDown) { _ =>
          require(isAllValidTrue(allDown),
            "Illegal sequence boundaries: step < 0 but start < stop")
        }

        // (step == 0 && start == stop)
        val equalTbl = withResource(step.equalTo(zero)) { zeroStep =>
          startStopTable.filter(zeroStep)
        }
        val allEq = withResource(equalTbl) { _ =>
          equalTbl.getColumn(0).equalTo(equalTbl.getColumn(1))
        }
        withResource(allEq) { _ =>
          require(isAllValidTrue(allEq),
            "Illegal sequence boundaries: step == 0 but start != stop")
        }
      }
    } // end of zero
  }

  /**
   * Compute the size of each sequence according to 'start', 'stop' and 'step'.
   * A row (Row[start, stop, step]) contains at least one null element will produce
   * a null in the output.
   *
   * The returned column should be closed.
   */
  def computeSequenceSize(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): ColumnVector = {
    checkSequenceInputs(start, stop, step)
    val actualSize = GetSequenceSize(start, stop, step)
    val sizeAsLong = withResource(actualSize) { _ =>
      val mergedEquals = withResource(start.equalTo(stop)) { equals =>
        if (step.hasNulls) {
          // Also set the row to null where step is null.
          equals.mergeAndSetValidity(BinaryOp.BITWISE_AND, equals, step)
        } else {
          equals.incRefCount()
        }
      }
      withResource(mergedEquals) { _ =>
        withResource(Scalar.fromLong(1L)) { one =>
          mergedEquals.ifElse(one, actualSize)
        }
      }
    }
    withResource(sizeAsLong) { _ =>
      // check max size
      withResource(Scalar.fromInt(MAX_ROUNDED_ARRAY_LENGTH)) { maxLen =>
        withResource(sizeAsLong.lessOrEqualTo(maxLen)) { allValid =>
          require(isAllValidTrue(allValid), GetSequenceSize.TOO_LONG_SEQUENCE)
        }
      }
      // cast to int and return
      sizeAsLong.castTo(DType.INT32)
    }
  }
}

case class GpuSequence(start: Expression, stop: Expression, stepOpt: Option[Expression],
    timeZoneId: Option[String] = None) extends TimeZoneAwareExpression with GpuExpression
    with ShimExpression {

  import GpuSequenceUtil._

  override def dataType: ArrayType = ArrayType(start.dataType, containsNull = false)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Some(timeZoneId))

  override def children: Seq[Expression] = Seq(start, stop) ++ stepOpt

  override def nullable: Boolean = children.exists(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  // can throw exceptions such as "Illegal sequence boundaries: step > 0 but start > stop"
  override def hasSideEffects: Boolean = true

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResource(start.columnarEval(batch)) { startGpuCol =>
      withResource(stepOpt.map(_.columnarEval(batch))) { stepGpuColOpt =>
        val startCol = startGpuCol.getBase

        // 1 Compute the sequence size for each row.
        val (sizeCol, stepCol) = withResource(stop.columnarEval(batch)) { stopGpuCol =>
          val stopCol = stopGpuCol.getBase
          val steps = stepGpuColOpt.map(_.getBase.incRefCount())
              .getOrElse(defaultStepsFunc(startCol, stopCol))
          closeOnExcept(steps) { _ =>
            (computeSequenceSize(startCol, stopCol, steps), steps)
          }
        }

        // 2 Generate the sequence
        //
        // cudf 'sequence' requires 'step' has the same type with 'start'.
        // And the step type may differ due to the default steps.
        val castedStepCol = withResource(stepCol) { _ =>
          closeOnExcept(sizeCol) { _ =>
            stepCol.castTo(startCol.getType)
          }
        }
        withResource(Seq(sizeCol, castedStepCol)) { _ =>
          GpuColumnVector.from(genSequence(startCol, sizeCol, castedStepCol), dataType)
        }
      }
    }
  }

  @transient
  private lazy val defaultStepsFunc: (ColumnView, ColumnView) => ColumnVector =
      dataType.elementType match {
    case _: IntegralType =>
      // Default step:
      //   start > stop, step == -1
      //   start <= stop, step == 1
      (starts, stops) => {
        // It is ok to always use byte, since it will be casted to the same type before
        // going into cudf sequence. Besides byte saves memory, and does not cause any
        // type promotion during computation.
        withResource(Scalar.fromByte((-1).toByte)) { minusOne =>
          withResource(Scalar.fromByte(1.toByte)) { one =>
            withResource(starts.greaterThan(stops)) { decrease =>
              decrease.ifElse(minusOne, one)
            }
          }
        }
      }
    // Timestamp and Date will come soon
    // case TimestampType =>
    // case DateType =>
  }

  private def genSequence(
      start: ColumnView,
      size: ColumnView,
      step: ColumnView): ColumnVector = {
    // size is calculated from start, stop and step, so its validity mask is equal to
    // the merged validity of the three columns, and can be used as the final output
    // validity mask directly.
    // Then checking nulls only in size column is enough.
    if(size.getNullCount > 0) {
      // Nulls are not acceptable in cudf 'list::sequences'. (Pls refer to
      //     https://github.com/rapidsai/cudf/issues/10012),
      //
      // So replace the nulls with 0 for size, and create temp views for start and
      // stop with forcing null count to be 0.
      val sizeNoNull = withResource(Scalar.fromInt(0)) { zero =>
        size.replaceNulls(zero)
      }
      val ret = withResource(sizeNoNull) { _ =>
        val startNoNull = new ColumnView(start.getType, start.getRowCount, Optional.of(0L),
          start.getData, null)
        withResource(startNoNull) { _ =>
          val stepNoNull = new ColumnView(step.getType, step.getRowCount, Optional.of(0L),
            step.getData, null)
          withResource(stepNoNull) { _ =>
            ColumnVector.sequence(startNoNull, sizeNoNull, stepNoNull)
          }
        }
      }
      withResource(ret) { _ =>
        // Restore the null rows by setting the validity mask.
        ret.mergeAndSetValidity(BinaryOp.BITWISE_AND, size)
      }
    } else {
      ColumnVector.sequence(start, size, step)
    }
  }
}
