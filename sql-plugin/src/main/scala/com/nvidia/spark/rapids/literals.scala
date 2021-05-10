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

package com.nvidia.spark.rapids

import java.util
import java.util.Objects
import javax.xml.bind.DatatypeConverter

import scala.reflect.runtime.universe.TypeTag

import ai.rapids.cudf.{DType, Scalar}
import org.json4s.JsonAST.{JField, JNull, JString}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuCreateArray
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object LiteralHelper {
  def apply(v: Any): Literal = v match {
    case u: UTF8String => Literal(u, StringType)
    case allOthers => Literal(allOthers)
  }
}

object GpuScalar extends Arm with Logging {

  def extract(v: Scalar): Any = {
    logWarning(s"Extracting data from the Scalar $v.")
    v.getType match {
      case DType.BOOL8 => v.getBoolean
      case DType.FLOAT32 => v.getFloat
      case DType.FLOAT64 => v.getDouble
      case DType.INT8 => v.getByte
      case DType.INT16 => v.getShort
      case DType.INT32 => v.getInt
      case DType.INT64 => v.getLong
      case DType.TIMESTAMP_DAYS => v.getInt
      case DType.TIMESTAMP_MICROSECONDS => v.getLong
      case DType.STRING => v.getJavaString
      case dt: DType if dt.isDecimalType => Decimal(v.getBigDecimal)
      case t => throw new IllegalStateException(s"$t is not a supported rapids scalar type yet")
    }
  }

  def castDateScalarToInt(s: Scalar): Scalar = {
    assert(s.getType == DType.TIMESTAMP_DAYS)
    if (s.isValid) {
      Scalar.fromInt(s.getInt)
    } else {
      Scalar.fromNull(DType.INT32)
    }
  }

  /**
   * The two `from` APIs are used to create a cudf Scalar from a Scala `Any`.
   *
   * Some nodes (e.g. nodes handling strings and predictions) require a cudf Scalar created
   * from a literal value to run their computations. We do not want to go through the
   * GpuLiteral to get one, so keep the APIs here.
   *
   * The parameter `converted` indicates whether the data `v` has been converted to the
   * catalyst type. It can be set to true for most simple types,e.g. Int, Float, String.
   * It is also true when the data is from GpuLiteral.
   * However for nested types(will come soon), setting to false is recommended.
   *
   * The returned Scalar should be closed to avoid memory leak.
   */
  def from(v: Any, t: DataType): Scalar = from(v, t, true)

  def from(v: Any, t: DataType, converted: Boolean): Scalar = {
    // Like the Spark, here also needs to do data conversion and type check before going ahead,
    // leveraging the Spark APIs.
    val convertedV = if (converted) {
      v
    } else {
      // `CatalystTypeConverters.convertToCatalyst(v)` is public but the `validateLiteralValue`
      // is not, so we have to get these done by creating a Literal first.
      GpuLiteral.create(v, t).value
    }
    convertedV match {
      case null => Scalar.fromNull(GpuColumnVector.getNonNestedRapidsType(t))
      case _ if t.isInstanceOf[DecimalType] =>
        var bigDec = v match {
          case vv: Decimal => vv.toBigDecimal.bigDecimal
          case vv: BigDecimal => vv.bigDecimal
          case vv: Double => BigDecimal(vv).bigDecimal
          case vv: Float => BigDecimal(vv.toDouble).bigDecimal
          case vv: String => BigDecimal(vv).bigDecimal
          case vv: Long => BigDecimal(vv).bigDecimal
          case vv: Int => BigDecimal(vv).bigDecimal
          case vv => throw new IllegalStateException(
            s"${vv.getClass} '$vv' is not supported as a scalar yet")
        }
        bigDec = bigDec.setScale(t.asInstanceOf[DecimalType].scale)
        if (bigDec.precision() > t.asInstanceOf[DecimalType].precision) {
          throw new IllegalArgumentException(s"BigDecimal $bigDec exceeds precision" +
            s" constraint of $t")
        }
        if (!DecimalType.is32BitDecimalType(t.asInstanceOf[DecimalType])) {
          Scalar.fromDecimal(-bigDec.scale(), bigDec.unscaledValue().longValue())
        } else {
          Scalar.fromDecimal(-bigDec.scale(), bigDec.unscaledValue().intValue())
        }
      case l: Long => t match {
        case LongType => Scalar.fromLong(l)
        case TimestampType => Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, l)
        case _ => throw new IllegalArgumentException(s"$t not supported for long values")
      }
      case d: Double => Scalar.fromDouble(d)
      case i: Int => t match {
        case IntegerType => Scalar.fromInt(i)
        case DateType => Scalar.timestampDaysFromInt(i)
        case _ => throw new IllegalArgumentException(s"$t not supported for int values")
      }
      case f: Float => Scalar.fromFloat(f)
      case s: Short => Scalar.fromShort(s)
      case b: Byte => Scalar.fromByte(b)
      case b: Boolean => Scalar.fromBool(b)
      case s: String => Scalar.fromString(s)
      case s: UTF8String => Scalar.fromString(s.toString)
      case _ =>
        throw new IllegalStateException(s"${v.getClass} '$v' is not supported as a scalar yet")
    }
  }

  def isNan(s: Scalar): Boolean = {
    if (s == null) throw new NullPointerException("Null scalar passed")
    s.getType match {
      case DType.FLOAT32 => s.isValid && s.getFloat.isNaN()
      case DType.FLOAT64 => s.isValid && s.getDouble.isNaN()
      case t => throw new IllegalStateException(s"$t is doesn't support NaNs")
    }
  }

  /**
   * The following 2 APIs are used to create a GpuScalar from a Scala `Any`.
   *
   * If the `Any` is a cudf Scalar, it will be taken over by the returned GpuScalar,
   * so do not close it.
   * But the returned GpuScalar should be closed to avoid memory leak.
   */
  def apply(any: Any, dataType: DataType): GpuScalar =
    GpuScalar(any, dataType, true)

  def apply(any: Any, dataType: DataType, converted: Boolean): GpuScalar = any match {
    case s: Scalar => wrap(s, dataType)
    case o => new GpuScalar(null, Some(o), dataType, converted)
  }

  /**
   * Creates a GpuScalar from a cudf Scalar.
   *
   * This will not increase the reference count of the input cudf Scalar. Users should
   * close either the cudf Scalar or the returned GpuScalar, not both.
   */
  def wrap(scalar: Scalar, dataType: DataType): GpuScalar = {
    assert(scalar != null, "The cudf Scalar should NOT be null.")
    assert(typeConversionAllowed(scalar, dataType), s"Type conversion is not allowed from " +
      s" $scalar to $dataType")
    new GpuScalar(scalar, None, dataType, true)
  }

  private def typeConversionAllowed(s: Scalar, sType: DataType): Boolean = {
    s.getType match {
      case dt if dt.isNestedType =>
        sType match {
          // Now supports only list for nested type.
          case ArrayType(elementType, _) =>
            if (DType.LIST.equals(dt)) {
              withResource(s.getListAsColumnView) { elementView =>
                GpuColumnVector.typeConversionAllowed(elementView, elementType)
              }
            } else {
              false
            }
          case _ => false // Unsupported type
        }
      case nonNested =>
        GpuColumnVector.getNonNestedRapidsType(sType).equals(nonNested)
    }
  }
}

/**
 * The wrapper of a Scala value and its corresponding cudf Scalar, along with its DataType.
 *
 * This class is introduced because many expressions require both the cudf Scalar and its
 * corresponding Scala value to complete their computations. e.g. 'GpuStringSplit',
 * 'GpuStringLocate', 'GpuDivide', 'GpuDateAddInterval', 'GpuTimeMath' ...
 * So only either a cudf Scalar or a Scala value can not support such cases, unless copying data
 * between the host and the device each time being asked for.
 *
 * This GpuScalar can be created from either a cudf Scalar or a Scala value. By initializing the
 * cudf Scalar or the Scala value lazily and caching them after being created, it can reduce the
 * unnecessary data copies.
 *
 * If a GpuScalar is created from a Scala value and is used only on the host side, there will be
 * no data copy and no cudf Scalar created. And if it is used on the device side, only need to
 * copy data to the device once to create a cudf Scalar.
 *
 * Similarly, if a GpuScalar is created from a cudf Scalar, no need to copy data to the host if
 * it is used only on the device side (This is the ideal case we like, since all is on the GPU).
 * And only need to copy the data to the host once if it is used on the host side.
 *
 * So a GpuScalar will have at most one data copy but support all the cases. No round-trip
 * happens.
 *
 * Another reason why storing the Scala value in addition to the cudf Scalar is
 * `GpuDateAddInterval` and 'GpuTimeMath' have different algorithms with the 3 members of
 * a `CalendarInterval`, which can not be supported by a single cudf Scalar now.
 *
 * Do not create a GpuScalar from the constructor, instead call the factory APIs above.
 */
class GpuScalar private(
    private var scalar: Scalar,
    private var value: Option[Any],
    val dataType: DataType, converted: Boolean) extends Arm with AutoCloseable {

  assert(scalar != null || value.nonEmpty, "GpuScalar requires at least a value or a Scalar")
  assert(value.isEmpty || !value.get.isInstanceOf[Scalar], "Value should not be Scalar")

  /**
   * Gets the internal cudf Scalar of this GpuScalar.
   *
   * This will not increase any reference count. So users need to close either the GpuScalar or
   * the return cudf Scalar, not both.
   */
  def getBase: Scalar = {
    if (scalar == null) {
      scalar = GpuScalar.from(value.get, dataType, converted)
    }
    scalar
  }

  /**
   * Gets the internal Scala value of this GpuScalar.
   */
  def getValue: Any = {
    if (value.isEmpty) {
      value = Some(GpuScalar.extract(scalar))
    }
    value.get
  }

  /**
   * A handy API to get the validity of the internal cudf Scalar, who will be initialized
   * automatically when needed.
   */
  def isValid: Boolean = getBase.isValid

  /**
   * A handy API to increase the reference count of the internal cudf Scalar, who will be
   * initialized automatically when needed.
   */
  def incRefCount: this.type = {
    getBase.incRefCount()
    this
  }

  override def close(): Unit = {
    if (scalar != null) scalar.close()
  }

}

object GpuLiteral {
  /**
   * The following 3 APIs are used to create a `GpuLiteral` from a Scala value, by leveraging the
   * corresponding CPU APIs to do the data conversion and type checking, which are quite
   * complicated. Fortunately Spark does this for us.
   */
  def apply(v: Any): GpuLiteral = {
    val cpuLiteral = Literal(v)
    GpuLiteral(cpuLiteral.value, cpuLiteral.dataType)
  }

  def create(value: Any, dataType: DataType): GpuLiteral = {
    val cpuLiteral = Literal.create(value, dataType)
    GpuLiteral(cpuLiteral.value, cpuLiteral.dataType)
  }

  def create[T : TypeTag](v: T): GpuLiteral = {
    val cpuLiteral = Literal.create(v)
    GpuLiteral(cpuLiteral.value, cpuLiteral.dataType)
  }
}

/**
 * In order to do type conversion and checking, use GpuLiteral.create() instead of constructor.
 */
case class GpuLiteral (value: Any, dataType: DataType) extends GpuLeafExpression {

  // Assume this came from Spark Literal and no need to call Literal.validateLiteralValue here.

  override def foldable: Boolean = true
  override def nullable: Boolean = value == null

  override def toString: String = value match {
    case null => "null"
    case binary: Array[Byte] => s"0x" + DatatypeConverter.printHexBinary(binary)
    case other => other.toString
  }

  override def hashCode(): Int = {
    val valueHashCode = value match {
      case null => 0
      case binary: Array[Byte] => util.Arrays.hashCode(binary)
      case other => other.hashCode()
    }
    31 * Objects.hashCode(dataType) + valueHashCode
  }

  override def equals(other: Any): Boolean = other match {
    case o: GpuLiteral if !dataType.equals(o.dataType) => false
    case o: GpuLiteral =>
      (value, o.value) match {
        case (null, null) => true
        case (a: Array[Byte], b: Array[Byte]) => util.Arrays.equals(a, b)
        case (a, b) => a != null && a.equals(b)
      }
    case _ => false
  }

  override protected def jsonFields: List[JField] = {
    // Turns all kinds of literal values to string in json field, as the type info is hard to
    // retain in json format, e.g. {"a": 123} can be an int, or double, or decimal, etc.
    val jsonValue = (value, dataType) match {
      case (null, _) => JNull
      case (i: Int, DateType) => JString(DateTimeUtils.toJavaDate(i).toString)
      case (l: Long, TimestampType) => JString(DateTimeUtils.toJavaTimestamp(l).toString)
      case (other, _) => JString(other.toString)
    }
    ("value" -> jsonValue) :: ("dataType" -> TrampolineUtil.jsonValue(dataType)) :: Nil
  }

  override def sql: String = (value, dataType) match {
    case (_, NullType | _: ArrayType | _: MapType | _: StructType) if value == null => "NULL"
    case _ if value == null => s"CAST(NULL AS ${dataType.sql})"
    case (v: UTF8String, StringType) =>
      // Escapes all backslashes and single quotes.
      "'" + v.toString.replace("\\", "\\\\").replace("'", "\\'") + "'"
    case (v: Byte, ByteType) => v + "Y"
    case (v: Short, ShortType) => v + "S"
    case (v: Long, LongType) => v + "L"
    // Float type doesn't have a suffix
    case (v: Float, FloatType) =>
      val castedValue = v match {
        case _ if v.isNaN => "'NaN'"
        case Float.PositiveInfinity => "'Infinity'"
        case Float.NegativeInfinity => "'-Infinity'"
        case _ => v
      }
      s"CAST($castedValue AS ${FloatType.sql})"
    case (v: Double, DoubleType) =>
      v match {
        case _ if v.isNaN => s"CAST('NaN' AS ${DoubleType.sql})"
        case Double.PositiveInfinity => s"CAST('Infinity' AS ${DoubleType.sql})"
        case Double.NegativeInfinity => s"CAST('-Infinity' AS ${DoubleType.sql})"
        case _ => v + "D"
      }
    case (v: Decimal, _: DecimalType) => v + "BD"
    case (v: Int, DateType) =>
      val formatter = DateFormatter(DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
      s"DATE '${formatter.format(v)}'"
    case (v: Long, TimestampType) =>
      val formatter = TimestampFormatter.getFractionFormatter(
        DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
      s"TIMESTAMP('${formatter.format(v)}')"
    case (v: Array[Byte], BinaryType) => s"X'${DatatypeConverter.printHexBinary(v)}'"
    case _ => value.toString
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    // Returns a Scalar instead of the value to support the scalar of nested type, and
    // simplify the handling of result from a `expr.columnarEval`.
    GpuScalar(value, dataType, true)
  }
}

class LiteralExprMeta(
    lit: Literal,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[Literal](lit, conf, p, r) {

  override def convertToGpu(): GpuExpression = {
    lit.dataType match {
      // NOTICE: There is a temporary transformation from Literal(ArrayType(BaseType)) into
      // CreateArray(Literal(BaseType):_*). The transformation is a walkaround support for Literal
      // of ArrayData under GPU runtime, because cuDF scalar doesn't support nested types.
      // related issue: https://github.com/NVIDIA/spark-rapids/issues/1902
      case ArrayType(baseType, _) =>
        val litArray = lit.value.asInstanceOf[ArrayData]
          .array.map(GpuLiteral(_, baseType))
        GpuCreateArray(litArray, useStringTypeWhenEmpty = false)
      case _ =>
        GpuLiteral(lit.value, lit.dataType)
    }
  }

  // There are so many of these that we don't need to print them out, unless it
  // will not work on the GPU
  override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {
    if (!this.canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan) {
      super.print(append, depth, all)
    }
  }
}
