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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat,
  Long => JLong, Short => JShort}
import java.util
import java.util.{List => JList, Objects}

import scala.collection.JavaConverters._

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector, Scalar}
import javax.xml.bind.DatatypeConverter
import org.json4s.JsonAST.{JField, JNull, JString}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.internal.SQLConf
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

object GpuScalar extends Arm {

  @deprecated("This will be removed. Since no need to infer the type again because" +
    " Spark does it already. Besides it is difficult to support nested type.", "")
  def scalaTypeToDType(v: Any): DType = {
    v match {
      case _: Long => DType.INT64
      case _: Double => DType.FLOAT64
      case _: Int => DType.INT32
      case _: Float => DType.FLOAT32
      case _: Short => DType.INT16
      case _: Byte => DType.INT8
      case _: Boolean => DType.BOOL8
      case _: String | _: UTF8String => DType.STRING
      case _ =>
        throw new IllegalArgumentException(s"${v.getClass} '$v' is not supported as a scalar yet")
    }
  }

  def extract(v: Scalar): Any = v.getType match {
    // We are not going to support list type for this 'extract', which is used for
    // Scalar unit tests only now.
    // It is not a good idea pulling data out of GPU unless there is no other ways to go.
    // And all the GPU expressions/operators should take care of the result of an expression,
    // it can be a Scalar or ColumnVector or Any.
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

  def castDateScalarToInt(s: Scalar): Scalar = {
    assert(s.getType == DType.TIMESTAMP_DAYS)
    if (s.isValid) {
      Scalar.fromInt(s.getInt)
    } else {
      Scalar.fromNull(DType.INT32)
    }
  }

  /**
   * Resolves a cudf `HostColumnVector.DataType` from a Spark `DataType`.
   * The returned type will be used by the `ColumnVector.fromXXX` family.
   */
  private def resolveElementType(dt: DataType): HostColumnVector.DataType = dt match {
    case ArrayType(elementType, _) =>
      new HostColumnVector.ListType(true, resolveElementType(elementType))
    case StructType(fields) =>
      new HostColumnVector.StructType(true, fields.map(f => resolveElementType(f.dataType)): _*)
    case other =>
      new HostColumnVector.BasicType(true, GpuColumnVector.getNonNestedRapidsType(other))
  }

  /** Casts each element in `data` array to type T */
  private def castTo[T](data: Array[Any]): Seq[T] = data.map(_.asInstanceOf[T])

  /** Casts each element in `data` array to type T, and applies 'f' on the casted element */
  private def castTo[T, R](data: Array[Any])(f: T => R): Seq[R] =
    data.map(e => f(e.asInstanceOf[T]))

  /** Applies 'f' on each element in `data` array, and casts the result to type T*/
  private def castTo[T](f: Any => Any)(data: Array[Any]): Seq[T] =
    data.map(e => f(e).asInstanceOf[T])

  /**
   * The "convertXXXTo" functions are used to convert the array data from catalyst type
   * to the Java type required by the `ColumnVector.fromXXX` family.
   *
   * The data received from the CPU `Literal` has been converted to catalyst type already.
   * The mapping from DataType to catalyst data type can be found in the function
   * 'validateLiteralValue' in Spark.
   */
  private def convertUTF8StringTo(us: UTF8String): String = us match {
    case null => null
    case s => s.asInstanceOf[UTF8String].toString
  }

  private def convertDecimalTo(dec: Decimal, dt: DecimalType): JLong = dec match {
    case null => null
    case d if d.precision > dt.precision =>
      throw new IllegalArgumentException(s"Decimal $d exceeds precision constraint of $dt")
    case _ => dec.toUnscaledLong
  }

  /** Converts an element for nested lists */
  private def convertElementTo(element: Any, elementType: DataType): Any = elementType match {
    case StringType => convertUTF8StringTo(element.asInstanceOf[UTF8String])
    case dt: DecimalType =>
      if (DecimalType.is32BitDecimalType(dt)) {
        convertDecimalTo(element.asInstanceOf[Decimal], dt).intValue()
      } else {
        convertDecimalTo(element.asInstanceOf[Decimal], dt)
      }
    case ArrayType(eType, _) => element.asInstanceOf[ArrayData] match {
      case null => null
      case ar => ar.array.map(convertElementTo(_, eType)).toList.asJava
    }
    case StructType(fields) => element.asInstanceOf[InternalRow] match {
      case null => null
      case row =>
        val data = fields.zipWithIndex.map { case (f, id) =>
          convertElementTo(row.get(id, f.dataType), f.dataType)
        }
        new HostColumnVector.StructData(data.asInstanceOf[Array[Object]]: _*)
    }
    case _ => element
  }

  /**
   * Creates a cudf Scalar from a ArrayData.
   *
   * It does not handle null because null is filtered out in 'from'.
   *
   * NOTE: This is done by leveraging the `ColumnVector.fromXXX` API family. These APIs are
   * designed for test only according to its doc, but it is ok to use here, since the size
   * of the literal values will not be large in most cases.
   *
   * @param data the data to build the Scalar. Should not be null.
   * @param elementType the element type
   * @return a cudf Scalar contains the array data.
   */
  private def createScalarFromArray(data: ArrayData, elementType: DataType): Scalar = {
    val array = data.array
    val listView = elementType match {
      // Uses the boxed version for primitive types to keep the nulls.
      case ByteType => ColumnVector.fromBoxedBytes(castTo[JByte](array): _*)
      case DateType => ColumnVector.timestampDaysFromBoxedInts(castTo[Integer](array): _*)
      case LongType => ColumnVector.fromBoxedLongs(castTo[JLong](array): _*)
      case ShortType => ColumnVector.fromBoxedShorts(castTo[JShort](array): _*)
      case FloatType => ColumnVector.fromBoxedFloats(castTo[JFloat](array): _*)
      case DoubleType => ColumnVector.fromBoxedDoubles(castTo[JDouble](array): _*)
      case IntegerType => ColumnVector.fromBoxedInts(castTo[Integer](array): _*)
      case BooleanType => ColumnVector.fromBoxedBooleans(castTo[JBoolean](array): _*)
      case TimestampType =>
        ColumnVector.timestampMicroSecondsFromBoxedLongs(castTo[JLong](array): _*)
      case StringType =>
        ColumnVector.fromStrings(castTo[UTF8String, String](array)(convertUTF8StringTo(_)): _*)
      case dt: DecimalType =>
        if (DecimalType.is32BitDecimalType(dt)) {
          val rows = castTo[Decimal, Integer](array)(convertDecimalTo(_, dt).intValue())
          ColumnVector.decimalFromBoxedInts(-dt.scale, rows: _*)
        } else {
          val rows = castTo[Decimal, JLong](array)(convertDecimalTo(_, dt))
          ColumnVector.decimalFromBoxedLongs(-dt.scale, rows: _*)
        }
      case ArrayType(_, _) =>
        val colType = resolveElementType(elementType)
        val rows = castTo[JList[_]](convertElementTo(_, elementType))(array)
        ColumnVector.fromLists(colType, rows: _*)
      case StructType(_) =>
        val colType = resolveElementType(elementType)
        val rows = castTo[HostColumnVector.StructData](convertElementTo(_, elementType))(array)
        ColumnVector.fromStructs(colType, rows: _*)
      case u =>
        throw new IllegalStateException(s"Unsupported element type ($u) for the list scalar.")
    }

    withResource(listView) { list =>
      Scalar.listFromColumnView(list);
    }
  }

  @deprecated("This will be removed. Since no need to infer the type again because" +
    " Spark does it already. Besides it is difficult to support nested type.", "")
  def from(v: Any): Scalar = v match {
    case s: Scalar => s.incRefCount()
    case _ if v == null => Scalar.fromNull(scalaTypeToDType(v))
    case l: Long => Scalar.fromLong(l)
    case d: Double => Scalar.fromDouble(d)
    case i: Int => Scalar.fromInt(i)
    case f: Float => Scalar.fromFloat(f)
    case s: Short => Scalar.fromShort(s)
    case b: Byte => Scalar.fromByte(b)
    case b: Boolean => Scalar.fromBool(b)
    case s: String => Scalar.fromString(s)
    case s: UTF8String => Scalar.fromString(s.toString)
    case dec: Decimal =>
      if (dec.precision <= Decimal.MAX_INT_DIGITS) {
        Scalar.fromDecimal(-dec.scale, dec.toUnscaledLong.toInt)
      } else {
        Scalar.fromDecimal(-dec.scale, dec.toUnscaledLong)
      }
    case dec: BigDecimal =>
      Scalar.fromDecimal(-dec.scale, dec.bigDecimal.unscaledValue().longValueExact())
    case _: ArrayData =>
      // Not sure why there are two independent `from`s. A little confused that why this one
      // is needed since Spark has already inferred the DataType for the literal value.
      throw new IllegalStateException("Please calls 'from(Any, DataType)' instead to" +
        " create a Scalar of array type.")
    case _ =>
      throw new IllegalStateException(s"${v.getClass} '$v' is not supported as a scalar yet")
  }

  def from(v: Any, t: DataType): Scalar = v match {
    case s: Scalar => s.incRefCount()
    case _ if v == null => Scalar.fromNull(GpuColumnVector.sparkDataTypeToRapidsType(t))
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
        throw new IllegalArgumentException(s"BigDecimal $bigDec exceeds precision constraint of $t")
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
    case array: ArrayData => t match {
      case ArrayType(e, _) => createScalarFromArray(array, e)
      case _ => throw new IllegalArgumentException(s"$t not supported for array values")
    }
    case _ =>
      throw new IllegalStateException(s"${v.getClass} '$v' is not supported as a scalar yet")
  }

  def isNan(s: Scalar): Boolean = {
    if (s == null) throw new NullPointerException("Null scalar passed")
    s.getType match {
      case DType.FLOAT32 => s.isValid && s.getFloat.isNaN()
      case DType.FLOAT64 => s.isValid && s.getDouble.isNaN()
      case t => throw new IllegalStateException(s"$t is doesn't support NaNs")
    }
  }
}

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

  override def columnarEval(batch: ColumnarBatch): Any = value
}

class LiteralExprMeta(
    lit: Literal,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends ExprMeta[Literal](lit, conf, p, r) {

  override def convertToGpu(): GpuExpression = GpuLiteral(lit.value, lit.dataType)

  // There are so many of these that we don't need to print them out, unless it
  // will not work on the GPU
  override def print(append: StringBuilder, depth: Int, all: Boolean): Unit = {
    if (!this.canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan) {
      super.print(append, depth, all)
    }
  }
}
