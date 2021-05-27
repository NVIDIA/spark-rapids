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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.util
import java.util.{List => JList, Objects}
import javax.xml.bind.DatatypeConverter

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector, Scalar}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import org.json4s.JsonAST.{JField, JNull, JString}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, DateTimeUtils, MapData, TimestampFormatter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuScalar extends Arm with Logging {

  // TODO Support interpreting the value to a Spark DataType
  def extract(v: Scalar): Any = {
    if (v != null && v.isValid) {
      logDebug(s"Extracting data from the Scalar $v.")
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
        case DType.STRING => UTF8String.fromBytes(v.getUTF8)
        case dt: DType if dt.isDecimalType => Decimal(v.getBigDecimal)
        // Extracting data for a list scalar is not supported, until there is a requirement
        // from expression computations on host side.
        // For now, it is only used to expand to a list column vector.
        case t => throw new UnsupportedOperationException(s"Extracting data from a cudf Scalar" +
          s" is not supported for type $t.")
      }
    } else {
      null
    }
  }

  /**
   * Resolves a cudf `HostColumnVector.DataType` from a Spark `DataType`.
   * The returned type will be used by the `ColumnVector.fromXXX` family.
   */
  private[rapids] def resolveElementType(dt: DataType): HostColumnVector.DataType = dt match {
    case ArrayType(elementType, _) =>
      new HostColumnVector.ListType(true, resolveElementType(elementType))
    case StructType(fields) =>
      new HostColumnVector.StructType(true, fields.map(f => resolveElementType(f.dataType)): _*)
    case MapType(keyType, valueType, _) =>
      new HostColumnVector.ListType(true,
        new HostColumnVector.StructType(true,
          resolveElementType(keyType),
          resolveElementType(valueType)))
    case other =>
      new HostColumnVector.BasicType(true, GpuColumnVector.getNonNestedRapidsType(other))
  }

  /**
   * The "convertXXXTo" functions are used to convert the data from Catalyst type
   * to the Java type required by the `ColumnVector.fromXXX` family.
   *
   * The data received from the CPU `Literal` has been converted to Catalyst type already.
   * The mapping from DataType to Catalyst data type can be found in the function
   * 'validateLiteralValue' in Spark.
   */
  /** Converts a decimal, `dec` should not be null. */
  private def convertDecimalTo(dec: Decimal, dt: DecimalType): Either[Integer, JLong] = {
    if (dec.scale > dt.scale) {
      throw new IllegalArgumentException(s"Unexpected decimals rounding.")
    }
    if (!dec.changePrecision(dt.precision, dt.scale)) {
      throw new IllegalArgumentException(s"Cannot change precision to $dt for decimal: $dec")
    }
    if (DecimalType.is32BitDecimalType(dt)) {
      Left(dec.toUnscaledLong.toInt)
    } else {
      Right(dec.toUnscaledLong)
    }
  }

  /** Converts an element for nested lists */
  private def convertElementTo(element: Any, elementType: DataType): Any = elementType match {
    case _ if element == null => null
    case StringType => element.asInstanceOf[UTF8String].getBytes
    case dt: DecimalType => convertDecimalTo(element.asInstanceOf[Decimal], dt) match {
      case Left(i) => i
      case Right(l) => l
    }
    case ArrayType(eType, _) =>
      val data = element.asInstanceOf[ArrayData]
      data.array.map(convertElementTo(_, eType)).toList.asJava
    case StructType(fields) =>
      val data = element.asInstanceOf[InternalRow]
      val row = fields.zipWithIndex.map { case (f, id) =>
        convertElementTo(data.get(id, f.dataType), f.dataType)
      }
      new HostColumnVector.StructData(row.asInstanceOf[Array[Object]]: _*)
    case MapType(keyType, valueType, _) =>
      val data = element.asInstanceOf[MapData]
      val keys = data.keyArray.array.map(convertElementTo(_, keyType)).toList
      val values = data.valueArray.array.map(convertElementTo(_, valueType)).toList
      keys.zip(values).map {
        case (k, v) => new HostColumnVector.StructData(
          k.asInstanceOf[Object],
          v.asInstanceOf[Object])
      }.asJava
    case _ => element
  }

  /**
   * Creates a cudf ColumnVector from the literal values in `seq`.
   *
   * @param seq the sequence of the literal values.
   * @param elementType the data type of each value in the `seq`
   * @return a cudf ColumnVector, its length is equal to the size of the `seq`.
   */
  def columnVectorFromLiterals(seq: Seq[Any], elementType: DataType): ColumnVector = {
    elementType match {
      // Uses the boxed version for primitive types to keep the nulls.
      case ByteType => ColumnVector.fromBoxedBytes(seq.asInstanceOf[Seq[JByte]]: _*)
      case LongType => ColumnVector.fromBoxedLongs(seq.asInstanceOf[Seq[JLong]]: _*)
      case ShortType => ColumnVector.fromBoxedShorts(seq.asInstanceOf[Seq[JShort]]: _*)
      case FloatType => ColumnVector.fromBoxedFloats(seq.asInstanceOf[Seq[JFloat]]: _*)
      case DoubleType => ColumnVector.fromBoxedDoubles(seq.asInstanceOf[Seq[JDouble]]: _*)
      case IntegerType => ColumnVector.fromBoxedInts(seq.asInstanceOf[Seq[Integer]]: _*)
      case BooleanType => ColumnVector.fromBoxedBooleans(seq.asInstanceOf[Seq[JBoolean]]: _*)
      case DateType => ColumnVector.timestampDaysFromBoxedInts(seq.asInstanceOf[Seq[Integer]]: _*)
      case TimestampType =>
        ColumnVector.timestampMicroSecondsFromBoxedLongs(seq.asInstanceOf[Seq[JLong]]: _*)
      case StringType =>
        ColumnVector.fromUTF8Strings(seq.asInstanceOf[Seq[UTF8String]].map {
          case null => null
          case us => us.getBytes
        }: _*)
      case dt: DecimalType =>
        val decs = seq.asInstanceOf[Seq[Decimal]]
        if (DecimalType.is32BitDecimalType(dt)) {
          val rows = decs.map {
            case null => null
            case d => convertDecimalTo(d, dt).left.get
          }
          ColumnVector.decimalFromBoxedInts(-dt.scale, rows: _*)
        } else {
          val rows = decs.map {
            case null => null
            case d => convertDecimalTo(d, dt).right.get
          }
          ColumnVector.decimalFromBoxedLongs(-dt.scale, rows: _*)
        }
      case ArrayType(_, _) =>
        val colType = resolveElementType(elementType)
        val rows = seq.map(convertElementTo(_, elementType))
        ColumnVector.fromLists(colType, rows.asInstanceOf[Seq[JList[_]]]: _*)
      case StructType(_) =>
        val colType = resolveElementType(elementType)
        val rows = seq.map(convertElementTo(_, elementType))
        ColumnVector.fromStructs(colType, rows.asInstanceOf[Seq[HostColumnVector.StructData]]: _*)
      case MapType(_, _, _) =>
        val colType = resolveElementType(elementType)
        val rows = seq.map(convertElementTo(_, elementType))
        ColumnVector.fromLists(colType, rows.asInstanceOf[Seq[JList[_]]]: _*)
      case NullType =>
        GpuColumnVector.columnVectorFromNull(seq.size, NullType)
      case u =>
        throw new IllegalArgumentException(s"Unsupported element type ($u) to create a" +
          s" ColumnVector.")
    }
  }

  /**
   * Creates a cudf Scalar from a 'Any' according to the DataType.
   *
   * Many expressions (e.g. nodes handling strings and predictions) require a cudf Scalar
   * created from a literal value to run their computations. We do not want to go through
   * the GpuLiteral or the GpuScalar to get one.
   *
   * @param v the Scala value
   * @param t the data type of the scalar
   * @return a cudf Scalar. It should be closed to avoid memory leak.
   */
  def from(v: Any, t: DataType): Scalar = t match {
    case nullType if v == null => nullType match {
      case ArrayType(elementType, _) =>
        Scalar.listFromNull(resolveElementType(elementType))
      case StructType(fields) =>
        Scalar.structFromNull(
          fields.map(f => resolveElementType(f.dataType)): _*)
      case MapType(keyType, valueType, _) =>
        Scalar.listFromNull(
          resolveElementType(StructType(
            Seq(StructField("key", keyType), StructField("value", valueType)))))
      case _ => Scalar.fromNull(GpuColumnVector.getNonNestedRapidsType(nullType))
    }
    case decType: DecimalType =>
      val dec = v match {
        case de: Decimal => de
        case vi: Int => Decimal(vi)
        case vl: Long => Decimal(vl)
        case vd: Double => Decimal(vd)
        case vs: String => Decimal(vs)
        case bd: BigDecimal => Decimal(bd)
        case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
          s" for DecimalType, expecting Decimal, Int, Long, Double, String, or BigDecimal.")
      }
      convertDecimalTo(dec, decType) match {
        case Left(i) => Scalar.fromDecimal(-decType.scale, i)
        case Right(l) => Scalar.fromDecimal(-decType.scale, l)
      }
    case LongType => v match {
      case l: Long => Scalar.fromLong(l)
      case i: Int => Scalar.fromLong(i.toLong)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for LongType, expecting Long, or Int.")
    }
    case DoubleType => v match {
        case d: Double => Scalar.fromDouble(d)
        case f: Float => Scalar.fromDouble(f.toDouble)
        case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
          s" for DoubleType, expecting Double or Float.")
    }
    case TimestampType => v match {
      // Usually the timestamp will be used by the `add/sub` operators for date/time related
      // calculations. But cuDF native does not support these for timestamp type, needing to
      // cast to long type.
      case l: Long => Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, l)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for TimestampType, expecting Long.")
    }
    case IntegerType => v match {
      case i: Int =>  Scalar.fromInt(i)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for IntegerType, expecting Int.")
    }
    case DateType => v match {
      // Usually the days will be used by the `add/sub` operators for date/time related
      // calculations. But cuDF native does not support these for timestamp days type, needing
      // to cast to int type.
      case i: Int =>  Scalar.timestampDaysFromInt(i)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for DateType, expecting Int.")
    }
    case FloatType => v match {
      case f: Float => Scalar.fromFloat(f)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for FloatType, expecting Float.")
    }
    case ShortType => v match {
      case s: Short => Scalar.fromShort(s)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for ShortType, expecting Short.")
    }
    case ByteType => v match {
      case b: Byte => Scalar.fromByte(b)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for ByteType, expecting Byte.")
    }
    case BooleanType => v match {
      case b: Boolean => Scalar.fromBool(b)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for BooleanType, expecting Boolean.")
    }
    case StringType => v match {
      case s: String => Scalar.fromString(s)
      case us: UTF8String => Scalar.fromUTF8String(us.getBytes)
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for StringType, expecting String or UTF8String.")
    }
    case ArrayType(elementType, _) => v match {
      case array: ArrayData =>
        withResource(columnVectorFromLiterals(array.array, elementType)) { list =>
          Scalar.listFromColumnView(list)
        }
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
        s" for ArrayType, expecting ArrayData")
    }
    case StructType(fields) => v match {
      case row: InternalRow =>
        val cvs = fields.zipWithIndex.safeMap {
          case (f, i) =>
            val dt = f.dataType
            columnVectorFromLiterals(Seq(row.get(i, dt)), dt)
        }
        withResource(cvs) { cvs =>
          Scalar.structFromColumnViews(cvs: _*)
        }
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
          s" for StructType, expecting InternalRow")
    }
    case MapType(keyType, valueType, _) => v match {
      case map: MapData =>
        val struct = withResource(columnVectorFromLiterals(map.keyArray().array, keyType)) { keys =>
          withResource(columnVectorFromLiterals(map.valueArray().array, valueType)) { values =>
            ColumnVector.makeStruct(map.numElements(), keys, values)
          }
        }
        withResource(struct) { struct =>
          Scalar.listFromColumnView(struct)
        }
      case _ => throw new IllegalArgumentException(s"'$v: ${v.getClass}' is not supported" +
          s" for MapType, expecting MapData")
    }
    case _ => throw new UnsupportedOperationException(s"${v.getClass} '$v' is not supported" +
        s" as a Scalar yet")
  }

  def isNan(s: Scalar): Boolean = s.getType match {
    case DType.FLOAT32 => s.isValid && s.getFloat.isNaN()
    case DType.FLOAT64 => s.isValid && s.getDouble.isNaN()
    case t => throw new IllegalStateException(s"$t is doesn't support NaNs")
  }

  /**
   * Creates a GpuScalar from a 'Any' according to the DataType.
   *
   * If the Any is a cudf Scalar, it will be taken over by the returned GpuScalar,
   * so no need to close it.
   * But the returned GpuScalar should be closed to avoid memory leak.
   */
  def apply(any: Any, dataType: DataType): GpuScalar = any match {
    case s: Scalar => wrap(s, dataType)
    case o => new GpuScalar(None, Some(o), dataType)
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
    new GpuScalar(Some(scalar), None, dataType)
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
    private var scalar: Option[Scalar],
    private var value: Option[Any],
    val dataType: DataType) extends Arm with AutoCloseable {

  private var refCount: Int = 0

  if (scalar.isEmpty && value.isEmpty) {
    throw new IllegalArgumentException("GpuScalar requires at least a value or a Scalar")
  }
  if (value.isDefined && value.get.isInstanceOf[Scalar]) {
    throw new IllegalArgumentException("Value should not be Scalar")
  }

  override def toString: String = s"GPU_SCALAR $dataType $value $scalar"

  /**
   * Gets the internal cudf Scalar of this GpuScalar.
   *
   * This will not increase any reference count. So users need to close either the GpuScalar or
   * the return cudf Scalar, not both.
   */
  def getBase: Scalar = {
    if (scalar.isEmpty) {
      scalar = Some(GpuScalar.from(value.get, dataType))
    }
    scalar.get
  }

  /**
   * Gets the internal Scala value of this GpuScalar.
   */
  def getValue: Any = {
    if (value.isEmpty) {
      value = Some(GpuScalar.extract(scalar.get))
    }
    value.get
  }

  /**
   * GpuScalar is valid when
   *   the Scala value is not null if it is defined, or
   *   the cudf Scalar is valid if the Scala value is not defined.
   * Because a cudf Scalar created from a null is invalid.
   */
  def isValid: Boolean = value.map(_ != null).getOrElse(getBase.isValid)

  /**
   * Whether the GpuScalar is Nan. It works only for float and double types, otherwise
   * an exception will be raised.
   */
  def isNan: Boolean = dataType match {
    case FloatType => getValue.asInstanceOf[Float].isNaN
    case DoubleType => getValue.asInstanceOf[Double].isNaN
    case o => throw new IllegalStateException(s"$o is doesn't support NaNs")
  }

  /**
   * Whether the GpuScalar is not a Nan. It works only for float and double types, otherwise
   * an exception will be raised.
   */
  def isNotNan: Boolean = !isNan

  /**
   * Increment the reference count for this scalar. You need to call close on this
   * to decrement the reference count again.
   */
  def incRefCount: this.type = incRefCountInternal(false)

  override def close(): Unit = {
    this.synchronized {
      refCount -= 1
      if (refCount == 0) {
        scalar.foreach(_.close())
        scalar = null
        value = null
      } else if (refCount < 0) {
        throw new IllegalStateException(s"Close called too many times $this")
      }
    }
  }

  private def incRefCountInternal(isFirstTime: Boolean): this.type = {
    this.synchronized {
      if (refCount <= 0 && !isFirstTime) {
        throw new IllegalStateException("GpuScalar is already closed")
      }
      refCount += 1
    }
    this
  }
  incRefCountInternal(true)
}

object GpuLiteral {
  /**
   * Create a `GpuLiteral` from a Scala value, by leveraging the corresponding CPU
   * APIs to do the data conversion and type checking, which are quite complicated.
   * Fortunately Spark does this for us.
   */
  def apply(v: Any): GpuLiteral = {
    val cpuLiteral = Literal(v)
    GpuLiteral(cpuLiteral.value, cpuLiteral.dataType)
  }

  /**
   * Create a `GpuLiteral` from a value according to the data type.
   */
  def create(value: Any, dataType: DataType): GpuLiteral = {
    val cpuLiteral = Literal.create(value, dataType)
    GpuLiteral(cpuLiteral.value, cpuLiteral.dataType)
  }

  def create[T : TypeTag](v: T): GpuLiteral = {
    val cpuLiteral = Literal.create(v)
    GpuLiteral(cpuLiteral.value, cpuLiteral.dataType)
  }

  /**
   * Create a GPU literal with default value for given DataType
   */
  def default(dataType: DataType): GpuLiteral = {
    val cpuLiteral = Literal.default(dataType)
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
    GpuScalar(value, dataType)
  }
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
