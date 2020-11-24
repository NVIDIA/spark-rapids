/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, MapType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Utilities for creating random inputs for unit tests.
 */
object FuzzerUtils {

  /**
   * Default options when generating random data.
   */
  private val DEFAULT_OPTIONS = FuzzerOptions(
    numbersAsStrings = true,
    asciiStringsOnly = false,
    maxStringLen = 64)

  /**
   * Create a schema with the specified data types.
   */
  def createSchema(dataTypes: DataType*): StructType = {
    new StructType(dataTypes.zipWithIndex
      .map(pair => StructField(s"c${pair._2}", pair._1, true)).toArray)
  }

  /**
   * Create a schema with the specified data types.
   */
  def createSchema(dataTypes: Seq[DataType], nullable: Boolean = true): StructType = {
    new StructType(dataTypes.zipWithIndex
      .map(pair => StructField(s"c${pair._2}", pair._1, nullable)).toArray)
  }

  /**
   * Creates a ColumnarBatch with random data based on the given schema.
   */
  def createColumnarBatch(
      schema: StructType,
      rowCount: Int,
      maxStringLen: Int = 64,
      options: FuzzerOptions = DEFAULT_OPTIONS,
      seed: Long = 0): ColumnarBatch = {
    val rand = new Random(seed)
    val r = new EnhancedRandom(rand, options)
    val builders = new GpuColumnarBatchBuilder(schema, rowCount, null)
    schema.fields.zipWithIndex.foreach {
      case (field, i) =>
        val builder = builders.builder(i)
        val rows = 0 until rowCount
        field.dataType match {
          case DataTypes.ByteType =>
            rows.foreach(_ => {
              maybeNull(rand, r.nextByte()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.ShortType =>
            rows.foreach(_ => {
              maybeNull(rand, r.nextShort) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.IntegerType =>
            rows.foreach(_ => {
              maybeNull(rand, r.nextInt()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.LongType =>
            rows.foreach(_ => {
              maybeNull(rand, r.nextLong()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.FloatType =>
            rows.foreach(_ => {
              maybeNull(rand, r.nextFloat()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.DoubleType =>
            rows.foreach(_ => {
              maybeNull(rand, r.nextDouble()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
          case DataTypes.StringType =>
            rows.foreach(_ => {
              maybeNull(rand, r.nextString()) match {
                case Some(value) => builder.append(value)
                case None => builder.appendNull()
              }
            })
        }
    }
    builders.build(rowCount)
  }

  /**
   * Creates a ColumnarBatch with provided data.
   */
  def createColumnarBatch(values: Seq[Option[Any]], dataType: DataType): ColumnarBatch = {
    val schema = createSchema(Seq(dataType))
    val rowCount = values.length
    val builders = new GpuColumnarBatchBuilder(schema, rowCount, null)
    schema.fields.zipWithIndex.foreach {
      case (field, i) =>
        val builder = builders.builder(i)
        field.dataType match {
          case DataTypes.DoubleType =>
            values.foreach {
              case Some(value) => builder.append(value.asInstanceOf[Double])
              case None => builder.appendNull()
            }
        }
    }
    builders.build(rowCount)
  }


  private def maybeNull[T](r: Random, nonNullValue: => T): Option[T] = {
    if (r.nextFloat() < 0.2) {
      None
    }  else {
      Some(nonNullValue)
    }
  }

  /**
   * Creates a DataFrame with random data based on the given schema.
   */
  def generateDataFrame(
        spark: SparkSession,
        schema: StructType,
        rowCount: Int = 1024,
        options: FuzzerOptions = DEFAULT_OPTIONS,
        seed: Long = 0): DataFrame = {
    val r = new Random(seed)
    val rows: Seq[Row] = (0 until rowCount).map(_ => generateRow(schema.fields, r, options))
    spark.createDataFrame(rows.asJava, schema)
  }

  /**
   * Creates a Row with random data based on the given field definitions.
   */
  def generateRow(fields: Array[StructField], rand: Random, options: FuzzerOptions): Row = {
    val r = new EnhancedRandom(rand, options)

    def handleDataType(dataType: DataType): Any = {
      dataType match {
        case DataTypes.BooleanType => r.nextBoolean()
        case DataTypes.ByteType => r.nextByte()
        case DataTypes.ShortType => r.nextShort()
        case DataTypes.IntegerType => r.nextInt()
        case DataTypes.LongType => r.nextLong()
        case DataTypes.FloatType => r.nextFloat()
        case DataTypes.DoubleType => r.nextDouble()
        case DataTypes.StringType => r.nextString()
        case DataTypes.TimestampType => r.nextTimestamp()
        case DataTypes.DateType => r.nextDate()
        case DataTypes.CalendarIntervalType => r.nextInterval()
        case DataTypes.NullType => null
        case ArrayType(elementType, _) =>
          val list = new ListBuffer[Any]
          //only generating 5 items in the array this can later be made configurable
          scala.Range(0, 5).foreach { _ =>
            list.append(handleDataType(elementType))
          }
          list.toList
        case MapType(keyType, valueType, _) =>
          val keyList = new ListBuffer[Any]
          //only generating 5 items in the array this can later be made configurable
          scala.Range(0, 5).foreach { _ =>
            keyList.append(handleDataType(keyType))
          }
          val valueList = new ListBuffer[Any]
          //only generating 5 items in the array this can later be made configurable
          scala.Range(0, 5).foreach { _ =>
            valueList.append(handleDataType(valueType))
          }
          val map = new mutable.HashMap[Any, Any]()
          keyList.zip(valueList).map { values =>
            map.put(values._1, values._2)
          }
          map.toMap
        case s: StructType =>
          generateRow(s.fields, rand, options)
        case _ => throw new IllegalStateException(
          s"fuzzer does not support data type $dataType")
      }
    }

    Row.fromSeq(fields.map { field =>
      if (field.nullable && r.nextFloat() < 0.2) {
        null
      } else {
        handleDataType(field.dataType)
      }
    })
  }

  def createDataFrame(dt: DataType)(spark: SparkSession, rowCount: Int = 100) = {
    generateDataFrame(spark, createSchema(Seq(dt)), rowCount)
  }
}

/**
 * Wrapper around Random that generates more useful data for unit testing.
 */
class EnhancedRandom(r: Random, options: FuzzerOptions) {

  def nextInterval(): CalendarInterval = {
    new CalendarInterval(nextInt(), nextInt(), nextLong())
  }

  def nextBoolean(): Boolean = r.nextBoolean()

  def nextByte(): Byte = {
    r.nextInt(5) match {
      case 0 => Byte.MinValue
      case 1 => Byte.MaxValue
      case 2 => (r.nextDouble() * Byte.MinValue).toByte
      case 3 => (r.nextDouble() * Byte.MaxValue).toByte
      case 4 => 0
    }
  }

  def nextShort(): Short = {
    r.nextInt(5) match {
      case 0 => Short.MinValue
      case 1 => Short.MaxValue
      case 2 => (r.nextDouble() * Short.MinValue).toShort
      case 3 => (r.nextDouble() * Short.MaxValue).toShort
      case 4 => 0
    }
  }

  def nextInt(): Int = {
    r.nextInt(5) match {
      case 0 => Int.MinValue
      case 1 => Int.MaxValue
      case 2 => (r.nextDouble() * Int.MinValue).toInt
      case 3 => (r.nextDouble() * Int.MaxValue).toInt
      case 4 => 0
    }
  }

  def nextLong(): Long = {
    r.nextInt(5) match {
      case 0 => Long.MinValue
      case 1 => Long.MaxValue
      case 2 => (r.nextDouble() * Long.MinValue).toLong
      case 3 => (r.nextDouble() * Long.MaxValue).toLong
      case 4 => 0
    }
  }

  def nextFloat(): Float = {
    r.nextInt(9) match {
      case 0 => Float.NaN
      case 1 => Float.PositiveInfinity
      case 2 => Float.NegativeInfinity
      case 3 => r.nextFloat() * Float.MinValue
      case 4 => r.nextFloat() * Float.MaxValue
      case 5 => 0 - r.nextFloat()
      case 6 => r.nextFloat()
      case 7 => 0f
      case 8 => -0f
    }
  }

  def nextDouble(): Double = {
    r.nextInt(9) match {
      case 0 => Double.NaN
      case 1 => Double.PositiveInfinity
      case 2 => Double.NegativeInfinity
      case 3 => r.nextDouble() * Double.MinValue
      case 4 => r.nextDouble() * Double.MaxValue
      case 5 => 0 - r.nextDouble()
      case 6 => r.nextDouble()
      case 7 => 0d
      case 8 => -0d
    }
  }

  def nextString(): String = {
    if (options.numbersAsStrings) {
      r.nextInt(5) match {
        case 0 => String.valueOf(r.nextInt())
        case 1 => String.valueOf(r.nextLong())
        case 2 => String.valueOf(r.nextFloat())
        case 3 => String.valueOf(r.nextDouble())
        case 4 => generateString()
      }
    } else {
      generateString()
    }
  }

  def nextDate(): Date = {
    val futureDate = 6321706291000L // Upper limit Sunday, April 29, 2170 9:31:31 PM
    new Date((futureDate * r.nextDouble()).toLong);
  }

  def nextTimestamp(): Timestamp = {
    val futureDate = 6321706291000L // Upper limit Sunday, April 29, 2170 9:31:31 PM
    new Timestamp((futureDate * r.nextDouble()).toLong)
  }

  private def generateString(): String = {
    if (options.asciiStringsOnly) {
      val b = new StringBuilder()
      for (_ <- 0 until options.maxStringLen) {
        b.append(ASCII_CHARS.charAt(r.nextInt(ASCII_CHARS.length)))
      }
      b.toString
    } else {
      r.nextString(r.nextInt(options.maxStringLen))
    }
  }

  private val ASCII_CHARS = "abcdefghijklmnopqrstuvwxyz"
}

case class FuzzerOptions(
    numbersAsStrings: Boolean = true,
    asciiStringsOnly: Boolean = false,
    maxStringLen: Int = 64)
