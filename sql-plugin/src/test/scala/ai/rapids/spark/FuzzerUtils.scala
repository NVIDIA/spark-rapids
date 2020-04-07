package ai.rapids.spark

import ai.rapids.spark.GpuColumnVector.GpuColumnarBatchBuilder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.util.Random
import scala.collection.JavaConverters._

/**
 * Utilities for creating random inputs for unit tests.
 */
object FuzzerUtils {

  /**
   * Create a schema with the specified data types.
   */
  def createSchema(dataTypes: Seq[DataType]): StructType = {
    new StructType(dataTypes.zipWithIndex.map(pair => StructField(s"c${pair._2}", pair._1)).toArray)
  }

  /**
   * Creates a ColumnarBatch with random data based on the given schema.
   */
  def createColumnarBatch(schema: StructType, rowCount: Int, maxStringLen: Int = 64, seed: Long = 0): ColumnarBatch = {
    val rand = new Random(seed)
    val r = new EnhancedRandom(rand, maxStringLen)
    val builders = new GpuColumnarBatchBuilder(schema, rowCount, null)
    schema.fields.zipWithIndex.foreach {
      case (field, i) =>
        val builder = builders.builder(i)
        val rows = 0 until rowCount
        field.dataType match {
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
  def generateDataFrame(spark: SparkSession, schema: StructType, rowCount: Int, maxStringLen: Int = 64, seed: Long = 0): DataFrame = {
    val r = new Random(seed)
    val rows: Seq[Row] = (0 until rowCount).map(_ => generateRow(schema.fields, r, maxStringLen))
    spark.createDataFrame(rows.asJava, schema)
  }

  /**
   * Creates a Row with random data based on the given field definitions.
   */
  def generateRow(fields: Array[StructField], rand: Random, maxStringLen: Int) = {
    val r = new EnhancedRandom(rand, maxStringLen)
    Row.fromSeq(fields.map { field =>
      if (field.nullable && r.nextFloat() < 0.2) {
        null
      } else {
        field.dataType match {
          case DataTypes.IntegerType => r.nextInt()
          case DataTypes.LongType => r.nextLong()
          case DataTypes.FloatType => r.nextFloat()
          case DataTypes.DoubleType => r.nextDouble()
          case DataTypes.StringType => r.nextString()
        }
      }
    })
  }
  
}

/**
 * Wrapper around Random that generates more useful data for unit testing.
 */
class EnhancedRandom(r: Random, val maxStringLen: Int) {

  def nextInt(): Int = {
    r.nextInt(4) match {
      case 0 => Int.MinValue
      case 1 => Int.MaxValue
      case 2 => (r.nextDouble() * Int.MinValue).toInt
      case _ => (r.nextDouble() * Int.MaxValue).toInt
    }
  }

  def nextLong(): Long = {
    r.nextInt(4) match {
      case 0 => Long.MinValue
      case 1 => Long.MaxValue
      case 2 => (r.nextDouble() * Long.MinValue).toLong
      case _ => (r.nextDouble() * Long.MaxValue).toLong
    }
  }

  def nextFloat(): Float = {
    r.nextInt(8) match {
      case 0 => Float.NaN
      case 1 => Float.PositiveInfinity
      case 2 => Float.NegativeInfinity
      case 3 => r.nextFloat() * Float.MinValue
      case 4 => r.nextFloat() * Float.MaxValue
      case 5 => 0 - r.nextFloat()
      case 6 => r.nextFloat()
      case _ => 0f
    }
  }

  def nextDouble(): Double = {
    r.nextInt(8) match {
      case 0 => Double.NaN
      case 1 => Double.PositiveInfinity
      case 2 => Double.NegativeInfinity
      case 3 => r.nextDouble() * Double.MinValue
      case 4 => r.nextDouble() * Double.MaxValue
      case 5 => 0 - r.nextDouble()
      case 6 => r.nextDouble()
      case _ => 0d
    }
  }

  def nextString(): String = {
    r.nextInt(5) match {
      case 0 => String.valueOf(r.nextInt())
      case 1 => String.valueOf(r.nextLong())
      case 2 => String.valueOf(r.nextFloat())
      case 3 => String.valueOf(r.nextDouble())
      case _ => r.nextString(r.nextInt(maxStringLen))
    }
  }
} 
