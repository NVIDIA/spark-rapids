/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
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
package com.nvidia.spark.rapids.tests.datasourcev2.parquet

import java.util

import scala.collection.JavaConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.arrow.vector.complex.{ListVector, MapVector}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}


object TestingV2Source {
  val schema =
    new StructType(Array(StructField("int1", IntegerType), StructField("int2", IntegerType)))
}

trait TestingV2Source extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    TestingV2Source.schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    getTable(new CaseInsensitiveStringMap(properties))
  }

  def getTable(options: CaseInsensitiveStringMap): Table
}

abstract class SimpleScanBuilder extends ScanBuilder
  with Batch with Scan {

  override def build(): Scan = this

  override def toBatch: Batch = this

  override def readSchema(): StructType = TestingV2Source.schema

  override def createReaderFactory(): PartitionReaderFactory
}

abstract class SimpleBatchTable extends Table with SupportsRead  {

  override def schema(): StructType = TestingV2Source.schema

  override def name(): String = this.getClass.toString

  override def capabilities(): util.Set[TableCapability] = Set(BATCH_READ).asJava
}

case class ArrowInputPartition(dt: Seq[DataType], numRows: Int) extends InputPartition


class ColumnarDataSourceV2 extends TestingV2Source {

  class MyScanBuilder extends SimpleScanBuilder {

    override def planInputPartitions(): Array[InputPartition] = {
      Array(ArrowInputPartition(Seq(IntegerType, IntegerType), 100),
        ArrowInputPartition(Seq(IntegerType, IntegerType), 100))
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      ColumnarReaderFactory
    }
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      new MyScanBuilder()
    }
  }
}

object ColumnarReaderFactory extends PartitionReaderFactory {
  private final val BATCH_SIZE = 20

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val ArrowInputPartition(dataTypes, numRows) = partition
    new PartitionReader[ColumnarBatch] {
      private var batch: ColumnarBatch = _

      private var current = 0

      override def next(): Boolean = {
        var count = 0
        val batchSize = if (current < numRows) {
          if (current + BATCH_SIZE > numRows) {
            numRows - current
          } else {
            BATCH_SIZE
          }
        } else {
          0
        }

        if (batchSize == 0) {
          false
        } else {
          var dtypeNum = 0
          val vecs = dataTypes.map { dtype =>
            val vector = setupArrowVector(s"v$current$dtypeNum", dtype)
            dtypeNum += 1
            fillArrowVec(dtype, vector, numRows)
            new ArrowColumnVector(vector)
          }
          batch = new ColumnarBatch(vecs.toArray)
          batch.setNumRows(count)
          current += batchSize
          true
        }
      }

      override def get(): ColumnarBatch = batch

      override def close(): Unit = batch.close()
    }
  }

  // this was copied from Spark ArrowUtils
  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  private def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType => ArrowType.Utf8.INSTANCE
    case BinaryType => ArrowType.Binary.INSTANCE
    // case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType =>
      if (timeZoneId == null) {
        throw new UnsupportedOperationException(
          s"${TimestampType.catalogString} must supply timeZoneId parameter")
      } else {
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
      }
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
  }

  // this was copied from Spark ArrowUtils
  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  private def toArrowField(
      name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(name, fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(name, fieldType,
          fields.map { field =>
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(name, mapType,
          Seq(toArrowField(MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            nullable = false,
            timeZoneId)).asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  private def fillArrowVec(dt: DataType, vec: ValueVector, numRows: Int): Unit = dt match {
    case IntegerType => {
      val vector = vec.asInstanceOf[IntVector]
      (0 until numRows).foreach { i =>
        vector.setSafe(i, i)
      }
      vector.setNull(numRows)
      vector.setValueCount(numRows + 1)
    }
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
  }

  private def setupArrowVector(name: String, dataType: DataType): ValueVector = {
    val rootAllocator = new RootAllocator(Long.MaxValue)
    val allocator = rootAllocator.newChildAllocator(s"$name", 0, Long.MaxValue)
    val vector = toArrowField(s"field$name", dataType, nullable = true, null)
      .createVector(allocator).asInstanceOf[IntVector]
    vector
  }
}

