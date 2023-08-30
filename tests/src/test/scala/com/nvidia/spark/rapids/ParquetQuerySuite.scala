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

package com.nvidia.spark.rapids

import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.UserDefinedType

class ParquetQuerySuite extends SparkQueryCompareTestSuite {

  test("SPARK-39086: support UDT type in ColumnVector") {
    withGpuSparkSession { spark =>
      val schema = StructType(
        StructField("col1", ArrayType(new TestPrimitiveUDT())) ::
        StructField("col2", ArrayType(new TestArrayUDT())) ::
        StructField("col3", ArrayType(new TestNestedStructUDT())) ::
        Nil)

      withTempPath { dir =>
        val rows = spark.sparkContext.parallelize(0 until 2).map { _ =>
          Row(
            Seq(new TestPrimitive(1)),
            Seq(new TestArray(Seq(1L, 2L, 3L))),
            Seq(new TestNestedStruct(1, 2L, 3.0)))
        }
        val df = spark.createDataFrame(rows, schema)
        df.write.parquet(dir.getCanonicalPath)

        for (offHeapEnabled <- Seq(true, false)) {
          withSQLConf(SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offHeapEnabled.toString) {
            withAllParquetReaders {
              val res = spark.read.parquet(dir.getCanonicalPath)
              // checkAnswer(res, df)
            }
          }
        }
      }
    }
  }

  def testStandardAndLegacyModes(testName: String)(f: => Unit): Unit = {
    test(s"Standard mode - $testName") {
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false") { f }
    }

    test(s"Legacy mode - $testName") {
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") { f }
    }
  }

  testStandardAndLegacyModes("SPARK-39086: UDT read support in vectorized reader") {
    withGpuSparkSession { spark =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        val df = spark
          .range(1)
          .selectExpr(
            """NAMED_STRUCT(
              |  'f0', CAST(id AS STRING),
              |  'f1', NAMED_STRUCT(
              |    'a', CAST(id + 1 AS INT),
              |    'b', CAST(id + 2 AS LONG),
              |    'c', CAST(id + 3.5 AS DOUBLE)
              |  ),
              |  'f2', CAST(id + 4 AS INT),
              |  'f3', ARRAY(id + 5, id + 6)
              |) AS s
            """.stripMargin
          )
          .coalesce(1)

        df.write.parquet(path)

        val userDefinedSchema =
          new StructType()
            .add(
              "s",
              new StructType()
                .add("f0", StringType)
                .add("f1", new TestNestedStructUDT())
                .add("f2", new TestPrimitiveUDT())
                .add("f3", new TestArrayUDT())
            )

        Seq(true, false).foreach { enabled =>
          withSQLConf(
              SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
              SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> s"$enabled") {
            spark.read.schema(userDefinedSchema).parquet(path)
          }
        }
      }
    }
  }

  class TestNestedStructUDT extends UserDefinedType[TestNestedStruct] {
    override def sqlType: DataType =
      new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", LongType, nullable = false)
        .add("c", DoubleType, nullable = false)

    override def serialize(n: TestNestedStruct): Any = {
      val row = new SpecificInternalRow(sqlType.asInstanceOf[StructType].map(_.dataType))
      row.setInt(0, n.a)
      row.setLong(1, n.b)
      row.setDouble(2, n.c)
      row
    }

    override def userClass: Class[TestNestedStruct] = classOf[TestNestedStruct]

    override def deserialize(datum: Any): TestNestedStruct = {
      datum match {
        case row: InternalRow =>
          TestNestedStruct(row.getInt(0), row.getLong(1), row.getDouble(2))
      }
    }
  }

  @SQLUserDefinedType(udt = classOf[TestArrayUDT])
  case class TestArray(value: Seq[Long])

  class TestArrayUDT extends UserDefinedType[TestArray] {
    override def sqlType: DataType = ArrayType(LongType)

    override def serialize(obj: TestArray): Any = ArrayData.toArrayData(obj.value.toArray)

    override def userClass: Class[TestArray] = classOf[TestArray]

    override def deserialize(datum: Any): TestArray = datum match {
      case value: ArrayData => TestArray(value.toLongArray.toSeq)
    }
  }

  @SQLUserDefinedType(udt = classOf[TestPrimitiveUDT])
  case class TestPrimitive(value: Int)

  class TestPrimitiveUDT extends UserDefinedType[TestPrimitive] {
    override def sqlType: DataType = IntegerType

    override def serialize(obj: TestPrimitive): Any = obj.value

    override def userClass: Class[TestPrimitive] = classOf[TestPrimitive]

    override def deserialize(datum: Any): TestPrimitive = datum match {
      case value: Int => TestPrimitive(value)
    }
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    pairs.foreach { case (k, v) =>
      SQLConf.get.setConfString(k, v)
    }
    try f finally {
      pairs.foreach { case (k, _) =>
        SQLConf.get.unsetConf(k)
      }
    }
  }

  object TestingUDT {
    case class SingleElement(element: Long)

    @SQLUserDefinedType(udt = classOf[TestNestedStructUDT])
    case class TestNestedStruct(a: Integer, b: Long, c: Double)

    class TestNestedStructUDT extends UserDefinedType[TestNestedStruct] {
      override def sqlType: DataType =
        new StructType()
          .add("a", IntegerType, nullable = true)
          .add("b", LongType, nullable = false)
          .add("c", DoubleType, nullable = false)

      override def serialize(n: TestNestedStruct): Any = {
        val row = new SpecificInternalRow(sqlType.asInstanceOf[StructType].map(_.dataType))
        row.setInt(0, n.a)
        row.setLong(1, n.b)
        row.setDouble(2, n.c)
        row
      }

      override def userClass: Class[TestNestedStruct] = classOf[TestNestedStruct]

      override def deserialize(datum: Any): TestNestedStruct = {
        datum match {
          case row: InternalRow =>
            TestNestedStruct(row.getInt(0), row.getLong(1), row.getDouble(2))
        }
      }
    }
  }

}