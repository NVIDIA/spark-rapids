/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import collection.JavaConverters._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.shims.GpuToPrettyString

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal, NamedExpression, ToPrettyString}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

class ToPrettyStringSuite extends GpuUnitTests {

  private def executeOnCpuAndReturn(dataType: DataType, value: Any): String = {
    ToPrettyString(Literal(value, dataType), Some("UTC"))
      .eval(null).asInstanceOf[UTF8String].toString()
  }

  test("test show() BinaryType") {
    val dataType = DataTypes.BinaryType
    val stringData = "this is a string"
    // Execute on CPU
    val cpuResult = executeOnCpuAndReturn(dataType, stringData.getBytes())

    val child = GpuBoundReference(0, dataType, true)(NamedExpression.newExprId, "binary_arg")
    val gpuToPrettyStr = GpuToPrettyString(child, Some("UTC"))
    withResource(
      GpuColumnVector.from(ColumnVector.fromStrings(cpuResult),
        DataTypes.StringType)) { expected0 =>
      val dt = new HostColumnVector.ListType(true,
        new HostColumnVector.BasicType(true, DType.UINT8))
      withResource(GpuColumnVector.from(
        ColumnVector.fromLists(
          dt,
          Array(stringData.getBytes("UTF-8").toList.asJava): _*),
        DataTypes.BinaryType)) {
        binaryCol =>
          val batch = new ColumnarBatch(Array(binaryCol), 1)
          checkEvaluation(gpuToPrettyStr, expected0, batch)
      }
    }
  }

  test("test show() Array") {
    val dataType = ArrayType(DataTypes.IntegerType)
    val v: Array[_] = Array(1, 2, 3, null)
    // Execute on CPU
    val cpuResult = executeOnCpuAndReturn(dataType, ArrayData.toArrayData(v))

    val child = GpuBoundReference(0, dataType, true)(NamedExpression.newExprId, "array_arg")
    val gpuToPrettyStr = GpuToPrettyString(child, Some("UTC"))
    withResource(
      GpuColumnVector.from(ColumnVector.fromStrings(cpuResult),
      DataTypes.StringType)) { expected0 =>
      val dt = new HostColumnVector.ListType(true,
        new HostColumnVector.BasicType(true, DType.INT32))
      withResource(GpuColumnVector.from(ColumnVector.fromLists(dt,
        v.toList.asJava),
        ArrayType(DataTypes.IntegerType))) { input =>
        val batch = new ColumnarBatch(List(input).toArray, 1)
        checkEvaluation(gpuToPrettyStr, expected0, batch)
      }
    }
  }

  test("test show() Map") {
    val dataType = MapType(DataTypes.IntegerType, DataTypes.IntegerType)
    val map = Map(1 -> 2, 2 -> 3, 4 -> null)
    val cpuResult = executeOnCpuAndReturn(dataType, ArrayBasedMapData(map))

    val child = GpuBoundReference(0, dataType, true)(NamedExpression.newExprId, "map_arg")
    val gpuToPrettyStr = GpuToPrettyString(child, Some("UTC"))

    withResource(
      GpuColumnVector.from(
        ColumnVector.fromStrings(cpuResult), DataTypes.StringType)) {
      expected0 =>
        val list1 = map.map {
          case (k, v) =>
            new HostColumnVector.StructData(Array[Integer](k, v.asInstanceOf[Integer]): _*)
        }.toList.asJava
        val structType = new HostColumnVector.StructType(true,
          List[HostColumnVector.DataType](new HostColumnVector.BasicType(true, DType.INT32),
              new HostColumnVector.BasicType(true, DType.INT32)).asJava)
        withResource(GpuColumnVector.from(ColumnVector.fromLists(
          new HostColumnVector.ListType(true, structType), list1),
          MapType(DataTypes.IntegerType, DataTypes.IntegerType))) { input =>
          val batch = new ColumnarBatch(List(input).toArray, 1)
          checkEvaluation(gpuToPrettyStr, expected0, batch)
        }
    }
  }

  test("test show() Struct") {
    val dataType = StructType(Seq(StructField("a", DataTypes.IntegerType),
      StructField("b", DataTypes.IntegerType),
      StructField("c", DataTypes.IntegerType)))
    val v = Array(1, 2, 3)
    val cpuResult = executeOnCpuAndReturn(dataType, InternalRow(v: _*))
    val child = GpuBoundReference(0, dataType, true)(NamedExpression.newExprId, "struct_arg")
    val gpuToPrettyStr = GpuToPrettyString(child, Some("UTC"))
    val l =
      List[HostColumnVector.DataType](
        new HostColumnVector.BasicType(false, DType.INT32),
        new HostColumnVector.BasicType(false, DType.INT32),
        new HostColumnVector.BasicType(false, DType.INT32)
      ).asJava
    val structType = new HostColumnVector.StructType(false, l)
    withResource(
      GpuColumnVector.from(
        ColumnVector.fromStrings(cpuResult), DataTypes.StringType)) { expected =>
      withResource(GpuColumnVector.from(
        ColumnVector.fromStructs(structType,
          List(new HostColumnVector.StructData(v.map(Int.box): _*)).asJava
        ), dataType)) { input =>
        val batch = new ColumnarBatch(List(input).toArray, 1)
        checkEvaluation(gpuToPrettyStr, expected, batch)
      }
    }
  }

  private def testDataType(dataType: DataType) {
    val schema = (new StructType)
      .add(StructField("a", dataType, true))
    val numRows = 100
    val inputRows = GpuBatchUtilsSuite.createRows(schema, numRows)
    val cpuOutput: Array[String] = inputRows.map {
      input => 
        ToPrettyString(BoundReference(0, dataType, true), Some("UTC"))
        .eval(input).asInstanceOf[UTF8String].toString()
    }
    // cpuOutput.foreach(println)
    val child = GpuBoundReference(0, dataType, true)(NamedExpression.newExprId, "arg")
    val gpuToPrettyStr = GpuToPrettyString(child, Some("UTC"))

    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { batchBuilder =>
      val r2cConverter = new GpuRowToColumnConverter(schema)
      inputRows.foreach(r2cConverter.convert(_, batchBuilder))
      withResource {
        closeOnExcept(batchBuilder.build(numRows)) { columnarBatch =>
          withResource(GpuColumnVector.from(ColumnVector.fromStrings(cpuOutput: _*), 
          DataTypes.StringType)) { expected =>
            checkEvaluation(gpuToPrettyStr, expected, columnarBatch)
          }
          columnarBatch
        }
      }(cb => null)
    }
  }

  test("test strings") {
    testDataType(DataTypes.StringType)    
  } 

  test("test floats") {
    testDataType(DataTypes.FloatType)
  } 

  test("test doubles") {
    testDataType(DataTypes.DoubleType)
  } 

  test("test ints") {
    testDataType(DataTypes.IntegerType)
  } 

  test("test longs") {
    testDataType(DataTypes.LongType)
  } 

  test("test decimals") {
    testDataType(DecimalType(8,2))
  } 
}
