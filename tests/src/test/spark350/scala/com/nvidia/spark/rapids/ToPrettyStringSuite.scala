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
import com.nvidia.spark.rapids.shims.GpuToPrettyString

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, NamedExpression, ToPrettyString}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, MapType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

class ToPrettyStringSuite extends GpuUnitTests {

  private val rapidsConf = new RapidsConf(Map[String, String]())

  private def tagExpressionForGpu(lit: Literal): RapidsMeta[_, _, _] = {
    val toPrettyString = ToPrettyString(lit, Some("UTC"))
    val wrapperLit = GpuOverrides.wrapExpr(toPrettyString, rapidsConf, None)
    wrapperLit.initReasons()
    wrapperLit.tagExprForGpu()
    wrapperLit
  }

  test("test ToPrettyString is accelerated") {
    val lit = Literal(1, DataTypes.IntegerType)
    val wrapperLit = tagExpressionForGpu(lit)
    assertResult(true)(wrapperLit.canThisBeReplaced)
  }

  test("test ToPrettyString should not be accelerated for BinaryType") {
    val lit = Literal("1".getBytes(), DataTypes.BinaryType)
    val wrapperLit = tagExpressionForGpu(lit)
    assertResult(false)(wrapperLit.canThisBeReplaced)
  }

  private def executeOnCpuAndReturn(dataType: DataType, value: Any): String = {
    ToPrettyString(Literal(value, dataType), Some("UTC"))
      .eval(null).asInstanceOf[UTF8String].toString()
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
        val batch = new ColumnarBatch(List(input).toArray, 2)
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
        new HostColumnVector.BasicType(false, DType.INT32),
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
}
