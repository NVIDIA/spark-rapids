/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import scala.collection.mutable.LinkedHashMap

import org.scalatest.FunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

class JCudfUtilSuite extends FunSuite with Logging {

  private val fieldsArray = Seq(
    ("col-str-00", StringType),
    ("col-bool-01", BooleanType),
    ("col-int-02", IntegerType),
    ("col-str-03", StringType),
    ("col-dbl-04", DoubleType),
    ("col-dbl-05", DoubleType),
    ("col-long-06", LongType),
    ("col-long-07", LongType),
    ("col-long-08", LongType),
    ("col-long-09", LongType),
    ("col-dbl-10", DoubleType),
    ("col-str-11", StringType),
    ("col-str-12", StringType),
    ("col-str-13", StringType),
    ("col-str-14", StringType),
    ("col-str-15", StringType),
    ("col-str-16", StringType),
    ("col-long-17", LongType),
    ("col-long-18", LongType),
    ("col-long-19", LongType),
    ("col-long-20", LongType),
    ("col-dbl-21", DoubleType),
    ("col-bool-22", BooleanType),
    ("col-bool-23", IntegerType))

  private val schema = new StructType(fieldsArray.map(f => StructField(f._1, f._2, true)).toArray)
  private val typeOrderMap = LinkedHashMap[DataType, Int](
    StringType -> 39,
    DoubleType -> 80,
    LongType -> 80,
    BooleanType -> 10,
    IntegerType -> 40)

  private val dataTypeAlignmentMap = LinkedHashMap[DataType, Int](
    StringType -> 1,
    DoubleType -> 8,
    LongType -> 8,
    BooleanType -> 1,
    IntegerType -> 4)

  test("test dataTypeOrder used for sorting columns in CUDF") {
    schema.foreach { colF =>
      assert(JCudfUtil.getDataTypeOrder(colF.dataType) == typeOrderMap(colF.dataType))
    }
  }

  test("test dataType Alignment used for the JCudf") {
    schema.foreach { colF =>
      val rapidsType = GpuColumnVector.getNonNestedRapidsType(colF.dataType)
      assert(
        JCudfUtil.getDataAlignmentForDataType(rapidsType) == dataTypeAlignmentMap(colF.dataType))
    }
  }

  test("test offset calculations") {
    val expectedPackedMap: Array[Int] =
      Array(4, 5, 6, 7, 8, 9, 10, 17, 18, 19, 20, 21, 2, 23, 0, 3, 11, 12, 13, 14, 15, 16, 1, 22)
    val expectedUnpackedMap: Array[Int] =
      Array(14, 22, 12, 15, 0, 1, 2, 3, 4, 5, 6, 16, 17, 18, 19, 20, 21, 7, 8, 9, 10, 11, 23, 13)
    val expectedOffsets: Array[Int] =
      Array(0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 100, 104, 112, 120, 128, 136, 144,
        152, 160, 168, 169)
    // test packing
    val attributes = TrampolineUtil.toAttributes(schema)
    val packedMaps = CudfRowTransitions.reorderSchemaToPackedColumns(attributes)
    assert(packedMaps.deep == expectedPackedMap.deep)
    val unpackedMap = CudfRowTransitions.getUnpackedMapForSchema(packedMaps)
    assert(unpackedMap.deep == expectedUnpackedMap.deep)
    // test offset calculator
    val startOffsets: Array[Int] = new Array[Int](attributes.length)
    val jCudfBuilder = JCudfUtil.getJCudfRowEstimator(packedMaps.map(attributes(_)))
    val fixedWidthSize = jCudfBuilder.setColumnOffsets(startOffsets)

    assert(jCudfBuilder.hasVarSizeData)
    assert(170 == fixedWidthSize)
    assert(startOffsets.deep == expectedOffsets.deep)
  }
}
