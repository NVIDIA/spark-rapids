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

import scala.collection.mutable

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

  private val dataTypeAlignmentMap = mutable.LinkedHashMap[DataType, Int](
    StringType -> 1,
    DoubleType -> 8,
    LongType -> 8,
    BooleanType -> 1,
    IntegerType -> 4)

  private val expectedPackedMap : Array[Int] =
    Array(4, 5, 6, 7, 8, 9, 10, 17, 18, 19, 20, 21, 0, 2, 3, 11, 12, 13, 14, 15, 16, 23, 1, 22)

  private val expectedPackedOffsets : Array[Int] =
    Array(0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 108, 116, 124, 132, 140, 148, 156,
      164, 168, 169)

  private val expectedUnPackedMap : Array[Int] =
    Array(12, 22, 13, 14, 0, 1, 2, 3, 4, 5, 6, 15, 16, 17, 18, 19, 20, 7, 8, 9, 10, 11, 23, 21)

  test("test dataType Alignment used for the JCudf") {
    schema.foreach { colF =>
      val rapidsType = GpuColumnVector.getNonNestedRapidsType(colF.dataType)
      assert(
        JCudfUtil.getDataAlignmentForDataType(rapidsType) == dataTypeAlignmentMap(colF.dataType))
    }
  }

  test("test JCudf offset calculations") {
    // test packing
    val attributes = TrampolineUtil.toAttributes(schema)
    val packedMaps = CudfRowTransitions.reorderSchemaToPackedColumns(attributes)
    assert(packedMaps.deep == expectedPackedMap.deep)
    val unpackedMap = CudfRowTransitions.getUnpackedMapForSchema(packedMaps)
    assert(unpackedMap.deep == expectedUnPackedMap.deep)
    // test offset calculator
    val startOffsets: Array[Int] = new Array[Int](attributes.length)
    val jCudfBuilder = JCudfUtil.getJCudfRowEstimator(packedMaps.map(attributes(_)))
    val validityBytesOffset = jCudfBuilder.setColumnOffsets(startOffsets)

    assert(jCudfBuilder.hasVarSizeData)
    assert(170 == validityBytesOffset)
    assert(startOffsets.deep == expectedPackedOffsets.deep)
  }

  test("test JCudf Row Visitor used in CodeGen with packed Schema") {
    val attributes = TrampolineUtil.toAttributes(schema)
    val packedMaps = CudfRowTransitions.reorderSchemaToPackedColumns(attributes)
    val packedAttributes = packedMaps.map(attributes(_))
    val cudfRowVisitor = JCudfUtil.getJCudfRowVisitor(packedAttributes)
    val stringColumns = packedMaps.filter(packedAttributes(_).dataType.isInstanceOf[StringType])

    assert(cudfRowVisitor.getValidityBytesOffset == 170)
    assert(cudfRowVisitor.getValiditySizeInBytes == (attributes.length + 7) / 8)
    var varDataOffsetRef = 173
    val cudfDataOffset = cudfRowVisitor.getVariableDataOffset
    assert(cudfDataOffset == 170 + cudfRowVisitor.getValiditySizeInBytes)
    assert(cudfDataOffset == 173)
    (0 until schema.length).map { colIndex =>
      cudfRowVisitor.getNextCol
      val cudfColOff = cudfRowVisitor.getColOffset
      assert(cudfColOff == expectedPackedOffsets(colIndex))
      val colLength = cudfRowVisitor.getColLength
      colLength match {
        case -15 =>
          // assume length is 20;
          varDataOffsetRef += 20
          assert(packedAttributes(colIndex).dataType.isInstanceOf[StringType])
        case _ => assert(!stringColumns.contains(colIndex))
      }
    }
    assert(varDataOffsetRef == cudfDataOffset + 20 * stringColumns.length)
  }

  test("test JCudf Row Size Estimator for unpacked schema") {
    val attributes = TrampolineUtil.toAttributes(schema)
    val cudfRowEstimator = JCudfUtil.getJCudfRowEstimator(attributes.toArray)
    val sizePerRowEstimate = cudfRowEstimator.getEstimateSize
    val stringColumns = attributes.count(_.dataType.isInstanceOf[StringType])
    // validity offset is 176
    // size of validity is 3 bytes
    // data size is number of strings * JCUDF_TYPE_STRING_LENGTH_ESTIMATE
    val estimatedSize = JCudfUtil.alignOffset(
      176 + 3 + stringColumns * JCudfUtil.JCUDF_TYPE_STRING_LENGTH_ESTIMATE,
      JCudfUtil.JCUDF_ROW_ALIGNMENT)
    assert(sizePerRowEstimate == estimatedSize)
  }
}
