/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import ai.rapids.cudf
import ai.rapids.cudf.{BinaryOp, CudfException, DType}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.Protobuf
import com.nvidia.spark.rapids.shims.NullIntolerantShim

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types._

/**
 * GPU implementation for Spark's `from_protobuf` decode path.
 *
 * This is designed to replace `org.apache.spark.sql.protobuf.ProtobufDataToCatalyst` when
 * supported.
 *
 * The implementation uses a two-pass approach in the CUDA kernel:
 * - Pass 1: Scan all messages once, recording (offset, length) for each requested field
 * - Pass 2: Extract data in parallel using the recorded locations
 *
 * This is significantly faster than per-field parsing when decoding multiple fields,
 * as each message is only parsed once regardless of the number of fields.
 *
 * @param fullSchema The complete output schema (must match the original expression's dataType)
 * @param decodedFieldIndices Indices into fullSchema for fields that will be decoded by GPU.
 *                            Fields not in this array will be null columns.
 * @param fieldNumbers Protobuf field numbers for decoded fields (parallel to decodedFieldIndices)
 * @param cudfTypeIds cuDF type IDs for ALL fields in fullSchema
 * @param cudfTypeScales Encodings for decoded fields (parallel to decodedFieldIndices)
 * @param failOnErrors If true, throw exception on malformed data; if false, return null
 */
case class GpuFromProtobuf(
    fullSchema: StructType,
    decodedFieldIndices: Array[Int],
    fieldNumbers: Array[Int],
    cudfTypeIds: Array[Int],
    cudfTypeScales: Array[Int],
    failOnErrors: Boolean,
    child: Expression)
  extends GpuUnaryExpression with ExpectsInputTypes with NullIntolerantShim {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = fullSchema.asNullable

  override def nullable: Boolean = true

  // Lazy computation of unsupported field indices (complex types like StructType)
  @transient
  private lazy val unsupportedFieldIndices: Set[Int] = {
    fullSchema.fields.zipWithIndex.collect {
      case (sf, idx) if !GpuFromProtobuf.isTypeSupported(sf.dataType) => idx
    }.toSet
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    val numRows = input.getRowCount.toInt

    // Call the optimized JNI API that:
    // 1. Uses fused kernel to scan all fields in one pass
    // 2. Creates LIST<INT8> directly for bytes fields (no intermediate strings column)
    // 3. Returns struct with decoded fields + null columns for supported types
    val jniResult = try {
      Protobuf.decodeToStruct(
        input.getBase,
        fullSchema.fields.length,  // total number of fields in output
        decodedFieldIndices,       // which fields to decode
        fieldNumbers,              // protobuf field numbers
        cudfTypeIds,               // types for ALL fields (INT8 placeholder for unsupported)
        cudfTypeScales,            // encodings for decoded fields
        failOnErrors)
    } catch {
      case e: CudfException if failOnErrors =>
        // Re-throw as a SparkException for consistent error handling
        throw new org.apache.spark.SparkException("Malformed protobuf message", e)
    }

    // If there are fields with unsupported types, we need to replace placeholder columns
    // with properly typed null columns
    val result = if (unsupportedFieldIndices.isEmpty) {
      jniResult
    } else {
      withResource(jniResult) { struct =>
        // Build children array, replacing placeholders with properly typed null columns
        val children = new Array[cudf.ColumnVector](fullSchema.fields.length)
        try {
          for (i <- fullSchema.fields.indices) {
            if (unsupportedFieldIndices.contains(i)) {
              // Create properly typed null column for unsupported types
              children(i) = GpuFromProtobuf.createNullColumn(fullSchema.fields(i).dataType, numRows)
            } else {
              // Copy the column from JNI result (incRefCount to share ownership)
              children(i) = struct.getChildColumnView(i).copyToColumnVector()
            }
          }
          cudf.ColumnVector.makeStruct(numRows, children: _*)
        } finally {
          children.foreach(c => if (c != null) c.close())
        }
      }
    }

    // Apply input nulls to output
    if (input.getBase.hasNulls) {
      withResource(result) { _ =>
        result.mergeAndSetValidity(BinaryOp.BITWISE_AND, input.getBase)
      }
    } else {
      result
    }
  }
}

object GpuFromProtobuf {
  // Encodings from com.nvidia.spark.rapids.jni.Protobuf
  val ENC_DEFAULT = 0
  val ENC_FIXED   = 1
  val ENC_ZIGZAG  = 2

  /**
   * Maps a Spark DataType to the corresponding cuDF native type ID.
   * Note: The encoding (varint/zigzag/fixed) is determined by the protobuf field type,
   * not the Spark data type, so it must be set separately based on the protobuf schema.
   *
   * @return Some(typeId) for supported types, None for unsupported types
   */
  def sparkTypeToCudfIdOpt(dt: DataType): Option[Int] = dt match {
    case BooleanType => Some(DType.BOOL8.getTypeId.getNativeId)
    case IntegerType => Some(DType.INT32.getTypeId.getNativeId)
    case LongType => Some(DType.INT64.getTypeId.getNativeId)
    case FloatType => Some(DType.FLOAT32.getTypeId.getNativeId)
    case DoubleType => Some(DType.FLOAT64.getTypeId.getNativeId)
    case StringType => Some(DType.STRING.getTypeId.getNativeId)
    case BinaryType => Some(DType.LIST.getTypeId.getNativeId)
    case _ => None
  }

  /**
   * Check if a Spark DataType is supported by the GPU protobuf decoder.
   */
  def isTypeSupported(dt: DataType): Boolean = sparkTypeToCudfIdOpt(dt).isDefined

  /**
   * Create an all-null column of the specified Spark DataType.
   * This is used for fields with unsupported types (nested structs, arrays, etc.)
   * that are not decoded but need to be present in the output struct.
   */
  def createNullColumn(dt: DataType, numRows: Int): cudf.ColumnVector = {
    // Helper to create null arrays for boxed types
    def nullBools = Array.fill[java.lang.Boolean](numRows)(null)
    def nullInts = Array.fill[java.lang.Integer](numRows)(null)
    def nullLongs = Array.fill[java.lang.Long](numRows)(null)
    def nullFloats = Array.fill[java.lang.Float](numRows)(null)
    def nullDoubles = Array.fill[java.lang.Double](numRows)(null)

    dt match {
      case BooleanType => cudf.ColumnVector.fromBoxedBooleans(nullBools: _*)
      case IntegerType => cudf.ColumnVector.fromBoxedInts(nullInts: _*)
      case LongType => cudf.ColumnVector.fromBoxedLongs(nullLongs: _*)
      case FloatType => cudf.ColumnVector.fromBoxedFloats(nullFloats: _*)
      case DoubleType => cudf.ColumnVector.fromBoxedDoubles(nullDoubles: _*)
      case StringType => cudf.ColumnVector.fromStrings(Array.fill[String](numRows)(null): _*)
      case BinaryType =>
        // Binary is LIST<INT8> - create all-null list column using Scalar API
        val elementType = new cudf.HostColumnVector.BasicType(true, DType.INT8)
        withResource(cudf.Scalar.listFromNull(elementType)) { nullScalar =>
          cudf.ColumnVector.fromScalar(nullScalar, numRows)
        }
      case st: StructType =>
        // Recursively create null columns for struct fields
        val children = st.fields.map(f => createNullColumn(f.dataType, numRows))
        try {
          withResource(cudf.ColumnVector.makeStruct(numRows, children: _*)) { structCol =>
            // Set all rows to null - mergeAndSetValidity returns a NEW column
            withResource(cudf.ColumnVector.fromBoxedBooleans(nullBools: _*)) { nullMask =>
              structCol.mergeAndSetValidity(BinaryOp.BITWISE_AND, nullMask)
            }
          }
        } finally {
          children.foreach(_.close())
        }
      case ArrayType(elementType, _) =>
        // Create empty arrays with all nulls using Scalar API
        val cudfElementDType = sparkTypeToCudfIdOpt(elementType)
          .map(id => DType.fromNative(id, 0))
          .getOrElse(DType.INT8)  // fallback for nested complex types
        val elemType = new cudf.HostColumnVector.BasicType(true, cudfElementDType)
        withResource(cudf.Scalar.listFromNull(elemType)) { nullScalar =>
          cudf.ColumnVector.fromScalar(nullScalar, numRows)
        }
      case MapType(keyType, valueType, _) =>
        // Maps are represented as LIST<STRUCT<key, value>> in cuDF
        // For all-null maps, we create a list column with STRUCT<key, value> element type
        val cudfKeyDType = sparkTypeToCudfIdOpt(keyType)
          .map(id => DType.fromNative(id, 0))
          .getOrElse(DType.INT8)
        val cudfValueDType = sparkTypeToCudfIdOpt(valueType)
          .map(id => DType.fromNative(id, 0))
          .getOrElse(DType.INT8)
        // Create the struct type for map entries (key, value)
        val keyFieldType = new cudf.HostColumnVector.BasicType(true, cudfKeyDType)
        val valueFieldType = new cudf.HostColumnVector.BasicType(true, cudfValueDType)
        val structType = new cudf.HostColumnVector.StructType(true, keyFieldType, valueFieldType)
        // Create an all-null map column (list of structs)
        withResource(cudf.Scalar.listFromNull(structType)) { nullScalar =>
          cudf.ColumnVector.fromScalar(nullScalar, numRows)
        }
      case _ =>
        // Fallback for any other types - create INT8 nulls as placeholder
        // This should not happen in practice since unsupported types should be caught earlier
        cudf.ColumnVector.fromBoxedBytes(Array.fill[java.lang.Byte](numRows)(null): _*)
    }
  }
}
