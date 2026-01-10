/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types._

/**
 * GPU implementation for Spark's `from_protobuf` decode path.
 *
 * This is designed to replace `org.apache.spark.sql.protobuf.ProtobufDataToCatalyst` when
 * supported.
 *
 * @param fullSchema The complete output schema (must match the original expression's dataType)
 * @param decodedFieldIndices Indices into fullSchema for fields that will be decoded by GPU.
 *                            Fields not in this array will be null columns.
 * @param fieldNumbers Protobuf field numbers for decoded fields (parallel to decodedFieldIndices)
 * @param cudfTypeIds cuDF type IDs for decoded fields (parallel to decodedFieldIndices)
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

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    val numRows = input.getRowCount.toInt

    // Decode only the requested fields from protobuf
    val decoded = try {
      Protobuf.decodeToStruct(
        input.getBase,
        fieldNumbers,
        cudfTypeIds,
        cudfTypeScales,
        failOnErrors)
    } catch {
      case e: CudfException if failOnErrors =>
        // Convert CudfException to Spark's standard protobuf error for consistent error handling.
        // This allows user code to catch the same exception type regardless of CPU/GPU execution.
        throw QueryExecutionErrors.malformedProtobufMessageDetectedInMessageParsingError(e)
    }

    // Build the full struct with all fields from fullSchema
    // Decoded fields come from the GPU result, others are null columns
    val result = withResource(decoded) { decodedStruct =>
      val fullChildren = new Array[cudf.ColumnVector](fullSchema.fields.length)
      var decodedIdx = 0

      try {
        for (i <- fullSchema.fields.indices) {
          if (decodedIdx < decodedFieldIndices.length && decodedFieldIndices(decodedIdx) == i) {
            // This field was decoded - extract from decoded struct
            fullChildren(i) = decodedStruct.getChildColumnView(decodedIdx).copyToColumnVector()
            decodedIdx += 1
          } else {
            // This field was not decoded - create null column
            fullChildren(i) = GpuFromProtobuf.createNullColumn(
                fullSchema.fields(i).dataType, numRows)
          }
        }
        cudf.ColumnVector.makeStruct(numRows, fullChildren: _*)
      } finally {
        fullChildren.foreach(c => if (c != null) c.close())
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
   */
  def sparkTypeToCudfId(dt: DataType): Int = dt match {
    case BooleanType => DType.BOOL8.getTypeId.getNativeId
    case IntegerType => DType.INT32.getTypeId.getNativeId
    case LongType => DType.INT64.getTypeId.getNativeId
    case FloatType => DType.FLOAT32.getTypeId.getNativeId
    case DoubleType => DType.FLOAT64.getTypeId.getNativeId
    case StringType => DType.STRING.getTypeId.getNativeId
    case BinaryType => DType.LIST.getTypeId.getNativeId
    case other =>
      throw new IllegalArgumentException(s"Unsupported Spark type for protobuf: $other")
  }

  /**
   * Creates a null column of the specified Spark data type with the given number of rows.
   * Used for fields that are not decoded (schema projection optimization).
   */
  def createNullColumn(dataType: DataType, numRows: Int): cudf.ColumnVector = {
    val cudfType = dataType match {
      case BooleanType => DType.BOOL8
      case IntegerType => DType.INT32
      case LongType => DType.INT64
      case FloatType => DType.FLOAT32
      case DoubleType => DType.FLOAT64
      case StringType => DType.STRING
      case BinaryType =>
        // Binary is LIST<INT8> in cuDF
        return withResource(cudf.Scalar.listFromNull(
          new cudf.HostColumnVector.BasicType(false, DType.INT8))) { nullScalar =>
          withResource(cudf.ColumnVector.fromScalar(nullScalar, numRows)) { col =>
            col.incRefCount()
          }
        }
      case st: StructType =>
        // For nested struct, create struct with null children and set all rows to null
        val nullChildren = st.fields.map(f => createNullColumn(f.dataType, numRows))
        return withResource(new AutoCloseableArray(nullChildren)) { _ =>
          withResource(cudf.ColumnVector.makeStruct(numRows, nullChildren: _*)) { struct =>
            // Create a validity mask of all nulls
            withResource(cudf.Scalar.fromBool(false)) { falseBool =>
              withResource(cudf.ColumnVector.fromScalar(falseBool, numRows)) { allFalse =>
                struct.mergeAndSetValidity(BinaryOp.BITWISE_AND, allFalse)
              }
            }
          }
        }
      case ArrayType(elementType, _) =>
        val elementDType = elementType match {
          case BooleanType => DType.BOOL8
          case IntegerType => DType.INT32
          case LongType => DType.INT64
          case FloatType => DType.FLOAT32
          case DoubleType => DType.FLOAT64
          case StringType => DType.STRING
          case _ => DType.INT8 // fallback
        }
        return withResource(cudf.Scalar.listFromNull(
          new cudf.HostColumnVector.BasicType(false, elementDType))) { nullScalar =>
          withResource(cudf.ColumnVector.fromScalar(nullScalar, numRows)) { col =>
            col.incRefCount()
          }
        }
      case _ =>
        // Fallback: use INT8 and hope for the best (shouldn't happen for supported types)
        DType.INT8
    }

    withResource(cudf.Scalar.fromNull(cudfType)) { nullScalar =>
      cudf.ColumnVector.fromScalar(nullScalar, numRows)
    }
  }

  /** Helper class to auto-close an array of ColumnVectors */
  private class AutoCloseableArray(cols: Array[cudf.ColumnVector]) extends AutoCloseable {
    override def close(): Unit = cols.foreach(c => if (c != null) c.close())
  }
}