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

package org.apache.spark.sql.rapids

import ai.rapids.cudf
import com.nvidia.spark.rapids.{GpuColumnVector, GpuScalar, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuCast.doCast
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.MapUtils
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.types._

case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerant {

  private def constructEmptyRow(schema: DataType): String = {
    schema match {
      case struct: StructType if struct.fields.nonEmpty =>
        s"""{"${StringEscapeUtils.escapeJson(struct.head.name)}":null}"""
      case other =>
        throw new IllegalArgumentException(s"$other is not supported as a top level type")    }
  }

  lazy val emptyRowStr = constructEmptyRow(schema)
  
  private def cleanAndConcat(input: cudf.ColumnVector): (cudf.ColumnVector, cudf.ColumnVector) = {
    withResource(cudf.Scalar.fromString(emptyRowStr)) { emptyRow =>

      val stripped = if (input.getData == null) {
        input.incRefCount
      } else {
        withResource(cudf.Scalar.fromString(" ")) { space =>
          input.strip(space)
        }
      }

      withResource(stripped) { stripped =>
        val isNullOrEmptyInput = withResource(input.isNull) { isNull =>
          val isEmpty = withResource(stripped.getCharLengths) { lengths =>
            withResource(cudf.Scalar.fromInt(0)) { zero =>
              lengths.lessOrEqualTo(zero)
            }
          }
          withResource(isEmpty) { isEmpty =>
            isNull.binaryOp(cudf.BinaryOp.NULL_LOGICAL_OR, isEmpty, cudf.DType.BOOL8)
          }
        }
        closeOnExcept(isNullOrEmptyInput) { _ =>
          withResource(isNullOrEmptyInput.ifElse(emptyRow, stripped)) { cleaned =>
            withResource(cudf.Scalar.fromString("\n")) { lineSep =>
              withResource(cudf.Scalar.fromString("\r")) { returnSep =>
                withResource(cleaned.stringContains(lineSep)) { inputHas =>
                  withResource(inputHas.any()) { anyLineSep =>
                    if (anyLineSep.isValid && anyLineSep.getBoolean) {
                      throw new IllegalArgumentException("We cannot currently support parsing " +
                          "JSON that contains a line separator in it")
                    }
                  }
                }
                withResource(cleaned.stringContains(returnSep)) { inputHas =>
                  withResource(inputHas.any()) { anyReturnSep =>
                    if (anyReturnSep.isValid && anyReturnSep.getBoolean) {
                      throw new IllegalArgumentException("We cannot currently support parsing " +
                          "JSON that contains a carriage return in it")
                    }
                  }
                }
              }
              (isNullOrEmptyInput, cleaned.joinStrings(lineSep, emptyRow))
            }
          }
        }
      }
    }
  }

  // Process a sequence of field names. If there are duplicated field names, we only keep the field
  // name with the largest index in the sequence, for others, replace the field names with null.
  // Example:
  // Input = [("a", StringType), ("b", StringType), ("a", IntegerType)]
  // Output = [(null, StringType), ("b", StringType), ("a", IntegerType)]
  private def processFieldNames(names: Seq[(String, DataType)]): Seq[(String, DataType)] = {
    val zero = (Set.empty[String], Seq.empty[(String, DataType)])
    val (_, resultFields) = names.foldRight (zero) { case ((name, dtype), (existingNames, acc)) =>
      if (existingNames(name)) {
        (existingNames, (null, dtype) +: acc)
      } else {
        (existingNames + name, (name, dtype) +: acc)
      }
    }
    resultFields
  }

  // Given a cudf column, return its Spark type
  private def getSparkType(col: cudf.ColumnView): DataType = {
    col.getType match {
      case cudf.DType.INT8 | cudf.DType.UINT8 => ByteType
      case cudf.DType.INT16 | cudf.DType.UINT16 => ShortType
      case cudf.DType.INT32 | cudf.DType.UINT32 => IntegerType
      case cudf.DType.INT64 | cudf.DType.UINT64 => LongType
      case cudf.DType.FLOAT32 => FloatType
      case cudf.DType.FLOAT64 => DoubleType
      case cudf.DType.BOOL8 => BooleanType
      case cudf.DType.STRING => StringType
      case cudf.DType.LIST => ArrayType(getSparkType(col.getChildColumnView(0)))
      case cudf.DType.STRUCT =>
        val structFields = (0 until col.getNumChildren).map { i =>
          val child = col.getChildColumnView(i)
          StructField("", getSparkType(child))
        }
        StructType(structFields)
      case t => throw new IllegalArgumentException(
        s"GpuJsonToStructs currently cannot process CUDF column of type $t.")
    }
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    schema match {
      case _: MapType =>
        MapUtils.extractRawMapFromJsonString(input.getBase)
      case struct: StructType => {
        // We cannot handle all corner cases with this right now. The parser just isn't
        // good enough, but we will try to handle a few common ones.
        val numRows = input.getRowCount.toInt

        // Step 1: verify and preprocess the data to clean it up and normalize a few things
        // Step 2: Concat the data into a single buffer
        val (isNullOrEmpty, combined) = cleanAndConcat(input.getBase)
        withResource(isNullOrEmpty) { isNullOrEmpty =>
          // Step 3: copy the data back to the host so we can parse it.
          val combinedHost = withResource(combined) { combined =>
            combined.copyToHost()
          }
          // Step 4: Have cudf parse the JSON data
          val (names, rawTable) = withResource(combinedHost) { combinedHost =>
            val data = combinedHost.getData
            val start = combinedHost.getStartListOffset(0)
            val end = combinedHost.getEndListOffset(0)
            val length = end - start

            withResource(cudf.Table.readJSON(cudf.JSONOptions.DEFAULT, data, start,
              length)) { tableWithMeta =>
              val names = tableWithMeta.getColumnNames
              (names, tableWithMeta.releaseTable())
            }
          }

          // process duplicated field names in input struct schema
          val fieldNames = processFieldNames(struct.fields.map (f => (f.name, f.dataType)))

          withResource(rawTable) { rawTable =>
            // Step 5: verify that the data looks correct
            if (rawTable.getRowCount != numRows) {
              throw new IllegalStateException("The input data didn't parse correctly and we read " +
                  s"a different number of rows than was expected. Expected $numRows, " +
                  s"but got ${rawTable.getRowCount}")
            }

            // Step 6: get the data based on input struct schema
            val columns = fieldNames.safeMap { case (name, dtype) =>
              val i = names.indexOf(name)
              if (i == -1) {
                GpuColumnVector.columnVectorFromNull(numRows, dtype)
              } else {
                val col = rawTable.getColumn(i)
                // getSparkType is only used to get the from type for cast
                doCast(col, getSparkType(col), dtype)
              }
            }

            // Step 7: turn the data into a Struct
            withResource(columns) { columns =>
              withResource(cudf.ColumnVector.makeStruct(columns: _*)) { structData =>
                // Step 8: put nulls back in for nulls and empty strings
                withResource(GpuScalar.from(null, struct)) { nullVal =>
                  isNullOrEmpty.ifElse(nullVal, structData)
                }
              }
            }
          }
        }
      }
      case _ => throw new IllegalArgumentException(
        s"GpuJsonToStructs currently does not support schema of type $schema.")
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = schema.asNullable

  override def nullable: Boolean = true
}
