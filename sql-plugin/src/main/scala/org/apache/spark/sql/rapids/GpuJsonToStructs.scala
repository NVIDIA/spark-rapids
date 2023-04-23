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

import scala.collection.mutable.Set

import ai.rapids.cudf
import com.nvidia.spark.rapids.{GpuColumnVector, GpuScalar, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.MapUtils

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.types.{AbstractDataType, DataType, MapType, StringType, StructType}

case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerant {
  
  private def cleanAndConcat(input: cudf.ColumnVector): (cudf.ColumnVector, cudf.ColumnVector) ={
    withResource(cudf.Scalar.fromString("{}")) { emptyRow =>
      val stripped = withResource(cudf.Scalar.fromString(" ")) { space =>
        input.strip(space)
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
              withResource(cleaned.stringContains(lineSep)) { inputHas =>
                withResource(inputHas.any()) { anyLineSep =>
                  if (anyLineSep.isValid && anyLineSep.getBoolean) {
                    throw new IllegalArgumentException("We cannot currently support parsing " +
                        "JSON that contains a line separator in it")
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

  private def processFieldNames(names: Seq[(String, DataType)]): Seq[(String, DataType)] = {
    val existingNames = Set[String]()
    // for duplicated field names, only keep the one with the largest index
    names.foldRight(Seq[(String, DataType)]())((elem, acc) => {
      val (name, dtype) = elem
      if (existingNames(name)) (null, dtype)+:acc else {existingNames += name; (name, dtype)+:acc}})
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
          val fieldNames = processFieldNames(struct.fields.map { field => 
              (field.name, field.dataType)})

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
                rawTable.getColumn(i).incRefCount
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
        s"GpuJsonToStructs currently does not support schema of type ${schema}.")
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = schema.asNullable

  override def nullable: Boolean = true
}
