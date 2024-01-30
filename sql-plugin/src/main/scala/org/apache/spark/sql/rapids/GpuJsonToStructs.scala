/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import scala.collection.mutable.ListBuffer
import ai.rapids.cudf
import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar, Schema, TableDebug}
import com.nvidia.spark.rapids.{GpuCast, GpuColumnVector, GpuScalar, GpuUnaryExpression}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.MapUtils
import com.nvidia.spark.rapids.shims.GpuJsonToStructsShim
import org.apache.commons.text.StringEscapeUtils
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.types._

case class GpuJsonToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    enableMixedTypesAsString: Boolean,
    timeZoneId: Option[String] = None)
    extends GpuUnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes
        with NullIntolerant {

  lazy val emptyRowStr = constructEmptyRow(schema)

  private def constructEmptyRow(schema: DataType): String = {
    schema match {
      case struct: StructType if struct.fields.nonEmpty =>
        s"""{"${StringEscapeUtils.escapeJson(struct.head.name)}":null}"""
      case other =>
        throw new IllegalArgumentException(s"$other is not supported as a top level type")    }
  }

  private def cleanAndConcat(input: cudf.ColumnVector): (cudf.ColumnVector, cudf.ColumnVector) = {
    val stripped = if (input.getData == null) {
      input.incRefCount
    } else {
      withResource(cudf.Scalar.fromString(" ")) { space =>
        input.strip(space)
      }
    }

    withResource(stripped) { stripped =>
      val isEmpty = withResource(stripped.getCharLengths) { lengths =>
        withResource(cudf.Scalar.fromInt(0)) { zero =>
          lengths.lessOrEqualTo(zero)
        }
      }
      val isNullOrEmptyInput = withResource(isEmpty) { _ =>
        withResource(input.isNull) { isNull =>
          isNull.binaryOp(cudf.BinaryOp.NULL_LOGICAL_OR, isEmpty, cudf.DType.BOOL8)
        }
      }
      closeOnExcept(isNullOrEmptyInput) { _ =>
        withResource(cudf.Scalar.fromString(emptyRowStr)) { emptyRow =>
          withResource(isNullOrEmptyInput.ifElse(emptyRow, stripped)) { nullsReplaced =>
            val isLiteralNull = withResource(Scalar.fromString("null")) { literalNull =>
              nullsReplaced.equalTo(literalNull)
            }
            withResource(isLiteralNull) { _ =>
              withResource(isLiteralNull.ifElse(emptyRow, nullsReplaced)) { cleaned =>
                checkForNewline(cleaned, "\n", "line separator")
                checkForNewline(cleaned, "\r", "carriage return")

                // add a newline to each JSON line
                val withNewline = withResource(cudf.Scalar.fromString("\n")) { lineSep =>
                  withResource(ColumnVector.fromScalar(lineSep, cleaned.getRowCount.toInt)) {
                    newLineCol =>
                      ColumnVector.stringConcatenate(Array[ColumnView](cleaned, newLineCol))
                  }
                }

                // join all the JSON lines into one string
                val joined = withResource(withNewline) { _ =>
                  withResource(Scalar.fromString("")) { emptyString =>
                    withNewline.joinStrings(emptyString, emptyRow)
                  }
                }

                (isNullOrEmptyInput, joined)
              }
            }
          }
        }
      }
    }
  }

  private def checkForNewline(cleaned: ColumnVector, newlineStr: String, name: String): Unit = {
    withResource(cudf.Scalar.fromString(newlineStr)) { newline =>
      withResource(cleaned.stringContains(newline)) { hasNewline =>
        withResource(hasNewline.any()) { anyNewline =>
          if (anyNewline.isValid && anyNewline.getBoolean) {
            throw new IllegalArgumentException(
              s"We cannot currently support parsing JSON that contains a $name in it")
          }
        }
      }
    }
  }

  override protected def doColumnar(input: GpuColumnVector): cudf.ColumnVector = {
    schema match {
      case _: MapType =>
        MapUtils.extractRawMapFromJsonString(input.getBase)
      case struct: StructType => {
        // read boolean and numeric columns as strings in cuDF
        val dataSchemaWithStrings = StructType(struct.fields
          .map(f => {
            f.dataType match {
              case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
                   DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType |
                   DataTypes.DoubleType | _: DecimalType | DataTypes.DateType |
                   DataTypes.TimestampType =>
                f.copy(dataType = DataTypes.StringType)
              case _ =>
                f
            }
          }))

        val builder = Schema.builder
        for (field <- dataSchemaWithStrings.fields) {
          val dt = field.dataType match {
            case _: StructType =>
              // note we cannot specify to read primitives in the struct as strings yet
              DType.STRUCT
            case _ => GpuColumnVector.getNonNestedRapidsType(field.dataType)
          }
          builder.column(dt, field.name)
        }
        val cudfSchema = builder.build

        val debug = TableDebug.builder().build()

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
          val table = withResource(combinedHost) { combinedHost =>
            val data = combinedHost.getData
            val start = combinedHost.getStartListOffset(0)
            val end = combinedHost.getEndListOffset(0)
            val length = end - start

            val jsonOptions = cudf.JSONOptions.builder()
              .withRecoverWithNull(true)
              .withMixedTypesAsStrings(enableMixedTypesAsString)
              .withNormalizeSingleQuotes(true)
              .build()
            cudf.Table.readJSON(cudfSchema, jsonOptions, data, start, length)
          }

          debug.debug("table", table)

          // process duplicated field names in input struct schema
//          val fieldNames = processFieldNames(struct.fields.map (f => (f.name, f.dataType)))

          withResource(table) { _ =>
            // Step 5: verify that the data looks correct
            if (table.getRowCount != numRows) {
              throw new IllegalStateException("The input data didn't parse correctly and we read " +
                  s"a different number of rows than was expected. Expected $numRows, " +
                  s"but got ${table.getRowCount}")
            }

            val columns = new ListBuffer[ColumnVector]()

            // Step 6: get the data based on input struct schema
            for (i <- 0 until table.getNumberOfColumns) {
              val col = table.getColumn(i)
              val dataSchemaType = struct.fields(i).dataType
              val readSchemaType = dataSchemaWithStrings.fields(i).dataType

              println(s"*** dataSchemaType=$dataSchemaType, readSchemaType=$readSchemaType")

              val castColumn = (readSchemaType, dataSchemaType) match {
                case (DataTypes.StringType, DataTypes.BooleanType) =>
                  castJsonStringToBool(col)
                case (DataTypes.StringType, DataTypes.DateType) =>
                  GpuJsonToStructsShim.castJsonStringToDate(col, options)
                case (_, DataTypes.DateType) =>
                  castToNullDate(input.getBase)
                case (DataTypes.StringType, DataTypes.TimestampType) =>
                  GpuJsonToStructsShim.castJsonStringToTimestamp(col, options)
//                case (DataTypes.LongType, DataTypes.TimestampType) =>
//                  GpuCast.castLongToTimestamp(col, DataTypes.TimestampType)
                case (_, DataTypes.TimestampType) =>
                  castToNullTimestamp(input.getBase)

      // TODO other primitive types - byte, short, int, long, float, double, decimal
                // TODO add tests that cover all types

//                case DataTypes.ByteType =>
//                  castStringToInt(table.getColumn(i), DType.INT8)
//                case DataTypes.ShortType =>
//                  castStringToInt(table.getColumn(i), DType.INT16)
//                case DataTypes.IntegerType =>
//                  castStringToInt(table.getColumn(i), DType.INT32)
//                case DataTypes.LongType =>
//                  castStringToInt(table.getColumn(i), DType.INT64)
//                case DataTypes.FloatType =>
//                  castStringToFloat(table.getColumn(i), DType.FLOAT32)
//                case DataTypes.DoubleType =>
//                  castStringToFloat(table.getColumn(i), DType.FLOAT64)
//                case dt: DecimalType =>
//                  castStringToDecimal(table.getColumn(i), dt)
//                case DataTypes.DateType =>
//                  castStringToDate(table.getColumn(i), DType.TIMESTAMP_DAYS)
//                case DataTypes.TimestampType =>
//                  castStringToTimestamp(table.getColumn(i), timestampFormat,
//                    DType.TIMESTAMP_MICROSECONDS)
                case _ =>
                  GpuCast.doCast(col, readSchemaType, dataSchemaType)
              }
              println(s"castColumn=${castColumn.getType}")
              columns += castColumn
            }

            // Step 7: turn the data into a Struct
            withResource(columns) { columns =>
              withResource(cudf.ColumnVector.makeStruct(columns: _*)) { structData =>
                // Step 8: put nulls back in for nulls and empty strings
                if (columns.exists(_.getType == DType.STRUCT)) {
                  // TODO cannot call ifElse due to mismatch between
                  //  scalar when structs are present
                  // cannot even print the scalar to debug this due to
                  // https://github.com/rapidsai/cudf/issues/14855
                  structData.incRefCount()
                } else {
                  withResource(GpuScalar.from(null, struct)) { nullVal =>
                    isNullOrEmpty.ifElse(nullVal, structData)
                  }
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

  private def castJsonStringToBool(input: ColumnVector): ColumnVector = {
    val isTrue = withResource(Scalar.fromString("true")) { trueStr =>
      input.equalTo(trueStr)
    }
    withResource(isTrue) { _ =>
      val isFalse = withResource(Scalar.fromString("false")) { falseStr =>
        input.equalTo(falseStr)
      }
      val falseOrNull = withResource(isFalse) { _ =>
        withResource(Scalar.fromBool(false)) { falseLit =>
          withResource(Scalar.fromNull(DType.BOOL8)) { nul =>
            isFalse.ifElse(falseLit, nul)
          }
        }
      }
      withResource(falseOrNull) { _ =>
        withResource(Scalar.fromBool(true)) { trueLit =>
          isTrue.ifElse(trueLit, falseOrNull)
        }
      }
    }
  }

  private def castToNullDate(input: ColumnVector): ColumnVector = {
    withResource(Scalar.fromNull(DType.TIMESTAMP_DAYS)) { nullScalar =>
      ColumnVector.fromScalar(nullScalar, input.getRowCount.toInt)
    }
  }

  private def castToNullTimestamp(input: ColumnVector): ColumnVector = {
    withResource(Scalar.fromNull(DType.TIMESTAMP_MICROSECONDS)) { nullScalar =>
      ColumnVector.fromScalar(nullScalar, input.getRowCount.toInt)
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def dataType: DataType = schema.asNullable

  override def nullable: Boolean = true
}
