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

package com.nvidia.spark.rapids.shims

import org.apache.parquet.schema._
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

object ParquetSchemaClipShims {
  /** Stubs for configs not defined before Spark 330 */
  def useFieldId(conf: SQLConf): Boolean = false

  def ignoreMissingIds(conf: SQLConf): Boolean = false

  def checkIgnoreMissingIds(ignoreMissingIds: Boolean, parquetFileSchema: MessageType,
      catalystRequestedSchema: StructType): Unit = {}

  def hasFieldId(field: StructField): Boolean =
    throw new IllegalStateException("This shim should not invoke `hasFieldId`")

  def hasFieldIds(schema: StructType): Boolean =
    throw new IllegalStateException("This shim should not invoke `hasFieldIds`")

  def getFieldId(field: StructField): Int =
    throw new IllegalStateException("This shim should not invoke `getFieldId`")

  def fieldIdToFieldMap(useFieldId: Boolean, fileType: Type): Map[Int, Type] = Map.empty[Int, Type]

  def fieldIdToNameMap(useFieldId: Boolean,
      fileType: Type): Map[Int, String] = Map.empty[Int, String]

  /**
   * Convert a Parquet primitive type to a Spark type.
   * Based on Spark's ParquetSchemaConverter.convertPrimitiveField
   */
  def convertPrimitiveField(field: PrimitiveType): DataType = {
    val typeName = field.getPrimitiveTypeName
    val originalType = field.getOriginalType

    def typeString =
      if (originalType == null) s"$typeName" else s"$typeName ($originalType)"

    def typeNotSupported() =
      TrampolineUtil.throwAnalysisException(s"Parquet type not supported: $typeString")

    def typeNotImplemented() =
      TrampolineUtil.throwAnalysisException(s"Parquet type not yet supported: $typeString")

    def illegalType() =
      TrampolineUtil.throwAnalysisException(s"Illegal Parquet type: $typeString")

    // When maxPrecision = -1, we skip precision range check, and always respect the precision
    // specified in field.getDecimalMetadata.  This is useful when interpreting decimal types stored
    // as binaries with variable lengths.
    def makeDecimalType(maxPrecision: Int = -1): DecimalType = {
      val precision = field.getDecimalMetadata.getPrecision
      val scale = field.getDecimalMetadata.getScale

      if (!(maxPrecision == -1 || 1 <= precision && precision <= maxPrecision)) {
       TrampolineUtil.throwAnalysisException(
         s"Invalid decimal precision: $typeName " +
             s"cannot store $precision digits (max $maxPrecision)")
      }

      DecimalType(precision, scale)
    }

    typeName match {
      case BOOLEAN => BooleanType

      case FLOAT => FloatType

      case DOUBLE => DoubleType

      case INT32 =>
        originalType match {
          case INT_8 => ByteType
          case INT_16 => ShortType
          case INT_32 | null => IntegerType
          case DATE => DateType
          case DECIMAL => makeDecimalType(Decimal.MAX_INT_DIGITS)
          case UINT_8 => typeNotSupported()
          case UINT_16 => typeNotSupported()
          case UINT_32 => typeNotSupported()
          case TIME_MILLIS => typeNotImplemented()
          case _ => illegalType()
        }

      case INT64 =>
        originalType match {
          case INT_64 | null => LongType
          case DECIMAL => makeDecimalType(Decimal.MAX_LONG_DIGITS)
          case UINT_64 => typeNotSupported()
          case TIMESTAMP_MICROS => TimestampType
          case TIMESTAMP_MILLIS => TimestampType
          case _ => illegalType()
        }

      case INT96 =>
        if (!SQLConf.get.isParquetINT96AsTimestamp) {
          TrampolineUtil.throwAnalysisException(
            "INT96 is not supported unless it's interpreted as timestamp. " +
                s"Please try to set ${SQLConf.PARQUET_INT96_AS_TIMESTAMP.key} to true.")
        }
        TimestampType

      case BINARY =>
        originalType match {
          case UTF8 | ENUM | JSON => StringType
          case null if SQLConf.get.isParquetBinaryAsString => StringType
          case null => BinaryType
          case BSON => BinaryType
          case DECIMAL => makeDecimalType()
          case _ => illegalType()
        }

      case FIXED_LEN_BYTE_ARRAY =>
        originalType match {
          case DECIMAL => makeDecimalType(Decimal.maxPrecisionForBytes(field.getTypeLength))
          case INTERVAL => typeNotImplemented()
          case _ => illegalType()
        }

      case _ => illegalType()
    }
  }
}
