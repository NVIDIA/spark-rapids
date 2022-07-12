/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import java.util.Optional

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import ai.rapids.cudf._
import ai.rapids.cudf.ColumnWriterOptions._
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import org.apache.orc.TypeDescription

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.types._

object SchemaUtils extends Arm {
  // Parquet field ID metadata key
  val FIELD_ID_METADATA_KEY = "parquet.field.id"

  /**
   * Convert a TypeDescription to a Catalyst StructType.
   */
  implicit def toCatalystSchema(schema: TypeDescription): StructType = {
    // Here just follows the implementation of Spark3.0.x, so it does not replace the
    // CharType/VarcharType with StringType. It is OK because GPU does not support
    // these two char types yet.
    CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
  }

  private def getPrecisionsList(dt: DataType): Seq[Int] = dt match {
    case ArrayType(et, _) => getPrecisionsList(et)
    case MapType(kt, vt, _) => getPrecisionsList(kt) ++ getPrecisionsList(vt)
    case StructType(fields) => fields.flatMap(f => getPrecisionsList(f.dataType))
    case d: DecimalType => Seq(d.precision)
    case _ => Seq.empty[Int]
  }

  private def buildTypeIdMapFromSchema(schema: StructType,
      isCaseSensitive: Boolean): Map[String, (DataType, Int)] = {
    val typeIdSeq = schema.map(_.dataType).zipWithIndex
    val name2TypeIdSensitiveMap = schema.map(_.name).zip(typeIdSeq).toMap
    if (isCaseSensitive) {
      name2TypeIdSensitiveMap
    } else {
      CaseInsensitiveMap[(DataType, Int)](name2TypeIdSensitiveMap)
    }
  }

  /**
   * Execute the schema evolution, which includes
   *  1) Casting decimal columns with precision that can be stored in an int to cuDF DECIMAL32.
   *     The reason to do this is the plugin requires decimals being stored as DECIMAL32 if the
   *     precision is small enough to fit in an int. And getting this wrong may lead to a number
   *     of problems later on. For example, the cuDF ORC reader always read decimals as DECIMAL64.
   *  2) Adding columns filled with nulls for names are in the "readSchema"
   *     but not in the "tableSchema",
   *  3) Re-ordering columns according to the `readSchema`,
   *  4) Removing columns not being required by the `readSchema`,
   *  5) Running type casting when the required type is not equal to the column type.
   *     "castFunc" is required in this case, otherwise it will blow up.
   *
   * (This is designed for the GPU Parquet/ORC readers to support the schema evolution. Will
   *  update the Parquet reader in the future.)
   *
   * @param table The input table, will be closed after returning
   * @param tableSchema The schema of the table
   * @param readSchema  The read schema from Spark
   * @param isCaseSensitive Whether the name check should be case sensitive or not
   * @param castFunc optional, function to cast the input column to the required type
   * @return a new table mapping to the "readSchema". Users should close it if no longer needed.
   */
  private[rapids] def evolveSchemaIfNeededAndClose(
      table: Table,
      tableSchema: StructType,
      readSchema: StructType,
      isCaseSensitive: Boolean,
      castFunc: Option[(ColumnView, DataType) => ColumnView] = None): Table = {
    // Schema evolution is needed when
    //   1) there are columns with precision can be stored in an int, or
    //   2) "readSchema" is not equal to "tableSchema".
    val isSchemaEvolutionNeeded = closeOnExcept(table) { _ =>
      assert(table.getNumberOfColumns == tableSchema.length)
      val hasDec32Col = getPrecisionsList(tableSchema).exists(p => p <= Decimal.MAX_INT_DIGITS)
      hasDec32Col || readSchema != tableSchema
    }
    if (isSchemaEvolutionNeeded) {
      withResource(table) { _ =>
        val name2TypeIdMap = buildTypeIdMapFromSchema(tableSchema, isCaseSensitive)
        val newColumns = readSchema.safeMap { rf =>
          if (name2TypeIdMap.contains(rf.name)) {
            // Found the column in the table, so start the column evolution.
            val typeAndId = name2TypeIdMap(rf.name)
            val cv = table.getColumn(typeAndId._2)
            withResource(new ArrayBuffer[ColumnView]) { toClose =>
              val newCol = evolveColumnRecursively(cv, typeAndId._1, rf.dataType, isCaseSensitive,
                toClose, castFunc)
              if (newCol == cv) {
                cv.incRefCount()
              } else {
                toClose += newCol
                newCol.copyToColumnVector()
              }
            }
          } else {
            // Return a null column if the name is not found in the table.
            GpuColumnVector.columnVectorFromNull(table.getRowCount.toInt, rf.dataType)
          }
        }
        withResource(newColumns) { newCols =>
          new Table(newCols: _*)
        }
      }
    } else {
      table
    }
  }

  private def evolveColumnRecursively(col: ColumnView, colType: DataType, targetType: DataType,
      isCaseSensitive: Boolean, toClose: ArrayBuffer[ColumnView],
      castFunc: Option[(ColumnView, DataType) => ColumnView]): ColumnView = {
    // An util function to add a view to the buffer "toClose".
    val addToClose = (v: ColumnView) => {
      toClose += v
      v
    }

    (colType, targetType) match {
      case (colSt: StructType, toSt: StructType) =>
        // This is for the case of nested columns.
        val needSchemaEvo = colSt != toSt ||
          getPrecisionsList(colSt).exists(p => p <= Decimal.MAX_INT_DIGITS)
        if (needSchemaEvo) {
          val typeIdMap = buildTypeIdMapFromSchema(colSt, isCaseSensitive)
          val newViews = toSt.safeMap { f =>
            if (typeIdMap.contains(f.name)) {
              val typeAndId = typeIdMap(f.name)
              val cv = addToClose(col.getChildColumnView(typeAndId._2))
              val newChild = evolveColumnRecursively(cv, typeAndId._1, f.dataType,
                isCaseSensitive, toClose, castFunc)
              if (newChild != cv) {
                addToClose(newChild)
              }
              newChild
            } else {
              // Return a null column if the name is not found in the table.
              addToClose(
                GpuColumnVector.columnVectorFromNull(col.getRowCount.toInt, f.dataType))
            }
          }
          val opNullCount = Optional.of(col.getNullCount.asInstanceOf[java.lang.Long])
          new ColumnView(col.getType, col.getRowCount, opNullCount, col.getValid,
            col.getOffsets, newViews.toArray)
        } else {
          col
        }
      case (colAt: ArrayType, toAt: ArrayType) =>
        val child = addToClose(col.getChildColumnView(0))
        val newChild = evolveColumnRecursively(child, colAt.elementType, toAt.elementType,
          isCaseSensitive, toClose, castFunc)
        if (child == newChild) {
          col
        } else {
          col.replaceListChild(addToClose(newChild))
        }
      case (colMt: MapType, toMt: MapType) =>
        val listChild = addToClose(col.getChildColumnView(0))
        // listChild is struct with two fields: key and value.
        val newStructChildren = new ArrayBuffer[ColumnView](2)
        val newStructIndices = new ArrayBuffer[Int](2)

        // An until function to handle key and value view
        val processView = (id: Int, srcType: DataType, distType: DataType) => {
          val view = addToClose(listChild.getChildColumnView(id))
          val newView = evolveColumnRecursively(view, srcType, distType, isCaseSensitive,
            toClose, castFunc)
          if (newView != view) {
            newStructChildren += addToClose(newView)
            newStructIndices += id
          }
        }
        // key and value
        processView(0, colMt.keyType, toMt.keyType)
        processView(1, colMt.valueType, toMt.valueType)

        if (newStructChildren.nonEmpty) {
          // Have new key or value, or both
          col.replaceListChild(
            addToClose(listChild.replaceChildrenWithViews(newStructIndices.toArray,
              newStructChildren.toArray))
          )
        } else {
          col
        }
      case (fromDec: DecimalType, toDec: DecimalType) if fromDec == toDec &&
          !GpuColumnVector.getNonNestedRapidsType(fromDec).equals(col.getType) =>
        col.castTo(DecimalUtil.createCudfDecimal(fromDec))
      case _ if colType != targetType =>
        castFunc.map(f => f(col, targetType))
          .getOrElse(throw new QueryExecutionException("Casting function is missing for " +
            s"type conversion from $colType to $targetType"))
      case _ => col
    }
  }

  private def writerOptionsFromField[T <: NestedBuilder[T, V], V <: ColumnWriterOptions](
      builder: NestedBuilder[T, V],
      dataType: DataType,
      name: String,
      nullable: Boolean,
      writeInt96: Boolean,
      fieldMeta: Metadata,
      parquetFieldIdWriteEnabled: Boolean): T = {

    // Parquet specific field id
    val parquetFieldId: Option[Int] = if (fieldMeta.contains(FIELD_ID_METADATA_KEY)) {
      Option(Math.toIntExact(fieldMeta.getLong(FIELD_ID_METADATA_KEY)))
    } else {
      Option.empty
    }

    dataType match {
      case dt: DecimalType =>
        if(parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          builder.withDecimalColumn(name, dt.precision, nullable, parquetFieldId.get)
        } else {
          builder.withDecimalColumn(name, dt.precision, nullable)
        }
      case TimestampType =>
        if(parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          builder.withTimestampColumn(name, writeInt96, nullable, parquetFieldId.get)
        } else {
          builder.withTimestampColumn(name, writeInt96, nullable)
        }
      case s: StructType =>
        val structB = if(parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          structBuilder(name, nullable, parquetFieldId.get)
        } else {
          structBuilder(name, nullable)
        }
        builder.withStructColumn(writerOptionsFromSchema(
          structB,
          s,
          writeInt96, parquetFieldIdWriteEnabled).build())
      case a: ArrayType =>
        builder.withListColumn(
          writerOptionsFromField(
            listBuilder(name, nullable),
            a.elementType,
            name,
            a.containsNull,
            writeInt96, fieldMeta, parquetFieldIdWriteEnabled).build())
      case m: MapType =>
        // It is ok to use `StructBuilder` here for key and value, since either
        // `OrcWriterOptions.Builder` or `ParquetWriterOptions.Builder` is actually an
        // `AbstractStructBuilder`, and here only handles the common column metadata things.
        builder.withMapColumn(
          mapColumn(name,
            writerOptionsFromField(
              structBuilder(name, nullable),
              m.keyType,
              "key",
              nullable = false,
              writeInt96, fieldMeta, parquetFieldIdWriteEnabled).build().getChildColumnOptions()(0),
            writerOptionsFromField(
              structBuilder(name, nullable),
              m.valueType,
              "value",
              m.valueContainsNull,
              writeInt96,
              fieldMeta,
              parquetFieldIdWriteEnabled).build().getChildColumnOptions()(0)))
      case _ =>
        if(parquetFieldIdWriteEnabled && parquetFieldId.nonEmpty) {
          builder.withColumn(nullable, name, parquetFieldId.get)
        } else {
          builder.withColumns(nullable, name)
        }
    }
    builder.asInstanceOf[T]
  }

  /**
   * Build writer options from schema for both ORC and Parquet writers.
   *
   * (There is an open issue "https://github.com/rapidsai/cudf/issues/7654" for Parquet writer,
   * but it is circumvented by https://github.com/rapidsai/cudf/pull/9061, so the nullable can
   * go back to the actual setting, instead of the hard-coded nullable=true before.)
   */
  def writerOptionsFromSchema[T <: NestedBuilder[T, V], V <: ColumnWriterOptions](
      builder: NestedBuilder[T, V],
      schema: StructType,
      writeInt96: Boolean = false,
      parquetFieldIdEnabled: Boolean = false): T = {
    schema.foreach(field =>
      writerOptionsFromField(builder, field.dataType, field.name, field.nullable, writeInt96,
        field.metadata, parquetFieldIdEnabled)
    )
    builder.asInstanceOf[T]
  }
}
