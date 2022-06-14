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
   * Now the schema evolution covers only two things. (Full type casting is not supported yet).
   *  1) Cast decimal columns with precision that can be stored in an int to DECIMAL32.
   *     The reason to do this is the plugin requires decimals being stored as DECIMAL32 if the
   *     precision is small enough to fit in an int. And getting this wrong may lead to a number
   *     of problems later on. For example, the cuDF ORC reader always read decimals as DECIMAL64.
   *  2) Add columns for names are in the "readSchema" but not in the "tableSchema".
   *     It will create a new column with nulls for each missing name instead of throwing an
   *     exception.
   *     Column pruning will be done implicitly by iterating the readSchema.
   *
   * (This is mainly used by the GPU Parquet/ORC readers to partially support the schema
   *  evolution.)
   *
   * @param table The input table, will be closed after returning
   * @param tableSchema The schema of the table
   * @param readSchema  The read schema from Spark
   * @param isCaseSensitive Whether the name check should be case sensitive or not
   * @return a new table mapping to the "readSchema". Users should close it if no longer needed.
   */
  private[rapids] def evolveSchemaIfNeededAndClose(
      table: Table,
      tableSchema: StructType,
      readSchema: StructType,
      isCaseSensitive: Boolean): Table = {
    assert(table.getNumberOfColumns == tableSchema.length)
    // Check if schema evolution is needed. It is true when
    //   there are columns with precision can be stored in an int, or
    //   "readSchema" is not equal to "tableSchema".
    val isDecCastNeeded = getPrecisionsList(tableSchema).exists(p => p <= Decimal.MAX_INT_DIGITS)
    val isAddOrPruneColNeeded = readSchema != tableSchema
    if (isDecCastNeeded || isAddOrPruneColNeeded) {
      val name2TypeIdMap = buildTypeIdMapFromSchema(tableSchema, isCaseSensitive)
      withResource(table) { t =>
        val newColumns = readSchema.safeMap { rf =>
          if (name2TypeIdMap.contains(rf.name)) {
            // Found the column in the table, so start the column evolution.
            val typeAndId = name2TypeIdMap(rf.name)
            val cv = t.getColumn(typeAndId._2)
            withResource(new ArrayBuffer[ColumnView]) { toClose =>
              val newCol =
                evolveColumnRecursively(cv, typeAndId._1, rf.dataType, isCaseSensitive, toClose)
              if (newCol == cv) {
                cv.incRefCount()
              } else {
                toClose += newCol
                newCol.copyToColumnVector()
              }
            }
          } else {
            // Return a null column if the name is not found in the table.
            GpuColumnVector.columnVectorFromNull(t.getRowCount.toInt, rf.dataType)
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
      isCaseSensitive: Boolean, toClose: ArrayBuffer[ColumnView]): ColumnView = {
    // An util function to add a view to the buffer "toClose".
    val addToClose = (v: ColumnView) => {
      toClose += v
      v
    }

    // Type casting is not supported yet.
    assert(colType.getClass == targetType.getClass)
    colType match {
      case st: StructType =>
        // This is for the case of nested columns.
        val typeIdMap = buildTypeIdMapFromSchema(st, isCaseSensitive)
        var changed = false
        val newViews = targetType.asInstanceOf[StructType].safeMap { f =>
          if (typeIdMap.contains(f.name)) {
            val typeAndId = typeIdMap(f.name)
            val cv = addToClose(col.getChildColumnView(typeAndId._2))
            val newChild =
              evolveColumnRecursively(cv, typeAndId._1, f.dataType, isCaseSensitive, toClose)
            if (newChild != cv) {
              addToClose(newChild)
              changed = true
            }
            newChild
          } else {
            changed = true
            // Return a null column if the name is not found in the table.
            addToClose(GpuColumnVector.columnVectorFromNull(col.getRowCount.toInt, f.dataType))
          }
        }

        if (changed) {
          // Create a new struct column view with only different children.
          // It would be better to add a dedicate API in cuDF for this.
          val opNullCount = Optional.of(col.getNullCount.asInstanceOf[java.lang.Long])
          new ColumnView(col.getType, col.getRowCount, opNullCount, col.getValid,
            col.getOffsets, newViews.toArray)
        } else {
          col
        }
      case at: ArrayType =>
        val targetElemType = targetType.asInstanceOf[ArrayType].elementType
        val child = addToClose(col.getChildColumnView(0))
        val newChild =
          evolveColumnRecursively(child, at.elementType, targetElemType, isCaseSensitive, toClose)
        if (child == newChild) {
          col
        } else {
          col.replaceListChild(addToClose(newChild))
        }
      case mt: MapType =>
        val targetMapType = targetType.asInstanceOf[MapType]
        val listChild = addToClose(col.getChildColumnView(0))
        // listChild is struct with two fields: key and value.
        val newStructChildren = new ArrayBuffer[ColumnView](2)
        val newStructIndices = new ArrayBuffer[Int](2)

        // An until function to handle key and value view
        val processView = (id: Int, srcType: DataType, distType: DataType) => {
          val view = addToClose(listChild.getChildColumnView(id))
          val newView = evolveColumnRecursively(view, srcType, distType, isCaseSensitive, toClose)
          if (newView != view) {
            newStructChildren += addToClose(newView)
            newStructIndices += id
          }
        }
        // key and value
        processView(0, mt.keyType, targetMapType.keyType)
        processView(1, mt.valueType, targetMapType.valueType)

        if (newStructChildren.nonEmpty) {
          // Have new key or value, or both
          col.replaceListChild(
            addToClose(listChild.replaceChildrenWithViews(newStructIndices.toArray,
              newStructChildren.toArray))
          )
        } else {
          col
        }
      case dt: DecimalType if !GpuColumnVector.getNonNestedRapidsType(dt).equals(col.getType) =>
        col.castTo(DecimalUtil.createCudfDecimal(dt))
      case _ => col
    }
  }

  private def writerOptionsFromField[T <: NestedBuilder[_, _], V <: ColumnWriterOptions](
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
  def writerOptionsFromSchema[T <: NestedBuilder[_, _], V <: ColumnWriterOptions](
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
