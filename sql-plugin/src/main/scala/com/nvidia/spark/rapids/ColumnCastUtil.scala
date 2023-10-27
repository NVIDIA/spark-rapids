/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable.{ArrayBuffer, ArrayBuilder}

import ai.rapids.cudf.{ColumnVector, ColumnView, DType}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.sql.types.{ArrayType, BinaryType, ByteType, DataType, MapType, StructField, StructType}

/**
 * This class casts a column to another column if the predicate passed resolves to true.
 * This method should be able to handle nested or non-nested types
 *
 * At this time this is strictly a place for casting methods
 */
object ColumnCastUtil {

  /**
   * Transforms a ColumnView into a new ColumnView using a `PartialFunction` or returns None
   * indicating that no transformation happened (nothing matched). If the partial function matches
   * it is assumed that this takes ownership of the returned view. A lot of caution needs to be
   * taken when using this method because of ownership of the data. This will handle
   * reference counting and return not just the updated ColumnView but also any views and or data
   * that were generated along the way and need to be closed. This includes the returned view
   * itself. So you should not explicitly close the returned view. It will be closed by closing
   * everything in the returned collection of AutoCloseable values.
   *
   * @param cv the view to be updated
   * @param dt the Spark's data type of the input view (if applicable)
   * @param convert the partial function used to convert the data. If this matches and returns
   *                a updated view this function takes ownership of that view.
   * @return None if there were no changes to the view or the updated view along with anything else
   *         that needs to be closed.
   */
  def deepTransformView(cv: ColumnView, dt: Option[DataType] = None)
      (convert: PartialFunction[(ColumnView, Option[DataType]), ColumnView]):
  (Option[ColumnView], ArrayBuffer[AutoCloseable]) = {
    closeOnExcept(ArrayBuffer.empty[AutoCloseable]) { needsClosing =>
      val updated = convert.lift((cv, dt))
      needsClosing ++= updated

      updated match {
        case Some(newCv) =>
          (Some(newCv), needsClosing)
        case None =>
          // Recurse down if needed and check children
          cv.getType.getTypeId match {
            case DType.DTypeEnum.STRUCT =>
              withResource(ArrayBuffer.empty[ColumnView]) { tmpNeedsClosed =>
                val structFields = dt match {
                  case None => Array.empty[StructField]
                  case Some(t: StructType) => t.fields
                  case Some(t) => /* this should never be reach out */
                    throw new IllegalStateException("Invalid input DataType: " +
                      s"Expect StructType but got ${t.toString}")
                }
                var childrenUpdated = false
                val newChildren = ArrayBuffer.empty[ColumnView]
                (0 until cv.getNumChildren).foreach { index =>
                  val child = cv.getChildColumnView(index)
                  tmpNeedsClosed += child
                  val childDt = if (structFields.nonEmpty) {
                    Some(structFields(index).dataType)
                  } else {
                    None
                  }
                  val (updatedChild, needsClosingChild) = deepTransformView(child, childDt)(convert)
                  needsClosing ++= needsClosingChild
                  updatedChild match {
                    case Some(newChild) =>
                      newChildren += newChild
                      childrenUpdated = true
                    case None =>
                      newChildren += child
                  }
                }
                if (childrenUpdated) {
                  withResource(cv.getValid) { valid =>
                    val ret = new ColumnView(DType.STRUCT, cv.getRowCount,
                      Optional.empty[java.lang.Long](), valid, null, newChildren.toArray)
                    (Some(ret), needsClosing)
                  }
                } else {
                  (None, needsClosing)
                }
              }
            case DType.DTypeEnum.LIST =>
              withResource(cv.getChildColumnView(0)) { child =>
                // A ColumnView of LIST type may have data type is ArrayType or MapType in Spark.
                // If it is a MapType, its child will be a column of type struct<key, value>.
                // In such cases, we need to generate the corresponding Spark's data type
                // for the child column as a StructType.
                val childDt = dt match {
                  case None => None
                  case Some(t: ArrayType) => Some(t.elementType)
                  case Some(_: BinaryType) => Some(ByteType)
                  case Some(t: MapType) => Some(StructType(Array(
                    StructField("key", t.keyType, nullable = false),
                    StructField("value", t.valueType, nullable = t.valueContainsNull))))
                  case Some(t) => /* this should never be reach out */
                    throw new IllegalStateException("Invalid input DataType: " +
                      s"Expect ArrayType/BinaryType/MapType but got ${t.toString}")
                }
                val (updatedData, needsClosingData) = deepTransformView(child, childDt)(convert)
                needsClosing ++= needsClosingData
                updatedData match {
                  case Some(updated) =>
                    (Some(GpuListUtils.replaceListDataColumnAsView(cv, updated)), needsClosing)
                  case None =>
                    (None, needsClosing)
                }
              }

            case _ =>
              (None, needsClosing)
          }
      }
    }
  }

  /**
   * Transforms a ColumnVector into a new ColumnVector using a `PartialFunction`.
   * If the partial function matches it is assumed that this takes ownership of the returned view.
   * A lot of caution needs to be taken when using this method because of ownership of the data.
   *
   * @param cv the vector to be updated
   * @param dt the Spark's data type of the input vector (if applicable)
   * @param convert the partial function used to convert the data. If this matches and returns
   *                a updated view this function takes ownership of that view.
   * @return the updated vector
   */
  def deepTransform(cv: ColumnVector, dt: Option[DataType] = None)
      (convert: PartialFunction[(ColumnView, Option[DataType]), ColumnView]): ColumnVector = {
    val (retView, needsClosed) = deepTransformView(cv, dt)(convert)
    withResource(needsClosed) { _ =>
      retView match {
        case Some(updated) =>
          // Don't need to close updated because it is covered by needsClosed
          updated.copyToColumnVector()
        case None =>
          cv.incRefCount()
      }
    }
  }

  /**
   * This method deep casts the input ColumnView to a new column if the predicate passed to this
   * method resolves to true.
   * Note: This method will also cast children of nested types to the given type if predicate
   * succeeds
   * @param cv              The column view that could potentially have a type to replace
   * @param predicate       Condition on which to cast the column or child column view
   * @param convert         Method used to convert the column view to a new column view
   * @return
   */
  def ifTrueThenDeepConvertTypeAtoTypeB(
      cv: ColumnVector,
      dataType: DataType,
      predicate: (DataType, ColumnView) => Boolean,
      convert: (DataType, ColumnView) => ColumnView): ColumnVector = {
    /*
     * 'convertTypeAtoTypeB' method returns a ColumnView that should be copied out to a
     * ColumnVector  before closing the `toClose` views otherwise it will close the returned view
     * and it's children as well.
     */
    def convertTypeAToTypeB(
        cv: ColumnView,
        dataType: DataType,
        predicate: (DataType, ColumnView) => Boolean,
        toClose: ArrayBuffer[ColumnView]): ColumnView = {
      dataType match {
        case a: ArrayType =>
          val child = cv.getChildColumnView(0)
          toClose += child
          val newChild = convertTypeAToTypeB(child, a.elementType, predicate, toClose)
          if (child == newChild) {
            cv
          } else {
            val newView = cv.replaceListChild(newChild)
            toClose += newView
            newView
          }
        case s: StructType =>
          val newColumns = ArrayBuilder.make[ColumnView]
          newColumns.sizeHint(cv.getNumChildren)
          val newColIndices = ArrayBuilder.make[Int]
          newColIndices.sizeHint(cv.getNumChildren)
          (0 until cv.getNumChildren).foreach { i =>
            val child = cv.getChildColumnView(i)
            toClose += child
            val newChild = convertTypeAToTypeB(child, s.fields(i).dataType, predicate, toClose)
            if (newChild != child) {
              newColumns += newChild
              newColIndices += i
            }
          }
          val cols = newColumns.result()
          if (cols.nonEmpty) {
            // create a new struct column with the new ones
            val newView = cv.replaceChildrenWithViews(newColIndices.result(), cols)
            toClose += newView
            newView
          } else {
            cv
          }
        case m: MapType =>
          // map is list of structure
          val struct = cv.getChildColumnView(0)
          toClose += struct

          if(cv.getType != DType.LIST || struct.getType != DType.STRUCT) {
            throw new IllegalStateException("Map should be List(Structure) in column view")
          }

          val newChild = convertTypeAToTypeB(struct,
            StructType(Array(StructField("", m.keyType), StructField("", m.valueType))),
            predicate, toClose)

          if (struct == newChild) {
            cv
          } else {
            val newView = cv.replaceListChild(newChild)
            toClose += newView
            newView
          }
        case _ =>
          if (predicate(dataType, cv)) {
            val col = convert(dataType, cv)
            toClose += col
            col
          } else {
            cv
          }
      }
    }

    withResource(new ArrayBuffer[ColumnView]) { toClose =>
      val tmp = convertTypeAToTypeB(cv, dataType, predicate, toClose)
      if (tmp != cv) {
        tmp.copyToColumnVector()
      } else {
        tmp.asInstanceOf[ColumnVector].incRefCount()
      }
    }
  }

}
