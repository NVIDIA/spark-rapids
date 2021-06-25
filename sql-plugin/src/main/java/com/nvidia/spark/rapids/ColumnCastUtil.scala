/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import scala.collection.mutable.{ArrayBuffer, ArrayBuilder}

import ai.rapids.cudf.{ColumnVector, ColumnView}

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

/**
 * This class casts a column to another column if the predicate passed resolves to true.
 * This method should be able to handle nested or non-nested types
 *
 * At this time this is strictly a place for casting methods
 */
object ColumnCastUtil extends Arm {

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
        case a:ArrayType =>
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
        case s:StructType =>
          val newColumns = ArrayBuilder.make[ColumnView]()
          newColumns.sizeHint(cv.getNumChildren)
          val newColIndices = ArrayBuilder.make[Int]()
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
