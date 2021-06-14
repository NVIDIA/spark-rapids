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

import ai.rapids.cudf.{ColumnVector, ColumnView, DType}

object ColumnUtil extends Arm {

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
  def ifTrueThenDeepConvertTypeAtoTypeB(cv: ColumnVector,
      predicate: ColumnView => Boolean, convert: ColumnView => ColumnView): ColumnVector = {
    /*
     * 'convertTypeAtoTypeB' method returns a ColumnView that should be copied out to a
     * ColumnVector  before closing the `toClose` views otherwise it will close the returned view
     * and it's children as well.
     */
    def convertTypeAToTypeB(
        cv: ColumnView,
        predicate: ColumnView => Boolean,
        toClose: ArrayBuffer[ColumnView]): ColumnView = {
      val dt = cv.getType
      if (!dt.isNestedType) {
        if (predicate(cv)) {
          val col = convert(cv)
          toClose += col
          col
        } else {
          cv
        }
      } else if (dt == DType.LIST) {
        val child = cv.getChildColumnView(0)
        toClose += child
        val newChild = convertTypeAToTypeB(child, predicate, toClose)
        if (child == newChild) {
          cv
        } else {
          val newView = cv.replaceListChild(newChild)
          toClose += newView
          newView
        }
      } else if (dt == DType.STRUCT) {
        val newColumns = ArrayBuilder.make[ColumnView]()
        newColumns.sizeHint(cv.getNumChildren)
        val newColIndices = ArrayBuilder.make[Int]()
        newColIndices.sizeHint(cv.getNumChildren)
        (0 until cv.getNumChildren).foreach { i =>
          val child = cv.getChildColumnView(i)
          toClose += child
          val newChild = convertTypeAToTypeB(child, predicate, toClose)
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
      } else {
        throw new IllegalArgumentException(s"Unsupported data type ${dt.getTypeId}")
      }
    }

    withResource(new ArrayBuffer[ColumnView]) { toClose =>
      val tmp = convertTypeAToTypeB(cv, predicate, toClose)
      if (tmp != cv) {
        tmp.copyToColumnVector()
      } else {
        tmp.asInstanceOf[ColumnVector].incRefCount()
      }
    }
  }

}
