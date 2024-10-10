/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "350db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.unsafe.types.VariantVal
import org.apache.spark.unsafe.types.geo._


final class CudfUnsafeRow(
   attributes: Array[Attribute],
   remapping: Array[Int]) extends CudfUnsafeRowBase(attributes, remapping) {

  def getGeography(x$1: Int): GeographyVal = {
    throw new UnsupportedOperationException("Not Implemented yet")
  }

  def getGeometry(x$1: Int): GeometryVal = {
    throw new UnsupportedOperationException("Not Implemented yet")
  }

  def getVariant(x$1: Int): VariantVal = {
    throw new UnsupportedOperationException("Not Implemented yet")
  }

}

object CudfUnsafeRow extends CudfUnsafeRowTrait
