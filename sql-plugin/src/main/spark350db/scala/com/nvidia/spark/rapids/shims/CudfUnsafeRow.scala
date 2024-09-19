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

final class CudfUnsafeRow(
   attributes: Array[Attribute],
   remapping: Array[Int]) extends CudfUnsafeRowBase(attributes, remapping) {

  def getGeography(x: Int): org.apache.spark.unsafe.types.geo.GeographyVal = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  def getGeometry(x: Int): org.apache.spark.unsafe.types.geo.GeometryVal = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  def getVariant(x: Int): org.apache.spark.unsafe.types.VariantVal = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }
}

object CudfUnsafeRow extends CudfUnsafeRowTrait