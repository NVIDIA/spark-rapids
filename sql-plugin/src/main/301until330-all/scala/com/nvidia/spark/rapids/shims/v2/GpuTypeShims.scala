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
package com.nvidia.spark.rapids.shims.v2

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.GpuRowToColumnConverter.TypeConverter

import org.apache.spark.sql.types.DataType

object GpuTypeShims {

  // If support Shims special type
  def hasConverterForType(otherType: DataType) : Boolean = false

  // Get Shim special converter
  def getConverterForType(t: DataType, nullable: Boolean): TypeConverter = {
    throw new RuntimeException("Wrong logic.")
  }

  // Get type that shim supporting
  def toRapidsOrNull(t: DataType): DType = null
}
