/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark

import org.apache.iceberg.Schema
import org.apache.iceberg.types.TypeUtil

import org.apache.spark.sql.types.StructType

object GpuTypeToSparkType {
  def toSparkType(schema: Schema): StructType = {
    TypeUtil.visit(schema, new TypeToSparkType).asInstanceOf[StructType]
  }
}
