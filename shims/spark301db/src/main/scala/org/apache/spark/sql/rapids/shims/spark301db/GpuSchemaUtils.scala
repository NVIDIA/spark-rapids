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

package org.apache.spark.sql.rapids.shims.spark301db

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SchemaUtils

object GpuSchemaUtils {

  def checkColumnNameDuplication(
      schema: StructType,
      colType: String,
      resolver: Resolver): Unit = {
    SchemaUtils.checkColumnNameDuplication(schema.map(_.name), colType, resolver)
  }
}
