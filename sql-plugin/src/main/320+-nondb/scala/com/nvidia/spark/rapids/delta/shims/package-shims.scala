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

package com.nvidia.spark.rapids.delta.shims

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaUDF}
import org.apache.spark.sql.delta.expressions.JoinedProjection
import org.apache.spark.sql.delta.stats.UsesMetadataFields
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructField

object ShimDeltaColumnMapping {
  def getPhysicalName(field: StructField): String = DeltaColumnMapping.getPhysicalName(field)
}

object ShimDeltaUDF {
  def stringStringUdf(f: String => String): UserDefinedFunction = DeltaUDF.stringStringUdf(f)
}

object ShimJoinedProjection {
  def bind(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      projectList: Seq[Expression],
      leftCanBeNull: Boolean = false,
      rightCanBeNull: Boolean = false): Seq[Expression] = {
    JoinedProjection.bind(
      leftAttributes,
      rightAttributes,
      projectList,
      leftCanBeNull,
      rightCanBeNull
    )
  }
}

object ShimJsonUtils {
  def fromJson[T: Manifest](json: String): T = JsonUtils.fromJson[T](json)
}

trait ShimUsesMetadataFields extends UsesMetadataFields
