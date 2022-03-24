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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, DataTypeUtils, GpuOverrides, RapidsConf, RapidsMeta, UnaryExprMeta}

import org.apache.spark.sql.catalyst.expressions.{GetArrayItem, GetArrayStructFields, GetMapValue}

class GpuGetArrayItemMeta(
    expr: GetArrayItem,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends BinaryExprMeta[GetArrayItem](expr, conf, parent, rule) {
}

class GpuGetMapValueMeta(
  expr: GetMapValue,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _]],
  rule: DataFromReplacementRule)
  extends BinaryExprMeta[GetMapValue](expr, conf, parent, rule) {
}

class GpuGetArrayStructFieldsMeta(
     expr: GetArrayStructFields,
     conf: RapidsConf,
     parent: Option[RapidsMeta[_, _]],
     rule: DataFromReplacementRule)
  extends UnaryExprMeta[GetArrayStructFields](expr, conf, parent, rule) {
}
