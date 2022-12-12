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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{BinaryExprMeta, DataFromReplacementRule, GpuExpression, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, GetMapValue}
import org.apache.spark.sql.rapids.GpuGetMapValue

/**
 * We define this type in the shim layer because `GetMapValue` doesn't
 * have the field `failOnError` since Spark 3.4.0 and it always returns `null`
 * on invalid access to map column in ANSI mode.
 */
abstract class GetMapValueMeta (
    expr: GetMapValue,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends BinaryExprMeta[GetMapValue](expr, conf, parent, rule) {

    override def convertToGpu(map: Expression, key: Expression): GpuExpression =
        GpuGetMapValue(map, key, expr.failOnError)
}
