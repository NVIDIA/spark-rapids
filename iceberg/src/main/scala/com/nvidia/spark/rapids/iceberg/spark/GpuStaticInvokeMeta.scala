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

package com.nvidia.spark.rapids.iceberg.spark

import com.nvidia.spark.rapids.{DataFromReplacementRule, ExprMeta, GpuExpression, RapidsConf, RapidsMeta}
import org.apache.iceberg.spark.functions.{BucketFunction, GpuBucketExpression}

import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke

/**
 * Meta class for overriding StaticInvoke expressions.
 * <br/>
 * When writing to partitioned table, iceberg needs to compute the partition values based on the 
 * partition spec using [[StaticInvoke]] expression.
 */
class GpuStaticInvokeMeta(expr: StaticInvoke,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule) extends ExprMeta[StaticInvoke](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    if (classOf[BucketFunction.BucketBase].isAssignableFrom(expr.staticObject)) {
      GpuBucketExpression.tagExprForGpu(this)
    } else {
      willNotWorkOnGpu(s"StaticInvoke of ${expr.staticObject.getName} is not supported on GPU")
    }
  }

  override def convertToGpu(): GpuExpression = {
    if (classOf[BucketFunction.BucketBase].isAssignableFrom(expr.staticObject)) {
      val Seq(left, right) = childExprs.map(_.convertToGpu().asInstanceOf[GpuExpression])
      GpuBucketExpression(left, right)
    } else {
      throw new IllegalStateException(
        s"Should have been caught in tagExprForGpu: ${expr.staticObject.getName}")
    }
  }
}