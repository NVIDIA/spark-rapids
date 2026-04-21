/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.rapids.{ExternalSource, GpuStringDecode}

/**
 * Meta class for overriding StaticInvoke expressions.
 * <br/>
 * Handles two cases:
 * - Iceberg partition value computation via StaticInvoke
 * - Spark 4.0+ StringDecode (RuntimeReplaceable replaced by StaticInvoke)
 */
class StaticInvokeMeta(expr: StaticInvoke,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule) extends ExprMeta[StaticInvoke](expr, conf, parent, rule) {

  private val isStringDecode: Boolean = {
    expr.staticObject.getName == "org.apache.spark.sql.catalyst.expressions.StringDecode" &&
      expr.functionName == "decode"
  }

  private var charsetName: String = null

  override val childExprs: Seq[BaseExprMeta[_]] = if (isStringDecode) {
    // StringDecode StaticInvoke: decode(bin, charset, legacyCharsets, legacyErrorAction)
    // Only wrap the first child (bin) as the GPU expression input
    expr.arguments.take(1).map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  } else {
    expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  }

  override def tagExprForGpu(): Unit = {
    if (isStringDecode) {
      tagStringDecode()
    } else {
      ExternalSource.tagForGpu(expr, this)
    }
  }

  private def tagStringDecode(): Unit = {
    if (expr.arguments.size < 2) {
      willNotWorkOnGpu("StringDecode StaticInvoke has unexpected argument count")
      return
    }
    // charset is the second argument, must be a foldable string literal
    val charsetExpr = expr.arguments(1)
    GpuOverrides.extractLit(charsetExpr).map(_.value) match {
      case Some(cs: org.apache.spark.unsafe.types.UTF8String) if cs != null =>
        charsetName = cs.toString.toUpperCase
        if (charsetName != "GBK") {
          willNotWorkOnGpu(s"only GBK charset is supported on GPU, got: $charsetName")
        }
      case _ =>
        willNotWorkOnGpu("charset must be a string literal for GPU StringDecode")
    }
  }

  override def convertToGpuImpl(): GpuExpression = {
    if (isStringDecode) {
      val bin = childExprs.head.convertToGpu().asInstanceOf[Expression]
      GpuStringDecode(bin, charsetName)
    } else {
      ExternalSource.convertToGpu(expr, this)
    }
  }
}
