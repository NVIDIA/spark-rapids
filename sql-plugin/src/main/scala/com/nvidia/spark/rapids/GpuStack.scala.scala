/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions._

class GpuStackMeta(
    stack: Stack,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends BaseExprMeta[Stack](stack, conf, parent, rule) {

//   private val gpuProjections: Seq[Seq[BaseExprMeta[_]]] = {
//     val n = stack.children.head.asInstanceOf[Literal].value.asInstanceOf[Int]
//     val numExprs = stack.children.length - 1
//     val numStacks = numExprs / n
//     val numRemainder = numExprs % n
//     val numExprsPerStack = Array.fill(numStacks)(n)
//     if (numRemainder > 0) {
//       numExprsPerStack(numStacks - 1) = numRemainder
//     }
//     val exprs = stack.children.iterator
//     numExprsPerStack.map { numExprs =>
//       (0 until numExprs).map(_ => GpuOverrides.wrapExpr(exprs.next(), conf, Some(this)))
//     }
//   }

  override val childExprs: Seq[BaseExprMeta[_]] = stack.children
      .map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def convertToGpu(): GpuExpression = {
    // print all the childExprs
    println("Stack childExprs:")
    val n = childExprs.head.convertToGpu().asInstanceOf[GpuLiteral].value.asInstanceOf[Int]
    println(s"n: ${n}")
    childExprs.foreach(child => println(child.getClass.getSimpleName))
    // not implemented yet
    throw new UnsupportedOperationException(s"Stack is not supported yet: ${stack}")
  }
}
