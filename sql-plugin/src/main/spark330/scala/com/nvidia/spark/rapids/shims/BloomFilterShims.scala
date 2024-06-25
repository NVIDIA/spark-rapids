/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.rapids.aggregate.GpuBloomFilterAggregate

object BloomFilterShims {
  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[BloomFilterMightContain](
        "Bloom filter query",
        ExprChecks.binaryProject(
          TypeSig.BOOLEAN,
          TypeSig.BOOLEAN,
          ("lhs", TypeSig.BINARY + TypeSig.NULL, TypeSig.BINARY + TypeSig.NULL),
          ("rhs", TypeSig.LONG + TypeSig.NULL, TypeSig.LONG + TypeSig.NULL)),
        (a, conf, p, r) => new BinaryExprMeta[BloomFilterMightContain](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuBloomFilterMightContain(lhs, rhs)
        }),
      GpuOverrides.expr[BloomFilterAggregate](
        "Bloom filter build",
        ExprChecksImpl(Map(
          (ReductionAggExprContext,
            ContextChecks(TypeSig.BINARY, TypeSig.BINARY,
              Seq(ParamCheck("child", TypeSig.LONG, TypeSig.LONG),
                ParamCheck("estimatedItems",
                  TypeSig.lit(TypeEnum.LONG), TypeSig.lit(TypeEnum.LONG)),
                ParamCheck("numBits",
                  TypeSig.lit(TypeEnum.LONG), TypeSig.lit(TypeEnum.LONG))))))),
        (a, conf, p, r) => new ExprMeta[BloomFilterAggregate](a, conf, p, r) {
          override def convertToGpu(): GpuExpression = {
            GpuBloomFilterAggregate(
              childExprs.head.convertToGpu(),
              a.estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue,
              a.numBitsExpression.eval().asInstanceOf[Number].longValue)
          }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
