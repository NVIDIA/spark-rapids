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

/*** spark-rapids-shim-json-lines
{"spark": "401"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.jni.Hash

import org.apache.spark.sql.catalyst.expressions.{CollationAwareMurmur3Hash, CollationAwareXxHash64, Expression}
import org.apache.spark.sql.rapids.{GpuMurmur3Hash, GpuXxHash64, XxHash64Utils}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object SparkShimImpl extends Spark400PlusCommonShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[CollationAwareMurmur3Hash](
        "Collation-aware murmur3 hash operator",
        ExprChecks.projectOnly(TypeSig.INT, TypeSig.INT,
          repeatingParamCheck = Some(RepeatingParamCheck("input",
            (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
                TypeSig.STRUCT + TypeSig.ARRAY).nested() +
                TypeSig.psNote(TypeEnum.ARRAY, "Arrays of structs are not supported"),
            TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[CollationAwareMurmur3Hash](a, conf, p, r) {
          override val childExprs: Seq[BaseExprMeta[_]] = a.children
            .map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))

          override def tagExprForGpu(): Unit = {
            val arrayWithStructsHashing = a.children.exists(e =>
              TrampolineUtil.dataTypeExistsRecursively(e.dataType,
                {
                  case ArrayType(_: StructType, _) => true
                  case _ => false
                })
            )
            if (arrayWithStructsHashing) {
              willNotWorkOnGpu("hashing arrays with structs is not supported")
            }
          }

          override def convertToGpu(): GpuExpression =
            GpuMurmur3Hash(childExprs.map(_.convertToGpu()), a.seed)
        }
      ),
      GpuOverrides.expr[CollationAwareXxHash64](
        "Collation-aware xxhash64 operator",
        ExprChecks.projectOnly(TypeSig.LONG, TypeSig.LONG,
          repeatingParamCheck = Some(RepeatingParamCheck("input",
            XxHash64Shims.supportedTypes, TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[CollationAwareXxHash64](a, conf, p, r) {
          override val childExprs: Seq[BaseExprMeta[_]] = a.children
            .map(GpuOverrides.wrapExpr(_, this.conf, Some(this)))

          override def tagExprForGpu(): Unit = {
            val maxDepth = a.children.map(
              c => XxHash64Utils.computeMaxStackSize(c.dataType)).max
            if (maxDepth > Hash.MAX_STACK_DEPTH) {
              willNotWorkOnGpu(s"The data type requires a stack depth of $maxDepth, " +
                s"which exceeds the GPU limit of ${Hash.MAX_STACK_DEPTH}. " +
                "The algorithm to calculate stack depth: " +
                "1: Primitive type counts 1 depth; " +
                "2: Array of Structure counts:  1  + depthOf(Structure); " +
                "3: Array of Other counts: depthOf(Other); " +
                "4: Structure counts: 1 + max of depthOf(child); " +
                "5: Map counts: 2 + max(depthOf(key), depthOf(value)); "
              )
            }
          }
          override def convertToGpu(): GpuExpression =
            GpuXxHash64(childExprs.map(_.convertToGpu()), a.seed)
        }
      )
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }
}
