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

import com.nvidia.spark.rapids._

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.expressions.{ElementAt, Expression}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{CollectLimitExec, GlobalLimitExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.V1WritesUtils.Empty2Null
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.rapids.GpuElementAt
import org.apache.spark.sql.rapids.GpuV1WriteUtils.GpuEmpty2Null
import org.apache.spark.sql.types.{ArrayType, MapType}

trait Spark340PlusShims extends Spark331PlusShims {

  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[GlobalLimitExec](
      "Limiting of results across partitions",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (globalLimit, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimit, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(
              globalLimit.limit, childPlans.head.convertIfNeeded(), globalLimit.offset)
        }),
    GpuOverrides.exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (collectLimit, conf, p, r) => new GpuCollectLimitMeta(collectLimit, conf, p, r) {
        override def convertToGpu(): GpuExec =
          GpuGlobalLimitExec(collectLimit.limit,
            GpuShuffleExchangeExec(
              GpuSinglePartitioning,
              GpuLocalLimitExec(collectLimit.limit, childPlans.head.convertIfNeeded()),
              ENSURE_REQUIREMENTS
            )(SinglePartition), collectLimit.offset)
      }
    ).disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
        "of rows in a batch it could help by limiting the number of rows transferred from " +
        "GPU to CPU")
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ shimExecs

  // AnsiCast is removed from Spark3.4.0
  override def ansiCastRule: ExprRule[_ <: Expression] = null

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      // Empty2Null is pulled out of FileFormatWriter by default since Spark 3.4.0,
      // so it is visible in the overriding stage.
      GpuOverrides.expr[Empty2Null](
        "Converts the empty string to null for writing data",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.STRING, TypeSig.STRING),
        (a, conf, p, r) => new UnaryExprMeta[Empty2Null](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuEmpty2Null(child)
        }
      ),
      GpuOverrides.expr[ElementAt](
        "Returns element of array at given(1-based) index in value if column is array. " +
          "Returns value for the given key in value if column is map.",
        ExprChecks.binaryProject(
          (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
            TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(), TypeSig.all,
          ("array/map", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY) +
            TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
              TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY)
              .withPsNote(TypeEnum.MAP ,"If it's map, only primitive key types are supported."),
            TypeSig.ARRAY.nested(TypeSig.all) + TypeSig.MAP.nested(TypeSig.all)),
          ("index/key", (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128)
            .withPsNote(
              Seq(TypeEnum.BOOLEAN, TypeEnum.BYTE, TypeEnum.SHORT, TypeEnum.LONG,
                TypeEnum.FLOAT, TypeEnum.DOUBLE, TypeEnum.DATE, TypeEnum.TIMESTAMP,
                TypeEnum.STRING, TypeEnum.DECIMAL), "Unsupported as array index."),
            TypeSig.all)),
        (in, conf, p, r) => new BinaryExprMeta[ElementAt](in, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            // To distinguish the supported nested type between Array and Map
            val checks = in.left.dataType match {
              case _: MapType =>
                // Match exactly with the checks for GetMapValue
                ExprChecks.binaryProject(
                  (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                    TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
                  TypeSig.all,
                  ("map",
                    TypeSig.MAP.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT +
                      TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY),
                    TypeSig.MAP.nested(TypeSig.all)),
                  ("key", TypeSig.commonCudfTypes + TypeSig.DECIMAL_128, TypeSig.all))
              case _: ArrayType =>
                // Match exactly with the checks for GetArrayItem
                ExprChecks.binaryProject(
                  (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                    TypeSig.DECIMAL_128 + TypeSig.MAP + TypeSig.BINARY).nested(),
                  TypeSig.all,
                  ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
                    TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP +
                    TypeSig.BINARY),
                    TypeSig.ARRAY.nested(TypeSig.all)),
                  ("ordinal", TypeSig.INT, TypeSig.INT))
              case _ => throw new IllegalStateException("Only Array or Map is supported as input.")
            }
            checks.tag(this)
          }
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
            GpuElementAt(lhs, rhs, failOnError = in.failOnError && lhs.dataType.isInstanceOf[ArrayType])
          }
        }),
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }
}
