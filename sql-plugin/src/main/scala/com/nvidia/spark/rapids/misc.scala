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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.jni.Misc
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{Expression, Uuid}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Returns a universally unique identifier (UUID) string. The value is returned
 * as a canonical UUID 36-character string. E.g.:
 *   46707d92-02f4-4817-8116-a4c3b23e6266
 * It ignores the seed in `Uuid`, the `jni.Misc` always uses a random seed to
 * generate UUIDs.
 */
case class GpuUuid() extends GpuExpression with ShimExpression {

  override def hasSideEffects: Boolean = false

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = Nil

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    GpuColumnVector.from(Misc.randomUuid(batch.numRows), dataType)
  }
}

class GpuUuidMeta(
    expr: Uuid,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends ExprMeta[Uuid](expr, conf, parent, rule) {

  override def convertToGpu(): GpuExpression = {
    GpuUuid()
  }
}
