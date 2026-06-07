/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.Predicate

trait ShimPredicate extends Predicate {
  def contextIndependentFoldable: Boolean = children.forall(_.foldable)
}

trait ShimDataWritingCommand
    extends org.apache.spark.sql.execution.command.DataWritingCommand
    with ShimUnaryCommand {
  def runColumnar(
      sparkSession: org.apache.spark.sql.SparkSession,
      child: org.apache.spark.sql.execution.SparkPlan):
      Seq[org.apache.spark.sql.vectorized.ColumnarBatch]

  def runColumnarFromAny(
      sparkSession: AnyRef,
      child: org.apache.spark.sql.execution.SparkPlan):
      Seq[org.apache.spark.sql.vectorized.ColumnarBatch] = {
    runColumnar(sparkSession.asInstanceOf[org.apache.spark.sql.SparkSession], child)
  }

  override def run(
      sparkSession: org.apache.spark.sql.SparkSession,
      child: org.apache.spark.sql.execution.SparkPlan): Seq[org.apache.spark.sql.Row] = {
    com.nvidia.spark.rapids.Arm.withResource(runColumnar(sparkSession, child)) { batches =>
      assert(batches.isEmpty)
    }
    Seq.empty[org.apache.spark.sql.Row]
  }
}
