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

package com.databricks.sql.transaction.tahoe.rapids

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand

// The shared Databricks conversion path still expects a GPU DELETE command type.
// DB-17.3 DELETE is tagged to run on CPU, so this placeholder exists only to satisfy
// that signature and to fail clearly if the guard is missed.
case class GpuDeleteCommand(
    gpuDeltaLog: GpuDeltaLog,
    target: LogicalPlan,
    condition: Option[Expression]) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] =
    throw new UnsupportedOperationException(
      "Delta Lake DELETE is not yet supported on GPU for DB-17.3 " +
        "(tracked in GitHub issue #14597)")
}
