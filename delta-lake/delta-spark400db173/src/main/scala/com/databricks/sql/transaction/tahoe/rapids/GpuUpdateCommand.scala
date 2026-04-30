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

import com.databricks.sql.transaction.tahoe.files.TahoeFileIndex

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand

// Stub on DB-17.3. The shared Databricks `UpdateCommandMeta.convertToGpu` constructs
// this case class unconditionally, so the type must exist with a matching signature.
// At runtime, `UpdateCommandMetaShim.tagForGpu` forces CPU fallback, so this is
// unreachable — GPU UPDATE for DB-17.3 is tracked in GitHub issue #14597.
case class GpuUpdateCommand(
    gpuDeltaLog: GpuDeltaLog,
    tahoeFileIndex: TahoeFileIndex,
    target: LogicalPlan,
    updateExpressions: Seq[Expression],
    condition: Option[Expression]) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] =
    throw new UnsupportedOperationException(
      "Delta Lake UPDATE is not yet supported on GPU for DB-17.3 " +
        "(tracked in GitHub issue #14597)")
}
