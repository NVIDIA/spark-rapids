/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * This file was derived from UpdateCommand.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.rapids.delta40x

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuUpdateCommandBase}

/**
 * Delta 4.0.x GPU version of UpdateCommand.
 */
case class GpuUpdateCommand(
    gpuDeltaLog: GpuDeltaLog,
    tahoeFileIndex: TahoeFileIndex,
    catalogTable: Option[CatalogTable],
    target: LogicalPlan,
    updateExpressions: Seq[Expression],
    condition: Option[Expression])
  extends GpuUpdateCommandBase(
    gpuDeltaLog, tahoeFileIndex, catalogTable, target, updateExpressions, condition)
  with Delta40xCommandShims
