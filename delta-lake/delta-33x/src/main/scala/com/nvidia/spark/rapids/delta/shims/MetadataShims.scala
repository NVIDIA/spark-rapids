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
package com.nvidia.spark.rapids.delta.shims

import org.apache.spark.sql.delta.stats.DeltaStatistics

trait ShimUsesMetadataFields {
  protected val MAX = DeltaStatistics.MAX
  protected val MIN = DeltaStatistics.MIN
  protected val NUM_RECORDS = DeltaStatistics.NUM_RECORDS
  protected val NULL_COUNT = DeltaStatistics.NULL_COUNT
}
