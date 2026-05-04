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

package com.nvidia.spark.rapids

/**
 * Per-build update sink for bloom-filter build-side cost
 * observability.
 *
 * Implementations receive `(buildWallNanos, bfBytes)` once per BF
 * build at finalize time, never once per input batch. The trait
 * keeps the per-build contract testable without a GPU dependency:
 * unit tests substitute a counting spy and assert `update` is
 * invoked once per BF build.
 */
trait BloomFilterBuildCostUpdater {
  def update(buildWallNanos: Long, bfBytes: Long): Unit
}
