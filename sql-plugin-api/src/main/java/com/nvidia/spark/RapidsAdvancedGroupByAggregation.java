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

package com.nvidia.spark;

import ai.rapids.cudf.ColumnVector;

/**
 * Custom aggregation step for complex operations that cannot easily be
 * expressed using standard CUDF aggregations. This interface provides direct
 * access to grouped data for custom processing.
 * <br/>
 * Note: This API may require joining results with standard CUDF aggregations
 * afterward, which can result in significant performance penalties.
 */
public interface RapidsAdvancedGroupByAggregation extends RapidsUDAFGroupByAggregation {
  /**
   * Performs custom aggregation on data that has been grouped by keys.
   * The data is grouped, with offsets indicating group boundaries.
   *
   * @param keyOffsets A ColumnVector containing the start offset for each group.
   *                   The end offset for group i is `keyOffsets[i+1]` (or total
   *                   rows for the last group).
   * @param groupedData An array of ColumnVectors containing the actual data
   *                    columns, sorted and organized by the grouping keys.
   * @return An array of ColumnVectors with one row per group, containing the
   * aggregated results.
   */
  ColumnVector[] aggregateGrouped(ColumnVector keyOffsets, ColumnVector[] groupedData);
}
