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

package com.nvidia.spark.rapids.delta.delta33x

import com.nvidia.spark.rapids.delta.common.{DropAllRowsFilterBase, DropMarkedRowsFilterBase, KeepAllRowsFilterBase, KeepMarkedRowsFilterBase, RapidsRowIndexFilterBase, RowIndexMarkingFiltersBuilderBase}

import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray

trait RapidsRowIndexFilter extends RapidsRowIndexFilterBase

/**
 * This creates a Rapids filter to create a skip_row column that will keep all the rows marked
 * in the bitmap. It's the inverse of RapidsDropMarkedRowsFilter
 */
final class RapidsKeepMarkedRowsFilter(bitmap: RoaringBitmapArray)
  extends KeepMarkedRowsFilterBase(bitmap) with RapidsRowIndexFilter

/**
 * This creates a Rapids filter to create a skip_row column that will drop all the rows marked
 * in the bitmap.
 */
final class RapidsDropMarkedRowsFilter(bitmap: RoaringBitmapArray)
  extends DropMarkedRowsFilterBase(bitmap) with RapidsRowIndexFilter

/**
 * The object class used to create the keep marked rows filter
 */
object RapidsKeepMarkedRowsFilter extends RapidsRowIndexMarkingFiltersBuilder {

  override def getFilterForEmptyDeletionVector(): RapidsRowIndexFilter = RapidsDropAllRowsFilter

  override def getFilterForNonEmptyDeletionVector(
      bitmap: RoaringBitmapArray): RapidsRowIndexFilter = new RapidsKeepMarkedRowsFilter(bitmap)
}

/**
 * The object class used to create the drop marked rows filter
 */
object RapidsDropMarkedRowsFilter extends RapidsRowIndexMarkingFiltersBuilder {

  override def getFilterForEmptyDeletionVector(): RapidsRowIndexFilter = RapidsKeepAllRowsFilter

  override def getFilterForNonEmptyDeletionVector(
      bitmap: RoaringBitmapArray): RapidsRowIndexFilter = new RapidsDropMarkedRowsFilter(bitmap)

}

object RapidsDropAllRowsFilter extends DropAllRowsFilterBase with RapidsRowIndexFilter

object RapidsKeepAllRowsFilter extends KeepAllRowsFilterBase with RapidsRowIndexFilter

trait RapidsRowIndexMarkingFiltersBuilder extends RowIndexMarkingFiltersBuilderBase {
  override def getFilterForEmptyDeletionVector(): RapidsRowIndexFilter
  override def getFilterForNonEmptyDeletionVector(
      bitmap: RoaringBitmapArray): RapidsRowIndexFilter
}
