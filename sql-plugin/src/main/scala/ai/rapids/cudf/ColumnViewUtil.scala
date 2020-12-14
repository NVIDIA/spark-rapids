/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.cudf

object ColumnViewUtil {
  /**
   * Create a `ColumnVector` from a `cudf::column_view` describing a column in a contiguous table
   * @param viewHandle address of the `cudf::column_view` instance
   * @param buffer device buffer backing the data referenced by the view
   */
  def fromViewWithContiguousAllocation(viewHandle: Long, buffer: DeviceMemoryBuffer): ColumnVector =
    ColumnVector.fromViewWithContiguousAllocation(viewHandle, buffer)

  /**
   * Create a `cudf::column_view` instance
   * @note `deleteColumnView` must be called to destroy the native view instance
   * @param typeId native ID of cudf data type
   * @param typeScale scale of type (used by decimal types)
   * @param dataAddress GPU address of the column's data buffer or 0
   * @param dataSize size of the data buffer in bytes
   * @param offsetsAddress GPU address of the column's offsets buffer or 0
   * @param validityAddress GPU address of the column's validity buffer or 0
   * @param nullCount number of null values in the column
   * @param numRows number of rows in the column
   * @param childViews addresses of `cudf::column_view` instances for child columns or null
   * @return address of the `cudf::column_view` instance
   */
  def makeCudfColumnView(
      typeId: Int,
      typeScale: Int,
      dataAddress: Long,
      dataSize: Long,
      offsetsAddress: Long,
      validityAddress: Long,
      nullCount: Int,
      numRows: Int,
      childViews: Array[Long]): Long = ColumnView.makeCudfColumnView(
    typeId,
    typeScale,
    dataAddress,
    dataSize,
    offsetsAddress,
    validityAddress,
    nullCount,
    numRows,
    childViews)

  /**
   * Delete a `cudf::column_view` instance
   * @param viewHandle address of the cudf column view
   */
  def deleteColumnView(viewHandle: Long): Unit = ColumnView.deleteColumnView(viewHandle)
}
