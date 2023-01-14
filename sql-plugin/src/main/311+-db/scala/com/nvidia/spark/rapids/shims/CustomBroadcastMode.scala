/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode

object CustomBroadcastMode {

  def buildCustomBroadcastMode: BroadcastMode = {
    new BroadcastMode {
      override def transform(rows: Array[InternalRow], mm: TaskMemoryManager): Any = rows

      override def transform(
          rows: Iterator[InternalRow],
          sizeHint: Option[Long],
          mm: TaskMemoryManager): Any = null

      override def canonicalized: BroadcastMode = this
    }
  }
}
