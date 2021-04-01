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

object CudaUtil {
  /**
   * Allocate a `size` buffer on the device using stream `stream`.
   *
   * This needs to be replaced by `DeviceMemoryBuffer.allocate(size, stream)`
   *
   * @param size - size of buffer to allocate
   * @param stream - stream to use for the allocation
   * @return - a `DeviceMemoryBuffer` instance
   */
  def deviceAllocateOnStream(size: Long, stream: Cuda.Stream): DeviceMemoryBuffer = {
    Rmm.alloc(size, stream)
  }
}
