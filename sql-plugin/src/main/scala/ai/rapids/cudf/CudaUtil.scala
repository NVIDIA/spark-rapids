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
    * Copy from [[src]] buffer, starting at [[srcOffset]], to a destination buffer [[dst]] starting at [[dstOffset]],
    * [[length]] bytes, in the default stream.
    * @param src - source buffer
    * @param srcOffset - source offset
    * @param dst - destination buffer
    * @param dstOffset - destination offset
    * @param length - amount to copy
    */
  def copy(src: MemoryBuffer, srcOffset: Long, dst: MemoryBuffer, dstOffset: Long, length: Long): Unit = {
    Cuda.memcpy(
      dst.getAddress + dstOffset,
      src.getAddress + srcOffset,
      length,
      CudaMemcpyKind.DEFAULT)
  }
}
