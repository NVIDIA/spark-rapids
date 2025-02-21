/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import org.mockito.Mockito

trait MoreMockitoSugar {
  /**
   * Since Mockito 4, org.mockito.Mockito#spy start to have more overloaded methods, and without
   * this sugar spy you'll meet ambiguous error like described in
   * https://github.com/scala/bug/issues/4775
   */
  def spy[T](x: T): T = {
    Mockito.spy[T](x)
  }
}
