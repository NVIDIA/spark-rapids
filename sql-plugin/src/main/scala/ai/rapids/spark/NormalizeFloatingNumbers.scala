/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.vectorized.ColumnarBatch

// This will ensure that:
//  - input NaNs become Float.NaN, or Double.NaN
//  - that -0.0f and -0.0d becomes 0.0f, and 0.0d respectively
// TODO: need coalesce as a feature request in cudf
class GpuNormalizeNaNAndZero(child: GpuExpression) extends NormalizeNaNAndZero(child)
  with GpuExpression {
  override def columnarEval(input: ColumnarBatch): Any = child.columnarEval(input)
}