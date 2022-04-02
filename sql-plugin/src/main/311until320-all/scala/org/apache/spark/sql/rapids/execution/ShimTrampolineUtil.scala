/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import org.apache.spark.sql.types.{DataType, StructType}

object ShimTrampolineUtil {

  // unionLikeMerge was only added in Spark 3.2 so be bug compatible and call merge
  // https://issues.apache.org/jira/browse/SPARK-36673
  def unionLikeMerge(left: DataType, right: DataType): DataType =
    StructType.merge(left, right)
}
