/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.AnalysisException, SparkDateTimeException}

object RapidsErrorUtils extends RapidsErrorUtilsFor340Plus {
  def dynamicPartitionParentError: Throwable = {
    new AnalysisException("Dynamic partition cannot be the parent of a static partition.")
  }
}