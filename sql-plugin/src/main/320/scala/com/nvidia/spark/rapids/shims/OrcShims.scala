/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

// {"spark-distros":["320","321","321db","322","323"]}
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.types.DataType

// 320+ ORC shims
object OrcShims extends OrcShims320untilAllBase {

  // orcTypeDescriptionString is renamed to getOrcSchemaString from 3.3+
  def getOrcSchemaString(dt: DataType): String = {
    OrcUtils.orcTypeDescriptionString(dt)
  }

}
