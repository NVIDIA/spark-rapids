/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{Attribute, FileSourceMetadataAttribute}

object RapidsFileSourceMetaUtils {
  /**
   * Cleanup the internal metadata information of an attribute if it is
   * a 'FileSourceMetadataAttribute', it will remove both 'METADATA_COL_ATTR_KEY' and
   * 'FILE_SOURCE_METADATA_COL_ATTR_KEY' from the attribute 'Metadata'
   */
  def cleanupFileSourceMetadataInformation(attr: Attribute): Attribute =
    FileSourceMetadataAttribute.cleanupFileSourceMetadataInformation(attr)
}
