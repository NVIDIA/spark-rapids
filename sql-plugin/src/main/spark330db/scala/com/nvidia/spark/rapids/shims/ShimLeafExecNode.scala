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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "341db"}
{"spark": "350db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase

trait ShimLeafExecNode extends LeafExecNode {
  // For AQE support in Databricks, all Exec nodes implement computeStats(). This is actually
  // a recursive call to traverse the entire physical plan to aggregate this number. For the
  // end of the computation, this means that all LeafExecNodes must implement this method to
  // avoid a stack overflow. For now, based on feedback from Databricks, Long.MaxValue is
  // sufficient to satisfy this computation.
  override def computeStats(): Statistics = {
    Statistics(
      sizeInBytes = Long.MaxValue
    )
  }
}

// DataSourceV2ScanExecBase actually extends LeafExecNode, so we extend that shim as well here.
trait ShimDataSourceV2ScanExecBase extends DataSourceV2ScanExecBase {
  override def computeStats(): Statistics = {
    Statistics(
      sizeInBytes = Long.MaxValue
    )
  }

}

