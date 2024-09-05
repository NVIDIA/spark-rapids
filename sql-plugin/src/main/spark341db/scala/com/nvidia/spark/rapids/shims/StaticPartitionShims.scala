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

/*** spark-rapids-shim-json-lines
{"spark": "341db"}
{"spark": "350db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.databricks.sql.transaction.tahoe.files.TahoeFileIndexWithStaticPartitions

import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

object StaticPartitionShims {
  /** Get the static partitions associated with a relation, if any. */
  def getStaticPartitions(relation: HadoopFsRelation): Option[Seq[FilePartition]] = {
    relation.location match {
      case t: TahoeFileIndexWithStaticPartitions => Some(t.getStaticPartitions)
      case _ => None
    }
  }
}
