/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.catalyst.util.InternalRowSet
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition}

object KeyGroupedPartitioningShim {

  def checkPartitions(p: KeyGroupedPartitioning, newPartitions: Array[InputPartition]): Unit = {

    val newRows = new InternalRowSet(p.expressions.map(_.dataType))
    newRows ++= newPartitions.map(_.asInstanceOf[HasPartitionKey].partitionKey())
    val oldRows = p.partitionValuesOpt.get

    if (oldRows.size != newRows.size) {
      throw new SparkException("Data source must have preserved the original partitioning " +
        "during runtime filtering: the number of unique partition values obtained " +
        s"through HasPartitionKey changed: before ${oldRows.size}, after ${newRows.size}")
    }

    if (!oldRows.forall(newRows.contains)) {
      throw new SparkException("Data source must have preserved the original partitioning " +
        "during runtime filtering: the number of unique partition values obtained " +
        s"through HasPartitionKey remain the same but do not exactly match")
    }
  }
}
