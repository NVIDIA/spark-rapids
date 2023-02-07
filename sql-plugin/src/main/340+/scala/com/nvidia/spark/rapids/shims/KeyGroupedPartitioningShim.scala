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
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition}

object KeyGroupedPartitioningShim {

  def checkPartitions(p: KeyGroupedPartitioning, newPartitions: Array[InputPartition]): Unit = {
    if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
      throw new SparkException("Data source must have preserved the original partitioning " +
        "during runtime filtering: not all partitions implement HasPartitionKey after " +
        "filtering")
    }
    val newPartitionValues = newPartitions.map(partition =>
      InternalRowComparableWrapper(partition.asInstanceOf[HasPartitionKey], p.expressions))
      .toSet
    val oldPartitionValues = p.partitionValues
      .map(partition => InternalRowComparableWrapper(partition, p.expressions)).toSet
    // We require the new number of partition values to be equal or less than the old number
    // of partition values here. In the case of less than, empty partitions will be added for
    // those missing values that are not present in the new input partitions.
    if (oldPartitionValues.size < newPartitionValues.size) {
      throw new SparkException("During runtime filtering, data source must either report " +
        "the same number of partition values, or a subset of partition values from the " +
        s"original. Before: ${oldPartitionValues.size} partition values. " +
        s"After: ${newPartitionValues.size} partition values")
    }

    if (!newPartitionValues.forall(oldPartitionValues.contains)) {
      throw new SparkException("During runtime filtering, data source must not report new " +
        "partition values that are not present in the original partitioning.")
    }
  }
}
