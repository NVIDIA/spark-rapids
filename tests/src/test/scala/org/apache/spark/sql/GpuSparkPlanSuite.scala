/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql

import com.nvidia.spark.rapids.SparkSessionHolder
import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.Range

class GpuSparkPlanSuite extends FunSuite {

  test("leafNodeDefaultParallelism for GpuRangeExec") {

    val conf = new SparkConf()
      .set("spark.sql.leafNodeDefaultParallelism", "7")
      .set("spark.rapids.sql.enabled", "true")

    SparkSessionHolder.withSparkSession(conf, spark => {
      val defaultSlice = SparkShimImpl.leafNodeDefaultParallelism(spark)
      val ds = new Dataset(spark, Range(0, 20, 1, None), Encoders.LONG)
      val partitions = ds.rdd.getNumPartitions
      assert(partitions == defaultSlice)
    })

  }

}

