/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.RapidsConf

/**
 * BloomFilter fallback check for Spark 4.1.1.
 * 
 * Spark 4.1.1 uses BloomFilter V2 format (SPARK-47547) by default. The GPU implementation
 * only produces V1 format. This works fine when BOTH build (BloomFilterAggregate) and 
 * probe (BloomFilterMightContain) are on GPU.
 * 
 * However, if partial/final aggregates are split between CPU and GPU:
 * - GPU partial aggregate produces V1 format
 * - CPU final aggregate expects V2 format
 * - Spark throws IncompatibleMergeException: "Cannot merge bloom filter of class BloomFilterImpl"
 * 
 * Therefore, when replaceMode is not "all", BOTH BloomFilterAggregate and 
 * BloomFilterMightContain must fall back to CPU to ensure consistent V2 format.
 * 
 * See: https://github.com/NVIDIA/spark-rapids/issues/14148
 */
object BloomFilterFallbackShim {
  
  /**
   * Check if BloomFilter operations must run entirely on CPU.
   * This happens when the partial/final aggregates might be split between CPU and GPU,
   * which would cause V1/V2 format incompatibility.
   */
  def mustFallbackToCpu(conf: RapidsConf): Boolean = {
    // Check if BloomFilterAggregate is explicitly disabled
    val bloomFilterAggKey = "spark.rapids.sql.expression.BloomFilterAggregate"
    val isExplicitlyDisabled = !conf.isOperatorEnabled(bloomFilterAggKey, 
      incompat = false, isDisabledByDefault = false)
    
    // Check if hashAgg.replaceMode is set to partial or final (not "all" which is the default)
    // This causes partial/final aggregates to be split between CPU and GPU,
    // leading to V1/V2 format incompatibility
    val replaceMode = conf.hashAggReplaceMode.toLowerCase
    val isReplaceModeRestricted = replaceMode != "all"
    
    isExplicitlyDisabled || isReplaceModeRestricted
  }
  
  /** Fallback message for Spark 4.1.1 */
  val fallbackMessage: Option[String] = Some(
    "BloomFilter operations must run on CPU when " +
    "spark.rapids.sql.hashAgg.replaceMode is not 'all' or BloomFilterAggregate is disabled, " +
    "because Spark 4.1.1 uses V2 bloom filter format which is incompatible with GPU's V1 format.")
}
