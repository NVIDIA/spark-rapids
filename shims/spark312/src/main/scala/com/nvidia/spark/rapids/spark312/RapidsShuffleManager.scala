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

package com.nvidia.spark.rapids.spark312

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.shims.spark311.RapidsShuffleInternalManager

/** A shuffle manager optimized for the RAPIDS Plugin for Apache Spark. */
sealed class RapidsShuffleManager(
    conf: SparkConf,
    isDriver: Boolean) extends RapidsShuffleInternalManager(conf, isDriver) {
}
