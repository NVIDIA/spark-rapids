/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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
{"spark": "311"}
{"spark": "312"}
{"spark": "312db"}
{"spark": "313"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids.ShuffleBufferCatalog

import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.sql.rapids.GpuShuffleBlockResolverBase

class GpuShuffleBlockResolver(resolver: IndexShuffleBlockResolver, catalog: ShuffleBufferCatalog)
    extends GpuShuffleBlockResolverBase(resolver, catalog)

