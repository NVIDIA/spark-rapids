/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.shims

import com.nvidia.spark.rapids.delta.{DeleteCommandEdgeMeta, DeleteCommandMeta}

object DeleteCommandMetaShim {
  def tagForGpu(meta: DeleteCommandMeta): Unit = {
    // GPU DELETE not yet enabled for DB-17.3 (will be enabled in a follow-up issue)
    meta.willNotWorkOnGpu("Delta Lake DELETE is not yet supported on GPU for DB-17.3")
  }

  def tagForGpu(meta: DeleteCommandEdgeMeta): Unit = {
    // GPU DELETE not yet enabled for DB-17.3 (will be enabled in a follow-up issue)
    meta.willNotWorkOnGpu("Delta Lake DELETE is not yet supported on GPU for DB-17.3")
  }
}
