/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.{ProjectExec, SampleExec}

class GpuProjectExecMeta(
    proj: ProjectExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[ProjectExec](proj, conf, p, r)
    with Logging {
}

class GpuSampleExecMeta(
    sample: SampleExec,
    conf: RapidsConf,
    p: Option[RapidsMeta[_, _]],
    r: DataFromReplacementRule) extends SparkPlanMeta[SampleExec](sample, conf, p, r)
    with Logging {
}
