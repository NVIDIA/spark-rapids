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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{DataFromReplacementRule, ExprMeta, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.Sequence

class GpuSequenceMeta(
    expr: Sequence,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[Sequence](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    //  We have to fall back to the CPU if the timeZoneId is not UTC when
    //  we are processing date/timestamp.
    //  Date/Timestamp are not enabled right now so this is probably fine.
  }
}
