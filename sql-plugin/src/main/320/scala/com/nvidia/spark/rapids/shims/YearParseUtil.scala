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

// {"spark-distros":["320","321","321cdh","321db","322","323","330","330cdh","331","332","340"]}
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{RapidsConf, RapidsMeta}

object YearParseUtil {
  def tagParseStringAsDate(conf: RapidsConf, meta: RapidsMeta[_, _, _]): Unit = {
    if (conf.hasExtendedYearValues) {
      meta.willNotWorkOnGpu("Parsing the full rage of supported years is not supported. " +
          "If your years are limited to 4 positive digits set " +
          s"${RapidsConf.HAS_EXTENDED_YEAR_VALUES} to false.")
    }
  }
}
