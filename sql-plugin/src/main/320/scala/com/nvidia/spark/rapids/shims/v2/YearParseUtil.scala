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
package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids.{RapidsConf, RapidsMeta}

object YearParseUtil {
  def tagParseStringAsDate(conf: RapidsConf, meta: RapidsMeta[_, _, _]): Unit = {
    if (!conf.isExtendedRangeYearParsingEnabled) {
      meta.willNotWorkOnGpu("Parsing the full rage of supported years is not supported. " +
          "SPARK-35780 extended the allowed years in Spark 3.2.0 to all supported values. " +
          "The RAPIDS accelerator is limited to just 4 positive digits, like prior to " +
          "SPARK-35780. If you want to enable GPU acceleration despite this set " +
          s"${RapidsConf.ENABLE_EXTENDED_YEAR_PARSING} to true")
    }
  }
}
