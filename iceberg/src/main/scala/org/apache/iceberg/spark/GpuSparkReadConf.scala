/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark

import com.nvidia.spark.rapids.iceberg.spark.{GpuSparkReadOptions, GpuSparkSQLProperties}
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.FieldUtils

class GpuSparkReadConf(val delegate: SparkReadConf) {
  private val confParser = FieldUtils.readField(delegate, "confParser", true)
    .asInstanceOf[SparkConfParser]

  /**
   * Enables reading a timestamp without time zone as a timestamp with time zone.
   * <p>
   * Generally, this is not safe as a timestamp without time zone is supposed to represent the
   * wall-clock time, i.e. no matter the reader/writer timezone 3PM should always be read as 3PM,
   * but a timestamp with time zone represents instant semantics, i.e. the timestamp
   * is adjusted so that the corresponding time in the reader timezone is displayed.
   * <p>
   * When set to false (default), an exception must be thrown while reading a timestamp without
   * time zone.
   *
   * @return boolean indicating if reading timestamps without timezone is allowed
   */
  def handleTimestampWithoutZone(): Boolean = confParser.booleanConf()
    .option(GpuSparkReadOptions.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE)
    .sessionConf(GpuSparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE)
    .defaultValue(GpuSparkSQLProperties.HANDLE_TIMESTAMP_WITHOUT_TIMEZONE_DEFAULT)
    .parse()
}
