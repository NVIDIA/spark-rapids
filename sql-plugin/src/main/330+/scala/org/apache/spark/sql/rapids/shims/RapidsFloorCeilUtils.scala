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
package org.apache.spark.sql.rapids.shims

import org.apache.spark.sql.rapids.GpuFloorCeil
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}

object RapidsFloorCeilUtils {

  def outputDataType(dataType: DataType): DataType = {
    dataType match {
      // For Ceil/Floor function we calculate the precision by calling unboundedOutputPrecision and
      // for RoundCeil/RoundFloor we take the precision as it is. Here the actual precision is
      // calculated by taking the max of these 2 to make sure we don't overflow while
      // creating the DecimalType.
      case dt: DecimalType =>
        val maxPrecision = math.max(GpuFloorCeil.unboundedOutputPrecision(dt), dt.precision)
        DecimalType.bounded(maxPrecision, 0)
      case _ => LongType
    }
  }
}
