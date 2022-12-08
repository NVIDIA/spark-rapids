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

// {"spark-distros":["311","312","312db","313","314","320","321","321cdh","321db","322","323"]}
package org.apache.spark.sql.rapids.shims

import org.apache.spark.SparkUpgradeException

object SparkUpgradeExceptionShims {

  def newSparkUpgradeException(
      version: String,
      message: String,
      cause: Throwable): SparkUpgradeException = {
    new SparkUpgradeException(version, message, cause)
  }

}
