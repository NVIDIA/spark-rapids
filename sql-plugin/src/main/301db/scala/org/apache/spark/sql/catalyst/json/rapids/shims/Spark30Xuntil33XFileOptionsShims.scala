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

package org.apache.spark.sql.catalyst.json.rapids.shims

import com.nvidia.spark.rapids.SparkShims

import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.json.JSONOptions

trait Spark30Xuntil33XFileOptionsShims extends SparkShims {

  def timestampFormatInRead(fileOptions: Serializable): Option[String] = {
    fileOptions match {
      case csvOpts: CSVOptions => Option(csvOpts.timestampFormat)
      case jsonOpts: JSONOptions => Option(jsonOpts.timestampFormat)
      case _ => throw new RuntimeException("Wrong file options.")
    }
  }

}
