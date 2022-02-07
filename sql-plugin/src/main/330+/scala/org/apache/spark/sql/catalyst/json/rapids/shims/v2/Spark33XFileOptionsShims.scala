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

package org.apache.spark.sql.catalyst.json.rapids.shims.v2

import com.nvidia.spark.rapids.shims.v2.Spark321PlusShims

import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.json.JSONOptions

trait Spark33XFileOptionsShims extends Spark321PlusShims {

  def dateFormatInRead(fileOptions: Serializable): Option[String] = {
    fileOptions match {
      case csvOpts: CSVOptions => csvOpts.dateFormatInRead
      case jsonOpts: JSONOptions => jsonOpts.dateFormatInRead
      case _ => throw new RuntimeException("Wrong file options.")
    }
  }

  def timestampFormatInRead(fileOptions: Serializable): Option[String] = {
    fileOptions match {
      case csvOpts: CSVOptions => csvOpts.dateFormatInRead
      case jsonOpts: JSONOptions => jsonOpts.dateFormatInRead
      case _ => throw new RuntimeException("Wrong file options.")
    }
  }

}
