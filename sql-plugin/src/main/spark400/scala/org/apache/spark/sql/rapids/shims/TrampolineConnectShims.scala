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

/*** spark-rapids-shim-json-lines
{"spark": "400"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import org.apache.avro.NameValidator
import org.apache.avro.Schema

import org.apache.spark.sql.classic.SparkSession

object TrampolineConnectShims {

  type SparkSession = org.apache.spark.sql.classic.SparkSession

  def cleanupAnyExistingSession(): Unit = SparkSession.cleanupAnyExistingSession()

  def getActiveSession: SparkSession = {
    SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("No active SparkSession found")
    )
  }

  def createSchemaParser(): Schema.Parser = {
    // Spark-4.0+ depends on Avro-1.12.0 where validate() is removed and we need to use
    // NameValidator interface instead of validate() method.
    new Schema.Parser(NameValidator.NO_VALIDATION).setValidateDefaults(false)
  }
}