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

package org.apache.spark.sql.errors.rapids

// Copied from Spark: org/apache/spark/sql/errors/QueryErrorsBase.scala
// for for https://github.com/NVIDIA/spark-rapids/issues/6026
// It can be removed when Spark 3.3.0 is the least supported Spark version
trait QueryErrorsBase {}
