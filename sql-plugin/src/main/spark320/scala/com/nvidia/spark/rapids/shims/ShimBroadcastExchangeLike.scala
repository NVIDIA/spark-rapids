/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import scala.concurrent.Promise

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike

/**
 * This shim handles the completion future differences between
 * Apache Spark and Databricks.
 */
trait ShimBroadcastExchangeLike extends BroadcastExchangeLike {
  @transient
  protected lazy val promise = Promise[Broadcast[Any]]()

  /**
   * For registering callbacks on `relationFuture`.
   * Note that calling this field will not start the execution of broadcast job.
   */
  @transient
  lazy val completionFuture: concurrent.Future[Broadcast[Any]] = promise.future
}
