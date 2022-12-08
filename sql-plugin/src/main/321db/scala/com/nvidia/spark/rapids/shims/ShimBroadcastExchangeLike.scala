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

// {"spark-distros":["321db"]}:
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

  override def completionFuture: concurrent.Future[Broadcast[Any]] = promise.future
}
