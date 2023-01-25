/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.shims

import scala.util.Try

import com.nvidia.spark.rapids.ShimLoader

import org.apache.spark.sql.delta.{DeltaLog, DeltaUDF, Snapshot}
import org.apache.spark.sql.expressions.UserDefinedFunction

trait DeltaRuntimeShim {
  def stringFromStringUdf(f: String => String): UserDefinedFunction
  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot
}

object DeltaRuntimeShim {
  private lazy val shimInstance = {
    // Could not find a Delta Lake API to determine what version is being run,
    // so this resorts to "fingerprinting" via reflection probing.
    val shimClassName = Try {
      DeltaUDF.getClass.getMethod("stringStringUdf", classOf[String => String])
    }.map(_ => "com.nvidia.spark.rapids.delta.shims.PreDelta22RuntimeShim")
        .getOrElse("com.nvidia.spark.rapids.delta.shims.Delta22PlusRuntimeShim")
    val shimClass = ShimLoader.loadClass(shimClassName)
    shimClass.getConstructor().newInstance().asInstanceOf[DeltaRuntimeShim]
  }

  def stringFromStringUdf(f: String => String): UserDefinedFunction =
    shimInstance.stringFromStringUdf(f)

  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot =
    shimInstance.unsafeVolatileSnapshotFromLog(deltaLog)
}
