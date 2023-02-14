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

package org.apache.spark.sql.delta.rapids

import scala.util.Try

import com.nvidia.spark.rapids.{RapidsConf, ShimLoader, VersionUtils}
import com.nvidia.spark.rapids.delta.DeltaProvider

import org.apache.spark.sql.delta.{DeltaLog, DeltaUDF, Snapshot}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.util.Clock

trait DeltaRuntimeShim {
  def getDeltaProvider: DeltaProvider
  def startTransaction(log: DeltaLog, conf: RapidsConf, clock: Clock): GpuOptimisticTransactionBase
  def stringFromStringUdf(f: String => String): UserDefinedFunction
  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot
}

object DeltaRuntimeShim {
  private def getShimClassName: String = {
    if (VersionUtils.cmpSparkVersion(3, 2, 0) < 0) {
      throw new IllegalStateException("Delta Lake is not supported on Spark < 3.2.x")
    } else if (VersionUtils.cmpSparkVersion(3, 3, 0) < 0) {
      "org.apache.spark.sql.delta.rapids.delta20x.Delta20xRuntimeShim"
    } else if (VersionUtils.cmpSparkVersion(3, 4, 0) < 0) {
      // Could not find a Delta Lake API to determine what version is being run,
      // so this resorts to "fingerprinting" via reflection probing.
      Try {
        DeltaUDF.getClass.getMethod("stringStringUdf", classOf[String => String])
      }.map(_ => "org.apache.spark.sql.delta.rapids.delta21x.Delta21xRuntimeShim")
          .getOrElse("org.apache.spark.sql.delta.rapids.delta22x.Delta22xRuntimeShim")
    } else {
      throw new IllegalStateException("Delta Lake is not supported on Spark > 3.3.x")
    }
  }

  private lazy val shimInstance = {
    val shimClassName = getShimClassName
    val shimClass = ShimLoader.loadClass(shimClassName)
    shimClass.getConstructor().newInstance().asInstanceOf[DeltaRuntimeShim]
  }

  def getDeltaProvider: DeltaProvider = shimInstance.getDeltaProvider

  def startTransaction(
      log: DeltaLog,
      rapidsConf: RapidsConf)(implicit clock: Clock): GpuOptimisticTransactionBase =
    shimInstance.startTransaction(log, rapidsConf, clock)

  def stringFromStringUdf(f: String => String): UserDefinedFunction =
    shimInstance.stringFromStringUdf(f)

  def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot =
    shimInstance.unsafeVolatileSnapshotFromLog(deltaLog)
}
