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

package org.apache.spark.sql.delta.rapids.delta21x

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.DeltaProvider
import com.nvidia.spark.rapids.delta.delta21x.Delta21xProvider

import org.apache.spark.sql.delta.{DeltaLog, DeltaUDF, Snapshot}
import org.apache.spark.sql.delta.rapids.{DeltaRuntimeShim, GpuOptimisticTransactionBase}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.util.Clock

/**
 * Delta runtime shim for Delta 2.1.x on Spark 3.3.x.
 * @note This class is instantiated via reflection from DeltaProbeImpl
 */
class Delta21xRuntimeShim extends DeltaRuntimeShim {
  override def getDeltaProvider: DeltaProvider = Delta21xProvider

  override def startTransaction(
      log: DeltaLog,
      conf: RapidsConf,
      clock: Clock): GpuOptimisticTransactionBase = {
    new GpuOptimisticTransaction(log, conf)(clock)
  }

  override def stringFromStringUdf(f: String => String): UserDefinedFunction = {
    DeltaUDF.stringStringUdf(f)
  }

  override def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot = {
    deltaLog.snapshot
  }
}
