/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 */

package org.apache.spark.sql.delta.rapids

import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.delta.{AcceptAllConfigChecker, DeltaConfigChecker, DeltaProvider}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.delta.{DeltaLog, DeltaUDF, Snapshot, TransactionExecutionObserver}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.util.Clock

/**
 * Shared base for 3.3.x and 4.0.x runtime shims.
 * Version-specific shims override provider, catalog, and transaction construction.
 */
abstract class DeltaRuntimeShimBase extends DeltaRuntimeShim {
  override def getDeltaConfigChecker: DeltaConfigChecker = AcceptAllConfigChecker

  // Provider is version-specific
  override def getDeltaProvider: DeltaProvider

  // Default behavior shared across versions
  override def unsafeVolatileSnapshotFromLog(deltaLog: DeltaLog): Snapshot =
    deltaLog.unsafeVolatileSnapshot

  override def fileFormatFromLog(deltaLog: DeltaLog): FileFormat =
    deltaLog.fileFormat(deltaLog.unsafeVolatileSnapshot.protocol,
      deltaLog.unsafeVolatileSnapshot.metadata)

  override def getTightBoundColumnOnFileInitDisabled(spark: SparkSession): Boolean = false

  // Catalog is version-specific (GpuDeltaCatalog differs by version)
  override def getGpuDeltaCatalog(cpuCatalog: DeltaCatalog,
      rapidsConf: RapidsConf): StagingTableCatalog

  // Transaction construction is version-specific
  protected def constructOptimisticTransaction(arg: StartTransactionArg): GpuOptimisticTransactionBase

  override def startTransaction(log: DeltaLog, conf: RapidsConf, clock: Clock):
      GpuOptimisticTransactionBase = {
    startTransaction(StartTransactionArg(log, conf, clock))
  }

  override def startTransaction(arg: StartTransactionArg): GpuOptimisticTransactionBase = {
    TransactionExecutionObserver.getObserver.startingTransaction {
      constructOptimisticTransaction(arg)
    }.asInstanceOf[GpuOptimisticTransactionBase]
  }

  override def stringFromStringUdf(f: String => String): UserDefinedFunction =
    DeltaUDF.stringFromString(f)
}


