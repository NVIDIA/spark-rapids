/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.{DatabricksShimVersion, GpuColumnarToRowExec, GpuDataWritingCommandExec, GpuExec, RapidsConf, ShimLoader, ShimVersion, SparkShimVersion}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, ReusedSubqueryExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuCustomShuffleReaderExec}
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.SerializableConfiguration

case class LoreRDDMeta(numPartitions: Int, outputPartitions: Seq[Int], attrs: Seq[Attribute])

case class LoreRDDPartitionMeta(numBatches: Int, dataType: Seq[DataType])

trait GpuLoreRDD {
  def rootPath: Path

  def pathOfMeta: Path = new Path(rootPath, "rdd.meta")

  def pathOfPartition(partitionIndex: Int): Path = {
    new Path(rootPath, s"partition-$partitionIndex")
  }

  def pathOfPartitionMeta(partitionIndex: Int): Path = {
    new Path(pathOfPartition(partitionIndex), "partition.meta")
  }

  def pathOfBatch(partitionIndex: Int, batchIndex: Int): Path = {
    new Path(pathOfPartition(partitionIndex), s"batch-$batchIndex.parquet")
  }
}

object GpuLore extends Logging {
  private val NON_STRICT_SKIP_MARKER_PREFIX = ".lore-nonstrict-skip-"

  private[rapids] def nonStrictSkipMarkerPath(rootPath: Path): Path = {
    val parent = Option(rootPath.getParent).getOrElse(rootPath)
    new Path(parent, s"$NON_STRICT_SKIP_MARKER_PREFIX${rootPath.getName}")
  }

  private[rapids] def isLoreSkipActive(rootPath: Path, conf: Configuration): Boolean = {
    val marker = nonStrictSkipMarkerPath(rootPath)
    try {
      marker.getFileSystem(conf).exists(marker)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Unable to check non-strict skip marker at $marker", e)
        false
    }
  }

  private[rapids] def markLoreSkipped(
      rootPath: Path,
      conf: Configuration,
      reason: Throwable,
      loreId: Option[LoreId]): Unit = {
    val alreadySkipped = isLoreSkipActive(rootPath, conf)
    try {
      val fs = rootPath.getFileSystem(conf)
      if (fs.exists(rootPath)) {
        fs.delete(rootPath, true)
      }
    } catch {
      case NonFatal(cleanupErr) =>
        logWarning(s"Failed cleaning partial LORE dump at $rootPath", cleanupErr)
    }
    val marker = nonStrictSkipMarkerPath(rootPath)
    try {
      val fs = marker.getFileSystem(conf)
      if (!fs.exists(marker)) {
        val out = fs.create(marker, true)
        out.close()
      }
    } catch {
      case NonFatal(markerErr) =>
        logWarning(s"Failed creating non-strict marker at $marker", markerErr)
    }
    if (!alreadySkipped) {
      val prefix = loreId.map(id => s"loreId=$id ").getOrElse("")
      logWarning(
        s"Skipping LORE dump ${prefix}at ${rootPath.toString} because: ${reason.getMessage}",
        reason)
    }
  }
  /**
   * Lore id of a plan node.
   */
  val LORE_ID_TAG: TreeNodeTag[String] = new TreeNodeTag[String]("rapids.gpu.lore.id")
  /**
   * When a [[GpuExec]] node has this tag, it means that this node is a root node whose meta and
   * input should be dumped.
   */
  val LORE_DUMP_PATH_TAG: TreeNodeTag[String] = new TreeNodeTag[String]("rapids.gpu.lore.dump.path")
  /**
   * When a [[GpuExec]] node has this tag, it means that this node is a child node whose data
   * should be dumped.
   */
  val LORE_DUMP_RDD_TAG: TreeNodeTag[LoreDumpRDDInfo] = new TreeNodeTag[LoreDumpRDDInfo](
    "rapids.gpu.lore.dump.rdd.info")

  private type CleanupAction = () => Unit

  private def registerCleanup(cleanupActions: mutable.ArrayBuffer[CleanupAction])(
      cleanup: => Unit): Unit = {
    cleanupActions += (() => cleanup)
  }

  private def setLoreDumpPathTag(
      target: SparkPlan,
      path: String,
      cleanupActions: mutable.ArrayBuffer[CleanupAction]): Unit = {
    target.setTagValue(LORE_DUMP_PATH_TAG, path)
    registerCleanup(cleanupActions)(target.unsetTagValue(LORE_DUMP_PATH_TAG))
  }

  private def setLoreDumpRDDTag(
      target: SparkPlan,
      info: LoreDumpRDDInfo,
      cleanupActions: mutable.ArrayBuffer[CleanupAction]): Unit = {
    target.setTagValue(LORE_DUMP_RDD_TAG, info)
    registerCleanup(cleanupActions)(target.unsetTagValue(LORE_DUMP_RDD_TAG))
  }

  private def executeWithNonStrictGuard(
      loreId: LoreId,
      nodeName: String,
      rapidsConf: RapidsConf,
      cleanupActions: mutable.ArrayBuffer[CleanupAction])(
      body: => Unit): Boolean = {
    try {
      body
      cleanupActions.clear()
      true
    } catch {
      case NonFatal(e) if rapidsConf.loreNonStrictMode =>
        cleanupActions.foreach(action => action())
        cleanupActions.clear()
        logWarning(
          s"Skipping LORE dump for loreId=$loreId on $nodeName because: ${e.getMessage}",
          e)
        false
    }
  }

  def pathOfRootPlanMeta(rootPath: Path): Path = {
    new Path(rootPath, "plan.meta")
  }

  def dumpPlan[T <: SparkPlan : ClassTag](plan: T, rootPath: Path): Unit = {
    dumpObject(plan, pathOfRootPlanMeta(rootPath),
      SparkSessionUtils.sessionFromPlan(plan).sparkContext.hadoopConfiguration)
  }

  def dumpObject[T: ClassTag](obj: T, path: Path, hadoopConf: Configuration): Unit = {
    val fs = path.getFileSystem(hadoopConf)
    withResource(fs.create(path, true)) { fout =>
      val serializerStream = SparkEnv.get.serializer.newInstance().serializeStream(fout)
      withResource(serializerStream) { ser =>
        ser.writeObject(obj)
      }
    }
  }

  def loadObject[T: ClassTag](path: Path, hadoopConf: Configuration): T = {
    val fs = path.getFileSystem(hadoopConf)
    withResource(fs.open(path)) { fin =>
      val serializerStream = SparkEnv.get.serializer.newInstance().deserializeStream(fin)
      withResource(serializerStream) { ser =>
        ser.readObject().asInstanceOf[T]
      }
    }
  }

  def pathOfChild(rootPath: Path, childIndex: Int): Path = {
    new Path(rootPath, s"input-$childIndex")
  }

  def restoreGpuExec(rootPath: Path, spark: SparkSession): GpuExec = {
    val rootExec = loadObject[SparkPlan](pathOfRootPlanMeta(rootPath),
      spark.sparkContext.hadoopConfiguration)

    checkUnsupportedOperator(rootExec)

    val broadcastHadoopConf = {
      val sc = spark.sparkContext
      sc.broadcast(new SerializableConfiguration(spark.sparkContext.hadoopConfiguration))
    }

    rootExec match {
      case gpuExec: GpuExec =>
        // Handle regular GpuExec (existing logic)
        restoreGpuExecInternal(gpuExec, rootPath, broadcastHadoopConf)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported plan type for restoration: ${rootExec.getClass.getSimpleName}")
    }
  }

  private def restoreGpuExecInternal(rootExec: GpuExec, rootPath: Path,
      broadcastHadoopConf: Broadcast[SerializableConfiguration]): GpuExec = {
    // Load children
    val newChildren = rootExec.children.zipWithIndex.map { case (plan, idx) =>
      val newChild = GpuLoreReplayExec(idx, rootPath.toString, broadcastHadoopConf)
      plan match {
        case b: GpuBroadcastExchangeExec =>
          b.withNewChildren(Seq(newChild))
        case b: BroadcastQueryStageExec =>
          b.broadcast.withNewChildren(Seq(newChild))
        case _ => newChild
      }
    }

    var nextId = rootExec.children.length

    rootExec.transformExpressionsUp {
      case sub: ExecSubqueryExpression =>
        val newSub = restoreSubqueryPlan(nextId, sub, rootPath, broadcastHadoopConf)
        nextId += 1
        newSub
    }.withNewChildren(newChildren).asInstanceOf[GpuExec]
  }

  private def restoreSubqueryPlan(id: Int, sub: ExecSubqueryExpression,
      rootPath: Path, hadoopConf: Broadcast[SerializableConfiguration]): ExecSubqueryExpression = {
    val innerPlan = sub.plan.child

    if (innerPlan.isInstanceOf[GpuExec]) {
      var newChild: SparkPlan = GpuLoreReplayExec(id, rootPath.toString, hadoopConf)

      if (!innerPlan.supportsColumnar) {
        newChild = GpuColumnarToRowExec(newChild)
      }
      val newSubqueryExec = sub.plan match {
        case ReusedSubqueryExec(subqueryExec) => subqueryExec.withNewChildren(Seq(newChild))
          .asInstanceOf[BaseSubqueryExec]
        case p: BaseSubqueryExec => p.withNewChildren(Seq(newChild))
          .asInstanceOf[BaseSubqueryExec]
      }
      sub.withNewPlan(newSubqueryExec)
    } else {
      throw new IllegalArgumentException(s"Subquery plan ${innerPlan.getClass.getSimpleName} " +
        s"is not a GpuExec")
    }
  }

  /**
   * Lore id generator. Key is [[SQLExecution.EXECUTION_ID_KEY]].
   */
  private val idGen: ConcurrentMap[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]()

  private def nextLoreIdOf(plan: SparkPlan): Option[Int] = {
    // When the execution id is not set, it means there is no actual execution happening, in this
    // case we don't need to generate lore id.
    Option(SparkSessionUtils.sessionFromPlan(plan)
      .sparkContext
      .getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map { executionId =>
        idGen.computeIfAbsent(executionId, _ => new AtomicInteger(0)).getAndIncrement()
      }
  }
  /**
   * Executions that have checked the lore output root path.
   * Key is [[SQLExecution.EXECUTION_ID_KEY]].
   */
  private val loreOutputRootPathChecked: ConcurrentHashMap[String, Boolean] =
    new ConcurrentHashMap[String, Boolean]()

  def tagForLore(sparkPlan: SparkPlan, rapidsConf: RapidsConf): SparkPlan = {
    val loreDumpIds = rapidsConf.loreDumpIds

    val newPlan = if (loreDumpIds.nonEmpty) {
      // We need to dump the output of nodes with the lore id in the dump ids
      val loreOutputRootPath = rapidsConf.loreDumpPath.getOrElse(throw
        new IllegalArgumentException(s"${RapidsConf.LORE_DUMP_PATH.key} must be set " +
          s"when ${RapidsConf.LORE_DUMP_IDS.key} is set."))

      val spark = SparkSessionUtils.sessionFromPlan(sparkPlan)

      Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).foreach {
        executionId =>
        loreOutputRootPathChecked.computeIfAbsent(executionId, _ => {
          val path = new Path(loreOutputRootPath)
          val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
          if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
            throw new IllegalArgumentException(
              s"LORE dump path $loreOutputRootPath already exists and is not empty.")
          }
          true
        })
      }

      val hadoopConf = {
        val sc = spark.sparkContext
        sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))
      }

      val subqueries = mutable.Set.empty[SparkPlan]

      sparkPlan.foreachUp {
        case g: GpuExec =>
          nextLoreIdOf(g).foreach { loreId =>
            g.setTagValue(LORE_ID_TAG, loreId.toString)

            loreDumpIds.get(loreId).foreach { outputLoreIds =>
              val cleanupActions = mutable.ArrayBuffer.empty[CleanupAction]
              executeWithNonStrictGuard(loreId, g.nodeName, rapidsConf, cleanupActions) {
                checkUnsupportedOperator(g)
                val currentExecRootPath = new Path(loreOutputRootPath, s"loreId-$loreId")
                setLoreDumpPathTag(g, currentExecRootPath.toString, cleanupActions)
                val loreOutputInfo = LoreOutputInfo(outputLoreIds,
                  currentExecRootPath.toString)

                g.children.zipWithIndex.foreach {
                  case (child, idx) =>
                    val dumpRDDInfo = LoreDumpRDDInfo(idx, loreOutputInfo, child.output, hadoopConf,
                      useOriginalSchemaNames = rapidsConf.loreParquetUseOriginalNames,
                      nonStrictMode = rapidsConf.loreNonStrictMode)
                    child match {
                      case c: BroadcastQueryStageExec =>
                        setLoreDumpRDDTag(c.broadcast, dumpRDDInfo, cleanupActions)
                      case o =>
                        setLoreDumpRDDTag(o, dumpRDDInfo, cleanupActions)
                    }
                }

                var nextId = g.children.length
                g.transformExpressionsUp {
                  case sub: ExecSubqueryExpression =>
                    if (spark.sessionState.conf.subqueryReuseEnabled) {
                      if (!subqueries.contains(sub.plan.canonicalized)) {
                        subqueries += sub.plan.canonicalized
                        registerCleanup(cleanupActions)(
                          subqueries -= sub.plan.canonicalized)
                      } else {
                        throw new IllegalArgumentException(
                          "Subquery reuse is enabled, and we found duplicated subqueries, " +
                            "which is currently not supported by LORE.")
                      }
                    }
                    tagSubqueryPlan(nextId, sub, loreOutputInfo, hadoopConf,
                      cleanupActions, rapidsConf.loreNonStrictMode)
                    nextId += 1
                    sub
                }
              }
            }
          }
        case _ =>
      }

      sparkPlan

    } else {
      // We don't need to dump the output of the nodes, just tag the lore id
      sparkPlan.foreachUp {
        case g: GpuExec =>
          nextLoreIdOf(g).foreach { loreId =>
            g.setTagValue(LORE_ID_TAG, loreId.toString)
          }
        case _ =>
      }

      sparkPlan
    }

    newPlan
  }

  def loreIdOf(node: SparkPlan): Option[String] = {
    node.getTagValue(LORE_ID_TAG)
  }

  private def tagSubqueryPlan(
      id: Int,
      sub: ExecSubqueryExpression,
      loreOutputInfo: LoreOutputInfo,
      hadoopConf: Broadcast[SerializableConfiguration],
      cleanupActions: mutable.ArrayBuffer[CleanupAction],
      nonStrictMode: Boolean): Unit = {
    val innerPlan = sub.plan.child
    if (innerPlan.isInstanceOf[GpuExec]) {
      val useOriginalSchemaNames = RapidsConf.LORE_PARQUET_USE_ORIGINAL_NAMES
        .get(SparkSessionUtils.sessionFromPlan(innerPlan).sessionState.conf)
      val dumpRDDInfo = LoreDumpRDDInfo(id, loreOutputInfo, innerPlan.output,
        hadoopConf,
        useOriginalSchemaNames = useOriginalSchemaNames,
        nonStrictMode = nonStrictMode)
      innerPlan match {
        case p: GpuColumnarToRowExec =>
          setLoreDumpRDDTag(p.child, dumpRDDInfo, cleanupActions)
        case c =>
          setLoreDumpRDDTag(c, dumpRDDInfo, cleanupActions)
      }
    } else {
      throw new IllegalArgumentException(s"Subquery plan ${innerPlan.getClass.getSimpleName} " +
        s"is not a GpuExec")
    }
  }

  private def checkUnsupportedOperator(plan: SparkPlan): Unit = {
    plan match {
      // Allow GpuDataWritingCommandExec
      case _: GpuDataWritingCommandExec =>
        checkGpuDataWritingCommandSupportedVersion()
      case _ =>
        if (plan.children.isEmpty ||
          plan.isInstanceOf[GpuCustomShuffleReaderExec]
        ) {
          throw new UnsupportedOperationException(s"Currently we don't support dumping input of " +
            s"${plan.getClass.getSimpleName} operator.")
        }
    }
  }

  /**
   * Check if the current Spark version is in the unsupported versions list for GpuWriteFiles
   */
  private def checkGpuDataWritingCommandSupportedVersion(): Unit = {
    val currentShimVersion = ShimLoader.getShimVersion
    // Get the list of unsupported versions
    val unsupportedVersions = getGpuWriteFilesUnsupportedVersions
    if (unsupportedVersions.contains(currentShimVersion)) {
      throw new UnsupportedOperationException(
        s"LORE dump is not supported for GpuDataWritingCommandExec on Spark" +
          s" version $currentShimVersion. " +
        s"Unsupported versions: ${unsupportedVersions.mkString(", ")}")
    }
  }

  /**
   * Get the unsupported versions for GpuWriteFiles
   * @return Set of unsupported ShimVersion instances
   */
  private[lore] lazy val getGpuWriteFilesUnsupportedVersions: Set[ShimVersion] = {
    // These versions are extracted from GpuWriteFiles.scala spark-rapids-shim-json-lines
    Set(
      // Spark versions
      SparkShimVersion(3, 4, 0),
      SparkShimVersion(3, 4, 1),
      SparkShimVersion(3, 4, 2),
      SparkShimVersion(3, 4, 3),
      SparkShimVersion(3, 4, 4),
      SparkShimVersion(3, 5, 0),
      SparkShimVersion(3, 5, 1),
      SparkShimVersion(3, 5, 2),
      SparkShimVersion(3, 5, 3),
      SparkShimVersion(3, 5, 4),
      SparkShimVersion(3, 5, 5),
      SparkShimVersion(3, 5, 6),
      SparkShimVersion(4, 0, 0),
      SparkShimVersion(4, 0, 1),
      // Databricks versions
      DatabricksShimVersion(3, 3, 2, "12.2"), // 332db
      DatabricksShimVersion(3, 4, 1, "13.3"), // 341db143
      DatabricksShimVersion(3, 5, 0, "14.3") // 350db143
    )
  }
}
