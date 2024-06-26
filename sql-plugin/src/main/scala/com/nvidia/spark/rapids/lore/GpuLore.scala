/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import scala.reflect.ClassTag

import com.nvidia.spark.rapids.{GpuColumnarToRowExec, GpuExec, GpuFilterExec, RapidsConf}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.{ShimLeafExecNode, SparkShimImpl}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuCustomShuffleReaderExec}
import org.apache.spark.sql.types.DataType

case class LoreRDDMeta(numPartitions: Int, outputPartitions: Seq[Int], attrs: Seq[Attribute])

case class LoreRDDPartitionMeta(numBatches: Int, dataType: Seq[DataType])

trait GpuLoreRDD {
  val rootPath: Path

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


object GpuLore {
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

  def pathOfRootPlanMeta(rootPath: Path): Path = {
    new Path(rootPath, "plan.meta")
  }

  def dumpPlan[T <: SparkPlan : ClassTag](plan: T, rootPath: Path): Unit = {
    dumpObject(plan, pathOfRootPlanMeta(rootPath),
      SparkShimImpl.sessionFromPlan(plan).sparkContext.hadoopConfiguration)
  }

  def dumpObject[T: ClassTag](obj: T, path: Path, hadoopConf: Configuration): Unit = {
    withResource(path.getFileSystem(hadoopConf)) { fs =>
      withResource(fs.create(path, false)) { fout =>
        val serializerStream = SparkEnv.get.serializer.newInstance().serializeStream(fout)
        withResource(serializerStream) { ser =>
          ser.writeObject(obj)
        }
      }
    }
  }

  def loadObject[T: ClassTag](path: Path, hadoopConf: Configuration): T = {
    withResource(path.getFileSystem(hadoopConf)) { fs =>
      withResource(fs.open(path)) { fin =>
        val serializerStream = SparkEnv.get.serializer.newInstance().deserializeStream(fin)
        withResource(serializerStream) { ser =>
          ser.readObject().asInstanceOf[T]
        }
      }
    }
  }

  def pathOfChild(rootPath: Path, childIndex: Int): Path = {
    new Path(rootPath, s"input-$childIndex")
  }

  def restoreGpuExec(rootPath: Path, hadoopConf: Configuration): GpuExec = {
    val rootExec = loadObject[GpuExec](pathOfRootPlanMeta(rootPath), hadoopConf)

    // Load children
    val newChildren = rootExec.children.zipWithIndex.map { case (plan, idx) =>
      val newChild = GpuLoreReplayExec(idx, rootPath)
      plan match {
        case b: GpuBroadcastExchangeExec =>
          b.withNewChildren(Seq(newChild))
        case b: BroadcastQueryStageExec =>
          b.broadcast.withNewChildren(Seq(newChild))
        case _ => newChild
      }
    }

    rootExec match {
      case b: GpuFilterExec =>
        val newExpr = restoreSubqueryExpression(1, b.condition, rootPath)
        b.makeCopy(Array(newExpr, newChildren.head)).asInstanceOf[GpuExec]
      case _ => rootExec.withNewChildren(newChildren)
        .asInstanceOf[GpuExec]
    }
  }

  private def restoreSubqueryExpression(startIdx: Int, expression: Expression,
      rootPath: Path): Expression = {
    var nextIdx = startIdx
    val newExpr = expression.transformUp {
      case sub: ExecSubqueryExpression if sub.plan.child.isInstanceOf[GpuExec] =>
        var newChild: SparkPlan = GpuLoreReplayExec(nextIdx, rootPath)
        if (!sub.plan.supportsColumnar) {
          newChild = GpuColumnarToRowExec(newChild)
        }
        val newSubqueryExec = sub.plan.withNewChildren(Seq(newChild)).asInstanceOf[BaseSubqueryExec]
        nextIdx += 1
        sub.withNewPlan(newSubqueryExec)
    }
    newExpr
  }

  /**
   * Lore id generator. Key is [[SQLExecution.EXECUTION_ID_KEY]].
   */
  private val idGen: ConcurrentMap[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]()

  private def nextLoreIdOf(plan: SparkPlan): Option[Int] = {
    // When the execution id is not set, it means there is no actual execution happening, in this
    // case we don't need to generate lore id.
    Option(SparkShimImpl.sessionFromPlan(plan)
      .sparkContext
      .getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .map { executionId =>
        idGen.computeIfAbsent(executionId, _ => new AtomicInteger(0)).getAndIncrement()
      }
  }

  def tagForLore(sparkPlan: SparkPlan, rapidsConf: RapidsConf): SparkPlan = {
    val loreDumpIds = rapidsConf.get(RapidsConf.LORE_DUMP_IDS).map(OutputLoreId.parse)

    val newPlan = loreDumpIds match {
      case Some(dumpIds) =>
        // We need to dump the output of the output of nodes with the lore id in the dump ids
        val loreOutputRootPath = rapidsConf.get(RapidsConf.LORE_DUMP_PATH).getOrElse(throw
          new IllegalArgumentException(s"${RapidsConf.LORE_DUMP_PATH.key} must be set " +
            s"when ${RapidsConf.LORE_DUMP_IDS.key} is set."))

        sparkPlan.foreachUp {
          case g: GpuExec =>
            nextLoreIdOf(g).foreach { loreId =>
              g.setTagValue(LORE_ID_TAG, loreId.toString)

              dumpIds.get(loreId).foreach { outputLoreIds =>
                checkUnsupportedOperator(g)
                val currentExecRootPath = new Path(loreOutputRootPath, s"loreId-$loreId")
                g.setTagValue(LORE_DUMP_PATH_TAG, currentExecRootPath.toString)
                val loreOutputInfo = LoreOutputInfo(outputLoreIds,
                  currentExecRootPath)

                g.children.zipWithIndex.foreach {
                  case (child, idx) =>
                    val dumpRDDInfo = LoreDumpRDDInfo(idx, loreOutputInfo, child.output)
                    child match {
                      case c: BroadcastQueryStageExec =>
                        c.broadcast.setTagValue(LORE_DUMP_RDD_TAG, dumpRDDInfo)
                      case o => o.setTagValue(LORE_DUMP_RDD_TAG, dumpRDDInfo)
                    }
                }

                g match {
                  case f: GpuFilterExec =>
                    tagForSubqueryPlan(1, f.condition, loreOutputInfo)
                  case _ =>
                }
              }
            }
          case _ =>
        }

        sparkPlan
      case None =>
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

  private def tagForSubqueryPlan(startId: Int, expression: Expression,
      loreOutputInfo: LoreOutputInfo): Int = {
    var nextPlanId = startId
    expression.foreachUp {
      case sub: ExecSubqueryExpression =>
        if (sub.plan.child.isInstanceOf[GpuExec]) {
          val dumpRDDInfo = LoreDumpRDDInfo(nextPlanId, loreOutputInfo, sub.plan.child.output)
          sub.plan.child match {
            case p: GpuColumnarToRowExec => p.child.setTagValue(LORE_DUMP_RDD_TAG, dumpRDDInfo)
            case c => c.setTagValue(LORE_DUMP_RDD_TAG, dumpRDDInfo)
          }

          nextPlanId += 1
        } else {
          throw new IllegalArgumentException(s"Subquery plan ${sub.plan} is not a GpuExec")
        }
      case _ =>
    }
    nextPlanId
  }

  private def checkUnsupportedOperator(plan: SparkPlan): Unit = {
    if (plan.isInstanceOf[ShimLeafExecNode] ||
      plan.isInstanceOf[GpuCustomShuffleReaderExec]
    ) {
      throw new UnsupportedOperationException(s"Currently we don't support dumping input of " +
        s"${plan.getClass.getSimpleName} operator.")
    }
  }
}
