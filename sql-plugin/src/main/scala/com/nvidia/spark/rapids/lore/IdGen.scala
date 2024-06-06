package com.nvidia.spark.rapids.lore

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.AtomicInteger

import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}

object IdGen {
  val LORE_ID_TAG: TreeNodeTag[String] = new TreeNodeTag[String]("rapids.gpu.lore.id")

  /**
   * Lore id generator. Key is [[SQLExecution.EXECUTION_ID_KEY]].
   */
  private val idGen: ConcurrentMap[String, AtomicInteger] =
    new ConcurrentHashMap[String, AtomicInteger]()

  private def nextLoreIdOfSparkPlan(plan: SparkPlan): Int = {
    val executionId = plan.session.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    idGen.computeIfAbsent(executionId, _ => new AtomicInteger(0)).getAndIncrement()
  }

  def tagLoreId(sparkPlan: SparkPlan): SparkPlan = {
    sparkPlan.foreachUp {
      case g: GpuExec =>
        val loreId = nextLoreIdOfSparkPlan(g)
        g.setTagValue(LORE_ID_TAG, loreId.toString)
      case _ =>
    }

    sparkPlan
  }

  def lordIdOf(node: SparkPlan): Option[String] = {
    node.getTagValue(LORE_ID_TAG)
  }
}
