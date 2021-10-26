/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class AdaptiveTransitionsSuite
    extends SparkQueryCompareTestSuite
    with BeforeAndAfterEach
    with Logging {

  var spark: SparkSession = _

  override protected def beforeEach(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    PluginRules.reset()
  }

  override protected def afterEach(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    PluginRules.reset()
  }

  test("Approx percentile final aggregate fallback to CPU - AQE on") {
    doApproxPercentileTest(aqe = true)
  }

  test("Approx percentile final aggregate fallback to CPU - AQE off") {
    doApproxPercentileTest(aqe = false)
  }

  private def doApproxPercentileTest(aqe: Boolean) {

    import RapidsMeta.gpuSupportedTag

    spark = SparkSession.builder()
      .master("local[1]")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, aqe)
      .config("spark.plugins", "com.nvidia.spark.rapids.DebugSQLPlugin")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.rapids.sql.expression.ApproximatePercentile", "true")
      .getOrCreate()

    val df = FuzzerUtils.generateDataFrame(spark, StructType(Array(
      StructField("k", DataTypes.ShortType),
      StructField("v", DataTypes.DoubleType)
    )))

    df.createOrReplaceTempView("t")

    val tagPlan: Rule[SparkPlan] = (plan: SparkPlan) => {
      val aggregateOps = ShimLoader.getSparkShims
        .findOperators(plan, _.isInstanceOf[ObjectHashAggregateExec])
      if (aggregateOps.length == 2) {
        val p = aggregateOps.head
        p.setTagValue(gpuSupportedTag, p.getTagValue(gpuSupportedTag).getOrElse(Set.empty) +
          "unit test is forcing final hash aggregate onto CPU")
      }
      plan
    }

    // apply the plan modification during the appropriate planning phase depending on
    // whether AQE is enabled or not
    if (aqe) {
      PluginRules.queryStagePrep.pre = Some(tagPlan)
    } else {
      PluginRules.overrides.pre = Some(tagPlan)
    }

    val df2 = spark.sql("SELECT k, approx_percentile(v, array(0.1, 0.2)) from t group by k")
    df2.collect()

    // assert that both stages of the aggregate ran on the CPU
    val cpuAggregate = ShimLoader.getSparkShims
      .findOperators(df2.queryExecution.executedPlan, _.isInstanceOf[ObjectHashAggregateExec])
    assert(cpuAggregate.length == 2)

    // assert that no aggregate ran on the GPU
    val gpuAggregate = ShimLoader.getSparkShims
      .findOperators(df2.queryExecution.executedPlan, _.isInstanceOf[GpuHashAggregateExec])
    assert(gpuAggregate.isEmpty)
  }

}

class DebugSQLPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = new DebugRapidsDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = ShimLoader.newExecutorPlugin()
}

class DebugRapidsDriverPlugin extends DriverPlugin with Logging {
  var rapidsShuffleHeartbeatManager: RapidsShuffleHeartbeatManager = _

  override def receive(msg: Any): AnyRef = {
    if (rapidsShuffleHeartbeatManager == null) {
      throw new IllegalStateException(
        s"Rpc message $msg received, but shuffle heartbeat manager not configured.")
    }
    msg match {
      case RapidsExecutorStartupMsg(id) =>
        rapidsShuffleHeartbeatManager.registerExecutor(id)
      case RapidsExecutorHeartbeatMsg(id) =>
        rapidsShuffleHeartbeatManager.executorHeartbeat(id)
      case m => throw new IllegalStateException(s"Unknown message $m")
    }
  }

  override def init(
      sc: SparkContext,
      pluginContext: PluginContext): java.util.Map[String, String] = {

    val sparkConf = pluginContext.conf
    RapidsPluginUtils.fixupConfigs(sparkConf, "com.nvidia.spark.rapids.DebugSQLExecPlugin")
    val conf = new RapidsConf(sparkConf)

    if (GpuShuffleEnv.isRapidsShuffleAvailable) {
      GpuShuffleEnv.initShuffleManager()
      if (conf.shuffleTransportEarlyStart) {
        rapidsShuffleHeartbeatManager =
          new RapidsShuffleHeartbeatManager(
            conf.shuffleTransportEarlyStartHeartbeatInterval,
            conf.shuffleTransportEarlyStartHeartbeatTimeout)
      }
    }
    conf.rapidsConfMap
  }
}

object PluginRules {
  lazy val overrides: RuleWrapper = RuleWrapper(GpuOverrides())
  lazy val overrideTransitions: RuleWrapper = RuleWrapper(new GpuTransitionOverrides())
  lazy val queryStagePrep: RuleWrapper = RuleWrapper(ShimLoader.newGpuQueryStagePrepOverrides())

  def reset(): Unit = {
    PluginRules.queryStagePrep.pre = None
    PluginRules.queryStagePrep.post = None
    PluginRules.overrides.pre = None
    PluginRules.overrides.post = None
    PluginRules.overrideTransitions.pre = None
    PluginRules.overrideTransitions.post = None
  }
}

/**
 * Debug version of plugin that allows input and output to optimizer rules to be
 * intercepted for inspection and mutation.
 */
class DebugSQLExecPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(columnarOverrides)
    extensions.injectQueryStagePrepRule(queryStagePrepOverrides)
  }

  private def columnarOverrides(sparkSession: SparkSession): ColumnarRule = {
    DebugColumnarOverrideRules
  }

  private def queryStagePrepOverrides(sparkSession: SparkSession): Rule[SparkPlan] = {
    DebugQueryStagePrepOverrides
  }
}

object DebugColumnarOverrideRules extends ColumnarRule with Logging {
  override def preColumnarTransitions : Rule[SparkPlan] = PluginRules.overrides
  override def postColumnarTransitions: Rule[SparkPlan] = PluginRules.overrideTransitions
}

object DebugQueryStagePrepOverrides extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = PluginRules.queryStagePrep.apply(plan)
}

/**
 * Wrap a transformation with optional pre and post transformations to
 * inspect and mutate the input and output
 */
case class RuleWrapper(rule: Rule[SparkPlan]) extends Rule[SparkPlan] {
  var pre: Option[Rule[SparkPlan]] = None
  var post: Option[Rule[SparkPlan]] = None
  override def apply(plan: SparkPlan): SparkPlan = {
    val input = pre.map(_.apply(plan)).getOrElse(plan)
    val output = rule.apply(input)
    post.map(_.apply(output)).getOrElse(output)
  }
}

