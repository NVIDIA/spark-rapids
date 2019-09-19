/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.cudf.{Cuda, Rmm, RmmAllocationMode}
import ai.rapids.spark

import org.apache.spark.{ExecutorPlugin, ExecutorPluginContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

trait GpuPartitioning extends Partitioning {
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[GpuPartitioning]
  }

  override def hashCode(): Int = super.hashCode()
}

case class ColumnarOverrideRules() extends ColumnarRule with Logging {
  val overrides = GpuOverrides()
  val overrideTransitions = new GpuTransitionOverrides()

  override def preColumnarTransitions : Rule[SparkPlan] = overrides

  override def postColumnarTransitions: Rule[SparkPlan] = overrideTransitions
}

/**
  * Extension point to enable GPU processing.
  *
  * To run on a GPU set spark.sql.extensions to ai.rapids.spark.Plugin
  */
class Plugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logWarning("Installing extensions to enable rapids GPU SQL support." +
      s" To disable GPU support set `${RapidsConf.SQL_ENABLED}` to false")
    extensions.injectColumnar(_ => ColumnarOverrideRules())
  }
}

/**
 * Config to enable pooled GPU memory allocation which can improve performance.  This should be off
 * if you want to use operators that also use GPU memory like XGBoost or Tensorflow, as the pool
 * it allocates cannot be used by other tools.
 *
 * To enable this set spark.executor.plugins to ai.rapids.spark.GpuResourceManager
 */
class GpuResourceManager extends ExecutorPlugin with Logging {
  var loggingEnabled = false

  override def init(pluginContext: ExecutorPluginContext): Unit = synchronized {
    // We eventually will need a way to know which GPU to use/etc, but for now, we will just
    // go with the default GPU.
    if (!Rmm.isInitialized) {
      val env = SparkEnv.get
      val conf = new spark.RapidsConf(env.conf)
      loggingEnabled = conf.isMemDebugEnabled
      val info = Cuda.memGetInfo()
      val initialAllocation = info.free / 4
      logInfo(s"Initializing RMM ${initialAllocation / 1024 / 1024.0} MB")
      try {
        Rmm.initialize(RmmAllocationMode.POOL, loggingEnabled, initialAllocation)
      } catch {
        case e: Exception => logError("Could not initialize RMM", e)
      }
    }
  }

  override def shutdown(): Unit = {
    if (loggingEnabled) {
      logWarning(s"RMM LOG\n${Rmm.getLog}")
    }
  }
}
