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

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import ai.rapids.spark
import org.apache.commons.lang3.mutable.MutableLong

import org.apache.spark.{ExecutorPlugin, ExecutorPluginContext, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.util.TaskCompletionListener

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

trait GpuSpillable {
  /**
   * Spill GPU memory if possible
   * @param target the amount of memory that we want to try and spill.
   */
  def spill(target: Long): Unit
}

object GpuResourceManager extends MemoryListener with Logging {
  private val totalActuallyUsed: AtomicLong = new AtomicLong(0)
  private val totalUsedAndReserved: AtomicLong = new AtomicLong(0)
  private val prediction = new ThreadLocal[MutableLong]()
  private val predictionName = new ThreadLocal[String]()
  private var spillCutoff: Long = -1
  private var stopAndSpillCutoff: Long = -1
  private var controller: Controller = _
  private val spillers = new AtomicReference[Set[GpuSpillable]](Set())

  class BufferTracking private[GpuResourceManager](
      val size: Long,
      val note: String) extends AutoCloseable with Logging {
    {
      val tots = totalActuallyUsed.addAndGet(size)
      totalUsedAndReserved.addAndGet(size)
      logDebug(s"BUFFER: $size TOTAL: $tots T: $totalUsedAndReserved ($note)")
      GpuResourceManager.spillIfNeeded()
    }

    override def close(): Unit = {
      val tots = totalActuallyUsed.addAndGet(- size)
      totalUsedAndReserved.addAndGet(- size)
      logDebug(s"CLOSE BUFFER: $size TOTAL: $tots T: $totalUsedAndReserved ($note)")
    }
  }

  def register(spiller: GpuSpillable): Unit =
    spillers.getAndUpdate(s => s + spiller)

  def deregister(spiller: GpuSpillable): Unit =
    spillers.getAndUpdate(s => s - spiller)

  private class Controller extends Thread {
    setDaemon(true)
    private var done = false
    private val signal = new Array[Byte](0)

    def setDone(): Unit = {
      done = true
      interrupt()
    }

    def waitForSpillToComplete(): Unit = {
      val range = new NvtxRange("WAIT FOR SPILL", NvtxColor.RED)
      try {
        logDebug("WAIT FOR SPILL")
        synchronized {
          signal.synchronized {
            // Wake up the spilling thread in case it is sleeping
            signal.notify()
          }
          wait(2000)
        }
        logDebug("DONE WAITING FOR SPILL")
      } finally {
        range.close()
      }
    }

    override def run(): Unit = {
      while (!done) {
        try {
          val used = totalUsedAndReserved.get()
          if (used > spillCutoff) {
            val needed = used - spillCutoff
            logInfo(s"SPILLING GPU MEMORY ${used / 1024 / 1024} MB USED " +
              s"${needed / 1024 / 1024} MB MORE NEEDED")
            val canSpill = spillers.get()
            canSpill.foreach(s => {
              val need = totalUsedAndReserved.get() - spillCutoff
              if (need > 0) {
                s.spill(need)
              }
            })
            val newUsed = totalUsedAndReserved.get()
            if (newUsed > stopAndSpillCutoff) {
              val needed = newUsed - spillCutoff
              logWarning(s"SPILL DID NOT FREE ENOUGH MEMORY ${newUsed / 1024 / 1024} MB USED " +
                s"${needed / 1024 / 1024} MB MORE NEEDED")
            }
          }
          synchronized {
            notifyAll()
          }
          try {
            signal.synchronized {
              signal.wait(100)
            }
          } catch {
            case _: InterruptedException => //Ignored
          }
        } catch {
          case e: Throwable => logError("Error during Spill", e)
        }
      }
    }
  }

  private[spark] def setCutoffs(spillAsync: Long, stopAndSpill: Long): Unit = {
    spillCutoff = spillAsync
    stopAndSpillCutoff = stopAndSpill
  }

  private[spark] def spillIfNeeded(): Unit = {
    val t = totalUsedAndReserved.get()
    if (t > spillCutoff) {
      synchronized {
        if (controller == null) {
          controller = new Controller()
          controller.start()
        }
      }
    }

    if (t > stopAndSpillCutoff) {
      controller.waitForSpillToComplete()
    }
  }

  private def getPrediction(): MutableLong = {
    var ret = prediction.get()
    if (ret eq null) {
      ret = new MutableLong(0)
      prediction.set(ret)
      val tc = TaskContext.get()
      if (tc != null) {
        logDebug(s"START FOR TASK ${tc.taskAttemptId()}")
        tc.addTaskCompletionListener(new TaskCompletionListener {
          override def onTaskCompletion(context: TaskContext): Unit = {
            logDebug(s"END FOR TASK ${context.taskAttemptId()}")
            prediction.remove()
          }
        })
      }
    }
    ret
  }

  def rawBuffer(amount: Long, note: String): BufferTracking = {
    new BufferTracking(amount, note)
  }

  override def prediction(amount: Long, note: String): Unit = {
    val prediction = getPrediction()
    predictionName.set(note)
    val previous = prediction.getValue
    prediction.setValue(amount)
    assert(previous == 0)
    totalUsedAndReserved.getAndAdd(amount)
    logDebug(s"PREDICTION: $amount USED: $totalActuallyUsed T: $totalUsedAndReserved ($note)")
    spillIfNeeded()
  }

  override def allocation(amount: Long, id: Long): Unit = {
    val prediction = getPrediction()
    val pred = predictionName.get()
    val newTotal = totalActuallyUsed.addAndGet(amount)
    val left = prediction.getValue()
    val newPrediction = if (left > amount) {
      prediction.addAndGet(-amount)
    } else {
      prediction.addAndGet(-left)
      totalUsedAndReserved.addAndGet(amount - left)
      0
    }
    logDebug(s"ALLOCATION: $id: $amount USED: $newTotal T: $totalUsedAndReserved PREDICTION LEFT: $newPrediction ($pred)")
    spillIfNeeded()
  }

  override def endPrediction(note: String): Unit = {
    val prediction = getPrediction()
    predictionName.remove()
    val wasLeft = prediction.getValue()
    prediction.setValue(0)
    totalUsedAndReserved.addAndGet(-wasLeft)
    logDebug(s"END PREDICTION: $wasLeft T: $totalUsedAndReserved ($note)")
  }

  override def deallocation(amount: Long, id: Long): Unit = {
    val newTotal = totalActuallyUsed.addAndGet(-amount)
    totalUsedAndReserved.addAndGet(-amount)
    logDebug(s"DEALLOCATION: $id: $amount USED: $newTotal T: $totalUsedAndReserved")
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
    MemoryListener.registerDeviceListener(GpuResourceManager)
    val env = SparkEnv.get
    val conf = new spark.RapidsConf(env.conf)
    val info = Cuda.memGetInfo()
    val async = (conf.rmmAsyncSpillPct * info.total).toLong
    val stop = (conf.rmmSpillPct * info.total).toLong
    GpuResourceManager.setCutoffs(async, stop)

    // We eventually will need a way to know which GPU to use/etc, but for now, we will just
    // go with the default GPU.
    if (!Rmm.isInitialized) {
      loggingEnabled = conf.isRmmDebugEnabled
      val info = Cuda.memGetInfo()
      val initialAllocation = (conf.rmmAllocPct * info.total).toLong
      if (initialAllocation > info.free) {
        logWarning(s"Initial RMM allocation(${initialAllocation / 1024 / 1024.0} MB) is " +
          s"larger than free memory(${info.free / 1024 /1024.0} MB)")
      }
      var init = RmmAllocationMode.CUDA_DEFAULT
      val features = ArrayBuffer[String]()
      if (conf.isPooledMemEnabled) {
        init = init | RmmAllocationMode.POOL
        features += "POOLED"
      }
      if (conf.isUvmEnabled) {
        init = init | RmmAllocationMode.CUDA_MANAGED_MEMORY
        features += "UVM"
      }

      logInfo(s"Initializing RMM${features.mkString(" "," ","")} ${initialAllocation / 1024 / 1024.0} MB")
      try {
        Rmm.initialize(init, loggingEnabled, initialAllocation)
      } catch {
        case e: Exception => logError("Could not initialize RMM", e)
      }

      if (conf.pinnedPoolSize > 0) {
        logInfo(s"Initializing pinned memory pool (${conf.pinnedPoolSize / 1024 / 1024.0} MB)")
        PinnedMemoryPool.initialize(conf.pinnedPoolSize)
      }
    }

    GpuSemaphore.initialize(conf.concurrentGpuTasks)
  }

  override def shutdown(): Unit = {
    if (loggingEnabled) {
      logWarning(s"RMM LOG\n${Rmm.getLog}")
    }

    GpuSemaphore.shutdown()
  }
}
