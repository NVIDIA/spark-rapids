/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.nio.file.{Files, Paths}

import one.profiler.{AsyncProfiler, AsyncProfilerLoader}
import org.apache.hadoop.fs.Path

import org.apache.spark.TaskContext
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.SerializableConfiguration

/**
 * Unlike ProfilerOnExecutor, which is used to create Nsys reports, this class is used to create
 * flame graph to understand where time is spent for each stage. Typically, we can use metrics to
 * achieve the same goal, but metrics cannot guarantee to capture all the time spent in a stage,
 * and it may not be detailed enough because it lacks the stacktraces information.
 *
 * From the perspective of profiling, we know that different stages may exhibit different patterns.
 * Since the output is a FUSED flame graph, it is not possible to distinguish between different
 * stages or threads, so the profiling is done per stage, and the output file is named
 * after each stage. We also require user to use FIFO scheduler mode so that task in
 * stage N+1 will not start until every task in stage N has finished, which allows us
 * to profile stage N without being polluted by tasks from stage N+1.
 *
 * With the above assumptions, we find that we don't need the complex driver-executor communication
 * protocol as in ProfilerOnExecutor, and we can simply start a NEW profile when the first task
 * of a new stage appears, and stop & finalize the OLD profile at the same time. Also, we found that
 * the generated profile file is small enough that we do need to limit which stage to profile,
 * we can just profile all stages. Still we provide a way to limit which executor to profile to
 * save disk resources across the cluster.
 */

object AsyncProfilerOnExecutor extends Logging {

  private var asyncProfilerPrefix: Option[String] = None
  private var asyncProfiler: Option[AsyncProfiler] = None
  private var pluginCtx: PluginContext = _
  private var profileOptions: String = _
  @volatile private var currentProfilingStage = -1

  private var needMoveFile: Boolean = false // true when `asyncProfilerPathPrefix` is non-local
  private var tempFilePath: java.nio.file.Path = _

  def init(ctx: PluginContext, conf: RapidsConf): Unit = {
    pluginCtx = ctx
    asyncProfilerPrefix = conf.asyncProfilerPathPrefix
    // Only initialize the profiler if the prefix is non-empty
    asyncProfilerPrefix.foreach(prefix => {

      val executorId = pluginCtx.executorID()
      asyncProfiler = if (shouldProfile(executorId, conf) && AsyncProfilerLoader.isSupported) {
        Some(AsyncProfilerLoader.load())
      } else {
        None
      }

      asyncProfiler.foreach(_ => {
        if (prefix.startsWith("/")) {
          // like /tmp
          needMoveFile = false
        } else {
          // like hdfs://namenode:port/tmp or file:///tmp
          needMoveFile = true
        }

        this.profileOptions = conf.asyncProfilerProfileOptions
      })
    })
  }


  def onTaskStart(): Unit = {
    asyncProfiler.foreach(profiler => {

      val stageId = TaskContext.get.stageId()
      if (stageId > currentProfilingStage) {
        asyncProfiler.synchronized {
          if (stageId > currentProfilingStage) {

            log.debug(s"In onTaskStart(), stageId: $stageId, " +
              s"currentProfilingStage: $currentProfilingStage")
            closeLastProfiler()
            currentProfilingStage = stageId

            try {
              val filePath = {
                if (needMoveFile) {
                  // if the asyncProfilerPathPrefix is non-local, we first write to a temp file
                  // then move it to the final location
                  tempFilePath = Files.createTempFile("temp-file", null)
                  tempFilePath.toString
                } else {
                  // if the path is local, we can just use it directly
                  val parentPath: java.nio.file.Path = Paths.get(asyncProfilerPrefix.get)
                  parentPath.resolve(
                    s"async-profiler-app-${getAppId}-exec-${pluginCtx.executorID()}" +
                      s"-stage-$currentProfilingStage.jfr").toString
                }
              }
              profiler.execute(s"start,$profileOptions,file=$filePath")
              log.info(s"successfully started profiling for stage $currentProfilingStage")
            } catch {
              case e: Exception =>
                log.error(s"error starting profiling for stage $currentProfilingStage", e)
            }
          }
        }
      }

    })
  }

  def shutdown(): Unit = {
    closeLastProfiler()
  }


  private def getAppId: String = {
    val appId = pluginCtx.conf.get("spark.app.id", "")
    if (appId.isEmpty) {
      java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    } else {
      appId
    }
  }

  private def shouldProfile(executorId: String, conf: RapidsConf): Boolean = {
    // special support for "*", which means profile all executors
    if ("*".equals(conf.asyncProfilerExecutors)) {
      return true
    }

    val matcher = new RangeConfMatcher(conf, RapidsConf.ASYNC_PROFILER_EXECUTORS)
    matcher.contains(executorId)
  }


  private def closeLastProfiler(): Unit = {
    asyncProfiler.foreach(profiler => {
      asyncProfiler.synchronized {
        if (currentProfilingStage != -1) { // for the first stage, skip stopping
          try {
            log.info(s"stop profiling for stage $currentProfilingStage")
            profiler.execute("stop")
            if (needMoveFile) {
              val executorId = pluginCtx.executorID()

              val outPath = new Path(asyncProfilerPrefix.get,
                s"async-profiler-app-${getAppId}-exec-${pluginCtx.executorID()}" +
                  s"-stage-$currentProfilingStage.jfr")
              val hadoopConf = pluginCtx.ask(ProfileInitMsg(executorId, outPath.toString))
                .asInstanceOf[SerializableConfiguration].value
              val fs = outPath.getFileSystem(hadoopConf)
              fs.copyFromLocalFile(new Path(tempFilePath.toString), outPath)
              tempFilePath.toFile.delete() // delete the temp file after moving
            }
            log.info(s"successfully stopped profiling stage $currentProfilingStage")
          } catch {
            case e: Exception =>
              log.error(s"error stopping profiling for stage $currentProfilingStage", e)
          }
        }
      }
    })
  }
}



