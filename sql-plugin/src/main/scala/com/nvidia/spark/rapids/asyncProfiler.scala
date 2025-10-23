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

import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util.zip.GZIPOutputStream

import com.nvidia.spark.rapids.Arm.withResource
import one.profiler.{AsyncProfiler, AsyncProfilerLoader}
import org.apache.hadoop.fs.Path

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
 * after each stage.
 *
 * It is strongly recommended to set 'spark.scheduler.mode' to 'FIFO' for better stage
 * separation. However, even with FIFO scheduling, we cannot guarantee perfect stage boundaries
 * due to various factors like out-of-order submissions, task failures, stragglers, etc.
 * To handle these cases, this implementation uses a task-count-based approach to determine stage
 * epochs. We periodically check which stage has the most running tasks (and more than 50% of total
 * tasks) to determine the current profiling epoch. This approach helps avoid profiling
 * anomalies when multiple stages have concurrent tasks, ensuring more accurate per-stage
 * flame graphs.
 *
 */
object AsyncProfilerOnExecutor extends Logging {

  private var asyncProfilerPrefix: Option[String] = None
  private var asyncProfiler: Option[AsyncProfiler] = None
  private var pluginCtx: PluginContext = _
  private var profileOptions: String = _
  private var currentProfilingStage = -1
  private var jfrCompressionEnabled: Boolean = false
  private var stageEpochInterval: Int = 5 // seconds

  private var needMoveFile: Boolean = false // true when `asyncProfilerPathPrefix` is non-local
  private var tempFilePath: java.nio.file.Path = _

  private var isShutdown = false
  private var isEpochCallbackRegistered = false

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
        this.jfrCompressionEnabled = conf.asyncProfilerJfrCompression
        this.stageEpochInterval = conf.stageEpochInterval

        // Register with the global StageEpochManager
        isEpochCallbackRegistered = StageEpochManager.registerCallback(
          name = "AsyncProfiler",
          callback = onStageTransition,
          epochInterval = this.stageEpochInterval
        )
        isShutdown = false
      })
    })
  }




  def shutdown(): Unit = {
    isShutdown = true
    if (isEpochCallbackRegistered) {
      StageEpochManager.unregisterCallback("AsyncProfiler")
      isEpochCallbackRegistered = false
    }
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

  /**
   * Called when stage transition occurs from the StageEpochManager.
   */
  private def onStageTransition(oldStage: Int, newStage: Int, 
      taskCount: Int, totalTasks: Int): Unit = {
    if (isShutdown || asyncProfiler.isEmpty) {
      return
    }
    
    asyncProfiler.synchronized {
      if (newStage != currentProfilingStage && !isShutdown) {
        log.info(s"Switching profiling from stage $currentProfilingStage " +
          s"to stage $newStage (has $taskCount/$totalTasks tasks)")
        
        closeLastProfiler()
        currentProfilingStage = newStage
        startProfilingForStage(newStage)
      }
    }
  }
  
  /**
   * Starts profiling for the specified stage.
   */
  private def startProfilingForStage(stageId: Int): Unit = {
    asyncProfiler.foreach(profiler => {
      try {
        // Get current epoch from StageEpochManager and increment for next time
        val currentEpoch = StageEpochManager.getStageEpochCount(stageId) - 1
        
        val filePath = {
          if (needMoveFile) {
            // if the asyncProfilerPathPrefix is non-local, we first write to a temp file
            // then move it to the final location
            tempFilePath = Files.createTempFile("async-profiler-temp-jfr", null)
            tempFilePath.toString
          } else {
            // if the path is local, we can just use it directly
            val parentPath: java.nio.file.Path = Paths.get(asyncProfilerPrefix.get)
            parentPath.resolve(
              s"async-profiler-app-${getAppId}-exec-${pluginCtx.executorID()}" +
                s"-stage-$stageId-epoch-$currentEpoch.jfr").toString
          }
        }
        profiler.execute(s"start,$profileOptions,file=$filePath")
        log.info(s"Successfully started profiling for stage $stageId epoch $currentEpoch")
      } catch {
        case e: Exception =>
          log.error(s"Error starting profiling for stage $stageId", e)
      }
    })
  }

  private def shouldProfile(executorId: String, conf: RapidsConf): Boolean = {
    // special support for "*", which means profile all executors
    if ("*".equals(conf.asyncProfilerExecutors)) {
      return true
    }

    val matcher = new RangeConfMatcher(conf, RapidsConf.ASYNC_PROFILER_EXECUTORS)
    matcher.contains(executorId)
  }

  /**
   * Compresses a JFR file using GZIP compression.
   *
   * @param inputPath  Path to the input JFR file
   * @param outputPath Path to the compressed output file (will have .gz extension)
   * @return true if compression was successful, false otherwise
   */
  private def compressJfrFile(
      inputPath: java.nio.file.Path, outputPath: java.nio.file.Path): Boolean = {
    try {
      val buffer = new Array[Byte](8192)
      withResource(new BufferedInputStream(new FileInputStream(inputPath.toFile))) { in =>
        withResource(new BufferedOutputStream(new GZIPOutputStream(
          new FileOutputStream(outputPath.toFile)))) { out =>
          var bytesRead = in.read(buffer)
          while (bytesRead != -1) {
            out.write(buffer, 0, bytesRead)
            bytesRead = in.read(buffer)
          }
        }
      }
      log.debug(s"Successfully compressed JFR file from $inputPath to $outputPath")
      true
    } catch {
      case e: Exception =>
        log.error(s"Failed to compress JFR file from $inputPath to $outputPath", e)
        false
    }
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
              val currentEpoch = StageEpochManager.getStageEpochCount(currentProfilingStage) - 1

              val baseFileName = s"async-profiler-app-${getAppId}-exec-${pluginCtx.executorID()}" +
                s"-stage-$currentProfilingStage-epoch-$currentEpoch.jfr"
              val outPath = new Path(asyncProfilerPrefix.get, 
                if (jfrCompressionEnabled) baseFileName + ".gz" else baseFileName)
              
              val hadoopConf = pluginCtx.ask(ProfileInitMsg(executorId, outPath.toString))
                .asInstanceOf[SerializableConfiguration].value
              val fs = outPath.getFileSystem(hadoopConf)

              if (jfrCompressionEnabled) {
                // Compress the temp file first
                val compressedTempPath = Files.createTempFile("compressed-jfr", ".gz")
                if (compressJfrFile(tempFilePath, compressedTempPath)) {
                  fs.copyFromLocalFile(new Path(compressedTempPath.toString), outPath)
                  compressedTempPath.toFile.delete() // delete compressed temp file
                } else {
                  // If compression fails, copy the original file
                  log.warn("JFR compression failed, copying uncompressed file")
                  fs.copyFromLocalFile(new Path(tempFilePath.toString), 
                    new Path(asyncProfilerPrefix.get, baseFileName))
                }
              } else {
                fs.copyFromLocalFile(new Path(tempFilePath.toString), outPath)
              }
              tempFilePath.toFile.delete() // delete the original temp file after moving
            } else {
              // For local files, compress in place if enabled
              if (jfrCompressionEnabled) {
                val currentEpoch =
                  Option(StageEpochManager.getStageEpochCount(currentProfilingStage))
                  .map(_ - 1)
                  .getOrElse {
                    log.warn(s"Stage $currentProfilingStage not found in epoch counters, " +
                      s"using epoch 0")
                    0
                  }
                val originalPath = Paths.get(asyncProfilerPrefix.get).resolve(
                  s"async-profiler-app-${getAppId}-exec-${pluginCtx.executorID()}" +
                    s"-stage-$currentProfilingStage-epoch-$currentEpoch.jfr")
                val compressedPath = Paths.get(originalPath.toString + ".gz")
                
                if (compressJfrFile(originalPath, compressedPath)) {
                  originalPath.toFile.delete() // delete original file after successful compression
                } else {
                  log.warn("JFR compression failed, keeping uncompressed file")
                }
              }
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



