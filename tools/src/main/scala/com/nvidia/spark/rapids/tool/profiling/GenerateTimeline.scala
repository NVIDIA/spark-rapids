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
package com.nvidia.spark.rapids.tool.profiling

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

abstract class TimelineTiming(
    val startTime: Long,
    val endTime: Long)

class TimelineTaskInfo(val stageId: Int, val taskId: Long,
    startTime: Long, endTime: Long, val duration: Long)
    extends TimelineTiming(startTime, endTime)

class TimelineStageInfo(val stageId: Int,
    startTime: Long,
    endTime:Long,
    val duration: Long) extends TimelineTiming(startTime, endTime)

class TimelineJobInfo(val jobId: Int,
    startTime: Long,
    endTime: Long,
    val duration: Long) extends TimelineTiming(startTime, endTime)

class TimelineSqlInfo(val sqlId: Long,
    startTime: Long,
    endTime: Long,
    val duration: Long) extends TimelineTiming(startTime, endTime)

/**
 * Generates an SVG graph that is used to show cluster timeline.
 */
object GenerateTimeline {
  private val TASK_HEIGHT = 20
  private val TITLE_BOX_WIDTH = 200
  private val PADDING = 5
  private val FONT_SIZE = 14
  private val TITLE_HEIGHT = FONT_SIZE + (PADDING * 2)
  private val FOOTER_HEIGHT = FONT_SIZE + (PADDING * 2)
  private val MS_PER_PIXEL = 5.0

  // Generated using https://mokole.com/palette.html
  private val COLORS = Array(
    "#696969",
    "#dcdcdc",
    "#556b2f",
    "#8b4513",
    "#483d8b",
    "#008000",
    "#3cb371",
    "#008b8b",
    "#000080",
    "#800080",
    "#b03060",
    "#ff4500",
    "#ffa500",
    // Going to be used by lines/etc "#00ff00",
    "#8a2be2",
    "#00ff7f",
    "#dc143c",
    "#00ffff",
    "#00bfff",
    "#f4a460",
    "#0000ff",
    "#f08080",
    "#adff2f",
    "#da70d6",
    "#ff00ff",
    "#1e90ff",
    "#eee8aa",
    "#ffff54",
    "#ff1493",
    "#7b68ee")

  def calcLayoutSlotsNeeded[A <: TimelineTiming](toSchedule: Iterable[A]): Int = {
    val slotsFreeUntil = ArrayBuffer[Long]()
    computeLayout(toSchedule, (_: A, _: Int) => (), false, slotsFreeUntil)
    slotsFreeUntil.length
  }

  def doLayout[A <: TimelineTiming](
      toSchedule: Iterable[A],
      numSlots: Int)(scheduleCallback: (A, Int) => Unit): Unit = {
    val slotsFreeUntil = new Array[Long](numSlots).toBuffer
    computeLayout(toSchedule, scheduleCallback, true, slotsFreeUntil)
  }

  def computeLayout[A <: TimelineTiming](
      toSchedule: Iterable[A],
      scheduleCallback: (A, Int) => Unit,
      errorOnMissingSlot: Boolean,
      slotsFreeUntil: mutable.Buffer[Long]): Unit = {
    toSchedule.toSeq.sortWith {
      case (a, b) => a.startTime < b.startTime
    }.foreach { timing =>
      val startTime = timing.startTime
      val slot = slotsFreeUntil.indices
          // There is some slop in how Spark reports this. Not sure why...
          .find(i => (startTime + 1) >= slotsFreeUntil(i))
          .getOrElse {
            if (errorOnMissingSlot) {
              throw new IllegalStateException("Not enough slots to schedule")
            } else {
              // Add a slot
              slotsFreeUntil.append(0L)
              slotsFreeUntil.length - 1
            }
          }
      slotsFreeUntil(slot) = timing.endTime
      scheduleCallback(timing, slot)
    }
  }

  private def textBoxVirtCentered(
      text: String,
      x: Number,
      y: Long,
      fileWriter: ToolTextFileWriter): Unit =
    fileWriter.write(
      s"""<text x="$x" y="$y" dominant-baseline="middle"
         | font-family="Courier,monospace" font-size="$FONT_SIZE">$text</text>
         |""".stripMargin)

  private def sectionBox(
      text: String,
      yStart: Long,
      numElements: Int,
      fileWriter: ToolTextFileWriter): Unit = {
    val boxHeight = numElements * TASK_HEIGHT
    val boxMiddleY = boxHeight/2 + yStart
    // Draw a box for the Host
    fileWriter.write(
      s"""<rect x="$PADDING" y="$yStart" width="$TITLE_BOX_WIDTH" height="$boxHeight"
         | style="fill:white;fill-opacity:0.0;stroke:black;stroke-width:2"/>
         |""".stripMargin)
    textBoxVirtCentered(text, PADDING * 2, boxMiddleY, fileWriter)
  }

  private def timingBox[A <: TimelineTiming](
      text: String,
      color: String,
      timing: A,
      slot: Int,
      xStart: Long,
      yStart: Long,
      minStart: Long,
      fileWriter: ToolTextFileWriter): Unit = {
    val startTime = timing.startTime
    val endTime = timing.endTime
    val x = xStart + (startTime - minStart)/MS_PER_PIXEL
    val y = (slot * TASK_HEIGHT) + yStart
    val width = (endTime - startTime)/MS_PER_PIXEL
    fileWriter.write(
      s"""<rect x="$x" y="$y" width="$width" height="$TASK_HEIGHT"
         | style="fill:$color;fill-opacity:1.0;stroke:#00ff00;stroke-width:1"/>
         |""".stripMargin)
    textBoxVirtCentered(text, x, y + TASK_HEIGHT/2, fileWriter)
  }

  private def scaleWithLines(x: Long,
      y: Long,
      minStart: Long,
      maxFinish: Long,
      height: Long,
      fileWriter: ToolTextFileWriter): Unit = {
    val timeRange = maxFinish - minStart
    val xEnd = x + timeRange/MS_PER_PIXEL
    val yEnd = y + height
    fileWriter.write(
      s"""<line x1="$x" y1="$yEnd" x2="$xEnd" y2="$yEnd" style="stroke:black;stroke-width:1"/>
         |<line x1="$x" y1="$y" x2="$xEnd" y2="$y" style="stroke:black;stroke-width:1"/>
         |""".stripMargin)
    (0L until timeRange).by(100L).foreach { timeForTick =>
      val xTick = timeForTick/MS_PER_PIXEL + x
      fileWriter.write(
        s"""<line x1="$xTick" y1="$y" x2="$xTick" y2="$yEnd"
           | style="stroke:black;stroke-width:1;opacity:0.5"/>
           |""".stripMargin)
      if (timeForTick % 1000 == 0) {
        fileWriter.write(
          s"""<line x1="$xTick" y1="$yEnd"
             | x2="$xTick" y2="${yEnd + PADDING}"
             | style="stroke:black;stroke-width:1"/>
             |<text x="$xTick" y="${yEnd + PADDING + FONT_SIZE}"
             |font-family="Courier,monospace" font-size="$FONT_SIZE">$timeForTick ms</text>
             |""".stripMargin)
      }
    }
  }

  private def calcTimingHeights(slots: Int): Int = slots * TASK_HEIGHT

  def generateFor(app: ApplicationInfo, outputDirectory: String): Unit = {
    // Gather the data
    val execHostToTaskList = new mutable.TreeMap[String, ArrayBuffer[TimelineTaskInfo]]()
    val stageIdToColor = mutable.HashMap[Int, String]()
    var colorIndex = 0
    var minStartTime = Long.MaxValue
    var maxEndTime = 0L

    app.taskEnd.foreach { tc =>
      val host = tc.host
      val execId = tc.executorId
      val stageId = tc.stageId
      val taskId = tc.taskId
      val launchTime = tc.launchTime
      val finishTime = tc.finishTime
      val duration = tc.duration
      val taskInfo = new TimelineTaskInfo(stageId, taskId, launchTime, finishTime, duration)
      val execHost = s"$execId/$host"
      execHostToTaskList.getOrElseUpdate(execHost, ArrayBuffer.empty) += taskInfo
      minStartTime = Math.min(launchTime, minStartTime)
      maxEndTime = Math.max(finishTime, maxEndTime)
      stageIdToColor.getOrElseUpdate(stageId, {
        val color = COLORS(colorIndex % COLORS.length)
        colorIndex += 1
        color
      })
    }

    val stageRangeInfo = execHostToTaskList.values.flatMap { taskList =>
      taskList
    }.groupBy { taskInfo =>
      taskInfo.stageId
    }.map {
      case (stageId, iter) =>
        val start = iter.map(_.startTime).min
        val end = iter.map(_.endTime).max
        new TimelineStageInfo(stageId, start, end, end-start)
    }

    val stageInfo = app.enhancedStage.map { sc =>
      val stageId = sc.stageId
      val submissionTime = sc.submissionTime.get
      val completionTime = sc.completionTime.get
      val duration = sc.duration.get
      minStartTime = Math.min(minStartTime, submissionTime)
      maxEndTime = Math.max(maxEndTime, completionTime)
      new TimelineStageInfo(stageId, submissionTime, completionTime, duration)
    }

    val execHostToSlots = execHostToTaskList.map {
      case (execHost, taskList) =>
        (execHost, calcLayoutSlotsNeeded(taskList))
    }.toMap

    val jobInfo = app.enhancedJob.map { jc =>
      val jobId = jc.jobID
      val startTime = jc.startTime
      val endTime = jc.endTime.get
      val duration = jc.duration.get
      minStartTime = Math.min(minStartTime, startTime)
      maxEndTime = Math.max(maxEndTime, endTime)
      new TimelineJobInfo(jobId, startTime, endTime, duration)
    }

    val sqlInfo = app.enhancedSql.flatMap { sc =>
      // If a SQL op fails, it may not have an end-time with it (So remove it from the graph)
      if (sc.endTime.isDefined) {
        val sqlId = sc.sqlID
        val startTime = sc.startTime
        val endTime = sc.endTime.get
        val duration = sc.duration.get
        minStartTime = Math.min(minStartTime, startTime)
        maxEndTime = Math.max(maxEndTime, endTime)
        Some(new TimelineSqlInfo(sqlId, startTime, endTime, duration))
      } else {
        None
      }
    }

    // Add 1 second for padding at the end...
    maxEndTime += 1000

    // Do the high level layout of what the output page should look like
    // TITLE
    // EXEC(s)      | TASK TIMING
    // STAGES       | STAGE TIMING (Scheduled Stage to completed Stage)
    // STAGE RANGES | STAGE RANGE TIMING (Start of first task to end of the last task in stage)
    // JOBS         | JOB TIMING
    // SQLS         | SQL TIMING
    val titleStartX = PADDING
    val titleStartY = 0
    val titleEndY = titleStartY + TITLE_HEIGHT

    // All of the timings start at the same place
    val titleBoxStartX = PADDING
    val titleBoxWidth = TITLE_BOX_WIDTH
    val timingsStartX = titleBoxStartX + titleBoxWidth
    val timingsWidth = (maxEndTime - minStartTime)/MS_PER_PIXEL
    val timingsEndX = timingsStartX + timingsWidth

    // EXEC(s)
    val execsStartY = titleEndY
    val numExecTaskSlotsTotal = execHostToSlots.values.sum
    val execsHeight = calcTimingHeights(numExecTaskSlotsTotal)
    val execsWithFooterHeight = execsHeight + FOOTER_HEIGHT
    val execsEndY = execsStartY + execsWithFooterHeight

    // STAGES
    val stagesStartY = execsEndY
    val numStageSlots = calcLayoutSlotsNeeded(stageInfo)
    val stagesHeight = calcTimingHeights(numStageSlots)
    val stagesWithFooterHeight = stagesHeight + FOOTER_HEIGHT
    val stagesEndY = stagesStartY + stagesWithFooterHeight

    // STAGE RANGES
    val stageRangesStartY = stagesEndY
    val numStageRangeSlots = calcLayoutSlotsNeeded(stageRangeInfo)
    val stageRangesHeight = calcTimingHeights(numStageRangeSlots)
    val stageRangesWithFooterHeight = stageRangesHeight + FOOTER_HEIGHT
    val stageRangesEndY = stageRangesStartY + stageRangesWithFooterHeight

    // JOBS
    val jobsStartY = stageRangesEndY
    val numJobsSlots = calcLayoutSlotsNeeded(jobInfo)
    val jobsHeight = calcTimingHeights(numJobsSlots)
    val jobsWithFooterHeight = jobsHeight + FOOTER_HEIGHT
    val jobsEndY = jobsStartY + jobsWithFooterHeight

    // SQLS
    val sqlsStartY = jobsEndY
    val numSqlsSlots = calcLayoutSlotsNeeded(sqlInfo)
    val sqlsHeight = calcTimingHeights(numSqlsSlots)
    val sqlsWithFooterHeight = sqlsHeight + FOOTER_HEIGHT
    val sqlsEndY = sqlsStartY + sqlsWithFooterHeight

    // TOTAL IMAGE
    val imageHeight = sqlsEndY + PADDING
    val imageWidth = timingsEndX

    val fileWriter = new ToolTextFileWriter(outputDirectory,
      s"${app.appId}-timeline.svg")
    try {
      fileWriter.write(
        s"""<?xml version="1.0" encoding="UTF-8" standalone="no"?>
           |<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
           | "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
           |<!-- Generated by Rapids Accelerator For Apache Spark Profiling Tool -->
           |<svg width="$imageWidth" height="$imageHeight"
           | xmlns="http://www.w3.org/2000/svg">
           | <title>${app.appId} Timeline</title>
           |""".stripMargin)
      // TITLE
      textBoxVirtCentered(s"${app.appId} Timeline",
        titleStartX,
        titleStartY + TITLE_HEIGHT/2,
        fileWriter)

      // EXEC(s)
      var currentExecsStartY = execsStartY
      execHostToTaskList.foreach {
        case (execHost, taskList) =>
          val numElements = execHostToSlots(execHost)
          val execHostHeight = calcTimingHeights(numElements)

          sectionBox(execHost, currentExecsStartY, numElements, fileWriter)
          doLayout(taskList, numElements) {
            case (taskInfo, slot) =>
              timingBox(s"${taskInfo.duration} ms",
                stageIdToColor(taskInfo.stageId),
                taskInfo,
                slot,
                timingsStartX,
                currentExecsStartY,
                minStartTime,
                fileWriter)
          }
          currentExecsStartY += execHostHeight

          // Add a line to show different executors from each other
          fileWriter.write(
            s"""<line x1="$timingsStartX" y1="$currentExecsStartY"
               |  x2="$timingsEndX" y2="$currentExecsStartY" style="stroke:black;stroke-width:1"/>
               |""".stripMargin)
      }

      scaleWithLines(timingsStartX,
        execsStartY,
        minStartTime,
        maxEndTime,
        execsHeight,
        fileWriter)

      // STAGES
      sectionBox("STAGES", stagesStartY, numStageSlots, fileWriter)

      doLayout(stageInfo, numStageSlots) {
        case (si, slot) =>
          timingBox(s"STAGE ${si.stageId} ${si.duration} ms",
            stageIdToColor(si.stageId),
            si,
            slot,
            timingsStartX,
            stagesStartY,
            minStartTime,
            fileWriter)
      }

      scaleWithLines(timingsStartX,
        stagesStartY,
        minStartTime,
        maxEndTime,
        stagesHeight,
        fileWriter)

      // STAGE RANGES
      sectionBox("STAGE RANGES", stageRangesStartY, numStageRangeSlots, fileWriter)

      doLayout(stageRangeInfo, numStageRangeSlots) {
        case (si, slot) =>
          timingBox(s"STAGE RANGE ${si.stageId} ${si.duration} ms",
            stageIdToColor(si.stageId),
            si,
            slot,
            timingsStartX,
            stageRangesStartY,
            minStartTime,
            fileWriter)
      }

      scaleWithLines(timingsStartX,
        stageRangesStartY,
        minStartTime,
        maxEndTime,
        stageRangesHeight,
        fileWriter)

      // JOBS
      sectionBox("JOBS", jobsStartY, numJobsSlots, fileWriter)

      doLayout(jobInfo, numJobsSlots) {
        case (ji, slot) =>
          timingBox(s"JOB ${ji.jobId} ${ji.duration} ms",
            "green",
            ji,
            slot,
            timingsStartX,
            jobsStartY,
            minStartTime,
            fileWriter)
      }
      scaleWithLines(timingsStartX,
        jobsStartY,
        minStartTime,
        maxEndTime,
        jobsHeight,
        fileWriter)

      // SQLS
      sectionBox("SQL", sqlsStartY, numSqlsSlots, fileWriter)

      doLayout(sqlInfo, numSqlsSlots) {
        case (sql, slot) =>
          timingBox(s"SQL ${sql.sqlId} ${sql.duration} ms",
            "blue",
            sql,
            slot,
            timingsStartX,
            sqlsStartY,
            minStartTime,
            fileWriter)
      }
      scaleWithLines(timingsStartX,
        sqlsStartY,
        minStartTime,
        maxEndTime,
        sqlsHeight,
        fileWriter)

      fileWriter.write(s"""</svg>""")
    } finally {
      fileWriter.close()
    }
  }
}
