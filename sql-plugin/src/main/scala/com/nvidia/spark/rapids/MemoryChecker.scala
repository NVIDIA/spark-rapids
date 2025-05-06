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

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}

import scala.io.Source
import scala.util.Try

import com.nvidia.spark.rapids.Arm.withResource

object MemoryChecker {
  def main(args: Array[String]): Unit = {
    println(s"Available memory: ${getAvailableMemoryBytes} bytes")
  }

  def getAvailableMemoryBytes: Long = {
    if (isSlurmContainer) {
      getSlurmMemory
    } else if (isDockerContainer) {
      getCgroupMemory.getOrElse(getHostMemory)
    } else {
      getHostMemory
    }
  }

  private def isDockerContainer: Boolean = {
    Files.exists(Paths.get("/.dockerenv")) ||
      withResource(Source.fromFile("/proc/1/cgroup")) {
        source => source.getLines().exists(line =>
        line.contains("docker") || line.contains("kubepods"))
      }
  }

  private def isSlurmContainer: Boolean = {
    System.getenv("SLURM_JOB_ID") != null
  }

  private def getVmRSS: Option[Int] = {
    val pid = ManagementFactory.getRuntimeMXBean.getName.split("@").head.toInt
    val statusFile = s"/proc/$pid/status"

    withResource(Source.fromFile(statusFile)) { source =>
      source.getLines()
        .find(_.startsWith("VmRSS:"))
        .flatMap { line =>
          line.split("\\s+") match {
            case Array(_, value, _*) => Try(value.toInt).toOption
            case _ => None
          }
        }
    }
  }

  private def getSlurmMemory: Long = {
    val free = Option(System.getenv("SLURM_MEM_PER_NODE"))
      .map(_.toLong * 1024 * 1024) // Convert MB to bytes
      .getOrElse(getHostMemory)
    val used = getVmRSS.getOrElse(0)

    free - used
  }

  private def getCgroupMemory: Option[Long] = {
    val (limitPath, usagePath) = getCgroupPaths
    for {
      limitStr <- readFile(limitPath)
      usageStr <- readFile(usagePath)
      if limitStr != "max"
      limit = limitStr.toLong
      usage = usageStr.toLong
    } yield limit - usage
  }

  private def getCgroupPaths: (String, String) = {
    val v2Limit = "/sys/fs/cgroup/memory.max"
    if (Files.exists(Paths.get(v2Limit))) {
      (v2Limit, "/sys/fs/cgroup/memory.current")
    } else {
      ("/sys/fs/cgroup/memory/memory.limit_in_bytes",
        "/sys/fs/cgroup/memory/memory.usage_in_bytes")
    }
  }

  private def getHostMemory: Long = {
    withResource(Source.fromFile("/proc/meminfo")) { source =>
      source.getLines()
      .collectFirst {
        case line if line.startsWith("MemAvailable:") =>
          line.split("\\s+")(1).toLong * 1024 // KB to bytes
      }
      .getOrElse(throw new RuntimeException("Couldn't read host memory"))
    }
  }

  private def readFile(path: String): Option[String] = {
    try {
      Some(
        withResource(Source.fromFile(path)) { source =>
          source.mkString.trim
        }
      )
    } catch {
      case _: Exception => None
    }
  }
}
