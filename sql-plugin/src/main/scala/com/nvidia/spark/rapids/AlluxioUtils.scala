/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import scala.io.Source
import scala.sys.process.Process

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.rapids.GpuPartitioningUtils

object AlluxioUtils extends Logging {
  val mountedBuckets: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
  var alluxioMountCmd: Option[Seq[String]] = None
  var alluxioMasterHost: Option[String] = None

  // Read out alluxio.master.hostname, alluxio.master.rpc.port
  // from Alluxio's conf alluxio-site.properties
  // We require an environment variable "ALLUXIO_HOME"
  // This function will only read once from ALLUXIO/conf.
  private def getAlluxioMasterHost() : Unit = {
    if (alluxioMasterHost.isEmpty) {
      var host = ""
      var port = "19998"
      val alluxio_home = scala.util.Properties.envOrNone("ALLUXIO_HOME")
      if (alluxio_home.isEmpty) {
        throw new RuntimeException("No environment variable ALLUXIO_HOME is set.")
      }
      val buffered_source = Source.fromFile(alluxio_home.get + "/conf/alluxio-site.properties")
      try {
        for (line <- buffered_source.getLines) {
          if (line.startsWith("alluxio.master.hostname")) {
            host = line.split('=')(1).trim
          } else if (line.startsWith("alluxio.master.rpc.port")) {
            port = line.split('=')(1).trim
          }
        }
      } finally {
        buffered_source.close
      }

      if (host.isEmpty) {
        throw new RuntimeException(
          "Can't find alluxio.master.hostname from ALLUXIO_HOME/conf/alluxio-site.properties.")
      }
      alluxioMasterHost = Some(host + ":" + port)
    }
  }

  private def getSchemeAndBucketFromPath(path: String) : (String, String) = {
    val i = path.split("//")
    val scheme = i(0)
    if (i.length <= 1) {
      throw new RuntimeException(s"path $path is not expected for Alluxio auto mount")
    }
    val bucket = i(1).split("/")(0)
    (scheme, bucket)
  }

  // path is like "s3://foo/test...", it mounts bucket "foo" by calling the alluxio CLI
  def autoMountBucket(scheme: String, bucket: String): Unit = {
    val remote_path = scheme + "//" + bucket
    if (!mountedBuckets.contains(bucket)) {
      // not mount yet, call mount command
      val command : Seq[String] = if (alluxioMountCmd.isDefined) {
        alluxioMountCmd.get
      } else {
        Seq("su", "ubuntu", "-c", "/opt/alluxio-2.8.0/bin/alluxio fs mount --readonly")
      }

      val params = command.tails.collect{
        case Seq(first, _, _*) => first
        case Seq(last) => last + s" /$bucket $remote_path"
      }.toSeq
      logInfo(s"Run command $params")
      val output = Process(params).!
      if (output != 0) {
        throw new RuntimeException(s"Mount bucket $bucket failed $output")
      }
      logInfo(s"Mounted remote $remote_path to /$bucket in Alluxio")
      mountedBuckets(bucket) = remote_path
    } else if (mountedBuckets(bucket).equals(remote_path)) {
      logInfo(s"Already mounted remote $remote_path to /$bucket in Alluxio")
    } else {
      throw new RuntimeException(s"Found a same bucket name in $remote_path " +
        s"and ${mountedBuckets(bucket)}")
    }
  }

  def replacePathIfNeeded(
      conf: RapidsConf,
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileIndex = {

    val alluxioPathsReplace: Option[Seq[String]] = conf.getAlluxioPathsToReplace
    val alluxioAutoMountEnabled = conf.getAlluxioAutoMountEnabled
    val alluxioBucketRegex: String = conf.getAlluxioBucketRegex
    alluxioMountCmd = conf.getAlluxioMountCmd

    val replaceFunc = if (alluxioPathsReplace.isDefined) {
      // alluxioPathsReplace: Seq("key->value", "key1->value1")
      // turn the rules to the Map with eg
      // { s3://foo -> alluxio://0.1.2.3:19998/foo,
      //   gs://bar -> alluxio://0.1.2.3:19998/bar,
      //   s3://baz -> alluxio://0.1.2.3:19998/baz }
      val replaceMapOption = alluxioPathsReplace.map(rules => {
        rules.map(rule => {
          val split = rule.split("->")
          if (split.size == 2) {
            split(0).trim -> split(1).trim
          } else {
            throw new IllegalArgumentException(s"Invalid setting for " +
              s"${RapidsConf.ALLUXIO_PATHS_REPLACE.key}")
          }
        }).toMap
      })
      if (replaceMapOption.isDefined) {
        Some((f: Path) => {
          val pathStr = f.toString
          val matchedSet = replaceMapOption.get.filter(a => pathStr.startsWith(a._1))
          if (matchedSet.size > 1) {
            // never reach here since replaceMap is a Map
            throw new IllegalArgumentException(s"Found ${matchedSet.size} same replacing rules " +
              s"from ${RapidsConf.ALLUXIO_PATHS_REPLACE.key} which requires only 1 rule " +
              s"for each file path")
          } else if (matchedSet.size == 1) {
            new Path(pathStr.replaceFirst(matchedSet.head._1, matchedSet.head._2))
          } else {
            f
          }
        })
      } else {
        None
      }
    } else if (alluxioAutoMountEnabled) { // alluxio master host is set
      Some((f: Path) => {
        val pathStr = f.toString
        if (pathStr.matches(alluxioBucketRegex)) {
          getAlluxioMasterHost()

          val (scheme, bucket) = getSchemeAndBucketFromPath(pathStr)
          autoMountBucket(scheme, bucket)

          // replace s3:/foo/.. to alluxio://alluxioMasterHost/foo/...
          val newPath = new Path(pathStr.replaceFirst(
            scheme + "/", "alluxio://" + alluxioMasterHost.get))
          logInfo(s"Replace $pathStr to ${newPath.toString}")
          newPath
        } else {
          f
        }
      })
    } else {
      None
    }

    if (replaceFunc.isDefined) {
      def isDynamicPruningFilter(e: Expression): Boolean =
        e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

      val partitionDirs = relation.location.listFiles(
        partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)

      // replace all of input files
      val inputFiles: Seq[Path] = partitionDirs.flatMap(partitionDir => {
        partitionDir.files.map(f => replaceFunc.get(f.getPath))
      })

      // replace all of rootPaths which are already unique
      val rootPaths = relation.location.rootPaths.map(replaceFunc.get)

      val parameters: Map[String, String] = relation.options

      // infer PartitionSpec
      val partitionSpec = GpuPartitioningUtils.inferPartitioning(
        relation.sparkSession,
        rootPaths,
        inputFiles,
        parameters,
        Option(relation.dataSchema),
        replaceFunc.get)

      // generate a new InMemoryFileIndex holding paths with alluxio schema
      new InMemoryFileIndex(
        relation.sparkSession,
        inputFiles,
        parameters,
        Option(relation.dataSchema),
        userSpecifiedPartitionSpec = Some(partitionSpec))
    } else {
      relation.location
    }
  }
}
