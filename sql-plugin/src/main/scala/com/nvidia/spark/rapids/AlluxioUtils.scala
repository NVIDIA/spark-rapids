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

import java.io.FileNotFoundException

import scala.io.Source
import scala.sys.process.{Process, ProcessLogger}

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.rapids.GpuPartitioningUtils

object AlluxioUtils extends Logging {
  val mountedBuckets: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
  var alluxioCmd: Seq[String] = null
  var alluxioMasterHost: Option[String] = None
  var alluxio_home: String = "/opt/alluxio-2.8.0"
  var isInit: Boolean = false

  // Read out alluxio.master.hostname, alluxio.master.rpc.port
  // from Alluxio's conf alluxio-site.properties
  // We require an environment variable "ALLUXIO_HOME"
  // This function will only read once from ALLUXIO/conf.
  private def initAlluxioInfo(conf: RapidsConf): Unit = {
    alluxio_home = scala.util.Properties.envOrElse("ALLUXIO_HOME", "/opt/alluxio-2.8.0")
    alluxioCmd = conf.getAlluxioCmd.getOrElse(
      Seq("su", "ubuntu", "-c", s"$alluxio_home/bin/alluxio"))
    this.synchronized {
      if (!isInit) {
        // Default to read from /opt/alluxio-2.8.0 if not setting ALLUXIO_HOME
        var alluxio_port: String = "19998"
        var alluxio_master: String = null
        var buffered_source: Source = null
        try {
          buffered_source = Source.fromFile(alluxio_home + "/conf/alluxio-site.properties")
          for (line <- buffered_source.getLines) {
            if (line.startsWith("alluxio.master.hostname")) {
              alluxio_master = line.split('=')(1).trim
            } else if (line.startsWith("alluxio.master.rpc.port")) {
              alluxio_port = line.split('=')(1).trim
            }
          }
        } catch {
          case e: FileNotFoundException =>
            throw new RuntimeException(s"Not found Alluxio config in " +
              s"$alluxio_home/conf/alluxio-site.properties, " +
              "please check if ALLUXIO_HOME is set correctly")
        } finally {
          if (buffered_source != null) buffered_source.close
        }

        if (alluxio_master == null) {
          throw new RuntimeException(
            s"Can't find alluxio.master.hostname from $alluxio_home/conf/alluxio-site.properties.")
        }
        alluxioMasterHost = Some(alluxio_master + ":" + alluxio_port)
        // load mounted point by call Alluxio mount command.
        // We also can get from REST API http://alluxio_master:alluxio_web_port/api/v1/master/info.
        val (ret, output) = runAlluxioCmd(" fs mount")
        if (ret == 0) {
          // parse the output, E.g.
          // s3a://bucket-foo/        on  /bucket-foo
          // s3a://bucket-another/    on  /bucket-another
          // /local_path              on  /
          for (line <- output) {
            val items = line.split(" ")
            logInfo(line)
            // We only support s3 remote path for now,
            // need to change below if we want to support other type of cloud storage
            if (items.length >= 3 && items(0).startsWith("s3") && !items(2).equals("/")) {
              val bucket = items(2).substring(1)
              val remote_path = items(0).substring(0, items(0).length-1)
              mountedBuckets(bucket) = remote_path
              logInfo(s"Found mounted bucket $remote_path to /$bucket")
            }
          }
        }
        isInit = true
      }
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

  private def runAlluxioCmd(param : String) : (Int,
    scala.collection.mutable.ArrayBuffer[String]) = {
    val params = alluxioCmd.tails.collect {
        case Seq(first, _, _*) => first
        case Seq(last) => last + param
      }.toSeq
    val id = Thread.currentThread().getId()
    logInfo(s"Run command ${params.last} in thread $id")
    val out : scala.collection.mutable.ArrayBuffer[String] =
      new scala.collection.mutable.ArrayBuffer[String](10)
    val ret = Process(params).!(ProcessLogger(out += _, _ => Unit))
    (ret, out)
  }

  // path is like "s3://foo/test...", it mounts bucket "foo" by calling the alluxio CLI
  // And we'll append --option to set access_key and secret_key if existing.
  // Suppose the key doesn't exist when using like Databricks's instance profile
  private def autoMountBucket(scheme: String, bucket: String,
                      access_key: Option[String],
                      secret_key: Option[String]): Unit = {
    val remote_path = scheme + "//" + bucket
    if (!mountedBuckets.contains(bucket)) {
      // not mount yet, call mount command
      val parameter = if (access_key.isEmpty) {
          s" fs mount --readonly /$bucket $remote_path"
        } else {
          s" fs mount --readonly --option s3a.accessKeyId=${access_key.get} " +
            s"--option s3a.secretKey=${secret_key.get} /$bucket $remote_path"
        }
      val (output, _) = runAlluxioCmd(parameter)
      if (output != 0) {
        throw new RuntimeException(s"Mount bucket $bucket failed $output")
      }
      logInfo(s"Mounted remote $remote_path to /$bucket in Alluxio $output")
      mountedBuckets(bucket) = remote_path
    } else if (mountedBuckets(bucket).equals(remote_path)) {
      logInfo(s"Already mounted remote $remote_path to /$bucket in Alluxio")
    } else {
      throw new RuntimeException(s"Found a same bucket name in $remote_path " +
        s"and ${mountedBuckets(bucket)}")
    }
  }

  // first try to get fs.s3a.access.key from spark config
  // second try to get from environment variables
  private def getKeyAndSecret(relation: HadoopFsRelation) : (Option[String], Option[String]) = {
    val hadoopAccessKey =
      relation.sparkSession.sparkContext.hadoopConfiguration.get("fs.s3a.access.key")
    val hadoopSecretKey =
      relation.sparkSession.sparkContext.hadoopConfiguration.get("fs.s3a.secret.key")
    if (hadoopAccessKey != null && hadoopSecretKey != null) {
      (Some(hadoopAccessKey), Some(hadoopSecretKey))
    } else {
      val accessKey = relation.sparkSession.conf.getOption("spark.hadoop.fs.s3a.access.key")
      val secretKey = relation.sparkSession.conf.getOption("spark.hadoop.fs.s3a.secret.key")
      if (accessKey.isDefined && secretKey.isDefined) {
        (accessKey, secretKey)
      } else {
        val envAccessKey = scala.util.Properties.envOrNone("AWS_ACCESS_KEY_ID")
        val envSecretKey = scala.util.Properties.envOrNone("AWS_ACCESS_SECRET_KEY")
        (envAccessKey, envSecretKey)
      }
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

    val replaceFunc = if (alluxioPathsReplace.isDefined) {
      // alluxioPathsReplace: Seq("key->value", "key1->value1")
      // turn the rules to the Map with eg
      // { s3://foo -> alluxio://0.1.2.3:19998/foo,
      //   gs://bar -> alluxio://0.1.2.3:19998/bar }
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
    } else if (alluxioAutoMountEnabled) {
      Some((f: Path) => {
        val pathStr = f.toString
        if (pathStr.matches(alluxioBucketRegex)) {
          initAlluxioInfo(conf)
          val (access_key, secret_key) = getKeyAndSecret(relation)

          val (scheme, bucket) = getSchemeAndBucketFromPath(pathStr)
          autoMountBucket(scheme, bucket, access_key, secret_key)

          // replace s3://foo/.. to alluxio://alluxioMasterHost/foo/...
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
