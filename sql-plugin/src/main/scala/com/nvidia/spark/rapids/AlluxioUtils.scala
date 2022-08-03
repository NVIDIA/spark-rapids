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
import java.util.Properties

import scala.io.{BufferedSource, Source}
import scala.sys.process.{Process, ProcessLogger}

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FileIndex, HadoopFsRelation, InMemoryFileIndex, PartitioningAwareFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.rapids.GpuPartitioningUtils

object AlluxioUtils extends Logging {
  private val checkedAlluxioPath = scala.collection.mutable.HashSet[String]()

  private def checkAlluxioMounted(sparkSession: SparkSession, alluxio_path: String): Unit = {
    this.synchronized {
      if (!checkedAlluxioPath.contains(alluxio_path)) {
        val path = new Path(alluxio_path)
        val fs = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
        if (!fs.exists(path)) {
          throw new FileNotFoundException(
            s"Alluxio path $alluxio_path does not exist, maybe forgot to mount it")
        }
        logInfo(s"Alluxio path $alluxio_path is mounted")
        checkedAlluxioPath.add(alluxio_path)
      } else {
        logDebug(s"Alluxio path $alluxio_path already mounted")
      }
    }
  }

  private val mountedBuckets: scala.collection.mutable.Map[String, String] =
    scala.collection.mutable.Map()
  private var alluxioCmd: Seq[String] = null
  private var alluxioMasterHost: Option[String] = None
  private var alluxioHome: String = "/opt/alluxio-2.8.0"
  private var isInit: Boolean = false

  // Read out alluxio.master.hostname, alluxio.master.rpc.port
  // from Alluxio's conf alluxio-site.properties
  // We require an environment variable "ALLUXIO_HOME"
  // This function will only read once from ALLUXIO/conf.
  private def initAlluxioInfo(conf: RapidsConf): Unit = {
    this.synchronized {
      alluxioHome = scala.util.Properties.envOrElse("ALLUXIO_HOME", "/opt/alluxio-2.8.0")
      alluxioCmd = conf.getAlluxioCmd

      if (!isInit) {
        // Default to read from /opt/alluxio-2.8.0 if not setting ALLUXIO_HOME
        var alluxio_port: String = null
        var alluxio_master: String = null
        var buffered_source: BufferedSource = null
        try {
          buffered_source = Source.fromFile(alluxioHome + "/conf/alluxio-site.properties")
          val prop : Properties = new Properties()
          prop.load(buffered_source.bufferedReader())
          alluxio_master = prop.getProperty("alluxio.master.hostname")
          alluxio_port = prop.getProperty("alluxio.master.rpc.port", "19998")
        } catch {
          case e: FileNotFoundException =>
            throw new RuntimeException(s"Not found Alluxio config in " +
              s"$alluxioHome/conf/alluxio-site.properties, " +
              "please check if ALLUXIO_HOME is set correctly")
        } finally {
          if (buffered_source != null) buffered_source.close
        }

        if (alluxio_master == null) {
          throw new RuntimeException(
            s"Can't find alluxio.master.hostname from $alluxioHome/conf/alluxio-site.properties.")
        }
        alluxioMasterHost = Some(alluxio_master + ":" + alluxio_port)
        // load mounted point by call Alluxio mount command.
        val (ret, output) = runAlluxioCmd("fs mount")
        if (ret == 0) {
          // parse the output, E.g.
          // s3a://bucket-foo/        on  /bucket-foo
          // s3a://bucket-another/    on  /bucket-another
          // /local_path              on  /
          for (line <- output) {
            val items = line.trim.split(" +")
            logDebug(line)
            if (items.length >= 3) {
              // if the first item contains the "://", it means it's a remote path.
              // record it as a mounted point
              if (items(0).contains("://")) {
                mountedBuckets(items(2)) = items(0)
                logInfo(s"Found mounted bucket ${items(0)} to ${items(2)}")
              }
            }
          }
        } else {
          logWarning(s"Failed to run alluxio fs mount $ret")
        }
        isInit = true
      }
    }
  }

  // The path should be like s3://bucket/... or s3a://bucket/...
  private def getSchemeAndBucketFromPath(path: String) : (String, String) = {
    val i = path.split("://")
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
        case Seq(last) => last + " " + param
      }.toSeq
    val out : scala.collection.mutable.ArrayBuffer[String] =
      new scala.collection.mutable.ArrayBuffer[String](10)
    val ret = if (params.length == 1) {
      // if alluxioCmd is like "alluxio fs mount", run by Process(String)
      Process(params.last).!(ProcessLogger(out += _, _ => Unit))
    } else {
      // if alluxioCmd has multiple strings in the seq like "su ubuntu -c 'alluxio fs mount'",
      // need to run by Process(Seq[String])
      Process(params).!(ProcessLogger(out += _, _ => Unit))
    }
    (ret, out)
  }

  // path is like "s3://foo/test...", it mounts bucket "foo" by calling the alluxio CLI
  // And we'll append --option to set access_key and secret_key if existing.
  // Suppose the key doesn't exist when using like Databricks's instance profile
  private def autoMountBucket(scheme: String, bucket: String,
                                access_key: Option[String],
                                secret_key: Option[String]): Unit = {
    // to match the output of alluxio fs mount, append / to remote_path
    // and add / before bucket name for absolute path in Alluxio
    val remote_path = scheme + "://" + bucket + "/"
    val local_bucket = "/" + bucket
    this.synchronized {
      if (!mountedBuckets.contains(local_bucket)) {
        // not mount yet, call mount command
        // we only support s3 or s3a bucket for now.
        // To support other cloud storage, we need to support credential parameters for the others
        val parameter = "fs mount --readonly " +(
          if (access_key.isDefined && secret_key.isDefined) {
            s"--option s3a.accessKeyId=${access_key.get} " +
            s"--option s3a.secretKey=${secret_key.get} " } else "") +
          s"$local_bucket $remote_path"

        val (output, _) = runAlluxioCmd(parameter)
        if (output != 0) {
          throw new RuntimeException(s"Mount bucket $remote_path to $local_bucket failed $output")
        }
        logInfo(s"Mounted bucket $remote_path to $local_bucket in Alluxio $output")
        mountedBuckets(local_bucket) = remote_path
      } else if (mountedBuckets(local_bucket).equals(remote_path)) {
        logInfo(s"Already mounted bucket $remote_path to $local_bucket in Alluxio")
      } else {
        throw new RuntimeException(s"Found a same bucket name in $remote_path " +
          s"and ${mountedBuckets(local_bucket)}")
      }
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

  private def genFuncForPathReplacement(replaceMapOption: Option[Map[String, String]]
                                       ) : Option[Path => Path] = {
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
  }

  private def genFuncForAutoMountReplacement(conf: RapidsConf, relation: HadoopFsRelation,
                                             alluxioBucketRegex: String) : Option[Path => Path] = {
    Some((f: Path) => {
      val pathStr = f.toString
      if (pathStr.matches(alluxioBucketRegex)) {
        initAlluxioInfo(conf)
        val (access_key, secret_key) = getKeyAndSecret(relation)

        val (scheme, bucket) = getSchemeAndBucketFromPath(pathStr)
        autoMountBucket(scheme, bucket, access_key, secret_key)

        // replace s3://foo/.. to alluxio://alluxioMasterHost/foo/...
        val newPath = new Path(pathStr.replaceFirst(
          scheme + ":/", "alluxio://" + alluxioMasterHost.get))
        logInfo(s"Replace $pathStr to ${newPath.toString}")
        newPath
      } else {
        f
      }
    })
  }

  def replacePathIfNeeded(
      conf: RapidsConf,
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileIndex = {

    val alluxioPathsReplace: Option[Seq[String]] = conf.getAlluxioPathsToReplace
    val alluxioAutoMountEnabled = conf.getAlluxioAutoMountEnabled
    val alluxioBucketRegex: String = conf.getAlluxioBucketRegex

    // alluxioPathsReplace: Seq("key->value", "key1->value1")
    // turn the rules to the Map with eg
    // { s3://foo -> alluxio://0.1.2.3:19998/foo,
    //   gs://bar -> alluxio://0.1.2.3:19998/bar }
    val replaceMapOption = if (alluxioPathsReplace.isDefined) {
      alluxioPathsReplace.map(rules => {
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
    } else {
      None
    }

    val replaceFunc = if (replaceMapOption.isDefined) {
      genFuncForPathReplacement(replaceMapOption)
    } else if (alluxioAutoMountEnabled) {
      genFuncForAutoMountReplacement(conf, relation, alluxioBucketRegex)
    } else {
      None
    }

    if (replaceFunc.isDefined) {
      def replacePathsInPartitionSpec(spec: PartitionSpec): PartitionSpec = {
        val partitionsWithPathsReplaced = spec.partitions.map { p =>
          val replacedPath = replaceFunc.get(p.path)
          org.apache.spark.sql.execution.datasources.PartitionPath(p.values, replacedPath)
        }
        PartitionSpec(spec.partitionColumns, partitionsWithPathsReplaced)
      }

      def createNewFileIndexWithPathsReplaced(
          spec: PartitionSpec,
          rootPaths: Seq[Path]): InMemoryFileIndex = {
        val specAdjusted = replacePathsInPartitionSpec(spec)
        val replacedPaths = rootPaths.map(replaceFunc.get)
        new InMemoryFileIndex(
          relation.sparkSession,
          replacedPaths,
          relation.options,
          Option(relation.dataSchema),
          userSpecifiedPartitionSpec = Some(specAdjusted))
      }

      // If we know the type of file index, try to reuse as much of the existing
      // FileIndex as we can to keep from having to recalculate it and potentially
      // mess it up. Things like partitioning are already included and some of the
      // Spark infer code didn't handle certain types properly. Here we try to just
      // update the paths to the new Alluxio path. If its not a type of file index
      // we know then fall back to inferring. The latter happens on certain CSPs
      // like Databricks where they have customer file index types.
      relation.location match {
        case pfi: PartitioningAwareFileIndex =>
          logDebug("Handling PartitioningAwareFileIndex")
          createNewFileIndexWithPathsReplaced(pfi.partitionSpec(), pfi.rootPaths)
        case cfi: CatalogFileIndex =>
          logDebug("Handling CatalogFileIndex")
          val memFI = cfi.filterPartitions(Nil)
          createNewFileIndexWithPathsReplaced(memFI.partitionSpec(), memFI.rootPaths)
        case _ => {
          logDebug(s"Handling file index type: ${relation.location.getClass}")

          // With the base Spark FileIndex type we don't know how to modify it to
          // just replace the paths so we have to try to recompute.
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

          // check the alluxio paths in root paths exist or not
          // throw out an exception to stop the job when any of them is not mounted
          if (replaceMapOption.isDefined) {
            rootPaths.foreach { rootPath =>
              replaceMapOption.get.values.find(value => rootPath.toString.startsWith(value)).
                foreach(matched => checkAlluxioMounted(relation.sparkSession, matched))
            }
          }

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
        }
      }
    } else {
      relation.location
    }
  }
}
