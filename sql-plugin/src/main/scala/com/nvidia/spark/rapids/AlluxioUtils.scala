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

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.io.BufferedSource
import scala.util.control.NonFatal

import alluxio.AlluxioURI
import alluxio.conf.{AlluxioProperties, InstancedConfiguration, PropertyKey}
import alluxio.grpc.MountPOptions
import alluxio.wire.MountPointInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}

/*
 * Utilities for using Alluxio with the plugin for reading.
 * Currently we only support Alluxio with the Datasource v1 Parquet reader.
 * We replaces the file path as late as possible on the task side when
 * we actually go to read the file. This makes is so that the original non-Alluxio
 * path gets reported for the input_file_name properly without having to convert
 * paths back to the original. This also has the benefit that it can be more performant
 * if it doesn't have to do the extra list leaf files, but you don't get the
 * locality information updated. So for small Alluxio clusters or with Spark
 * clusters short on task slots this may be a better fit.
 *
 * The way we do the actual replacement algorithm differs depending on the file reader
 * type we use: PERFILE, COALESCING or MULTITHREADED.
 * PERFILE is not supported with Alluxio due to not easily being able to fix up
 * input_file_name. We could but would require copying the FileScanRDD so skip for now.
 * The COALESCING reader is not support when input_file_name is requested so it falls
 * back to the MULTITHREADED reader if that is used, when input_file_name is not requested,
 * we replace the paths properly based on the replacement algorithm and don't have to worry
 * about calculating the original path.
 * In order to do the replacement at task time and to output the original path for convert
 * time, we need to have a mapping of the original scheme to the alluxio scheme. This has been
 * made a parameter to many of the readers. With auto mount and task time replacement,
 * we make a pass through the files on the driver side in GpuFileSourceScanExec in order to
 * do the mounting before the tasks try to access alluxio.
 * Note that Delta Lake uses the input_file_name functionality to do things like
 * Updates and Deletes and will fail if the path has the alluxio:// in it.
 *
 * Below we support 2 configs to turn on Alluxio, we have the automount which uses a regex
 * to replace paths and then we have the config that specifies direct paths to replace and
 * user has to manually mount those.
 */
object AlluxioUtils extends Logging with Arm {
  private val ALLUXIO_SCHEME = "alluxio://"
  private val mountedBuckets: scala.collection.mutable.Map[String, String] =
    scala.collection.mutable.Map()
  private var alluxioMasterHost: Option[String] = None
  private var alluxioMasterPort: Option[Int] = None
  private var alluxioMasterHostAndPort: Option[String] = None
  private var alluxioPathsToReplaceMap: Option[Map[String, String]] = None
  private var alluxioHome: String = "/opt/alluxio-2.8.0"
  private var alluxioBucketRegex: Option[String] = None
  private var isInitReplaceMap: Boolean = false
  private var isInitMountPointsForAutoMount: Boolean = false

  // Default to read from /opt/alluxio-2.8.0 if not setting ALLUXIO_HOME
  private def readAlluxioMasterAndPort: (String, String) = {
    var buffered_source: BufferedSource = null
    try {
      buffered_source = scala.io.Source.fromFile(alluxioHome + "/conf/alluxio-site.properties")
      val prop : Properties = new Properties()
      prop.load(buffered_source.bufferedReader())
      val alluxio_master = prop.getProperty("alluxio.master.hostname")
      val alluxio_port = prop.getProperty("alluxio.master.rpc.port", "19998")
      (alluxio_master, alluxio_port)
    } catch {
      case _: FileNotFoundException =>
        throw new RuntimeException(s"Not found Alluxio config in " +
          s"$alluxioHome/conf/alluxio-site.properties, " +
          "please check if ALLUXIO_HOME is set correctly")
    } finally {
      if (buffered_source != null) buffered_source.close
    }
  }

  // Read out alluxio.master.hostname, alluxio.master.rpc.port
  // from Alluxio's conf alluxio-site.properties
  // We require an environment variable "ALLUXIO_HOME"
  // This function will only read once from ALLUXIO/conf.
  private def initAlluxioInfo(conf: RapidsConf, hadoopConf: Configuration,
      runtimeConf: RuntimeConfig): Unit = {
    this.synchronized {
      // left outside isInit to allow changing at runtime
      alluxioHome = scala.util.Properties.envOrElse("ALLUXIO_HOME", "/opt/alluxio-2.8.0")
      AlluxioCfgUtils.checkAlluxioNotSupported(conf)

      if (AlluxioCfgUtils.enabledReplacementMap(conf)) {
        // replace-map is enabled, if set this will invalid the auto-mount
        if (!isInitReplaceMap) {
          alluxioPathsToReplaceMap = getReplacementMapOption(conf)
          isInitReplaceMap = true
        }
      } else if (conf.getAlluxioAutoMountEnabled) {
        // auto-mount is enabled
        if (!isInitMountPointsForAutoMount) {
          val (alluxioMasterHostStr, alluxioMasterPortStr) = readAlluxioMasterAndPort
          if (alluxioMasterHostStr == null) {
            throw new RuntimeException(
              s"Can't find alluxio.master.hostname from $alluxioHome/conf/alluxio-site.properties.")
          }
          alluxioMasterHost = Some(alluxioMasterHostStr)
          alluxioMasterPort = Some(alluxioMasterPortStr.toInt)
          alluxioMasterHostAndPort = Some(alluxioMasterHostStr + ":" + alluxioMasterPortStr)
          alluxioBucketRegex = Some(conf.getAlluxioBucketRegex)
          // load mounted point by call Alluxio client.
          try {
            val (access_key, secret_key) = getKeyAndSecret(hadoopConf, runtimeConf)
            val mountPoints = getExistS3MountPoints(conf.getAlluxioUser, access_key, secret_key)
            mountPoints.foreach { case (alluxioPath, mountPoint) =>
              val s3Path = mountPoint.getUfsUri
              mountedBuckets(alluxioPath) = s3Path
              logInfo(s"Found mounted bucket $s3Path to $alluxioPath")
            }
          } catch {
            case NonFatal(e) => logWarning(s"Failed to get alluxio mount table", e)
          }
          isInitMountPointsForAutoMount = true
        }
      } else {
        // disabled Alluxio feature, do nothing
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
    (scheme + "://", bucket)
  }

  private def getS3ClientConf(
      alluxioUser: String,
      s3AccessKey: Option[String],
      s3SecretKey: Option[String]): InstancedConfiguration = {
    val p = new AlluxioProperties()
    s3AccessKey.foreach(access => p.set(PropertyKey.S3A_ACCESS_KEY, access))
    s3SecretKey.foreach(secret => p.set(PropertyKey.S3A_SECRET_KEY, secret))
    alluxioMasterHost.foreach(host => p.set(PropertyKey.MASTER_HOSTNAME, host))
    alluxioMasterPort.foreach(port => p.set(PropertyKey.MASTER_RPC_PORT, port))
    p.set(PropertyKey.SECURITY_LOGIN_USERNAME, alluxioUser)
    new InstancedConfiguration(p)
  }

  private def getExistS3MountPoints(
      alluxioUser: String,
      s3AccessKey: Option[String],
      s3SecretKey: Option[String]): mutable.Map[String, MountPointInfo] = {
    val conf = getS3ClientConf(alluxioUser, s3AccessKey, s3SecretKey)
    // get s3 mount points by alluxio client
    withResource(alluxio.client.file.FileSystem.Factory.create(conf)) { fs =>
      val mountTable = fs.getMountTable
      mountTable.asScala.filter { case (_, mountPoint) =>
        // checked the alluxio code, the type should be s3
        // anyway let's keep both of them
        mountPoint.getUfsType == "s3" || mountPoint.getUfsType == "s3a"
      }
    }
  }

  // path is like "s3://foo/test...", it mounts bucket "foo" by calling the alluxio API
  // And we'll set access_key and secret_key if existing.
  // Suppose the key doesn't exist when using like Databricks's instance profile
  private def autoMountBucket(alluxioUser: String, scheme: String, bucket: String,
      s3AccessKey: Option[String], s3SecretKey: Option[String]): Unit = {
    val conf = getS3ClientConf(alluxioUser, s3AccessKey, s3SecretKey)

    // to match the output of alluxio fs mount, append / to remote_path
    // and add / before bucket name for absolute path in Alluxio
    val remote_path = scheme + bucket + "/"
    val local_bucket = "/" + bucket
    this.synchronized {
      if (!mountedBuckets.contains(local_bucket)) {
        try {
          withResource(alluxio.client.file.FileSystem.Factory.create(conf)) { fs =>
            // not mount yet, call mount command
            // we only support s3 or s3a bucket for now.
            // To support other cloud storage,
            // we need to support credential parameters for the others
            val mountOptionsBuilder = MountPOptions.newBuilder().setReadOnly(true)
            s3AccessKey.foreach(e => mountOptionsBuilder.putProperties("s3a.accessKeyId", e))
            s3SecretKey.foreach(e => mountOptionsBuilder.putProperties("s3a.secretKey", e))
            fs.mount(new AlluxioURI(local_bucket), new AlluxioURI(remote_path),
              mountOptionsBuilder.build())
            logInfo(s"Mounted bucket $remote_path to $local_bucket in Alluxio")
            mountedBuckets(local_bucket) = remote_path
          }
        } catch {
          case NonFatal(e) =>
            throw new RuntimeException(s"Mount bucket $remote_path to $local_bucket failed", e)
        }
      } else if (mountedBuckets(local_bucket).equals(remote_path)) {
        logDebug(s"Already mounted bucket $remote_path to $local_bucket in Alluxio")
      } else {
        throw new RuntimeException(s"Found a same bucket name in $remote_path " +
          s"and ${mountedBuckets(local_bucket)}")
      }
    }
  }

  // first try to get fs.s3a.access.key from spark config
  // second try to get from environment variables
  private def getKeyAndSecret(
      hadoopConfiguration: Configuration,
      runtimeConf: RuntimeConfig) : (Option[String], Option[String]) = {
    val hadoopAccessKey =
      hadoopConfiguration.get("fs.s3a.access.key")
    val hadoopSecretKey =
      hadoopConfiguration.get("fs.s3a.secret.key")
    if (hadoopAccessKey != null && hadoopSecretKey != null) {
      (Some(hadoopAccessKey), Some(hadoopSecretKey))
    } else {
      val accessKey = runtimeConf.getOption("spark.hadoop.fs.s3a.access.key")
      val secretKey = runtimeConf.getOption("spark.hadoop.fs.s3a.secret.key")
      if (accessKey.isDefined && secretKey.isDefined) {
        (accessKey, secretKey)
      } else {
        val envAccessKey = scala.util.Properties.envOrNone("AWS_ACCESS_KEY_ID")
        val envSecretKey = scala.util.Properties.envOrNone("AWS_ACCESS_SECRET_KEY")
        (envAccessKey, envSecretKey)
      }
    }
  }

  // Contains the file string to read and contains a boolean indicating if the
  // path was updated to an alluxio:// path.
  case class AlluxioPathReplaceTaskTime(fileStr: String, wasReplaced: Boolean)

  // Contains the file Path to read and optionally contains the prefix of the original path.
  // The original path is needed when using the input_file_name option with the reader so
  // it reports the original path and not the alluxio version
  case class AlluxioPathReplaceConvertTime(filePath: Path, origPrefix: Option[String])

  // Replaces the file name with Alluxio one if it matches.
  // Returns a tuple with the file path and whether or not it replaced the
  // scheme with the Alluxio one.
  private def genFuncForTaskTimeReplacement(pathsToReplace: Map[String, String])
    : Option[String => AlluxioPathReplaceTaskTime] = {
    Some((pathStr: String) => {
      // pathsToReplace contain strings of exact paths to replace
      val matchedSet = pathsToReplace.filter { case (pattern, _) => pathStr.startsWith(pattern) }
      if (matchedSet.size > 1) {
        // never reach here since replaceMap is a Map
        throw new IllegalArgumentException(s"Found ${matchedSet.size} same replacing rules " +
          s"from ${RapidsConf.ALLUXIO_PATHS_REPLACE.key} which requires only 1 rule " +
          s"for each file path")
      } else if (matchedSet.size == 1) {
        AlluxioPathReplaceTaskTime(
          pathStr.replaceFirst(matchedSet.head._1, matchedSet.head._2), wasReplaced = true)
      } else {
        AlluxioPathReplaceTaskTime(pathStr, wasReplaced = false)
      }
    })
  }

  private def getReplacementMapOption(conf: RapidsConf): Option[Map[String, String]] = {
    val alluxioPathsReplace: Option[Seq[String]] = conf.getAlluxioPathsToReplace
    // alluxioPathsReplace: Seq("key->value", "key1->value1")
    // turn the rules to the Map with eg
    // { s3://foo -> alluxio://0.1.2.3:19998/foo,
    //   gs://bar -> alluxio://0.1.2.3:19998/bar }
    if (alluxioPathsReplace.isDefined) {
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
  }

  // assumes Alluxio directories already mounted at this point
  def updateFilesTaskTimeIfAlluxio(
      origFiles: Array[PartitionedFile],
      alluxioPathReplacementMap: Option[Map[String, String]])
    : Array[PartitionedFileInfoOptAlluxio] = {
    val res: Array[PartitionedFileInfoOptAlluxio] =
      alluxioPathReplacementMap.map { pathsToReplace =>
      replacePathInPartitionFileTaskTimeIfNeeded(pathsToReplace, origFiles)
    }.getOrElse(origFiles.map(PartitionedFileInfoOptAlluxio(_, None)))
    logDebug(s"Updated files at TASK_TIME for Alluxio: ${res.mkString(",")}")
    res
  }

  // Replaces the path if needed and returns the replaced path and optionally the
  // original file if it replaced the scheme with an Alluxio scheme.
  def replacePathInPartitionFileTaskTimeIfNeeded(
      pathsToReplace: Map[String, String],
      files: Array[PartitionedFile]): Array[PartitionedFileInfoOptAlluxio] = {
    val replaceFunc = genFuncForTaskTimeReplacement(pathsToReplace)
    if (replaceFunc.isDefined) {
      files.map { file =>
        val replacedFileInfo = replaceFunc.get(file.filePath)
        if (replacedFileInfo.wasReplaced) {
          logDebug(s"TASK_TIME replaced ${file.filePath} with ${replacedFileInfo.fileStr}")
          PartitionedFileInfoOptAlluxio(PartitionedFile(file.partitionValues,
            replacedFileInfo.fileStr, file.start, file.length),
            Some(file))
        } else {
          PartitionedFileInfoOptAlluxio(file, None)
        }
      }
    } else {
      files.map(PartitionedFileInfoOptAlluxio(_, None))
    }
  }

  def autoMountIfNeeded(
      conf: RapidsConf,
      pds: Seq[PartitionDirectory],
      hadoopConf: Configuration,
      runtimeConf: RuntimeConfig): Option[Map[String, String]] = {
    val alluxioAutoMountEnabled = conf.getAlluxioAutoMountEnabled
    initAlluxioInfo(conf, hadoopConf, runtimeConf)
    if (alluxioAutoMountEnabled) {
      val (access_key, secret_key) = getKeyAndSecret(hadoopConf, runtimeConf)
      val replacedSchemes = pds.flatMap { pd =>
        pd.files.map(_.getPath.toString).flatMap { file =>
          if (file.matches(alluxioBucketRegex.get)) {
            val (scheme, bucket) = getSchemeAndBucketFromPath(file)
            autoMountBucket(conf.getAlluxioUser, scheme, bucket, access_key, secret_key)
            Some(scheme)
          } else {
            None
          }
        }
      }
      if (replacedSchemes.nonEmpty) {
        Some(replacedSchemes.map(_ -> (ALLUXIO_SCHEME + alluxioMasterHostAndPort.get + "/")).toMap)
      } else {
        None
      }
    } else {
      None
    }
  }

  def checkIfNeedsReplaced(
      conf: RapidsConf,
      pds: Seq[PartitionDirectory],
      hadoopConf: Configuration,
      runtimeConf: RuntimeConfig): Option[Map[String, String]] = {
    initAlluxioInfo(conf, hadoopConf, runtimeConf)
    val anyToReplace = pds.exists { pd =>
      pd.files.map(_.getPath.toString).exists { file =>
        val matchedSet = alluxioPathsToReplaceMap.get.filter(a => file.startsWith(a._1))
        if (matchedSet.size > 1) {
          // never reach here since replaceMap is a Map
          throw new IllegalArgumentException(s"Found ${matchedSet.size} same replacing rules " +
            s"from ${RapidsConf.ALLUXIO_PATHS_REPLACE.key} which requires only 1 rule " +
            s"for each file path")
        } else if (matchedSet.size == 1) {
          true
        } else {
          false
        }
      }
    }
    if (anyToReplace) {
      alluxioPathsToReplaceMap
    } else {
      None
    }
  }

  // reverse the replacePathIfNeeded, returns a tuple of the file passed in and then if it
  // was replaced the original file
  def getOrigPathFromReplaced(pfs: Array[PartitionedFile],
      pathsToReplace: Map[String,String]): Array[PartitionedFileInfoOptAlluxio] = {
    pfs.map { pf =>
      val file = pf.filePath
      // pathsToReplace contain strings of exact paths to replace
      val matchedSet = pathsToReplace.filter { case (_, alluxioPattern) =>
        file.startsWith(alluxioPattern)
      }
      if (matchedSet.size > 1) {
        // never reach here since replaceMap is a Map
        throw new IllegalArgumentException(s"Found ${matchedSet.size} same replacing rules " +
          s"from ${RapidsConf.ALLUXIO_PATHS_REPLACE.key} which requires only 1 rule " +
          s"for each file path")
      } else if (matchedSet.size == 1) {
        val replacedFile = file.replaceFirst(matchedSet.head._2, matchedSet.head._1)
        logDebug(s"getOrigPath replacedFile: $replacedFile")
        PartitionedFileInfoOptAlluxio(pf,
          Some(PartitionedFile(pf.partitionValues, replacedFile, pf.start, file.length)))
      } else {
        PartitionedFileInfoOptAlluxio(pf, None)
      }
    }
  }
}
