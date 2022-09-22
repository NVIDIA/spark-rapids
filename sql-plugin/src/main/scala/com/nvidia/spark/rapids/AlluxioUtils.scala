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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FileFormat, FileIndex, HadoopFsRelation, InMemoryFileIndex, PartitionDirectory, PartitionedFile, PartitioningAwareFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.rapids.GpuPartitioningUtils

/*
 * Utilities for using Alluxio with the plugin for reading.
 * Currently we only support Alluxio with the Datasource v1 Parquet reader.
 * We currently support 2 different replacement algorithms:
 *    CONVERT_TIME: this replaces the file path when we convert a FileSourceScanExec to
 *      a GpuFileSourceScanExec. This will create an entirely new FileIndex and potentially
 *      has to re-infer the partitioning if its not a FileIndex type we know. So this can
 *      cause an extra list leaf files which for many files will run another job and thus
 *      has additional overhead. This will update the file locations to be the
 *      Alluxio specific ones if the data is already cached. In order to support the
 *      input_file_name functionality we have to convert the alluxio:// path back to its
 *      original url when we go to actually read the file.
 *    TASK_TIME: this replaces the file path as late as possible on the task side when
 *      we actually go to read the file. This makes is so that the original non-Alluxio
 *      path gets reported for the input_file_name properly without having to convert
 *      paths back to the original. This also has the benefit that it can be more performant
 *      if it doesn't have to do the extra list leaf files, but you don't get the
 *      locality information updated. So for small Alluxio clusters or with Spark
 *      clusters short on task slots this may be a better fit.
 */
object AlluxioUtils extends Logging {
  private val checkedAlluxioPath = scala.collection.mutable.HashSet[String]()
  private val ALLUXIO_SCHEME = "alluxio://"
  private val mountedBuckets: scala.collection.mutable.Map[String, String] =
    scala.collection.mutable.Map()
  private var alluxioCmd: Seq[String] = null
  private var alluxioMasterHost: Option[String] = None
  private var alluxioPathsToReplaceMap: Option[Map[String, String]] = None
  private var alluxioHome: String = "/opt/alluxio-2.8.0"
  private var isInit: Boolean = false

  // Alluxio should be initialized before calling
  def getAlluxioMasterAndPort: Option[String] = alluxioMasterHost
  def getAlluxioPathsToReplace: Option[Map[String, String]] = alluxioPathsToReplaceMap

  def isAlluxioAutoMountTaskTime(rapidsConf: RapidsConf,
      fileFormat: FileFormat): Boolean = {
      rapidsConf.getAlluxioAutoMountEnabled && rapidsConf.isAlluxioReplacementAlgoTaskTime &&
        fileFormat.isInstanceOf[ParquetFileFormat]
  }

  def isAlluxioPathsToReplaceTaskTime(rapidsConf: RapidsConf,
      fileFormat: FileFormat): Boolean = {
    rapidsConf.getAlluxioPathsToReplace.isDefined && rapidsConf.isAlluxioReplacementAlgoTaskTime &&
      fileFormat.isInstanceOf[ParquetFileFormat]
  }

  private def checkAlluxioMounted(
      hadoopConfiguration: Configuration,
      alluxio_path: String): Unit = {
    this.synchronized {
      if (!checkedAlluxioPath.contains(alluxio_path)) {
        val path = new Path(alluxio_path)
        val fs = path.getFileSystem(hadoopConfiguration)
        if (!fs.exists(path)) {
          throw new FileNotFoundException(
            s"Alluxio path $alluxio_path does not exist, maybe forgot to mount it")
        }
        logDebug(s"Alluxio path $alluxio_path is mounted")
        checkedAlluxioPath.add(alluxio_path)
      } else {
        logDebug(s"Alluxio path $alluxio_path already mounted")
      }
    }
  }

  // Default to read from /opt/alluxio-2.8.0 if not setting ALLUXIO_HOME
  private def readAlluxioMasterAndPort: (String, String) = {
    var buffered_source: BufferedSource = null
    try {
      buffered_source = Source.fromFile(alluxioHome + "/conf/alluxio-site.properties")
      val prop : Properties = new Properties()
      prop.load(buffered_source.bufferedReader())
      val alluxio_master = prop.getProperty("alluxio.master.hostname")
      val alluxio_port = prop.getProperty("alluxio.master.rpc.port", "19998")
      (alluxio_master, alluxio_port)
    } catch {
      case e: FileNotFoundException =>
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
  private def initAlluxioInfo(conf: RapidsConf): Unit = {
    this.synchronized {
      // left outside isInit to allow changing at runtime
      alluxioHome = scala.util.Properties.envOrElse("ALLUXIO_HOME", "/opt/alluxio-2.8.0")
      alluxioCmd = conf.getAlluxioCmd

      if (!isInit) {
        if (conf.getAlluxioAutoMountEnabled) {
          val (alluxio_master, alluxio_port) = readAlluxioMasterAndPort
          if (alluxio_master == null) {
            throw new RuntimeException(
              s"Can't find alluxio.master.hostname from $alluxioHome/conf/alluxio-site.properties.")
          }
          alluxioMasterHost = Some(alluxio_master + ":" + alluxio_port)
          val alluxioBucketRegex: String = conf.getAlluxioBucketRegex
          // TODO - do we still need this since set later?
          alluxioPathsToReplaceMap =
            Some(Map(alluxioBucketRegex -> (ALLUXIO_SCHEME + alluxioMasterHost.get + "/")))
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
                  logDebug(s"Found mounted bucket ${items(0)} to ${items(2)}")
                }
              }
            }
          } else {
            logWarning(s"Failed to run alluxio fs mount $ret")
          }
        } else {
          alluxioPathsToReplaceMap = getReplacementMapOption(conf)
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
    (scheme + "://", bucket)
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
  private def autoMountBucket(
      scheme: String,
      bucket: String,
      access_key: Option[String],
      secret_key: Option[String]): Unit = {
    // to match the output of alluxio fs mount, append / to remote_path
    // and add / before bucket name for absolute path in Alluxio
    val remote_path = scheme + bucket + "/"
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

  private def replaceSchemeWithAlluxio(file: String, scheme: String, masterPort: String): String = {
    // replace s3://foo/.. to alluxio://alluxioMasterHost/foo/...
    val newFile = file.replaceFirst(scheme, ALLUXIO_SCHEME + masterPort + "/")
    logDebug(s"Replace $file to ${newFile}")
    newFile
  }

  private def genFuncForPathReplacement(
      replaceMapOption: Option[Map[String, String]]): Option[Path => (Path, Option[String])] = {
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
          (new Path(pathStr.replaceFirst(matchedSet.head._1, matchedSet.head._2)),
            Some(matchedSet.head._1))
        } else {
          (f, None)
        }
      })
    } else {
      None
    }
  }

  private def genFuncForAutoMountReplacement(
      runtimeConf: RuntimeConfig,
      hadoopConf: Configuration,
      alluxioBucketRegex: String) : Option[Path => (Path, Option[String])] = {
    Some((f: Path) => {
      val pathStr = f.toString
      if (pathStr.matches(alluxioBucketRegex)) {
        val (access_key, secret_key) = getKeyAndSecret(hadoopConf, runtimeConf)
        val (scheme, bucket) = getSchemeAndBucketFromPath(pathStr)
        autoMountBucket(scheme, bucket, access_key, secret_key)
        (new Path(replaceSchemeWithAlluxio(pathStr, scheme, alluxioMasterHost.get)), Some(scheme))
      } else {
        (f, None)
      }
    })
  }

  // Replaces the file name with Alluxio one if it matches.
  // Returns a tuple with the file path and whether or not it replaced the
  // scheme with the Alluxio one.
  private def genFuncForTaskTimeReplacement(
      pathsToReplace: Map[String, String]): Option[String => (String, Boolean)] = {
    Some((pathStr: String) => {

      // pathsToReplace contain strings of exact paths to replace
      val matchedSet = pathsToReplace.filter { case (pattern, _) => pathStr.startsWith(pattern) }
      if (matchedSet.size > 1) {
        // never reach here since replaceMap is a Map
        throw new IllegalArgumentException(s"Found ${matchedSet.size} same replacing rules " +
          s"from ${RapidsConf.ALLUXIO_PATHS_REPLACE.key} which requires only 1 rule " +
          s"for each file path")
      } else if (matchedSet.size == 1) {
        (pathStr.replaceFirst(matchedSet.head._1, matchedSet.head._2), true)
      } else {
        (pathStr, false)
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

  private def getReplacementFunc(
      conf: RapidsConf,
      runtimeConf: RuntimeConfig,
      hadoopConf: Configuration): Option[Path => (Path, Option[String])] = {
    if (conf.getAlluxioPathsToReplace.isDefined) {
      genFuncForPathReplacement(alluxioPathsToReplaceMap)
    } else if (conf.getAlluxioAutoMountEnabled) {
      val alluxioBucketRegex: String = conf.getAlluxioBucketRegex
      genFuncForAutoMountReplacement(runtimeConf, hadoopConf, alluxioBucketRegex)
    } else {
      None
    }
  }

  // assumes Alluxio directories already mounted at this point
  def updateFilesTaskTimeIfAlluxio(
      origFiles: Array[PartitionedFile],
      alluxionPathReplacementMap: Option[Map[String, String]]):
    Array[(PartitionedFile, Option[PartitionedFile])] = {
    val res: Array[(PartitionedFile, Option[PartitionedFile])] =
      alluxionPathReplacementMap.map { pathsToReplace =>
      replacePathInPartitionFileTaskTimeIfNeeded(pathsToReplace, origFiles)
    }.getOrElse(origFiles.map((_, None)))
    logWarning(s"Updated files at TASK_TIME for Alluxio: ${res.mkString(",")}")
    res
  }

  // Replaces the path if needed and returns the replaced path and optionally the
  // original file if it replaced the scheme with an Alluxio scheme.
  def replacePathInPartitionFileTaskTimeIfNeeded(
      pathsToReplace: Map[String, String],
      files: Array[PartitionedFile]): Array[(PartitionedFile, Option[PartitionedFile])] = {
    val replaceFunc = genFuncForTaskTimeReplacement(pathsToReplace)
    if (replaceFunc.isDefined) {
      files.map { file =>
        val (replacedFile, didReplace) = replaceFunc.get(file.filePath)
        if (didReplace) {
          logDebug(s"Task Time replaced ${file.filePath} with $replacedFile")
          (PartitionedFile(file.partitionValues, replacedFile, file.start, file.length), Some(file))
        } else {
          (file, None)
        }
      }
    } else {
      files.map((_, None))
    }
  }

  def autoMountIfNeeded(
      conf: RapidsConf,
      pds: Seq[PartitionDirectory],
      hadoopConf: Configuration,
      runtimeConf: RuntimeConfig): Option[Map[String, String]] = {
    val alluxioAutoMountEnabled = conf.getAlluxioAutoMountEnabled
    val alluxioBucketRegex: String = conf.getAlluxioBucketRegex
    initAlluxioInfo(conf)
    if (alluxioAutoMountEnabled) {
      val (access_key, secret_key) = getKeyAndSecret(hadoopConf, runtimeConf)
      val replacedSchemes = pds.flatMap { pd =>
        pd.files.map(_.getPath.toString).flatMap { file =>
          if (file.matches(alluxioBucketRegex)) {
            val (scheme, bucket) = getSchemeAndBucketFromPath(file)
            autoMountBucket(scheme, bucket, access_key, secret_key)
            Some(scheme)
          } else {
            None
          }
        }
      }
      if (replacedSchemes.nonEmpty) {
        Some(replacedSchemes.map(_ -> (ALLUXIO_SCHEME + alluxioMasterHost.get + "/")).toMap)
      } else {
        None
      }
    } else {
      None
    }
  }

  def checkIfNeedsReplaced(
      conf: RapidsConf,
      pds: Seq[PartitionDirectory]): Option[Map[String, String]] = {
    initAlluxioInfo(conf)
    val anyToReplace = pds.map { pd =>
      pd.files.map(_.getPath.toString).map { file =>
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
      }.contains(true)
    }.contains(true)
    if (anyToReplace) {
      alluxioPathsToReplaceMap
    } else {
      None
    }
  }

  // reverse the replacePathIfNeeded, returns a tuple of the file passed in and then if it
  // was replaced the original file
  def getOrigPathFromReplaced(pfs: Array[PartitionedFile],
      pathsToReplace: Map[String,String]): Array[(PartitionedFile, Option[PartitionedFile])] = {
    pfs.map { pf =>
      val file = pf.filePath
      // pathsToReplace contain strings of exact paths to replace
      val matchedSet = pathsToReplace.filter { case (_, alluxPattern) =>
        file.startsWith(alluxPattern)
      }
      if (matchedSet.size > 1) {
        // never reach here since replaceMap is a Map
        throw new IllegalArgumentException(s"Found ${matchedSet.size} same replacing rules " +
          s"from ${RapidsConf.ALLUXIO_PATHS_REPLACE.key} which requires only 1 rule " +
          s"for each file path")
      } else if (matchedSet.size == 1) {
        logWarning(s"matched set 1 file: $file")
        val replacedFile = file.replaceFirst(matchedSet.head._2, matchedSet.head._1)
        logWarning(s"matched set 1 replacedFile: $replacedFile")
        (pf, Some(PartitionedFile(pf.partitionValues, replacedFile, pf.start, file.length)))
      } else {
        (pf, None)
      }
    }
  }

  // This is used when replacement algorithm is CONVERT_TIME and causes
  // a new lookup on the alluxio files. For unknown FileIndex types it can
  // also cause us to have to infer the partitioning again.
  def replacePathIfNeeded(
      conf: RapidsConf,
      relation: HadoopFsRelation,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): (FileIndex, Option[Map[String, String]])= {
    val hadoopConf = relation.sparkSession.sparkContext.hadoopConfiguration
    val runtimeConf = relation.sparkSession.conf
    initAlluxioInfo(conf)
    val replaceFunc = getReplacementFunc(conf, runtimeConf, hadoopConf)

    val (location, allReplacedPrefixes) = if (replaceFunc.isDefined) {
      def replacePathsInPartitionSpec(spec: PartitionSpec): (PartitionSpec, Seq[String]) = {
        val partitionsWithPathsReplaced = spec.partitions.map { p =>
          val (replacedPath, replacedPrefix) = replaceFunc.get(p.path)
          (org.apache.spark.sql.execution.datasources.PartitionPath(p.values, replacedPath),
            replacedPrefix)
        }
        val paths = partitionsWithPathsReplaced.map(_._1)
        val replacedPrefixes = partitionsWithPathsReplaced.flatMap(_._2)
        (PartitionSpec(spec.partitionColumns, paths), replacedPrefixes)
      }

      def createNewFileIndexWithPathsReplaced(
          spec: PartitionSpec,
          rootPaths: Seq[Path]): (InMemoryFileIndex, Seq[String]) = {
        val (specAdjusted, replacedPrefixes) = replacePathsInPartitionSpec(spec)
        val replacedPathsAndIndicator = rootPaths.map(replaceFunc.get)
        val replacedPaths = replacedPathsAndIndicator.map(_._1)
        val didReplaceAnyRoots = replacedPathsAndIndicator.flatMap(_._2)
        val fi = new InMemoryFileIndex(
          relation.sparkSession,
          replacedPaths,
          relation.options,
          Option(relation.dataSchema),
          userSpecifiedPartitionSpec = Some(specAdjusted))
          (fi, didReplaceAnyRoots ++ replacedPrefixes)
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
        case _ =>
          logDebug(s"Handling file index type: ${relation.location.getClass}")

          // With the base Spark FileIndex type we don't know how to modify it to
          // just replace the paths so we have to try to recompute.
          def isDynamicPruningFilter(e: Expression): Boolean =
            e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

          val partitionDirs = relation.location.listFiles(
            partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)

          // replace all of input files
          val inputFilesAndDidReplace = partitionDirs.flatMap(partitionDir => {
            partitionDir.files.map(f => replaceFunc.get(f.getPath))
          })
          val inputFiles = inputFilesAndDidReplace.map(_._1)
          val didReplaceAny = inputFilesAndDidReplace.flatMap(_._2)

          // replace all of rootPaths which are already unique
          val rootPathsAndDidReplace = relation.location.rootPaths.map(replaceFunc.get)
          val rootPaths = rootPathsAndDidReplace.map(_._1)
          val rootPathsDidReplace = rootPathsAndDidReplace.flatMap(_._2)

          // check the alluxio paths in root paths exist or not
          // throw out an exception to stop the job when any of them is not mounted
          if (alluxioPathsToReplaceMap.isDefined) {
            rootPaths.foreach { rootPath =>
              alluxioPathsToReplaceMap.get.values.
                find(value => rootPath.toString.startsWith(value)).
                foreach(matched => checkAlluxioMounted(hadoopConf, matched))
            }
          }

          val parameters: Map[String, String] = relation.options

          // infer PartitionSpec
          val (partitionSpec, replacedBasePath) = GpuPartitioningUtils.inferPartitioning(
            relation.sparkSession,
            rootPaths,
            inputFiles,
            parameters,
            Option(relation.dataSchema),
            replaceFunc.get)

          val allReplacedPrefixes = didReplaceAny ++ rootPathsDidReplace ++ replacedBasePath
          // generate a new InMemoryFileIndex holding paths with alluxio schema
          val fi = new InMemoryFileIndex(
            relation.sparkSession,
            inputFiles,
            parameters,
            Option(relation.dataSchema),
            userSpecifiedPartitionSpec = Some(partitionSpec))
          (fi, allReplacedPrefixes)
      }
    } else {
      (relation.location, Seq.empty)
    }
    val mapIfReplacedPaths = if (allReplacedPrefixes.nonEmpty) {
      // with alluxio.automount.enabled we only have a regex so we need to track
      // the exact schemes we replaced in order to set the input_file_name properly,
      // with the alluxio.pathsToReplace it already contains the exact paths
      if (conf.getAlluxioAutoMountEnabled) {
        Some(allReplacedPrefixes.map(_ -> (ALLUXIO_SCHEME + alluxioMasterHost.get + "/")).toMap)
      } else {
        alluxioPathsToReplaceMap
      }
    } else {
      None
    }
    (location, mapIfReplacedPaths)
  }
}
