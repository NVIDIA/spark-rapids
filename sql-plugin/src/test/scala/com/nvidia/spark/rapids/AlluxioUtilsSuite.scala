/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable

import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar.mock

import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf

class AlluxioMasterAndPortReaderMock(master: String, port: String)
  extends AlluxioConfigReader {
  override def readAlluxioMasterAndPort(): (String, String) = (master, port)
}

class AlluxioFSMock extends AlluxioFS {
  private val mountPoints = mutable.Map[String, String]()

  /**
   * Get S3 mount points by Alluxio client
   *
   * @return mount points map, key of map is Alluxio path, value of map is S3 path.
   *         E.g.: returns a map: {'/bucket_1': 's3://bucket_1'}
   */
  override def getExistingMountPoints(): mutable.Map[String, String] = {
    mountPoints
  }

  /**
   * Mount an S3 path to Alluxio
   *
   * @param alluxioPath Alluxio path
   * @param s3Path      S3 path
   */
  override def mount(alluxioPath: String, s3Path: String): Unit = {
    mountPoints(alluxioPath) = s3Path
  }

  def getMountPoints(): mutable.Map[String, String] = {
    mountPoints
  }
}

class AlluxioUtilsSuite extends FunSuite {

  def setMockOnAlluxioUtils(): Unit = {
    AlluxioUtils.setAlluxioFS(new AlluxioFSMock())
    AlluxioUtils.setAlluxioMasterAndPortReader(
      new AlluxioMasterAndPortReaderMock("localhost", "19998"))
  }

  test("updateFilesTaskTimeIfAlluxio") {
    setMockOnAlluxioUtils()
    AlluxioUtils.resetInitInfo()
    val replaceMap = Map[String, String](("s3a://", "alluxio://localhost:19998/"))
    val partitionedFiles = Array[PartitionedFile](
      PartitionedFileUtilsShim.newPartitionedFile(null, "s3a://bucket_1/a.file", 0, 0),
      PartitionedFileUtilsShim.newPartitionedFile(null, "s3a://bucket_2/b.file", 0, 0),
      PartitionedFileUtilsShim.newPartitionedFile(null, "my_scheme://bucket_1/1.file", 0, 0)
    )
    val replaced = AlluxioUtils.updateFilesTaskTimeIfAlluxio(partitionedFiles, Option(replaceMap))
    assert(replaced.size == 3)
    assert(replaced(0).toRead.filePath.equals("alluxio://localhost:19998/bucket_1/a.file"))
    assert(replaced(0).original.get.filePath.equals("s3a://bucket_1/a.file"))
    assert(replaced(1).toRead.filePath.equals("alluxio://localhost:19998/bucket_2/b.file"))
    assert(replaced(1).original.get.filePath.equals("s3a://bucket_2/b.file"))
    assert(replaced(2).toRead.filePath.equals("my_scheme://bucket_1/1.file"))
    assert(replaced(2).original.isEmpty)
  }

  test("updateFilesTaskTimeIfAlluxio, multiple replacing rules") {
    setMockOnAlluxioUtils()
    AlluxioUtils.resetInitInfo()
    val replaceMap = Map[String, String](
      ("s3a://", "alluxio://localhost:19998/"), // the first rule
      ("s3a://bucket_1", "alluxio://localhost:19998/") // should not specify this rule!
    )
    val partitionedFiles = Array[PartitionedFile](
      PartitionedFileUtilsShim.newPartitionedFile(null, "s3a://bucket_1/a.file", 0, 0)
    )
    try {
      AlluxioUtils.updateFilesTaskTimeIfAlluxio(partitionedFiles, Option(replaceMap))
      assert(false)
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains("same replacing rules"))
    }
  }

  test("checkIfNeedsReplaced for PathsToReplace map, true") {
    setMockOnAlluxioUtils()
    AlluxioUtils.resetInitInfo()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.rapids.alluxio.pathsToReplace",
      "s3a://bucket_1->alluxio://0.1.2.3:19998/foo")
    val rapidsConf = new RapidsConf(sqlConf)
    val fs = new FileStatus(0, false, 1, 1024L, 0L,
      new Path("s3a://bucket_1/a.parquet"))
    val pds = Seq(PartitionDirectory(null, Array(fs)))
    val configuration = new Configuration()
    val runtimeConfig = mock[RuntimeConfig]
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.access.key")).thenReturn(Some("access key"))
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.secret.key")).thenReturn(Some("secret key"))

    assert(AlluxioUtils
      .checkIfNeedsReplaced(rapidsConf, pds, configuration, runtimeConfig).isDefined)
  }

  test("checkIfNeedsReplaced for PathsToReplace map, false") {
    setMockOnAlluxioUtils()
    AlluxioUtils.resetInitInfo()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.rapids.alluxio.pathsToReplace",
      "s3a://bucket_1->alluxio://0.1.2.3:19998/foo")
    val rapidsConf = new RapidsConf(sqlConf)
    val fs = new FileStatus(0, false, 1, 1024L, 0L,
      new Path("s3a://not_found/a.parquet"))
    val pds = Seq(PartitionDirectory(null, Array(fs)))
    val configuration = new Configuration()
    val runtimeConfig = mock[RuntimeConfig]
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.access.key")).thenReturn(Some("access key"))
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.secret.key")).thenReturn(Some("secret key"))

    assert(AlluxioUtils.checkIfNeedsReplaced(rapidsConf, pds, configuration, runtimeConfig).isEmpty)
  }

  test("checkIfNeedsReplaced for PathsToReplace map, exception") {
    setMockOnAlluxioUtils()
    AlluxioUtils.resetInitInfo()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.rapids.alluxio.pathsToReplace",
      "s3a://bucket_1->alluxio://0.1.2.3:19998/dir1," +
        "s3a://bucket_1/dir1->alluxio://4.4.4.4:19998/dir1"
    )
    val rapidsConf = new RapidsConf(sqlConf)
    val fs = new FileStatus(0, false, 1, 1024L, 0L,
      new Path("s3a://bucket_1/dir1/a.parquet")) // matches 2 rules
    val pds = Seq(PartitionDirectory(null, Array(fs)))
    val configuration = new Configuration()
    val runtimeConfig = mock[RuntimeConfig]
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.access.key")).thenReturn(Some("access key"))
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.secret.key")).thenReturn(Some("secret key"))

    try {
      AlluxioUtils.checkIfNeedsReplaced(rapidsConf, pds, configuration, runtimeConfig).isEmpty
      assert(false)
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains("same replacing rules"))
    }
  }

  test("checkIfNeedsReplaced for PathsToReplace map, invalid setting") {
    setMockOnAlluxioUtils()
    AlluxioUtils.resetInitInfo()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.rapids.alluxio.pathsToReplace",
      "s3a://bucket_1->alluxio://0.1.2.3:19998/->dir1" // contains 2 `->`
    )
    val rapidsConf = new RapidsConf(sqlConf)
    val fs = new FileStatus(0, false, 1, 1024L, 0L,
      new Path("s3a://bucket_1/dir1/a.parquet")) // matches 2 rules
    val pds = Seq(PartitionDirectory(null, Array(fs)))
    val configuration = new Configuration()
    val runtimeConfig = mock[RuntimeConfig]
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.access.key")).thenReturn(Some("access key"))
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.secret.key")).thenReturn(Some("secret key"))

    try {
      AlluxioUtils.checkIfNeedsReplaced(rapidsConf, pds, configuration, runtimeConfig).isEmpty
      assert(false)
    } catch {
      case e: IllegalArgumentException =>
        assert(e.getMessage.contains("Invalid setting"))
    }
  }

  test("autoMountIfNeeded, auto-mount is false") {
    setMockOnAlluxioUtils()
    AlluxioUtils.resetInitInfo()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.rapids.alluxio.automount.enabled", "false")
    val rapidsConf = new RapidsConf(sqlConf)
    val fs = new FileStatus(0, false, 1, 1024L, 0L,
      new Path("s3a://bucket_1/a.parquet"))
    val pds = Seq(PartitionDirectory(null, Array(fs)))
    val configuration = new Configuration()
    val runtimeConfig = mock[RuntimeConfig]
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.access.key")).thenReturn(Some("access key"))
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.secret.key")).thenReturn(Some("secret key"))

    assert(AlluxioUtils.autoMountIfNeeded(rapidsConf, pds, configuration, runtimeConfig).isEmpty)
  }

  test("autoMountIfNeeded, auto-mount is true") {
    setMockOnAlluxioUtils()
    val alluxioFSMock = new AlluxioFSMock()
    AlluxioUtils.setAlluxioFS(alluxioFSMock)
    AlluxioUtils.resetInitInfo()
    val sqlConf = new SQLConf()
    sqlConf.setConfString("spark.rapids.alluxio.automount.enabled", "true")
    val rapidsConf = new RapidsConf(sqlConf)

    val configuration = new Configuration()
    val runtimeConfig = mock[RuntimeConfig]
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.access.key")).thenReturn(Some("access key"))
    when(runtimeConfig.getOption("spark.hadoop.fs.s3a.secret.key")).thenReturn(Some("secret key"))

    assert(alluxioFSMock.getMountPoints().isEmpty)
    val fs = new FileStatus(0, false, 1, 1024L, 0L,
      new Path("s3a://bucket_1/a.parquet"))
    val pds = Seq(PartitionDirectory(null, Array(fs)))
    assert(AlluxioUtils.autoMountIfNeeded(rapidsConf, pds, configuration, runtimeConfig).isDefined)
    assert(alluxioFSMock.getMountPoints().contains("/bucket_1"))

    val fs2 = new FileStatus(0, false, 1, 1024L, 0L,
      new Path("myScheme://bucket_2/a.parquet"))
    val pds2 = Seq(PartitionDirectory(null, Array(fs2)))
    assert(AlluxioUtils.autoMountIfNeeded(rapidsConf, pds2, configuration, runtimeConfig).isEmpty)
    assert(alluxioFSMock.getMountPoints().size == 1)
  }
}
