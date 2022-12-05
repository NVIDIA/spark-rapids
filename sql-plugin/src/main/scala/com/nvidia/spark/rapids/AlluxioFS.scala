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

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

import alluxio.AlluxioURI
import alluxio.conf.{AlluxioProperties, InstancedConfiguration, PropertyKey}
import alluxio.grpc.MountPOptions

/**
 * interfaces for Alluxio file system.
 * Currently contains interfaces:
 *   get mount points
 *   mount
 */
class AlluxioFS extends Arm {
  private var masterHost: Option[String] = None
  private var masterPort: Option[Int] = None
  private var alluxioUser: String = ""
  private var s3AccessKey: Option[String] = None
  private var s3SecretKey: Option[String] = None

  def setHostAndPort(masterHost: Option[String], masterPort: Option[Int]): Unit = {
    this.masterHost = masterHost
    this.masterPort = masterPort
  }

  def setUserAndKeys(alluxioUser: String, s3AccessKey: Option[String],
      s3SecretKey: Option[String]): Unit = {
    this.alluxioUser = alluxioUser
    this.s3AccessKey = s3AccessKey
    this.s3SecretKey = s3SecretKey
  }

  private def getS3ClientConf(): InstancedConfiguration = {
    val p = new AlluxioProperties()
    masterHost.foreach(host => p.set(PropertyKey.MASTER_HOSTNAME, host))
    masterPort.foreach(port => p.set(PropertyKey.MASTER_RPC_PORT, port))
    s3AccessKey.foreach(access => p.set(PropertyKey.S3A_ACCESS_KEY, access))
    s3SecretKey.foreach(secret => p.set(PropertyKey.S3A_SECRET_KEY, secret))
    p.set(PropertyKey.SECURITY_LOGIN_USERNAME, alluxioUser)
    new InstancedConfiguration(p)
  }

  /**
   * Get S3 mount points by Alluxio client
   *
   * @return mount points map, key of map is Alluxio path, value of map is S3 path.
   *         E.g.: returns a map: {'/bucket_1': 's3://bucket_1'}
   */
  def getExistingMountPoints(): mutable.Map[String, String] = {
    val conf = getS3ClientConf()
    // get s3 mount points by Alluxio client
    withResource(alluxio.client.file.FileSystem.Factory.create(conf)) { fs =>
      val mountTable = fs.getMountTable
      mountTable.asScala.filter { case (_, mountPoint) =>
        // checked the alluxio code, the type should be s3
        // anyway let's keep both of them
        mountPoint.getUfsType == "s3" || mountPoint.getUfsType == "s3a"
      }.map { case (alluxioPath, s3Point) =>
        (alluxioPath, s3Point.getUfsUri)
      }
    }
  }

  /**
   * Mount an S3 path to Alluxio
   *
   * @param alluxioPath Alluxio path
   * @param s3Path      S3 path
   */
  def mount(alluxioPath: String, s3Path: String): Unit = {
    val conf = getS3ClientConf()
    withResource(alluxio.client.file.FileSystem.Factory.create(conf)) { fs =>
      val mountOptionsBuilder = MountPOptions.newBuilder().setReadOnly(true)
      s3AccessKey.foreach(e => mountOptionsBuilder.putProperties("s3a.accessKeyId", e))
      s3SecretKey.foreach(e => mountOptionsBuilder.putProperties("s3a.secretKey", e))
      fs.mount(new AlluxioURI(alluxioPath), new AlluxioURI(s3Path),
        mountOptionsBuilder.build())
    }
  }
}
