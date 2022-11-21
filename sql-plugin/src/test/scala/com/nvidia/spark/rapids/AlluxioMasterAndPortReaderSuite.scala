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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.FunSuite

class AlluxioMasterAndPortReaderSuite extends FunSuite {

  test("testReadAlluxioMasterAndPort") {
    val homeDir = Files.createTempDirectory("tmpAlluxioHomePrefix")
    val confDir = new File(homeDir.toFile, "conf")
    assert(confDir.mkdir())
    val f = new File(confDir, "alluxio-site.properties")

    try {
      val content =
        """
alluxio.master.hostname=host1.com
alluxio.master.rpc.port=200
        """
      Files.write(f.toPath, content.getBytes(StandardCharsets.UTF_8))
      val (host, port) = new AlluxioMasterAndPortReader()
        .readAlluxioMasterAndPort(homeDir.toFile.getAbsolutePath)
      assert(host.equals("host1.com"))
      assert(port == "200")
    } finally {
      f.delete()
      confDir.delete()
      homeDir.toFile.delete()
    }
  }

  test("testReadAlluxioMasterAndPort, get default port") {
    val homeDir = Files.createTempDirectory("tmpAlluxioHomePrefix")
    val confDir = new File(homeDir.toFile, "conf")
    assert(confDir.mkdir())
    val f = new File(confDir, "alluxio-site.properties")

    try {
      val content =
        """
alluxio.master.hostname=host1.com
        """
      Files.write(f.toPath, content.getBytes(StandardCharsets.UTF_8))
      val (host, port) = new AlluxioMasterAndPortReader()
        .readAlluxioMasterAndPort(homeDir.toFile.getAbsolutePath)
      assert(host.equals("host1.com"))
      assert(port == "19998")
    } finally {
      f.delete()
      confDir.delete()
      homeDir.toFile.delete()
    }
  }

  test("testReadAlluxioMasterAndPort, cfg does not specify master") {
    val homeDir = Files.createTempDirectory("tmpAlluxioHomePrefix")
    val confDir = new File(homeDir.toFile, "conf")
    assert(confDir.mkdir())
    val f = new File(confDir, "alluxio-site.properties")

    try {
      val content =
        """
xxx=yyy
        """
      Files.write(f.toPath, content.getBytes(StandardCharsets.UTF_8))
      try {
        new AlluxioMasterAndPortReader()
          .readAlluxioMasterAndPort(homeDir.toFile.getAbsolutePath)
        assert(false)
      } catch {
        case e: RuntimeException =>
          assert(e.getMessage.contains("Can't find alluxio.master.hostname"))
      }
    } finally {
      f.delete()
      confDir.delete()
      homeDir.toFile.delete()
    }
  }

  test("testReadAlluxioMasterAndPort, cfg file does not exist") {
    val homeDir = Files.createTempDirectory("tmpAlluxioHomePrefix")
    val confDir = new File(homeDir.toFile, "conf")
    assert(confDir.mkdir())

    try {
      try {
        new AlluxioMasterAndPortReader()
          .readAlluxioMasterAndPort(homeDir.toFile.getAbsolutePath)
        assert(false)
      } catch {
        case e: RuntimeException =>
          assert(e.getMessage.contains("Not found Alluxio config in"))
      }
    } finally {
      confDir.delete()
      homeDir.toFile.delete()
    }
  }
}
