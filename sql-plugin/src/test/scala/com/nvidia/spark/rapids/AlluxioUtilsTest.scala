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

import scala.collection.mutable

import alluxio.client.file.URIStatus
import alluxio.wire.FileInfo
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite


class AlluxioUtilsTest extends FunSuite {

  test("test get parent path") {
    val paths = Array(
      "/root/t1/a.parquet",
      "/root/t1/b.parquet",
      "/root/t2/a.parquet",
      "/root/t2/b.parquet",
      "/root/t3/a=1/a.parquet",
      "/root/t3/a=1/b.parquet",
      "/root/t4/a=1/a.parquet",
      "/root2/t1/a=1/a.parquet",
      "/root2/t2/a=1/b.parquet",
      "/root2/t3/a=1/a.parquet",
      "/non-root/t1/a",
      "/non-root/t2/a",
      "/non-root/t3/a",
      "/non-root/t4/a",
      "/non-root/t5/a")

    // Note: ignore the /root2, because /root contains more files
    assert(AlluxioUtils.getParentPathForTables(paths).get.equals("/root"))
  }

  test("find large tables") {
    val statusList = Array(
      ("/root/t1", "a.parquet", 100L),
      ("/root/t1", "b.parquet", 300L),
      ("/root/t2", "a.parquet", 200L),
      ("/root/t2", "b.parquet", 400L),
      ("/root/t3", "a.parquet", 200L),
      ("/root/t3", "b.parquet", 600L),
      ("/small_root/t_small_1", "a.parquet", 100L),
      ("/small_root/t_small_2", "a.parquet", 300L),
      ("/small_root/t_small_3", "a.parquet", 400L))
      .map { case (dir, name, size) =>
        genURIStatus(dir, name, size)
      }

    // find tables that avg size > 300L.
    // Note: ignore the tables in /small_root, because /root contains more files
    val largeTables = AlluxioUtils.findLargeTables(statusList, 300L)
    assert(largeTables.equals(mutable.Set("/root/t3")))
    val map = mutable.Map[String, mutable.Set[String]]()
    map("root") = largeTables
    var paths = Seq(
      new Path("s3://root/t1"),
      new Path("s3a://root/t2"),
      new Path("s3://root/t3"))
    assert(!AlluxioUtils.directlyReadLargeTableFromS3(map, paths))
    paths = Seq(
      new Path("s3://root/t3"))
    assert(AlluxioUtils.directlyReadLargeTableFromS3(map, paths))

    // find tables that avg size > 200L
    val largeTables2 = AlluxioUtils.findLargeTables(statusList, 200L)
    assert(largeTables2.equals(mutable.Set("/root/t2", "/root/t3")))
    map("root") = largeTables2
    paths = Seq(
      new Path("s3://root/t1"),
      new Path("s3a://root/t2"),
      new Path("s3://root/t3"))
    assert(!AlluxioUtils.directlyReadLargeTableFromS3(map, paths))

    paths = Seq(
      new Path("xxxx://root/t3")) // scheme is not s3 or s3a
    assert(!AlluxioUtils.directlyReadLargeTableFromS3(map, paths))

    paths = Seq(
      new Path("s3a://root/t2"),
      new Path("s3://root/t3"))
    assert(AlluxioUtils.directlyReadLargeTableFromS3(map, paths))
  }

  test("find large tables, corner case") {
    val statusList = Array(
      ("/root/t1", "_a.parquet", 100L),
      ("/root/t1", "_SUCCESS", 100L),
      ("/root/t1", ".a.parquet", 100L)
    )
      .map { case (dir, name, size) =>
        genURIStatus(dir, name, size)
      }

    // find tables that avg size > 300L
    val largeTables2 = AlluxioUtils.findLargeTables(statusList, 200L)
    assert(largeTables2.equals(mutable.Set()))
  }

  def genURIStatus(parentDir: String, fileName: String, fileSize: Long): URIStatus = {
    val f = new FileInfo()
    f.setPath(parentDir + "/" + fileName)
    f.setName(fileName)
    f.setLength(fileSize)
    new URIStatus(f)
  }
}
