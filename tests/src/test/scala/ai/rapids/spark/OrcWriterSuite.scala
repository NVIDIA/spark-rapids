/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Tests for writing Orc files with the GPU.
 */
class OrcWriterSuite extends SparkQueryCompareTestSuite {

  def readOrc(spark: SparkSession, path: String): DataFrame = spark.read.orc(path)

  def writeOrc(df: DataFrame, path: String): Unit = df.write.orc(path)

  def writeOrcBucket(colNames: String*): (DataFrame, String) => Unit =
    (df, path) => df.write.partitionBy(colNames:_*).orc(path)

  testSparkWritesAreEqual("simple orc write without nulls",
    mixedDf, writeOrc, readOrc)

  testSparkWritesAreEqual("simple orc write with nulls",
    mixedDfWithNulls, writeOrc, readOrc)

  testSparkWritesAreEqual("simple partitioned orc write",
    mixedDfWithBuckets, writeOrcBucket("bucket_1", "bucket_2"), readOrc,
    sort = true /*The order the data is read in on the CPU is not deterministic*/)
}
