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
import java.util.Random

import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.apache.orc.impl.HadoopShimsFactory

import org.apache.spark.SparkConf

class OrcEncryptionSuite extends SparkQueryCompareTestSuite {
  testGpuFallback(
    "Write encrypted ORC fallback",
    "DataWritingCommandExec",
    intsDf,
    conf = new SparkConf().set("hadoop.security.key.provider.path", "test:///")
        .set("orc.key.provider", "hadoop")
        .set("orc.encrypt", "pii:ints,more_ints")
        .set("orc.mask", "sha256:ints,more_ints")) {
    frame =>
      // ORC encryption is only allowed in 3.2+
      val isValidTestForSparkVersion = SparkShimImpl.getSparkShimVersion match {
        case SparkShimVersion(major, minor, _) => major == 3 && minor != 1
        case DatabricksShimVersion(major, minor, _, _) => major == 3 && minor != 1
        case ClouderaShimVersion(major, minor, _, _) => major == 3 && minor != 1
        case _ => true
      }
      assume(isValidTestForSparkVersion)

      withCpuSparkSession(session => {
        val conf = session.sessionState.newHadoopConf()
        val provider = HadoopShimsFactory.get.getHadoopKeyProvider(conf, new Random)
        assume(!provider.getKeyNames.isEmpty,
          s"$provider doesn't has the test keys. ORC shim is created with old Hadoop libraries")
      }
      )

      val tempFile = File.createTempFile("orc-encryption-test", "")
      frame.write.mode("overwrite").orc(tempFile.getAbsolutePath)
      frame.selectExpr("*")
  }
}
