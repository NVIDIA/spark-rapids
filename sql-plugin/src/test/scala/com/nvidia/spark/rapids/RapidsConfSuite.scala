/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.python.PythonConfEntries
import org.scalatest.funsuite.AnyFunSuite

class RapidsConfSuite extends AnyFunSuite {
  test("unknown RAPIDS configs ignores registered, dynamic, and shim gate configs") {
    val conf = Map(
      RapidsConf.SQL_ENABLED.key -> "true",
      RapidsConf.EXPLAIN.key -> "ALL",
      PythonConfEntries.CONCURRENT_PYTHON_WORKERS.key -> "2",
      "spark.rapids.sql.expression.Add" -> "false",
      "spark.rapids.sql.exec.SortExec" -> "false",
      "spark.rapids.sql.expression.combined.GpuContains" -> "false",
      "spark.rapids.sql.optimizer.gpu.exec.ProjectExec" -> "1.0",
      "spark.rapids.shims.spark350db143.enabled" -> "false",
      "spark.sql.shuffle.partitions" -> "1",
      "spark.cudf.sql.enabled" -> "true",
      "spark.rapids.sql.enabld" -> "true",
      "spark.rapids.shims.spark350db143.enabld" -> "true",
      "spark.rapids.sql.expression.cpuBrdge.enabled" -> "true")

    assert(RapidsConf.unknownRapidsConfs(conf) === Seq(
      "spark.rapids.shims.spark350db143.enabld",
      "spark.rapids.sql.enabld",
      "spark.rapids.sql.expression.cpuBrdge.enabled"))
  }
}
