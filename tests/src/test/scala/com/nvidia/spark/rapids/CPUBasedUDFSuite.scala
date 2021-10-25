/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col

class CPUBasedUDFSuite extends SparkQueryCompareTestSuite {

  val cpuEnabledConf: SparkConf = new SparkConf()
    .set(RapidsConf.ENABLE_CPU_BASED_UDF.key, "true")
    .set(RapidsConf.EXPLAIN.key, "all")

  testSparkResultsAreEqual("CPU Based Scala UDF-Boolean",
    booleanDf,
    cpuEnabledConf,
    decimalTypeEnabled = false) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[Boolean]())
    frame.select(noopUDF(col("bools")))
  }

  testSparkResultsAreEqual("CPU Based Scala UDF-Short",
    shortsFromCsv,
    enableCsvConf.set(RapidsConf.ENABLE_CPU_BASED_UDF.key, "true"),
    decimalTypeEnabled = false) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[Short]())
    frame.select(noopUDF(col("shorts")))
  }

  testSparkResultsAreEqual("CPU Based Scala UDF-Mixed(int,long,double,string)",
      mixedDfWithNulls,
      cpuEnabledConf,
      decimalTypeEnabled = false) { frame =>
    val iNoopUDF = frame.sparkSession.udf.register("iNoopUDF", new NoopUDF[Int]())
    val lNoopUDF = frame.sparkSession.udf.register("lNoopUDF", new NoopUDF[Long]())
    val dNoopUDF = frame.sparkSession.udf.register("dNoopUDF", new NoopUDF[Double]())
    val sNoopUDF = frame.sparkSession.udf.register("sNoopUDF", new NoopUDF[String]())
    frame.select(
        iNoopUDF(col("ints")),
        lNoopUDF(col("longs")),
        dNoopUDF(col("doubles")),
        sNoopUDF(col("strings")))
  }

  testSparkResultsAreEqual("CPU Based Scala UDF-Date",
    datesDf,
    cpuEnabledConf,
    decimalTypeEnabled = false) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[Date]())
    frame.select(noopUDF(col("dates")))
  }

  testSparkResultsAreEqual("CPU Based Scala UDF-Timestamp",
    timestampsDf,
    cpuEnabledConf,
    decimalTypeEnabled = false) { frame =>
    val noopUDF = frame.sparkSession.udf.register("NoopUDF", new NoopUDF[Timestamp]())
    frame.select(noopUDF(col("timestamps")))
  }

  // TODO Nested types: Array, Struct, Map
}

/** An UDF for test only, returning the input directly. */
class NoopUDF[T] extends Function[T, T] with Serializable {
  override def apply(v: T): T = v
}
