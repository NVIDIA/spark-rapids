package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.lore.GpuLore
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.sum

class GpuLoreSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir with Logging {
  test("Test aggregate") {
    withGpuSparkSession { spark =>
      val df = spark.range(0, 1000)
        .selectExpr("id % 100 as key", "id % 10 as value")
        .groupBy("key")
        .agg(sum("value").as("total"))
        .collect()

      val exec = GpuLore.restoreGpuExec(new Path("/tmp/agg"),
          spark.sparkContext.hadoopConfiguration)
        .executeCollect()

      assert(df === exec)
    }
  }
}
