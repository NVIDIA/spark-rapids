# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Java source code fixtures for integration tests.
"""

from .utils import replace_java_todo_method

NAME = "java"
REPLACE_TODO_FN = replace_java_todo_method
TEST_SELECTOR_FLAG = "-Dtest"

# ---------------------------------------------------------------------------
# UDF source code
# ---------------------------------------------------------------------------

UDF_SOURCE = """\
package com.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class IntegerMultiplyBy2UDF extends UDF {
    public Integer evaluate(Integer value) {
        if (value == null) return null;
        return value * 2;
    }
}
"""

RAPIDS_UDF_SOURCE = """\
package com.udf;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Scalar;
import com.nvidia.spark.RapidsUDF;
import org.apache.hadoop.hive.ql.exec.UDF;

public class IntegerMultiplyBy2RapidsUDF extends UDF implements RapidsUDF {
    public Integer evaluate(Integer value) {
        if (value == null) return null;
        return value * 2;
    }

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        try (Scalar two = Scalar.fromInt(2)) {
            return args[0].mul(two);
        }
    }
}
"""

SQL_SOURCE = """\
SELECT *,
  value * 2 AS result
FROM test_table
"""

# ---------------------------------------------------------------------------
# Unit test methods
# ---------------------------------------------------------------------------

UNIT_TEST_METHODS = {
    "createTestData": """\
    public static Dataset<Row> createTestData(SparkSession spark) {
        StructType schema = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("value", DataTypes.IntegerType, true)
        });
        List<Row> data = Arrays.asList(
            RowFactory.create(1, 123),
            RowFactory.create(2, 0),
            RowFactory.create(3, -5),
            RowFactory.create(4, null)
        );
        return spark.createDataFrame(data, schema);
    }""",
    "registerUDF": """\
    public static void registerUDF(SparkSession spark, String udfName) {
        spark.sql("CREATE TEMPORARY FUNCTION " + udfName
            + " AS 'com.udf.IntegerMultiplyBy2UDF'");
    }""",
    "executeUDF": """\
    public static Dataset<Row> executeUDF(SparkSession spark, String udfName, Dataset<Row> testDF) {
        testDF.createOrReplaceTempView("test_table");
        return spark.sql("SELECT *, " + udfName
            + "(value) AS result FROM test_table");
    }""",
    "verifyUDFResults": """\
    public static void verifyUDFResults(Dataset<Row> resultDF, Dataset<Row> testDF) {
        Row[] results = (Row[]) resultDF.sort("id").collect();
        Assert.assertEquals(246, (int) results[0].getAs("result"));
        Assert.assertEquals(0, (int) results[1].getAs("result"));
        Assert.assertEquals(-10, (int) results[2].getAs("result"));
        Assert.assertTrue(results[3].isNullAt(results[3].fieldIndex("result")));
    }""",
}

RAPIDS_UDF_REGISTER = """\
    public static void registerRapidsUDF(SparkSession spark, String udfName) {
        spark.sql("CREATE TEMPORARY FUNCTION " + udfName
            + " AS 'com.udf.IntegerMultiplyBy2RapidsUDF'");
    }"""

NATIVE_RAPIDS_UDF_REGISTER = """\
    public static void registerRapidsUDF(SparkSession spark, String udfName) {
        spark.sql("CREATE TEMPORARY FUNCTION " + udfName
            + " AS 'com.udf.IntegerMultiplyBy2NativeRapidsUDF'");
    }"""

# ---------------------------------------------------------------------------
# BenchUtils methods
# ---------------------------------------------------------------------------

BENCH_GENERATE = """\
    public static Dataset<Row> generateSyntheticData(
            SparkSession spark, long numRows, int numPartitions) {
        Dataset<Row> baseDF = spark.range(0, numRows, 1, numPartitions).toDF("id");
        return baseDF.select(
            col("id"),
            expr("CAST(rand() * 1000 AS INT)").alias("value")
        );
    }"""

BENCH_CPU = """\
    public static Dataset<Row> executeCpu(SparkSession spark, Dataset<Row> df) {
        df.createOrReplaceTempView("bench_table");
        spark.sql("CREATE TEMPORARY FUNCTION integer_multiply_by_2"
            + " AS 'com.udf.IntegerMultiplyBy2UDF'");
        return spark.sql("SELECT *, integer_multiply_by_2(value)"
            + " AS result FROM bench_table");
    }"""

BENCH_GPU_CUDF = """\
    public static Dataset<Row> executeGpu(SparkSession spark, Dataset<Row> df) {
        df.createOrReplaceTempView("bench_table");
        spark.sql("CREATE TEMPORARY FUNCTION integer_multiply_by_2_rapids"
            + " AS 'com.udf.IntegerMultiplyBy2RapidsUDF'");
        return spark.sql("SELECT *, integer_multiply_by_2_rapids(value)"
            + " AS result FROM bench_table");
    }"""

BENCH_GPU_CUDA = """\
    public static Dataset<Row> executeGpu(SparkSession spark, Dataset<Row> df) {
        df.createOrReplaceTempView("bench_table");
        spark.sql("CREATE TEMPORARY FUNCTION integer_multiply_by_2_native"
            + " AS 'com.udf.IntegerMultiplyBy2NativeRapidsUDF'");
        return spark.sql("SELECT *, integer_multiply_by_2_native(value)"
            + " AS result FROM bench_table");
    }"""

BENCH_GPU_SQL = """\
    public static Dataset<Row> executeGpu(SparkSession spark, Dataset<Row> df) {
        df.createOrReplaceTempView("bench_table");
        try {
            String sqlContent = new String(
                java.nio.file.Files.readAllBytes(
                    java.nio.file.Paths.get("src/main/resources/integer_multiply_by_2.sql")));
            String benchSql = sqlContent.replace("test_table", "bench_table");
            return spark.sql(benchSql);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }"""

# ---------------------------------------------------------------------------
# MicroBenchRunner methods
# ---------------------------------------------------------------------------

MICRO_PREPARE_CPU = """\
    public static Object[] prepareCpuData(HostColumnVector[] hostColumns, int numRows) {
        Integer[] values = new Integer[numRows];
        for (int i = 0; i < numRows; i++) {
            values[i] = hostColumns[1].isNull(i)
                ? null : hostColumns[1].getInt(i);
        }
        return new Object[] { values };
    }"""

MICRO_EXECUTE_CPU = """\
    public static void executeCpu(Object[] data, int numRows) {
        Integer[] values = (Integer[]) data[0];
        com.udf.IntegerMultiplyBy2UDF udf = new com.udf.IntegerMultiplyBy2UDF();
        for (int i = 0; i < numRows; i++) {
            udf.evaluate(values[i]);
        }
    }"""

MICRO_EXECUTE_GPU = """\
    public static ColumnVector executeGpu(Table table, int numRows) {
        com.udf.IntegerMultiplyBy2RapidsUDF udf = new com.udf.IntegerMultiplyBy2RapidsUDF();
        return udf.evaluateColumnar(numRows, table.getColumn(1));
    }"""

MICRO_EXECUTE_GPU_CUDA = """\
    public static ColumnVector executeGpu(Table table, int numRows) {
        com.udf.IntegerMultiplyBy2NativeRapidsUDF udf =
            new com.udf.IntegerMultiplyBy2NativeRapidsUDF();
        return udf.evaluateColumnar(numRows, table.getColumn(1));
    }"""
