/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlComparisonTest {

    private static SparkSession spark;
    private static ClassLoader origContextClassLoader;

    @BeforeClass
    public static void setUp() {
        origContextClassLoader = TestUtils.installMutableClassLoader();
        spark = SparkSession.builder()
            .appName("UDF vs. SQL Comparison Test")
            .master("local[*]")
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
            .config("spark.rapids.skipGpuArchitectureCheck", "true")
            .config("spark.rapids.sql.mode", "explainOnly")
            .config("spark.sql.adaptive.enabled", "false")
            .enableHiveSupport()
            .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        if (spark != null) spark.stop();
        if (origContextClassLoader != null) {
            Thread.currentThread().setContextClassLoader(origContextClassLoader);
        }
    }

    @Test
    public void testUdfVsSqlExpression() throws IOException {
        Dataset<Row> testDF = UnitTest.createTestData(spark).repartition(1);

        // Run CPU UDF
        UnitTest.registerUDF(spark, "placeholder_udf_name");
        Dataset<Row> udfResultDF = UnitTest.executeUDF(
            spark, "placeholder_udf_name", testDF);
        UnitTest.verifyUDFResults(udfResultDF, testDF);

        // Read and execute SQL expression
        testDF.createOrReplaceTempView("test_table");
        String sqlContent = new String(
            Files.readAllBytes(Paths.get("src/main/resources/placeholder_udf_name.sql")));
        Dataset<Row> sqlResultDF = spark.sql(sqlContent);
        UnitTest.verifyUDFResults(sqlResultDF, testDF);

        // Compare
        TestUtils.assertDataFrameEquals(sqlResultDF, udfResultDF);

        // Verify GPU compatibility
        SparkUtils.assertPlanRunsOnGpu(sqlResultDF);
    }

    /**
     * TODO: If UnitTest adds extra @Test methods beyond the main result checks,
     * add corresponding comparison tests here. Each case should run the same input
     * through the CPU UDF and the SQL expression, apply equivalent assertions to
     * both outputs, and compare the SQL output against the CPU output.
     */
}
