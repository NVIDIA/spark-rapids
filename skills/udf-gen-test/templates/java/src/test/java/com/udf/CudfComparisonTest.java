/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CudfComparisonTest {

    private static SparkSession spark;
    private static ClassLoader origContextClassLoader;

    @BeforeClass
    public static void setUp() {
        origContextClassLoader = TestUtils.installMutableClassLoader();
        spark = SparkSession.builder()
            .appName("UDF vs. RapidsUDF Comparison Test")
            .master("local[*]")
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
            .config("spark.rapids.memory.gpu.pool", "NONE")
            .config("spark.rapids.sql.explain", "NONE")
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

    /** TODO: Register the RapidsUDF with Spark. */
    public static void registerRapidsUDF(SparkSession spark, String udfName) { }

    @Test
    public void testCpuVsRapidsUDF() {
        Dataset<Row> testDF = UnitTest.createTestData(spark).repartition(1);

        // Run CPU UDF
        UnitTest.registerUDF(spark, "placeholder_udf_name");
        Dataset<Row> cpuResultDF = UnitTest.executeUDF(
            spark, "placeholder_udf_name", testDF);
        UnitTest.verifyUDFResults(cpuResultDF, testDF);

        // Run RapidsUDF
        registerRapidsUDF(spark, "placeholder_rapids_udf_name");
        Dataset<Row> gpuResultDF = UnitTest.executeUDF(
            spark, "placeholder_rapids_udf_name", testDF);
        UnitTest.verifyUDFResults(gpuResultDF, testDF);

        // Compare
        TestUtils.assertDataFrameEquals(gpuResultDF, cpuResultDF);
    }

    /**
     * TODO: If UnitTest adds extra @Test methods beyond the main result checks,
     * add corresponding comparison tests here. Each case should run the same input
     * through the CPU UDF and the RapidsUDF, apply equivalent assertions to both
     * outputs, and compare the RapidsUDF output against the CPU output.
     */
}
