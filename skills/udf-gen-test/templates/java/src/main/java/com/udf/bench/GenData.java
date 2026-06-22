/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf.bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.udf.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Generates benchmark data and optionally validates by running
 * BenchUtils.executeCpu and BenchUtils.executeGpu.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.udf.bench.GenData \
 *     -Dexec.args="--rows 1000 --validate --spark-conf k=v ..."
 */
public class GenData {

    public static void main(String[] args) {
        Map<String, String> argMap = new HashMap<>();
        List<String> sparkConfs = new ArrayList<>();
        parseArgs(args, argMap, sparkConfs);

        long rows = Long.parseLong(SparkUtils.requireArg(argMap, "rows"));
        int partitions = Integer.parseInt(argMap.getOrDefault("partitions", "32"));
        boolean validate = argMap.containsKey("validate");
        String outputPath = argMap.get("output-path");

        // Build Spark session
        SparkSession.Builder builder = SparkSession.builder().appName("GenData");
        SparkUtils.applySparkConfs(builder, sparkConfs);
        SparkSession spark = builder.enableHiveSupport().getOrCreate();

        try {
            // Generate synthetic data
            Dataset<Row> df = BenchUtils.generateSyntheticData(spark, rows, partitions);

            // Verify row count
            long actualRows = df.count();
            if (actualRows != rows) {
                System.err.println("Row count mismatch: expected=" + rows
                    + ", actual=" + actualRows);
                System.exit(1);
            }
            System.out.println("Generated " + actualRows + " rows across "
                + partitions + " partitions");

            if (validate) {
                // Validation mode — run both CPU and GPU execute, don't write
                for (String label : new String[]{"cpu", "gpu"}) {
                    try {
                        if ("cpu".equals(label)) {
                            BenchUtils.executeCpu(spark, df).collect();
                        } else {
                            BenchUtils.executeGpu(spark, df).collect();
                        }
                        System.out.println("Validation (" + label + ") passed.");
                    } catch (Exception e) {
                        System.err.println("Validation (" + label + ") failed: "
                            + e.getClass().getSimpleName() + ": " + e.getMessage());
                        e.printStackTrace(System.err);
                        System.exit(1);
                    }
                }
            } else {
                // Generation mode — write to output path
                if (outputPath == null) {
                    throw new IllegalArgumentException(
                        "--output-path is required when not in validation mode");
                }
                df.write().mode("overwrite").parquet(outputPath);
                System.err.println("Successfully generated dataset and saved to: " + outputPath);
            }
        } catch (Exception e) {
            System.err.println("Failed to generate dataset: "
                + e.getClass().getSimpleName());
            e.printStackTrace(System.err);
            System.exit(1);
        } finally {
            spark.stop();
        }

        System.exit(0);
    }

    /** Parse CLI arguments. */
    private static void parseArgs(String[] args, Map<String, String> map, List<String> sparkConfs) {
        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
                case "--rows":        map.put("rows", args[i + 1]); i += 2; break;
                case "--partitions":  map.put("partitions", args[i + 1]); i += 2; break;
                case "--validate":    map.put("validate", "true"); i += 1; break;
                case "--output-path": map.put("output-path", args[i + 1]); i += 2; break;
                case "--spark-conf":  sparkConfs.add(args[i + 1]); i += 2; break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
    }
}
