/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf;

import com.nvidia.spark.rapids.ExplainPlan;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Spark utility methods.
 */
public class SparkUtils {

    /**
     * Apply key=value Spark configs to a builder.
     *
     * @param builder     the SparkSession builder to configure
     * @param sparkConfs  "spark.key=value" config strings
     * @return the same builder, for chaining
     */
    public static SparkSession.Builder applySparkConfs(
            SparkSession.Builder builder, List<String> sparkConfs) {
        for (String conf : sparkConfs) {
            String[] kv = conf.split("=", 2);
            if (kv.length == 2) builder.config(kv[0], kv[1]);
        }
        return builder;
    }

    /**
     * Get a required argument from a parsed argument map, or throw.
     *
     * @param parsed the parsed argument map
     * @param key    the argument key (without "--" prefix)
     * @return the argument value
     * @throws IllegalArgumentException if the key is missing
     */
    public static String requireArg(Map<String, String> parsed, String key) {
        String val = parsed.get(key);
        if (val == null) {
            throw new IllegalArgumentException("--" + key + " is required");
        }
        return val;
    }

    /** 
     * Ops that cause fallback but can be ignored, since they are strictly used for testing:
     * - RDDScanExec/LocalTableScanExec: surfaces due to spark.createDataFrame()
     * - CollectLimitExec: surfaces during dataframe collection (e.g. df.show())
     * - ToPrettyString: surfaces due to df.show()
     */
    private static final Set<String> IGNORE_OPERATIONS = new HashSet<>(
        Arrays.asList("RDDScanExec", "LocalTableScanExec", "CollectLimitExec", "ToPrettyString")
    );

    /**
     * Assert that the DataFrame's plan can run on GPU.
     * NOTE: This is only reliable in explainOnly mode, with AQE disabled.
     *
     * @param df the DataFrame to check
     * @throws RuntimeException if any operations cannot run on GPU
     */
    public static void assertPlanRunsOnGpu(Dataset<Row> df) {
        assertPlanRunsOnGpu(df, false);
    }

    /**
     * Assert that the DataFrame's plan can run on GPU.
     * NOTE: This is only reliable in explainOnly mode, with AQE disabled.
     *
     * @param df             the DataFrame to check
     * @param returnFullPlan if true, include the full plan in the error message
     * @throws RuntimeException if any operations cannot run on GPU
     */
    public static void assertPlanRunsOnGpu(Dataset<Row> df, boolean returnFullPlan) {
        String plan = getGpuPlan(df);
        List<String> unsupportedOps = getUnsupportedOps(plan);
        if (!unsupportedOps.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Some operations cannot run on GPU.\nFound the following unsupported ops:\n");
            for (String op : unsupportedOps) {
                sb.append("- ").append(op).append("\n");
            }
            if (returnFullPlan) {
                sb.append("\nFull physical plan:\n").append(plan);
            }
            throw new RuntimeException(sb.toString());
        }
    }

    /** Get the potential GPU plan using the RAPIDS ExplainPlan API. */
    private static String getGpuPlan(Dataset<Row> df) {
        return ExplainPlan.explainPotentialGpuPlan(df, "NOT_ON_GPU");
    }

    /** Parse the plan for unsupported operations (lines starting with '!'). */
    private static List<String> getUnsupportedOps(String plan) {
        List<String> result = new ArrayList<>();
        for (String line : plan.split("\n")) {
            // Each unsupported line looks like: ![Exec] <OPERATION> cannot run on GPU
            String trimmed = line.trim();
            if (trimmed.startsWith("!")) {
                int start = trimmed.indexOf('<');
                int end = trimmed.indexOf('>');
                if (start >= 0 && end > start) {
                    String op = trimmed.substring(start + 1, end);
                    if (!IGNORE_OPERATIONS.contains(op)) {
                        result.add(trimmed);
                    }
                }
            }
        }
        return result;
    }
}
