/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf.bench;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.udf.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * UDF benchmark runner. Measures the end-to-end runtime of:
 *   Read Parquet -> Execute (CPU or GPU) -> Write no-op sink
 *
 * Produces a JSON file with the benchmark results.
 * On error, also produces a separate error log file.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.udf.bench.SparkBenchRunner \
 *     -Dexec.args="--mode cpu --data-path data/bench_data_10M_rows.parquet ..."
 */
public class SparkBenchRunner {

    private static final String DEFAULT_SPARK_LOG_LEVEL = "ERROR";

    public static void main(String[] args) {
        Map<String, String> argMap = new HashMap<>();
        List<String> sparkConfs = new ArrayList<>();
        parseArgs(args, argMap, sparkConfs);

        String mode = SparkUtils.requireArg(argMap, "mode");
        String dataPath = SparkUtils.requireArg(argMap, "data-path");
        String resultPath = SparkUtils.requireArg(argMap, "result-path");
        String sparkLogLevel = argMap.getOrDefault("spark-log-level", DEFAULT_SPARK_LOG_LEVEL);

        // Validate mode
        if (!"cpu".equals(mode) && !"gpu".equals(mode)) {
            throw new IllegalArgumentException(
                "Unknown mode: '" + mode + "'. Must be 'cpu' or 'gpu'.");
        }

        // Build Spark session
        SparkSession.Builder builder = SparkSession.builder();
        SparkUtils.applySparkConfs(builder, sparkConfs);
        SparkSession spark = builder.enableHiveSupport().getOrCreate();
        spark.sparkContext().setLogLevel(sparkLogLevel);

        try {
            // --- START JOB ---
            long startTime = System.nanoTime();
            Dataset<Row> df = spark.read().parquet(dataPath);
            Dataset<Row> resultDf = "cpu".equals(mode)
                ? BenchUtils.executeCpu(spark, df)
                : BenchUtils.executeGpu(spark, df);
            resultDf.write().format("noop").mode("overwrite").save();
            double elapsed = (System.nanoTime() - startTime) / 1e9;
            // --- END JOB ---

            System.err.printf("E2E Runtime (s): %.2f%n", elapsed);

            writeReport(resultPath, mode, dataPath, elapsed, "success", args, null, null);

        } catch (Exception e) {
            System.err.println("Benchmark run failed: " + e.getClass().getSimpleName());
            e.printStackTrace(System.err);

            // Error stack trace is written to a separate error log file.
            String errorLogPath = resultPath.replace("_result.json", "_error.log");
            writeErrorLog(errorLogPath, e);

            writeReport(resultPath, mode, dataPath, -1, "error", args,
                e.getMessage(), errorLogPath);

            System.exit(1);
        } finally {
            spark.stop();
        }

        System.exit(0);
    }

    /** Write a JSON benchmark report containing the result and args. */
    private static void writeReport(
            String path, String mode, String dataPath, double elapsed,
            String status, String[] cliArgs,
            String errorMessage, String errorLogFile) {
        File resultDir = new File(path).getParentFile();
        if (resultDir != null) resultDir.mkdirs();

        try {
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("mode", mode);
            report.put("data_path", dataPath);
            report.put("status", status);
            report.put("e2e_runtime", elapsed);
            report.put("cli_args", Arrays.asList(cliArgs));
            if (errorMessage != null) {
                Map<String, String> error = new LinkedHashMap<>();
                error.put("error_message", errorMessage);
                if (errorLogFile != null) {
                    error.put("error_log_file", errorLogFile);
                }
                report.put("error", error);
            }

            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            DefaultPrettyPrinter printer = new DefaultPrettyPrinter();
            printer.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
            mapper.writer(printer).writeValue(new File(path), report);
            System.err.println("Report written to: " + path);
        } catch (Exception e) {
            System.err.println("Failed to write report: " + e.getMessage());
        }
    }

    /** Write an exception to an error log file. */
    private static void writeErrorLog(String path, Exception e) {
        File logDir = new File(path).getParentFile();
        if (logDir != null) logDir.mkdirs();

        try (PrintWriter pw = new PrintWriter(path)) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            pw.print(sw.toString());
        } catch (Exception writeErr) {
            System.err.println("Failed to write error log: " + writeErr.getMessage());
        }
        System.err.println("Error details written to: " + path);
    }

    /** Parse CLI arguments. */
    private static void parseArgs(String[] args, Map<String, String> map, List<String> sparkConfs) {
        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
                case "--mode":            map.put("mode", args[i + 1]); i += 2; break;
                case "--data-path":       map.put("data-path", args[i + 1]); i += 2; break;
                case "--result-path":     map.put("result-path", args[i + 1]); i += 2; break;
                case "--spark-log-level": map.put("spark-log-level", args[i + 1]); i += 2; break;
                case "--spark-conf":      sparkConfs.add(args[i + 1]); i += 2; break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
    }
}
