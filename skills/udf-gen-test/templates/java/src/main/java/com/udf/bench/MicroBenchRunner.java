/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf.bench;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.CudaMemInfo;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Rmm;
import ai.rapids.cudf.RmmAllocationMode;
import ai.rapids.cudf.Table;

/**
 * Microbenchmark runner for CPU vs. RapidsUDF. Measures UDF execution time on in-memory dataset.
 *
 * Reads Parquet file (produced by GenData) via cuDF Table.readParquet.
 * Benchmarks CPU (row-by-row evaluate) and GPU (evaluateColumnar) paths.
 * Data loading and host/device transfers are not part of timing.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.udf.bench.MicroBenchRunner \
 *     -Dexec.args="--mode all --data-path data/bench_data --rows 1000000"
 */
public class MicroBenchRunner {

    private static final int DEFAULT_WARMUP = 2;
    private static final int DEFAULT_MEASURED = 4;
    private static final float DEFAULT_RMM_ALLOC_FRACTION = 0.9f;

    /**
     * TODO: Extract column data from host memory into Java objects.
     *
     * Called once before CPU timing loop. Convert HostColumnVectors to
     * array of Java objects for executeCpu.
     * Use hostColumns[i].getJavaString(row), .getInt(row), .getDouble(row),
     * .getStruct(row), .getList(row), etc. to extract values into typed arrays.
     *
     * This is outside of the timing loop due to overhead of extracting/boxing
     * Java types from cuDF.
     *
     * Example for a UDF that takes (String, int):
     * <pre>{@code
     *   String[] col0 = new String[numRows];
     *   int[] col1 = new int[numRows];
     *   for (int i = 0; i < numRows; i++) {
     *       col0[i] = hostColumns[0].getJavaString(i);
     *       col1[i] = hostColumns[1].getInt(i);
     *   }
     *   return new Object[] { col0, col1 };
     * }</pre>
     *
     * @param hostColumns all columns copied to host memory
     * @param numRows     number of rows in the dataset
     * @return array of typed Java arrays, one per UDF input column
     */
    public static Object[] prepareCpuData(HostColumnVector[] hostColumns, int numRows) {
        // TODO: Extract columns to Java arrays
        return null; // TODO
    }

    /**
     * TODO: Execute the CPU UDF on Java data row-by-row.
     *
     * Example:
     * <pre>{@code
     *   import com.udf.PlaceholderUDFName;
     *   String[] col0 = (String[]) data[0];
     *   int[] col1 = (int[]) data[1];
     *   PlaceholderUDFName udf = new PlaceholderUDFName();
     *   for (int i = 0; i < numRows; i++) {
     *       udf.evaluate(col0[i], col1[i]);
     *   }
     * }</pre>
     *
     * @param data    Java arrays from {@link #prepareCpuData}
     * @param numRows number of rows in the dataset
     */
    public static void executeCpu(Object[] data, int numRows) {
        // TODO: Cast arrays and call CPU UDF evaluate() per row
    }

    /**
     * TODO: Execute the GPU UDF via evaluateColumnar.
     *
     * Example:
     * <pre>{@code
     *   import com.udf.PlaceholderRapidsUDFName;
     *   PlaceholderRapidsUDFName udf = new PlaceholderRapidsUDFName();
     *   return udf.evaluateColumnar(numRows,
     *       table.getColumn(0), table.getColumn(1));
     * }</pre>
     *
     * @param table   the dataset loaded on GPU
     * @param numRows number of rows in the dataset
     * @return result ColumnVector (NOTE: caller must close)
     */
    public static ColumnVector executeGpu(Table table, int numRows) {
        // TODO: Instantiate RapidsUDF and call evaluateColumnar()
        return null; // TODO
    }

    public static void main(String[] args) {
        Map<String, String> argMap = new HashMap<>();
        parseArgs(args, argMap);

        String dataPath = argMap.get("data-path");
        if (dataPath == null) {
            throw new IllegalArgumentException("--data-path is required");
        }
        String mode = argMap.getOrDefault("mode", "all");
        int maxRows = Integer.parseInt(argMap.getOrDefault("rows", "-1"));
        float rmmAllocFraction = Float.parseFloat(argMap.getOrDefault("pool-fraction", String.valueOf(DEFAULT_RMM_ALLOC_FRACTION)));
        int warmup = Integer.parseInt(argMap.getOrDefault("warmup", String.valueOf(DEFAULT_WARMUP)));
        int measured = Integer.parseInt(argMap.getOrDefault("measured", String.valueOf(DEFAULT_MEASURED)));
        boolean profile = argMap.containsKey("profile");

        // Resolve execution mode
        if (!"cpu".equals(mode) && !"gpu".equals(mode) && !"all".equals(mode)) {
            throw new IllegalArgumentException(
                "Unknown mode: '" + mode + "'. Must be 'cpu', 'gpu', or 'all'.");
        }
        boolean runCpu = "cpu".equals(mode) || "all".equals(mode);
        boolean runGpu = "gpu".equals(mode) || "all".equals(mode);

        // Initialize RMM pool
        if (!Rmm.isInitialized()) {
            CudaMemInfo memInfo = Cuda.memGetInfo();
            long poolSize = (long) (memInfo.free * rmmAllocFraction) & ~255L;
            Rmm.initialize(RmmAllocationMode.POOL, null, poolSize);
        }

        // Read Parquet data into cuDF table
        try (Table table = readParquetData(dataPath, maxRows)) {
            int numRows = (int) table.getRowCount();
            int numCols = table.getNumberOfColumns();
            double mb = getTableSizeMB(table);
            System.out.printf("Loaded %,d rows x %d columns (%.1f MB) from: %s%n",
                numRows, numCols, mb, dataPath);
            System.out.printf("Microbenchmark: mode=%s, warmup=%d, measured=%d%n",
                mode, warmup, measured);

            double cpuMinMs = Double.NaN;
            double gpuMinMs = Double.NaN;

            // --- CPU Benchmark ---
            if (runCpu) {
                HostColumnVector[] hostColumns = copyAllToHost(table);
                try {
                    Object[] cpuData = prepareCpuData(hostColumns, numRows);
                    long[] times = runBenchmark(warmup, measured, false, () ->
                        executeCpu(cpuData, numRows));
                    double medianMs = times[times.length / 2] / 1e6;
                    cpuMinMs = times[0] / 1e6;
                    System.out.printf("   CPU  | %,14d rows | median %10.1f ms | min %10.1f ms%n",
                        numRows, medianMs, cpuMinMs);
                } catch (Exception e) {
                    System.err.printf("CPU benchmark failed: %s%n", e.getMessage());
                    e.printStackTrace(System.err);
                    System.exit(1);
                } finally {
                    closeAll(hostColumns);
                }
            }

            // --- GPU Benchmark ---
            if (runGpu) {
                try {
                    long[] times = runBenchmark(warmup, measured, profile, () -> {
                        try (ColumnVector result = executeGpu(table, numRows)) {}
                    });
                    double medianMs = times[times.length / 2] / 1e6;
                    gpuMinMs = times[0] / 1e6;
                    System.out.printf("   GPU  | %,14d rows | median %10.1f ms | min %10.1f ms%n",
                        numRows, medianMs, gpuMinMs);
                } catch (Exception e) {
                    System.err.printf("GPU benchmark failed: %s%n", e.getMessage());
                    e.printStackTrace(System.err);
                    System.exit(1);
                }
            }

            // --- Speedup ---
            if (!Double.isNaN(cpuMinMs) && !Double.isNaN(gpuMinMs)) {
                double speedup = cpuMinMs / gpuMinMs;
                System.out.printf(">> Speedup: %.2fx (CPU/GPU best)%n", speedup);
            }
        }

        System.exit(0);
    }

    /**
     * Run warmup + measured iterations. Profile the measured iterations if enabled.
     * @return sorted array of measured elapsed times in nanoseconds
     */
    private static long[] runBenchmark(int warmup, int measured, boolean profile, Runnable block) {
        for (int i = 0; i < warmup; i++) {
            block.run();
        }
        long[] times = new long[measured];
        for (int i = 0; i < measured; i++) {
            if (profile) Cuda.profilerStart();
            long start = System.nanoTime();
            block.run();
            times[i] = System.nanoTime() - start;
            if (profile) Cuda.profilerStop();
        }
        Arrays.sort(times);
        return times;
    }

    /**
     * Read Parquet partition files from a directory into a cuDF Table.
     * Reads files in sorted order, stopping once maxRows is reached.
     * @param maxRows stop after accumulating this many rows; -1 means read all.
     */
    private static Table readParquetData(String dataPath, int maxRows) {
        File[] partFiles = new File(dataPath).listFiles((dir, name) -> name.endsWith(".parquet"));
        if (partFiles == null || partFiles.length == 0) {
            throw new IllegalArgumentException("No .parquet files found in: " + dataPath);
        }
        Arrays.sort(partFiles);

        Table[] tables = new Table[partFiles.length];
        int count = 0;
        long totalRows = 0;
        try {
            for (int i = 0; i < partFiles.length; i++) {
                tables[i] = Table.readParquet(partFiles[i]);
                count++;
                totalRows += tables[i].getRowCount();
                if (maxRows > 0 && totalRows >= maxRows) break;
            }
            if (count == 1) {
                return limitTable(tables[0], maxRows);
            }
            try (Table combined = Table.concatenate(Arrays.copyOf(tables, count))) {
                return limitTable(combined, maxRows);
            }
        } finally {
            closeAll(tables);
        }
    }

    /** Return a new Table with at most numRows rows. */
    private static Table limitTable(Table table, int numRows) {
        int n = (numRows <= 0)
            ? (int) table.getRowCount()
            : (int) Math.min(numRows, table.getRowCount());
        ColumnVector[] cols = new ColumnVector[table.getNumberOfColumns()];
        try {
            for (int i = 0; i < cols.length; i++) {
                cols[i] = table.getColumn(i).subVector(0, n);
            }
            return new Table(cols);
        } finally {
            closeAll(cols);
        }
    }

    /** Get the size of the table in MB. */
    private static double getTableSizeMB(Table table) {
        long bytes = 0;
        for (int i = 0; i < table.getNumberOfColumns(); i++) {
            bytes += table.getColumn(i).getDeviceMemorySize();
        }
        return bytes / (1024.0 * 1024.0);
    }

    /** Copy all device columns to host memory. */
    private static HostColumnVector[] copyAllToHost(Table table) {
        HostColumnVector[] hostCols = new HostColumnVector[table.getNumberOfColumns()];
        try {
            for (int i = 0; i < hostCols.length; i++) {
                hostCols[i] = table.getColumn(i).copyToHost();
            }
            return hostCols;
        } catch (Exception e) {
            closeAll(hostCols);
            throw e;
        }
    }

    /** Close all resources in an array. */
    private static void closeAll(AutoCloseable[] resources) {
        if (resources != null) {
            for (AutoCloseable r : resources) {
                if (r != null) {
                    try { r.close(); } catch (Exception ignore) {}
                }
            }
        }
    }

    /** Parse CLI arguments. */
    private static void parseArgs(String[] args, Map<String, String> map) {
        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
                case "--mode":        map.put("mode", args[i + 1]); i += 2; break;
                case "--data-path":   map.put("data-path", args[i + 1]); i += 2; break;
                case "--warmup":      map.put("warmup", args[i + 1]); i += 2; break;
                case "--measured":    map.put("measured", args[i + 1]); i += 2; break;
                case "--rows":        map.put("rows", args[i + 1]); i += 2; break;
                case "--pool-fraction": map.put("pool-fraction", args[i + 1]); i += 2; break;
                case "--profile":     map.put("profile", "true"); i += 1; break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
    }
}
