/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.udf;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;

/**
 * Shared test utilities.
 */
public class TestUtils {

    /**
     * Install a URLClassLoader as the thread context classloader so that
     * RAPIDS ShimLoader.findURLClassLoader() can discover and mutate it.
     * https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin-api/src/main/scala/com/nvidia/spark/rapids/ShimLoader.scala
     *
     * On Java 17 in a forked Surefire JVM the only classloader is
     * AppClassLoader, which is not a URLClassLoader. Without a URL CL
     * the RAPIDS ShimLoader will throw since it will fail to install
     * shim classes, e.g. https://github.com/NVIDIA/spark-rapids/issues/13915.
     *
     * Must be called before plugin initialization, i.e., before SparkSession.getOrCreate(). 
     * Returns the original context classloader for the caller to restore on tearDown.
     */
    public static ClassLoader installMutableClassLoader() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        if (original instanceof URLClassLoader) {
            return original;
        }
        // Create a child URLClassLoader of original AppClassLoader with empty search path.
        // ShimLoader will populate w/shim directories via addURL(). 
        URLClassLoader wrapper = new URLClassLoader(new URL[0], original);
        Thread.currentThread().setContextClassLoader(wrapper);
        return original;
    }

    /** Compare two DataFrames row-by-row, reporting per-column mismatches. */
    public static void assertDataFrameEquals(Dataset<Row> actual, Dataset<Row> expected) {
        Assert.assertEquals("Schema mismatch", expected.schema(), actual.schema());

        Row[] actualRows = (Row[]) actual.collect();
        Row[] expectedRows = (Row[]) expected.collect();
        Arrays.sort(actualRows, (a, b) -> a.toString().compareTo(b.toString()));
        Arrays.sort(expectedRows, (a, b) -> a.toString().compareTo(b.toString()));

        Assert.assertEquals("Row count mismatch", expectedRows.length, actualRows.length);

        List<String> mismatches = new ArrayList<>();
        String[] fields = actual.schema().fieldNames();
        for (int i = 0; i < actualRows.length; i++) {
            for (String field : fields) {
                Object aVal = actualRows[i].getAs(field);
                Object eVal = expectedRows[i].getAs(field);
                boolean eq = (aVal == null && eVal == null)
                    || (aVal != null && aVal.equals(eVal));
                if (!eq) {
                    mismatches.add(String.format("  [row %d] %s: actual=%s, expected=%s",
                        i, field, aVal, eVal));
                }
            }
        }
        if (!mismatches.isEmpty()) {
            Assert.fail("\nFound " + mismatches.size() + " column-level mismatches:\n"
                + String.join("\n", mismatches) + "\n");
        }
    }
}
