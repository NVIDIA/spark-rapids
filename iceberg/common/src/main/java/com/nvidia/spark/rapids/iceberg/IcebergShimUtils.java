/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

import java.util.Map;

/**
 * Version-specific Iceberg API adapter.
 *
 * <p>Iceberg has breaking API changes across major versions (1.6.x, 1.9.x, 1.10.x).
 * Each version provides a {@code ShimUtilsImpl} in its own sub-package that implements
 * this interface. The correct implementation is selected at runtime by
 * {@link ShimUtils} based on the detected Iceberg version.
 */
public interface IcebergShimUtils {
    /**
     * Returns the fully qualified location URI of an Iceberg {@link ContentFile},
     * e.g. {@code "s3://bucket/path/to/file.parquet"} or {@code "file:/path/to/file.parquet"}.
     * The API to obtain this differs across Iceberg versions ({@code path()} in 1.6.x,
     * {@code location()} in 1.9.x+).
     */
    String locationOf(ContentFile<?> f);

    /**
     * Builds the constants map for a file scan task. Constants include partition values,
     * metadata columns, and any other fields that are constant for the entire scan task.
     *
     * @return a map where keys are Iceberg field IDs from the read schema, and values are
     *         the corresponding constant values converted to Spark's internal representation.
     *         The value type is a wildcard because the actual types vary per field (e.g.
     *         {@code UTF8String}, {@code Long}, {@code Integer}) depending on the Iceberg
     *         type-to-Spark type conversion, which differs across Iceberg versions.
     */
    Map<Integer, ?> constantsMap(FileScanTask task, Schema readSchema, Table table);

    /**
     * True if this Iceberg version exposes a public {@code S3AsyncClient} accessor on
     * {@code S3FileIO}. 1.6.x predates the accessor entirely (no async client built);
     * 1.9.x adds {@code asyncClient()} and 1.10.x adds {@code asyncClient(String)}.
     * Callers must gate {@link #s3AsyncClient} on this method.
     */
    boolean s3AsyncClientSupported();

    /**
     * Returns Iceberg's own {@code S3AsyncClient} for the given {@link FileIO}, dispatching
     * to {@code S3FileIO.asyncClient()} on 1.9.x and {@code S3FileIO.asyncClient(storagePath)}
     * on 1.10.x (which routes to the per-prefix {@code PrefixedS3Client} for tables that
     * configure {@code s3.<prefix>.endpoint} etc.). 1.6.x throws
     * {@link UnsupportedOperationException} since no public accessor exists.
     *
     * <p>Returned as {@link Object} so callers in {@code iceberg/common} avoid an
     * {@code aws-sdk-v2} compile dependency; PerfIO casts internally to
     * {@code S3AsyncClient} where the SDK is already on the classpath.
     */
    Object s3AsyncClient(FileIO fileIO, String storagePath);
}
