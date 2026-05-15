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

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;

import java.io.IOException;
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
     * Returns the per-prefix credential overlays from a {@link FileIO} that implements
     * Iceberg's {@code SupportsStorageCredentials} interface (1.7+). Each entry is a
     * storage prefix (e.g. {@code "s3://bucket/path/"}) → property overlay map (the
     * vended credentials Iceberg's REST catalog merges into the base FileIO properties
     * for clients targeting that prefix).
     *
     * <p>1.6.x predates {@code SupportsStorageCredentials} entirely and returns an empty
     * map. Keeping this resolution behind the shim lets {@code iceberg/common} compile
     * against 1.6.x without hard-referencing the missing interface.
     */
    Map<String, Map<String, String>> storageCredentialOverlays(FileIO fileIO);

    /**
     * Open a shaded {@link ParquetFileReader} for an iceberg {@link IcebergInputFile}.
     * Footer caching is version-dependent and handled by the per-version impl: 1.10.x can
     * inject a pre-parsed footer through the 4-arg
     * {@code (InputFile, ParquetMetadata, ParquetReadOptions, SeekableInputStream)}
     * constructor and route through {@code FileCache}; 1.6.x / 1.9.x predate that ctor and
     * fall back to {@code ParquetFileReader.open(InputFile, ParquetReadOptions)} which
     * always re-reads the footer from the file.
     */
    ParquetFileReader openParquetReader(
            IcebergInputFile inputFile,
            Path filePath,
            ParquetReadOptions options,
            scala.collection.immutable.Map<String, GpuMetric> metrics) throws IOException;
}
