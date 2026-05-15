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

package com.nvidia.spark.rapids.iceberg.iceberg19x;

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.NoopMetric$;
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile;
import com.nvidia.spark.rapids.iceberg.IcebergShimUtils;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.parquet.GpuParquetIO;
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.iceberg.spark.source.GpuStructInternalRow;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import scala.Option;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Iceberg 1.9.x shim: uses {@code IdentityPartitionConverters::convertConstant}. */
public class ShimUtilsImpl implements IcebergShimUtils {
    @Override
    public String locationOf(ContentFile<?> f) {
        return f.location();
    }

    private static Object convertConstant(Type type, Object value) {
        Object converted = IdentityPartitionConverters.convertConstant(type, value);
        if (converted instanceof StructLike && type instanceof Types.StructType) {
            GpuStructInternalRow row = new GpuStructInternalRow((Types.StructType) type);
            row.setStruct((StructLike) converted);
            return row;
        }
        return converted;
    }

    @Override
    public Map<Integer, ?> constantsMap(FileScanTask task, Schema readSchema, Table table) {
        if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
            Types.StructType partitionType = Partitioning.partitionType(table);
            return PartitionUtil.constantsMap(task,
                    partitionType,
                    ShimUtilsImpl::convertConstant);
        } else {
            return PartitionUtil.constantsMap(task,
                    ShimUtilsImpl::convertConstant);
        }
    }

    @Override
    public Map<String, Map<String, String>> storageCredentialOverlays(FileIO fileIO) {
        if (!(fileIO instanceof SupportsStorageCredentials)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, String>> result = new HashMap<>();
        for (StorageCredential sc : ((SupportsStorageCredentials) fileIO).credentials()) {
            result.put(sc.prefix(), sc.config());
        }
        return result;
    }

    /**
     * Iceberg 1.9.x stub: footer caching is not supported because the shaded
     * {@code ParquetFileReader} in 1.9.x has no public API to inject pre-parsed footer
     * metadata. Opens via {@code ParquetFileReader.open(InputFile, ParquetReadOptions)}
     * which always reads the footer from the file. The footer-miss counter is bumped
     * on every call so dashboards see non-zero activity instead of silently interpreting
     * "all zeros" as "everything was cached".
     */
    @Override
    public ParquetFileReader openParquetReader(
            IcebergInputFile inputFile,
            Path filePath,
            ParquetReadOptions options,
            scala.collection.immutable.Map<String, GpuMetric> metrics) throws IOException {
        Option<GpuMetric> opt = metrics.get(GpuMetric.FILECACHE_FOOTER_MISSES());
        GpuMetric missCounter = opt.isDefined() ? opt.get() : NoopMetric$.MODULE$;
        missCounter.$plus$eq(1L);
        return ParquetFileReader.open(GpuParquetIO.file(inputFile.getDelegate()), options);
    }
}
