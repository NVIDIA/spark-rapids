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

package com.nvidia.spark.rapids.iceberg.iceberg16x;

import com.nvidia.spark.rapids.iceberg.IcebergShimUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.source.GpuBaseReader;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.types.Types;

import java.util.Map;

/** Iceberg 1.6.x shim: uses {@code ContentFile.path()} and {@code GpuBaseReader::convertConstant}. */
public class ShimUtilsImpl implements IcebergShimUtils {
    @Override
    public String locationOf(ContentFile<?> f) {
        return f.path().toString();
    }

    @Override
    public Map<Integer, ?> constantsMap(FileScanTask task, Schema readSchema, Table table) {
        if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
            Types.StructType partitionType = Partitioning.partitionType(table);
            return PartitionUtil.constantsMap(task,
                    partitionType,
                    GpuBaseReader::convertConstant);
        } else {
            return PartitionUtil.constantsMap(task, GpuBaseReader::convertConstant);
        }
    }

    @Override
    public boolean s3AsyncClientSupported() {
        return false;
    }

    @Override
    public Object s3AsyncClient(FileIO fileIO, String storagePath) {
        throw new UnsupportedOperationException(
                "Iceberg 1.6.x does not expose a public S3AsyncClient on S3FileIO; "
                        + "PerfIO vectored S3 reads require Iceberg 1.9 or newer.");
    }
}
