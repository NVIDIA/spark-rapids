/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded.*;

import com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded;
import com.nvidia.spark.rapids.parquet.CpuCompressionConfig$;
import com.nvidia.spark.rapids.parquet.GpuParquetUtils;
import com.nvidia.spark.rapids.parquet.ParquetPartitionReader;
import org.apache.iceberg.parquet.ParquetDictionaryRowGroupFilter;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.shaded.org.apache.parquet.ParquetReadOptions;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.iceberg.shaded.org.apache.parquet.schema.GroupType;
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType;
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType;
import org.apache.iceberg.shaded.org.apache.parquet.schema.Type;
import scala.collection.Seq;

import com.nvidia.spark.rapids.DateTimeRebaseCorrected$;
import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.PartitionReaderWithBytesRead;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.iceberg.spark.source.GpuIcebergReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** GPU version of Apache Iceberg's ParquetReader class */
public class GpuParquetReader extends CloseableGroup implements CloseableIterable<ColumnarBatch> {
  private final InputFile input;
  private final Schema expectedSchema;
  private final ParquetReadOptions options;
  private final Expression filter;
  private final boolean caseSensitive;
  private final NameMapping nameMapping;
  private final Map<Integer, ?> idToConstant;
  private final GpuDeleteFilter deleteFilter;
  private final PartitionedFile partFile;
  private final Configuration conf;
  private final int maxBatchSizeRows;
  private final long maxBatchSizeBytes;
  private final long targetBatchSizeBytes;
  private final boolean useChunkedReader;
  private final long maxChunkedReaderMemoryUsageSizeBytes;
  private final scala.Option<String> debugDumpPrefix;
  private final boolean debugDumpAlways;
  private final scala.collection.immutable.Map<String, GpuMetric> metrics;

  public GpuParquetReader(
      InputFile input, Schema expectedSchema, ParquetReadOptions options,
      NameMapping nameMapping, Expression filter, boolean caseSensitive,
      Map<Integer, ?> idToConstant, GpuDeleteFilter deleteFilter,
      PartitionedFile partFile, Configuration conf, int maxBatchSizeRows,
      long maxBatchSizeBytes, long targetBatchSizeBytes, boolean useChunkedReader,
      long maxChunkedReaderMemoryUsageSizeBytes,
      scala.Option<String> debugDumpPrefix, boolean debugDumpAlways,
      scala.collection.immutable.Map<String, GpuMetric> metrics) {
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.nameMapping = nameMapping;
    this.filter = filter;
    this.caseSensitive = caseSensitive;
    this.idToConstant = idToConstant;
    this.deleteFilter = deleteFilter;
    this.partFile = partFile;
    this.conf = conf;
    this.maxBatchSizeRows = maxBatchSizeRows;
    this.maxBatchSizeBytes = maxBatchSizeBytes;
    this.targetBatchSizeBytes = targetBatchSizeBytes;
    this.useChunkedReader = useChunkedReader;
    this.maxChunkedReaderMemoryUsageSizeBytes = maxChunkedReaderMemoryUsageSizeBytes;
    this.debugDumpPrefix = debugDumpPrefix;
    this.debugDumpAlways = debugDumpAlways;
    this.metrics = metrics;
  }

  @Override
  public org.apache.iceberg.io.CloseableIterator<ColumnarBatch> iterator() {
    try (ParquetFileReader reader = newReader(input, options)) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();
      List<BlockMetaData> filteredRowGroups = filterRowGroups(reader, nameMapping,
          expectedSchema, filter, caseSensitive);
      List<org.apache.parquet.hadoop.metadata.BlockMetaData> filteredRowGroupsUnshade =
          filteredRowGroups.stream()
              .map(FromIcebergShaded::unshade)
              .collect(Collectors.toList());

      ReorderColumns reorder = ParquetSchemaUtil.hasIds(fileSchema) ? new ReorderColumns(idToConstant)
          : new ReorderColumnsFallback(idToConstant);
      MessageType fileReadSchema = (MessageType) TypeWithSchemaVisitor.visit(
          expectedSchema.asStruct(), fileSchema, reorder);
      org.apache.parquet.schema.MessageType fileReadSchemaUnshaded = unshade(fileReadSchema);

      Seq<org.apache.parquet.hadoop.metadata.BlockMetaData> clippedBlocks =
          GpuParquetUtils.clipBlocksToSchema(fileReadSchemaUnshaded,
          filteredRowGroupsUnshade, caseSensitive);
      StructType partReaderSparkSchema = (StructType) TypeWithSchemaVisitor.visit(
          expectedSchema.asStruct(), fileReadSchema, new SparkSchemaConverter());

      // reuse Parquet scan code to read the raw data from the file
      ParquetPartitionReader parquetPartReader = new ParquetPartitionReader(conf, partFile,
          new Path(input.location()), clippedBlocks, fileReadSchemaUnshaded, caseSensitive,
          partReaderSparkSchema, debugDumpPrefix, debugDumpAlways,
          maxBatchSizeRows, maxBatchSizeBytes, targetBatchSizeBytes, useChunkedReader,
          maxChunkedReaderMemoryUsageSizeBytes,
          CpuCompressionConfig$.MODULE$.disabled(),
          metrics,
          DateTimeRebaseCorrected$.MODULE$, // dateRebaseMode
          DateTimeRebaseCorrected$.MODULE$, // timestampRebaseMode
          true, // hasInt96Timestamps
          false // useFieldId
      );
      PartitionReaderWithBytesRead partReader = new PartitionReaderWithBytesRead(parquetPartReader);

      Map<Integer, ?> updatedConstants = addNullsForMissingFields(idToConstant, reorder.getMissingFields());
      return new GpuIcebergReader(expectedSchema, partReader, deleteFilter, updatedConstants);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create/close reader for file: " + input, e);
    }
  }

  public static List<BlockMetaData> filterRowGroups(ParquetFileReader reader,
      NameMapping nameMapping, Schema expectedSchema, Expression filter, boolean caseSensitive) {
    MessageType fileSchema = reader.getFileMetaData().getSchema();

    MessageType typeWithIds;
    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      typeWithIds = fileSchema;
    } else if (nameMapping != null) {
      typeWithIds = ParquetSchemaUtil.applyNameMapping(fileSchema, nameMapping);
    } else {
      typeWithIds = ParquetSchemaUtil.addFallbackIds(fileSchema);
    }

    List<BlockMetaData> rowGroups = reader.getRowGroups();
    List<BlockMetaData> filteredRowGroups = Lists.newArrayListWithCapacity(rowGroups.size());

    if (expectedSchema.findField(MetadataColumns.ROW_POSITION.fieldId()) != null) {
      throw new UnsupportedOperationException("row position meta column not implemented");
    }

    ParquetMetricsRowGroupFilter statsFilter = null;
    ParquetDictionaryRowGroupFilter dictFilter = null;
    if (filter != null) {
      statsFilter = new ParquetMetricsRowGroupFilter(expectedSchema, filter, caseSensitive);
      dictFilter = new ParquetDictionaryRowGroupFilter(expectedSchema, filter, caseSensitive);
    }

    for (BlockMetaData rowGroup : rowGroups) {
      boolean shouldRead = filter == null || (
          statsFilter.shouldRead(typeWithIds, rowGroup) &&
              dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup)));
      if (shouldRead) {
        filteredRowGroups.add(rowGroup);
      }
    }
    return filteredRowGroups;
  }

  public static ParquetFileReader newReader(InputFile file, ParquetReadOptions options) {
    try {
      return ParquetFileReader.open(ParquetIO.file(file), options);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open Parquet file: " + file.location(), e);
    }
  }

  public static Map<Integer, ?> addNullsForMissingFields(Map<Integer, ?> idToConstant,
                                                          Set<Integer> missingFields) {
    if (missingFields.isEmpty()) {
      return idToConstant;
    }
    Map<Integer, ?> updated = Maps.newHashMap(idToConstant);
    for (Integer field : missingFields) {
      updated.put(field, null);
    }
    return updated;
  }

  public static class ReorderColumns extends TypeWithSchemaVisitor<Type> {
    private final Map<Integer, ?> idToConstant;
    private final Set<Integer> missingFields = Sets.newHashSet();

    public ReorderColumns(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    public Set<Integer> getMissingFields() {
      return missingFields;
    }

    @Override
    public Type message(Types.StructType expected, MessageType message, List<Type> fields) {
      org.apache.iceberg.shaded.org.apache.parquet.schema.Types.MessageTypeBuilder builder =
          org.apache.iceberg.shaded.org.apache.parquet.schema.Types.buildMessage();
      List<Type> newFields = filterAndReorder(expected, fields);
      for (Type type : newFields) {
        builder.addField(type);
      }
      return builder.named(message.getName());
    }

    @Override
    public Type struct(Types.StructType expected, GroupType struct, List<Type> fields) {
      List<Type> newFields = filterAndReorder(expected, fields);
      return struct.withNewFields(newFields);
    }

    @Override
    public Type list(Types.ListType expectedList, GroupType list, Type element) {
      if (expectedList != null) {
        boolean hasConstant = expectedList.fields().stream()
          .anyMatch(f -> idToConstant.containsKey(f.fieldId()));
        if (hasConstant) {
          throw new UnsupportedOperationException("constant column in list");
        }
      }
      GroupType repeated = list.getType(0).asGroupType();
      Type originalElement = repeated.getType(0);
      if (Objects.equals(element, originalElement)) {
        return list;
      }
      return list.withNewFields(repeated.withNewFields(element));
    }

    @Override
    public Type map(Types.MapType expectedMap, GroupType map, Type key, Type value) {
      if (expectedMap != null) {
        boolean hasConstant = expectedMap.fields().stream()
          .anyMatch(f -> idToConstant.containsKey(f.fieldId()));
        if (hasConstant) {
          throw new UnsupportedOperationException("constant column in map");
        }
      }
      GroupType repeated = map.getFields().get(0).asGroupType();
      Type originalKey = repeated.getType(0);
      Type originalValue = repeated.getType(0);
      if (Objects.equals(key, originalKey) && Objects.equals(value, originalValue)) {
        return map;
      }
      return map.withNewFields(repeated.withNewFields(key, value));
    }

    @Override
    public Type primitive(org.apache.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive) {
      return primitive;
    }

    /** Returns true if a column with the specified ID should be ignored when loading the file data */
    private boolean shouldIgnoreFileColumn(int id) {
      return idToConstant.containsKey(id) ||
          id == MetadataColumns.ROW_POSITION.fieldId() &&
          id == MetadataColumns.IS_DELETED.fieldId();
    }

    private List<Type> filterAndReorder(Types.StructType expected, List<Type> fields) {
      // match the expected struct's order
      Map<Integer, Type> typesById = Maps.newHashMap();
      for (Type fieldType : fields) {
        if (fieldType.getId() != null) {
          int id = fieldType.getId().intValue();
          typesById.put(id, fieldType);
        }
      }

      List<Types.NestedField> expectedFields = expected != null ?
          expected.fields() : ImmutableList.of();
      List<Type> reorderedFields = Lists.newArrayListWithCapacity(expectedFields.size());
      for (Types.NestedField field : expectedFields) {
        int id = field.fieldId();
        if (!shouldIgnoreFileColumn(id)) {
          Type newField = typesById.get(id);
          if (newField != null) {
            reorderedFields.add(newField);
          } else {
            missingFields.add(id);
          }
        }
      }

      return reorderedFields;
    }
  }

  public static class ReorderColumnsFallback extends ReorderColumns {
    public ReorderColumnsFallback(Map<Integer, ?> idToConstant) {
      super(idToConstant);
    }

    @Override
    public Type message(Types.StructType expected, MessageType message, List<Type> fields) {
      // the top level matches by ID, but the remaining IDs are missing
      return super.struct(expected, message, fields);
    }

    @Override
    public Type struct(Types.StructType ignored, GroupType struct, List<Type> fields) {
      // the expected struct is ignored because nested fields are never found when the IDs are missing
      return struct;
    }
  }
}
