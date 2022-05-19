/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.GpuParquetUtils;
import com.nvidia.spark.rapids.ParquetPartitionReader;
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter;
import com.nvidia.spark.rapids.iceberg.spark.SparkSchemaUtil;
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
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types.MessageTypeBuilder;

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
  private final String debugDumpPrefix;
  private final scala.collection.immutable.Map<String, GpuMetric> metrics;

  public GpuParquetReader(
      InputFile input, Schema expectedSchema, ParquetReadOptions options,
      NameMapping nameMapping, Expression filter, boolean caseSensitive,
      Map<Integer, ?> idToConstant, GpuDeleteFilter deleteFilter,
      PartitionedFile partFile, Configuration conf, int maxBatchSizeRows,
      long maxBatchSizeBytes, String debugDumpPrefix,
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
    this.debugDumpPrefix = debugDumpPrefix;
    this.metrics = metrics;
  }

  @Override
  public org.apache.iceberg.io.CloseableIterator<ColumnarBatch> iterator() {
    try (ParquetFileReader reader = newReader(input, options)) {
      MessageType fileSchema = reader.getFileMetaData().getSchema();

      MessageType typeWithIds;
//      MessageType projection;
      if (ParquetSchemaUtil.hasIds(fileSchema)) {
        typeWithIds = fileSchema;
//        projection = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema);
      } else if (nameMapping != null) {
        typeWithIds = ParquetSchemaUtil.applyNameMapping(fileSchema, nameMapping);
//        projection = ParquetSchemaUtil.pruneColumns(typeWithIds, expectedSchema);
      } else {
        typeWithIds = ParquetSchemaUtil.addFallbackIds(fileSchema);
//        projection = ParquetSchemaUtil.pruneColumnsFallback(fileSchema, expectedSchema);
      }

      List<BlockMetaData> rowGroups = reader.getRowGroups();
      List<BlockMetaData> filteredRowGroups = Lists.newArrayListWithCapacity(rowGroups.size());

//      boolean[] startRowPositions[i] = new boolean[rowGroups.size()];
//
//      // Fetch all row groups starting positions to compute the row offsets of the filtered row groups
//      Map<Long, Long> offsetToStartPos = generateOffsetToStartPos(expectedSchema);
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
//        startRowPositions[i] = offsetToStartPos == null ? 0 : offsetToStartPos.get(rowGroup.getStartingPos());
        boolean shouldRead = filter == null || (
            statsFilter.shouldRead(typeWithIds, rowGroup) &&
                dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup)));
        if (shouldRead) {
          filteredRowGroups.add(rowGroup);
        }
      }

      StructType sparkSchema = SparkSchemaUtil.convertWithoutConstants(expectedSchema, idToConstant);
      MessageType fileReadSchema = buildFileReadSchema(fileSchema);
      Seq<BlockMetaData> clippedBlocks = GpuParquetUtils.clipBlocksToSchema(
          fileReadSchema, filteredRowGroups, caseSensitive);

      // reuse Parquet scan code to read the raw data from the file
      ParquetPartitionReader partReader = new ParquetPartitionReader(conf, partFile,
          new Path(input.location()), clippedBlocks, fileReadSchema, caseSensitive,
          sparkSchema, debugDumpPrefix, maxBatchSizeRows, maxBatchSizeBytes, metrics,
          true, true, true);

      return new GpuIcebergReader(expectedSchema, partReader, deleteFilter, idToConstant);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create/close reader for file: " + input, e);
    }
  }

//  private Map<Long, Long> generateOffsetToStartPos(Schema schema) {
//    if (schema.findField(MetadataColumns.ROW_POSITION.fieldId()) == null) {
//      return null;
//    }
//
//    try (ParquetFileReader fileReader = newReader(input, ParquetReadOptions.builder().build())) {
//      Map<Long, Long> offsetToStartPos = Maps.newHashMap();
//
//      long curRowCount = 0;
//      for (int i = 0; i < fileReader.getRowGroups().size(); i += 1) {
//        BlockMetaData meta = fileReader.getRowGroups().get(i);
//        offsetToStartPos.put(meta.getStartingPos(), curRowCount);
//        curRowCount += meta.getRowCount();
//      }
//
//      return offsetToStartPos;
//
//    } catch (IOException e) {
//      throw new UncheckedIOException("Failed to create/close reader for file: " + input, e);
//    }
//  }

  private static ParquetFileReader newReader(InputFile file, ParquetReadOptions options) {
    try {
      return ParquetFileReader.open(ParquetIO.file(file), options);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open Parquet file: " + file.location(), e);
    }
  }

  // Filter out any unreferenced and metadata columns and reorder the columns
  // to match the expected schema.
  private MessageType buildFileReadSchema(MessageType fileSchema) {
    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      return (MessageType)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new ReorderColumns(fileSchema, idToConstant));
    } else {
      return (MessageType)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new ReorderColumnsFallback(fileSchema, idToConstant));
    }
  }

  private static class ReorderColumns extends TypeWithSchemaVisitor<Type> {
    private final MessageType fileSchema;
    private final Map<Integer, ?> idToConstant;

    public ReorderColumns(MessageType fileSchema, Map<Integer, ?> idToConstant) {
      this.fileSchema = fileSchema;
      this.idToConstant = idToConstant;
    }

    @Override
    public Type message(Types.StructType expected, MessageType message, List<Type> fields) {
      MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();
      List<Type> newFields = filterAndReorder(expected, fields);
      // TODO: Avoid re-creating type if nothing changed
      for (Type type : newFields) {
        builder.addField(type);
      }
      return builder.named(message.getName());
    }

    @Override
    public Type struct(Types.StructType expected, GroupType struct, List<Type> fields) {
      // TODO: Avoid re-creating type if nothing changed
      List<Type> newFields = filterAndReorder(expected, fields);
      return struct.withNewFields(newFields);
    }

    @Override
    public Type list(Types.ListType expectedList, GroupType list, Type element) {
      boolean hasConstant = expectedList.fields().stream()
          .anyMatch(f -> idToConstant.containsKey(f.fieldId()));
      if (hasConstant) {
        throw new UnsupportedOperationException("constant column in list");
      }
      Type originalElement = list.getFields().get(0);
      if (Objects.equals(element, originalElement)) {
        return list;
      } else if (originalElement.isRepetition(Type.Repetition.REPEATED)) {
        return list.withNewFields(element);
      }
      return list.withNewFields(list.getType(0).asGroupType().withNewFields(element));
    }

    @Override
    public Type map(Types.MapType expectedMap, GroupType map, Type key, Type value) {
      boolean hasConstant = expectedMap.fields().stream()
          .anyMatch(f -> idToConstant.containsKey(f.fieldId()));
      if (hasConstant) {
        throw new UnsupportedOperationException("constant column in map");
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
          }
        }
      }

      return reorderedFields;
    }
  }

  private static class ReorderColumnsFallback extends ReorderColumns {
    public ReorderColumnsFallback(MessageType fileSchema, Map<Integer, ?> idToConstant) {
      super(fileSchema, idToConstant);
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
