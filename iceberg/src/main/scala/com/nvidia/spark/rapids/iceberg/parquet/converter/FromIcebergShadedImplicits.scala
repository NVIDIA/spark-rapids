/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg.parquet.converter

import java.util.Optional

import org.apache.iceberg.shaded.org.apache.parquet.column.{Encoding => ShadedEncoding, EncodingStats => ShadedEncodingStats}
import org.apache.iceberg.shaded.org.apache.parquet.column.statistics.{Statistics => ShadedStatistics}
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.{BlockMetaData => ShadedBlockMetaData, ColumnChunkMetaData => ShadedColumnChunkMetaData, ColumnChunkProperties => ShadedColumnChunkProperties, ColumnPath => ShadedColumnPath, CompressionCodecName => ShadedCompressionCodecName}
import org.apache.iceberg.shaded.org.apache.parquet.internal.hadoop.metadata.{IndexReference => ShadedIndexReference}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{ColumnOrder => ShadedColumnOrder, GroupType => ShadedGroupType, LogicalTypeAnnotation => ShadedLogicalTypeAnnotation, MessageType => ShadedMessageType, PrimitiveType => ShadedPrimitiveType, Type => ShadedType}
import org.apache.iceberg.shaded.org.apache.parquet.schema.LogicalTypeAnnotation.{LogicalTypeAnnotationVisitor => ShadedLogicalTypeAnnotationVisitor, TimeUnit => ShadedTimeUnit}
import org.apache.iceberg.shaded.org.apache.parquet.schema.PrimitiveType.{PrimitiveTypeName => ShadedPrimitiveTypeName}
import org.apache.parquet.column.{Encoding, EncodingStats}
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnChunkProperties, ColumnPath, CompressionCodecName}
import org.apache.parquet.internal.hadoop.metadata.IndexReference
import org.apache.parquet.schema.{ColumnOrder, GroupType, LogicalTypeAnnotation, MessageType, PrimitiveType, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import scala.collection.JavaConverters._



object FromIcebergShadedImplicits {
  implicit class ColumnOrderConverter(val columnOrder: ShadedColumnOrder) {
    def unshade: ColumnOrder = {
      columnOrder.getColumnOrderName match {
        case ShadedColumnOrder.ColumnOrderName.UNDEFINED => ColumnOrder.undefined()
        case ShadedColumnOrder.ColumnOrderName.TYPE_DEFINED_ORDER => ColumnOrder.typeDefined()
        case _ => throw new IllegalArgumentException(s"Unknown column order: $columnOrder")
      }
    }
  }

  implicit class PrimitiveTypeNameConverter(val typeName: ShadedPrimitiveTypeName) {
    def unshade: PrimitiveTypeName = {
      PrimitiveTypeName.valueOf(typeName.name())
    }
  }

  implicit class RepetitionConverter(val repetition: ShadedType.Repetition) {
    def unshade: Type.Repetition = {
      Type.Repetition.valueOf(repetition.name())
    }
  }

  implicit class TimeUnitConverter(val inner: ShadedTimeUnit) {
    def unshade: TimeUnit = {
      TimeUnit.valueOf(inner.name())
    }
  }

  implicit class LogicalTypeAnnotationConverter(val inner: ShadedLogicalTypeAnnotation) {
    def unshade: LogicalTypeAnnotation = {
      inner.accept(new ShadedLogicalTypeAnnotationVisitor[LogicalTypeAnnotation] {
        override def visit(stringLogicalType: ShadedLogicalTypeAnnotation
        .StringLogicalTypeAnnotation): Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.stringType())
        }

        override def visit(mapLogicalType: ShadedLogicalTypeAnnotation.MapLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.mapType())
        }

        override def visit(listLogicalType: ShadedLogicalTypeAnnotation.ListLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.listType())
        }

        override def visit(enumLogicalType: ShadedLogicalTypeAnnotation.EnumLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.enumType())
        }

        override def visit(shaded: ShadedLogicalTypeAnnotation
        .DecimalLogicalTypeAnnotation): Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.decimalType(
            shaded.getScale,
            shaded.getPrecision))
        }

        override def visit(dateLogicalType: ShadedLogicalTypeAnnotation.DateLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.dateType())
        }

        override def visit(inner: ShadedLogicalTypeAnnotation.TimeLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.timeType(
            inner.isAdjustedToUTC,
            inner.getUnit.unshade))
        }

        override def visit(inner: ShadedLogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.timestampType(
            inner.isAdjustedToUTC,
            inner.getUnit.unshade))
        }

        override def visit(inner: ShadedLogicalTypeAnnotation.IntLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.intType(
            inner.getBitWidth,
            inner.isSigned))
        }

        override def visit(inner: ShadedLogicalTypeAnnotation.JsonLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.jsonType())
        }

        override def visit(inner: ShadedLogicalTypeAnnotation.BsonLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.bsonType())
        }

        override def visit(inner: ShadedLogicalTypeAnnotation.UUIDLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.uuidType())
        }

        override def visit(inner: ShadedLogicalTypeAnnotation.IntervalLogicalTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance())
        }

        override def visit(inner: ShadedLogicalTypeAnnotation.MapKeyValueTypeAnnotation)
        : Optional[LogicalTypeAnnotation] = {
          Optional.of(LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance())
        }
      }).orElseGet(() => {
        throw new IllegalArgumentException(s"Unknown logical type annotation: $inner")
      })
    }
  }


  implicit class TypeConverter(val inner: ShadedType) {
    def unshade: Type = {
      inner match {
        case t: ShadedPrimitiveType => t.unshade
        case t: ShadedMessageType => t.unshade
        case t: ShadedGroupType => t.unshade
        case _ => throw new IllegalArgumentException(s"Unknown type: $inner")
      }
    }
  }

  implicit class MessageTypeConverter(val inner: ShadedMessageType) {
    def unshade: MessageType = {
      val builder = Types.buildMessage()
      inner.getFields.forEach { field =>
        builder.addField(field.unshade)
      }

      Option(inner.getId).foreach { id =>
        builder.id(id.intValue())
      }

      builder.named(inner.getName)
    }
  }

   implicit class GroupTypeConverter(val inner: ShadedGroupType) {
     def unshade: GroupType = {
       val builder = Types.buildGroup(inner.getRepetition.unshade)

       inner.getFields.forEach { field =>
         builder.addField(field.unshade)
       }

       Option(inner.getId).foreach { id =>
         builder.id(id.intValue())
       }

       builder.named(inner.getName)
     }
  }


  implicit class PrimitiveTypeConverter(val inner: ShadedPrimitiveType) {
    def unshade: PrimitiveType = {
      var builder = Types.primitive(inner.getPrimitiveTypeName.unshade,
          inner.getRepetition.unshade)
        .columnOrder(inner.columnOrder().unshade)
        .length(inner.getTypeLength)

      Option(inner.getLogicalTypeAnnotation).foreach { ann =>
        builder = builder.as(ann.unshade)
      }

      Option(inner.getId).foreach { id =>
        builder = builder.id(id.intValue())
      }

      builder.named(inner.getName)
    }
  }

  implicit class EncodingConverter(val inner: ShadedEncoding) {
    def unshade: Encoding = {
      Encoding.valueOf(inner.name())
    }
  }

  implicit class ColumnPathConverter(val inner: ShadedColumnPath) {
    def unshade: ColumnPath = {
      ColumnPath.get(inner.toArray: _*)
    }
  }

  implicit class CompressionCodecNameConverter(val inner: ShadedCompressionCodecName) {
    def unshade: CompressionCodecName = {
      CompressionCodecName.valueOf(inner.name())
    }
  }

  implicit class ColumnChunkPropertiesConverter(val inner: ShadedColumnChunkProperties) {
    def unshade: ColumnChunkProperties = {
      ColumnChunkProperties.get(
        inner.getPath.unshade,
        inner.getPrimitiveType.unshade,
        inner.getCodec.unshade,
        inner.getEncodings.asScala.map(_.unshade).asJava,
      )
    }
  }

  implicit class EncodingStatsConverter(val inner: ShadedEncodingStats) {
    def unshade: EncodingStats = {
      val builder = new EncodingStats.Builder()

      if (inner.usesV2Pages()) {
        builder.withV2Pages()
      }

      for (encoding <- inner.getDictionaryEncodings.asScala) {
        builder.addDictEncoding(encoding.unshade, inner.getNumDictionaryPagesEncodedAs(encoding))
      }
      for (encoding <- inner.getDataEncodings.asScala) {
        builder.addDataEncoding(encoding.unshade, inner.getNumDataPagesEncodedAs(encoding))
      }

      builder.build()
    }
  }

  implicit class IndexReferenceConverter(val inner: ShadedIndexReference) {
    def unshade: IndexReference = {
      new IndexReference(inner.getOffset, inner.getLength)
    }
  }

  implicit class StatisticsConverter(val inner: ShadedStatistics[_]) {
    def unshade: Statistics[_] = {
      Statistics.getBuilderForReading(inner.`type`().unshade)
        .withMin(inner.getMinBytes)
        .withMax(inner.getMaxBytes)
        .withNumNulls(inner.getNumNulls)
        .build()
    }
  }

  implicit class ColumnChunkMetadataConverter(val inner: ShadedColumnChunkMetaData) {
    def unshade: ColumnChunkMetaData  = {
      if (inner.isEncrypted) {
        throw new UnsupportedOperationException("Encrypted column chunks are not supported")
      }

      ColumnChunkMetaData.get(
        inner.getPath.unshade,
        inner.getPrimitiveType.unshade,
        inner.getCodec.unshade,
        inner.getEncodingStats.unshade,
        inner.getEncodings.asScala.map(_.unshade).asJava,
        inner.getStatistics.unshade,
        inner.getFirstDataPageOffset,
        inner.getDictionaryPageOffset,
        inner.getValueCount,
        inner.getTotalSize,
        inner.getTotalUncompressedSize)
    }
  }

  implicit class BlockMetaDataConverter(val inner: ShadedBlockMetaData) {
    def unshade: BlockMetaData = {
      val ret = new BlockMetaData
      ret.setRowCount(inner.getRowCount)
      ret.setTotalByteSize(inner.getTotalByteSize)
      ret.setPath(inner.getPath)
      ret.setOrdinal(inner.getOrdinal)
      ret.setRowIndexOffset(inner.getRowIndexOffset)

      inner.getColumns.asScala.foreach { column =>
        ret.addColumn(column.unshade)
      }

      ret
    }
  }
}
