/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.DType;

import java.util.ArrayList;
import java.util.List;

/**
 * Options for reading protobuf data.
 * This class is used to configure the protobuf reader with field schema information.
 */
public class ProtobufOptions {

    /**
     * Field information for protobuf parsing.
     */
    public static class FieldInfo {
        private final String name;
        private final int fieldNumber;
        private final DType dtype;
        private final int scale;

        public FieldInfo(String name, int fieldNumber, DType dtype) {
            this(name, fieldNumber, dtype, 0);
        }

        public FieldInfo(String name, int fieldNumber, DType dtype, int scale) {
            this.name = name;
            this.fieldNumber = fieldNumber;
            this.dtype = dtype;
            this.scale = scale;
        }

        public String getName() { return name; }
        public int getFieldNumber() { return fieldNumber; }
        public DType getDType() { return dtype; }
        public int getScale() { return scale; }
    }

    private final List<FieldInfo> fields;
    private final boolean hadoopSequenceFile;

    private ProtobufOptions(Builder builder) {
        this.fields = new ArrayList<>(builder.fields);
        this.hadoopSequenceFile = builder.hadoopSequenceFile;
    }

    /**
     * Get the list of field definitions.
     */
    public List<FieldInfo> getFields() {
        return fields;
    }

    /**
     * Get the schema (list of field definitions).
     * Alias for getFields() for compatibility.
     */
    public List<FieldInfo> getSchema() {
        return fields;
    }

    /**
     * Get the field names as an array.
     */
    public String[] getFieldNames() {
        return fields.stream().map(FieldInfo::getName).toArray(String[]::new);
    }

    /**
     * Get the field numbers as an array.
     */
    public int[] getFieldNumbers() {
        return fields.stream().mapToInt(FieldInfo::getFieldNumber).toArray();
    }

    /**
     * Get the DType IDs for all fields.
     */
    public int[] getDTypeIds() {
        return fields.stream().mapToInt(f -> f.getDType().getTypeId().getNativeId()).toArray();
    }

    /**
     * Get the scales for all fields.
     */
    public int[] getScales() {
        return fields.stream().mapToInt(FieldInfo::getScale).toArray();
    }

    /**
     * Whether the input is a Hadoop SequenceFile.
     */
    public boolean isHadoopSequenceFile() {
        return hadoopSequenceFile;
    }

    /**
     * Get the number of fields.
     */
    public int getNumFields() {
        return fields.size();
    }

    /**
     * Create a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for ProtobufOptions.
     */
    public static class Builder {
        private final List<FieldInfo> fields = new ArrayList<>();
        private boolean hadoopSequenceFile = false;

        /**
         * Add a field to read.
         *
         * @param name Field name
         * @param fieldNumber Protocol buffer field number
         * @param dtype Data type for the field
         * @return this builder
         */
        public Builder withField(String name, int fieldNumber, DType dtype) {
            fields.add(new FieldInfo(name, fieldNumber, dtype));
            return this;
        }

        /**
         * Set whether the input is a Hadoop SequenceFile.
         *
         * @param isSequenceFile true if the input is a Hadoop SequenceFile
         * @return this builder
         */
        public Builder withHadoopSequenceFile(boolean isSequenceFile) {
            this.hadoopSequenceFile = isSequenceFile;
            return this;
        }

        /**
         * Build the options.
         */
        public ProtobufOptions build() {
            if (fields.isEmpty()) {
                throw new IllegalStateException("Schema must have at least one field");
            }
            return new ProtobufOptions(this);
        }
    }
}

