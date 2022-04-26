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

package com.nvidia.spark.rapids.iceberg.orc;

import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcConf;

/** GPU version of Apache Iceberg's ORC class */
public class GpuORC {
  private GpuORC() {
  }

  public static ReadBuilder read(InputFile file) {
    return new ReadBuilder(file);
  }

  public static class ReadBuilder {
    private final InputFile file;
    private final Configuration conf;
    private Schema projectSchema = null;
    private Schema readerExpectedSchema = null;
    private Map<Integer, ?> idToConstant = null;
    private Long start = null;
    private Long length = null;
    private Expression filter = null;
    private boolean caseSensitive = true;
    private NameMapping nameMapping = null;

    private ReadBuilder(InputFile file) {
      Preconditions.checkNotNull(file, "Input file cannot be null");
      this.file = file;
      if (file instanceof HadoopInputFile) {
        this.conf = new Configuration(((HadoopInputFile) file).getConf());
      } else {
        this.conf = new Configuration();
      }

      // We need to turn positional schema evolution off since we use column name based schema evolution for projection
      this.conf.setBoolean(OrcConf.FORCE_POSITIONAL_EVOLUTION.getHiveConfName(), false);
    }

    /**
     * Restricts the read to the given range: [start, start + length).
     *
     * @param newStart the start position for this read
     * @param newLength the length of the range this read should scan
     * @return this builder for method chaining
     */
    public ReadBuilder split(long newStart, long newLength) {
      this.start = newStart;
      this.length = newLength;
      return this;
    }

    public ReadBuilder project(Schema newSchema) {
      this.projectSchema = newSchema;
      return this;
    }

    public ReadBuilder readerExpectedSchema(Schema newSchema) {
      this.readerExpectedSchema = newSchema;
      return this;
    }

    public ReadBuilder constants(Map<Integer, ?> constants) {
      this.idToConstant = constants;
      return this;
    }

    public ReadBuilder caseSensitive(boolean newCaseSensitive) {
      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(this.conf, newCaseSensitive);
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    public ReadBuilder config(String property, String value) {
      conf.set(property, value);
      return this;
    }

    public ReadBuilder filter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    public ReadBuilder withNameMapping(NameMapping newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    public <D> CloseableIterable<D> build() {
      Preconditions.checkNotNull(projectSchema, "Schema is required");
      throw new UnsupportedOperationException();
    }
  }
}
