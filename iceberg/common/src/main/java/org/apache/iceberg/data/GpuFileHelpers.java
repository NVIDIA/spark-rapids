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

package org.apache.iceberg.data;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * This class is inspired by {@link org.apache.iceberg.data.FileHelpers}. This class is only
 * used in tests.
 *
 * We need to copy its code since the original class is in `iceberg-data`'s test module., which
 * is not included in `iceberg-spark-runtime`. If we add `iceberg-data`'s test module as a
 * dependency, we will need to introduce all other dependencies like `iceberg-parquet`.
 */
public class GpuFileHelpers {
  public static DeleteFile writeDeleteFile(
      Table table,
      OutputFile out,
      StructLike partition,
      List<Record> deletes,
      Schema deleteRowSchema)
      throws IOException {
    int[] equalityFieldIds =
        deleteRowSchema.columns().stream().mapToInt(Types.NestedField::fieldId).toArray();
    FileWriterFactory<Record> factory =
        GenericFileWriterFactory.builderFor(table)
            .equalityDeleteRowSchema(deleteRowSchema)
            .equalityFieldIds(equalityFieldIds)
            .build();

    EqualityDeleteWriter<Record> writer =
        factory.newEqualityDeleteWriter(encrypt(out), table.spec(), partition);
    try (Closeable toClose = writer) {
      writer.write(deletes);
    }

    return writer.toDeleteFile();
  }

  private static EncryptedOutputFile encrypt(OutputFile out) {
    return EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY);
  }
}
