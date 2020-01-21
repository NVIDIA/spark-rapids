/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.cudf;

import java.util.Optional;

public class CudfColumnVector {
  public static long getAddress(MemoryBuffer buff) {
    return buff.getAddress();
  }

  public static void copyFromDeviceBuffer(HostMemoryBuffer hb, DeviceMemoryBuffer db) {
    hb.copyFromDeviceBuffer(db);
  }

  public static ColumnVector createColumnVector(
      DType type, long rows,
      long nullCount, DeviceMemoryBuffer dataBuffer, DeviceMemoryBuffer validityBuffer,
      DeviceMemoryBuffer offsetBuffer) {
    return new ColumnVector(type, rows, Optional.of(nullCount), dataBuffer,
        validityBuffer, offsetBuffer);
  }

  public static ColumnVector createColumnVector(
      DType type,
      long rows,
      long nullCount,
      DeviceMemoryBuffer dataBuffer,
      DeviceMemoryBuffer validityBuffer,
      DeviceMemoryBuffer offsetBuffer,
      boolean resetOffsetsFromFirst) {
    if (type == DType.STRING) { //string column
      return new ColumnVector(
          type,
          rows,
          Optional.of(nullCount),
          dataBuffer,
          validityBuffer,
          offsetBuffer);
    } else {
      return new ColumnVector(type, rows, Optional.of(nullCount), dataBuffer,
          validityBuffer, null);
    }
  }
}
