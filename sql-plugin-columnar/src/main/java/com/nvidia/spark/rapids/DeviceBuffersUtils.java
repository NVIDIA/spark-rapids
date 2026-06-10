/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.DeviceMemoryBuffer;

public final class DeviceBuffersUtils {
  private DeviceBuffersUtils() {}

  public static BaseDeviceMemoryBuffer[] incRefCount(BaseDeviceMemoryBuffer[] bufs) {
    BaseDeviceMemoryBuffer[] ret = new BaseDeviceMemoryBuffer[bufs.length];
    int initialized = 0;
    try {
      for (BaseDeviceMemoryBuffer buf : bufs) {
        buf.incRefCount();
        ret[initialized] = buf;
        initialized++;
      }
      return ret;
    } catch (Throwable t) {
      closeAll(ret, initialized, t);
      throw t;
    }
  }

  public static DeviceMemoryBuffer[] allocateBuffers(long[] bufSizes) {
    DeviceMemoryBuffer[] ret = new DeviceMemoryBuffer[bufSizes.length];
    int initialized = 0;
    try (DeviceMemoryBuffer singleBuf = DeviceMemoryBuffer.allocate(sum(bufSizes))) {
      long curPos = 0L;
      for (long len : bufSizes) {
        ret[initialized] = singleBuf.slice(curPos, len);
        initialized++;
        curPos += len;
      }
      return ret;
    } catch (Throwable t) {
      closeAll(ret, initialized, t);
      throw t;
    }
  }

  private static long sum(long[] values) {
    long ret = 0L;
    for (long value : values) {
      ret += value;
    }
    return ret;
  }

  private static void closeAll(AutoCloseable[] values, int count, Throwable cause) {
    for (int i = 0; i < count; i++) {
      AutoCloseable value = values[i];
      if (value != null) {
        try {
          value.close();
        } catch (Throwable t) {
          cause.addSuppressed(t);
        }
      }
    }
  }
}
