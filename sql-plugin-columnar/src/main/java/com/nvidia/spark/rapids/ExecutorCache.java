/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import java.lang.management.ManagementFactory;

import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.CudaComputeMode;

/**
 * Caches executor-related information. Values are initialized lazily to match the previous Scala
 * object semantics.
 */
final class ExecutorCache {
  private ExecutorCache() {
  }

  static CudaComputeMode getCurrentDeviceComputeMode() {
    return CurrentDeviceComputeModeHolder.VALUE;
  }

  static byte[] getCurrentDeviceUuid() {
    return CurrentDeviceUuidHolder.VALUE;
  }

  static String getProcessName() {
    return ProcessNameHolder.VALUE;
  }

  private static final class CurrentDeviceComputeModeHolder {
    private static final CudaComputeMode VALUE = Cuda.getComputeMode();
  }

  private static final class CurrentDeviceUuidHolder {
    private static final byte[] VALUE = Cuda.getGpuUuid();
  }

  private static final class ProcessNameHolder {
    private static final String VALUE = ManagementFactory.getRuntimeMXBean().getName();
  }
}
