/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.spill;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DiskHandleBuilderMockUtil {
  public static MockedStatic<?> mock(DiskHandleStore.DiskHandleBuilder builder) {
    MockedStatic<DiskHandleStore> mockedStatic = Mockito.mockStatic(DiskHandleStore.class);
    mockedStatic.when(() -> DiskHandleStore.makeBuilder()).thenReturn(builder);
    System.out.println("Result: " + DiskHandleStore.makeBuilder().blockId());
    return mockedStatic;
  }

  public static void foo() {
    System.out.println("Result2: " + DiskHandleStore.makeBuilder().blockId());
  }
}
