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

import java.util.Objects;

public class ProfileInitMsg implements ProfileMsg {
  private static final long serialVersionUID = 1L;

  private final String executorId;
  private final String path;

  public ProfileInitMsg(String executorId, String path) {
    this.executorId = executorId;
    this.path = path;
  }

  public String executorId() {
    return executorId;
  }

  public String path() {
    return path;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ProfileInitMsg)) {
      return false;
    }
    ProfileInitMsg that = (ProfileInitMsg) other;
    return Objects.equals(executorId, that.executorId) &&
        Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(executorId, path);
  }

  @Override
  public String toString() {
    return "ProfileInitMsg(" + executorId + "," + path + ")";
  }
}
