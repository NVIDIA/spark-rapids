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

public class ProfileJobStageQueryMsg implements ProfileMsg {
  private static final long serialVersionUID = 1L;

  private final int[] activeJobs;
  private final int[] activeStages;

  public ProfileJobStageQueryMsg(int[] activeJobs, int[] activeStages) {
    this.activeJobs = activeJobs;
    this.activeStages = activeStages;
  }

  public int[] activeJobs() {
    return activeJobs;
  }

  public int[] activeStages() {
    return activeStages;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ProfileJobStageQueryMsg)) {
      return false;
    }
    ProfileJobStageQueryMsg that = (ProfileJobStageQueryMsg) other;
    return Objects.equals(activeJobs, that.activeJobs) &&
        Objects.equals(activeStages, that.activeStages);
  }

  @Override
  public int hashCode() {
    return Objects.hash(activeJobs, activeStages);
  }

  @Override
  public String toString() {
    return "ProfileJobStageQueryMsg(" + activeJobs + "," + activeStages + ")";
  }
}
