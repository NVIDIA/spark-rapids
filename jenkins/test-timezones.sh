#!/bin/bash
#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex

# Test cases are used in both pre-merge test and nightly-build test.
# We choose up to 7 cases so as to run through all time zones every week during nightly-build test.
export time_zones_test_cases=(
  "Asia/Shanghai"   # CST
  "America/New_York"   # PST
  "Asia/Hebron"   # contains the most zone transitions
  "Canada/Newfoundland"   # contains may zone transitions as well
  "America/Belize"   # contains the most intra-hour zone transitions
  "America/Punta_Arenas"   # contains the most intra-minute zone transitions
  "Africa/Casablanca"   # frequently changing transition rules between the release of Java8 and Java17
)
