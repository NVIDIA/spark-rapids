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

 let qualReportSummary = {
  "totalApps": {
    "numeric": 0,
    "header": "Total Applications",
    "statsPercentage": "%",
    "statsTimeFrame": "Apps with Estimated End Time",
    "totalAppsDurations": "0 ms",
    "totalAppsDurationLabel": "Total Run Durations",
  },
  "candidates": {
    "numeric": 0,
    "header": "RAPIDS Candidates",
    "statsPercentage": "%",
    "statsTimeFrame": "Fit for GPU acceleration",
  },
  "speedups": {
    "numeric": "N/A",
    "header": "Speedups",
    "statsPercentage": "% Expected GPU Accelration",
    "statsTimeFrame": "of Total Durations",
    "totalSqlDFDurations" : "0 ms",
    "totalSqlDFDurationsLabel" : "Total SqlDF Durations",
  },
  "tlc": {
    "numeric": 0,
    "header": "Apps that need TLC",
    "statsPercentage": "% Needs more information",
    "statsTimeFrame": "We found apps with potential problems",
  },
};
