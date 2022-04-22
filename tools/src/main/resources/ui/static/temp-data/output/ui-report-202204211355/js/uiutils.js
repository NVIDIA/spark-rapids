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

/* globals $, Mustache, qualReportSummary */

const twoDecimalFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function padZeroes(num) {
  return ("0" + num).slice(-2);
}

/* eslint-disable no-unused-vars */
function formatTimeMillis(timeMillis) {
  if (timeMillis <= 0) {
    return "-";
  } else {
    var dt = new Date(timeMillis);
    return formatDateString(dt);
  }
}

/* eslint-enable no-unused-vars */

function formatDateString(dt) {
  return dt.getFullYear() + "-" +
      padZeroes(dt.getMonth() + 1) + "-" +
      padZeroes(dt.getDate()) + " " +
      padZeroes(dt.getHours()) + ":" +
      padZeroes(dt.getMinutes()) + ":" +
      padZeroes(dt.getSeconds());
}

function formatDuration(milliseconds) {
  if (milliseconds < 100) {
    return parseInt(milliseconds).toFixed(1) + " ms";
  }
  var seconds = milliseconds * 1.0 / 1000;
  if (seconds < 1) {
    return seconds.toFixed(1) + " s";
  }
  if (seconds < 60) {
    return seconds.toFixed(0) + " s";
  }
  var minutes = seconds / 60;
  if (minutes < 10) {
    return minutes.toFixed(1) + " min";
  } else if (minutes < 60) {
    return minutes.toFixed(0) + " min";
  }
  var hours = minutes / 60;
  return hours.toFixed(1) + " h";
}

// don't filter on hidden html elements for an sType of title-numeric
function getColumnIndex(columns, columnName) {
  for (var i = 0; i < columns.length; i++) {
    if (columns[i].name == columnName)
      return i;
  }
  return -1;
}

/** calculations of CPU Processor **/

var CPUPercentThreshold = 40.0;

function totalCPUPercentageStyle(cpuPercent) {
  // Red if GC time over GCTimePercent of total time
  return (cpuPercent < CPUPercentThreshold) ?
      ("hsla(0, 100%, 50%, " + totalCPUPercentageAlpha(CPUPercentThreshold - cpuPercent) + ")") : "";
}

function totalCPUPercentageAlpha(actualCPUPercentage) {
  return actualCPUPercentage >= 0 ?
      (Math.min(actualCPUPercentage / 40.0 + 0.4, 1)) : 1;
}

function totalCPUPercentageColor(cpuPercent) {
  return (cpuPercent < CPUPercentThreshold) ? "white" : "black";
}


/* define recommendation grouping */
const recommendationRanges = {
  "A": {low: 7.5, high: 1000.0},
  "B": {low: 3.0, high: 7.7},
  "C": {low: 0.5, high: 3.0},
  "D": {low: -100.0, high: 0.5},
}

class GpuRecommendationCategory {
  constructor(id, printName, descr, initCollapsed = false) {
    this.id = id;
    this.displayName = printName;
    this.range = recommendationRanges[id];
    this.collapsed = initCollapsed;
    this.description = descr;
  }

  // Getter
  get area() {
    return this.calcArea();
  }

  // Method
  isGroupOf(row) {
    return row.gpuRecommendation >= this.range.low
        && row.gpuRecommendation < this.range.high;
  }

  toggleCollapsed() {
    this.collapsed = !this.collapsed;
  }
}

let recommendationContainer = [
  new GpuRecommendationCategory("A", "Strongly Recommended", "TODO: Explain what Strongly-recommended means..."),
  new GpuRecommendationCategory("B", "Recommended", "TODO: Explain what Recommended means <em>WOW</em>..."),
  new GpuRecommendationCategory("C", "Discouraged", "TODO: Explain what Discouraged means..."),
  new GpuRecommendationCategory("D", "Insufficient", "TODO: Explain what Insufficient means..."),
];

var recommendationsMap = recommendationContainer.reduce(function (map, obj) {
  map[obj.displayName] = obj;
  return map;
}, {});

/* define constants for the tables configurations */
let defaultPageLength = 20;
let defaultLengthMenu = [[20, 40, 60, 100, -1], [20, 40, 60, 100, "All"]];
let appFieldAccCriterion = "sqlDataframeTaskDuration";//"sqlDFTaskDuration";

function setGPURecommendations(appsArray, maxScore) {
  for (var i in appsArray) {
    appsArray[i].gpuRecommendation = (appsArray[i][appFieldAccCriterion] * 10.00) / maxScore;
  }
}

function processRawData(rawRecords) {
  var processedRecords = [];
  var maxOpportunity = 0;
  for (var i in rawRecords) {
    var appRecord = JSON.parse(JSON.stringify(rawRecords[i]));
    appRecord["estimated"] = appRecord["appDurationEstimated"];
    appRecord["cpuPercent"] = appRecord["executorCPUPercent"];
    appRecord["durationCollection"] = {
      "appDuration": formatDuration(appRecord["appDuration"]),
      "sqlDFDuration": formatDuration(appRecord["sqlDFDuration"]),
      "sqlDFTaskDuration": formatDuration(appRecord["sqlDFTaskDuration"]),
      "sqlDurationProblems": formatDuration(appRecord["sqlDurationProblems"]),
    }
    maxOpportunity =
        (maxOpportunity < appRecord[appFieldAccCriterion])
            ? appRecord[appFieldAccCriterion] : maxOpportunity;
    appRecord["attemptDetailsURL"] = "application.html?app_id=" + appRecord.appId;
    processedRecords.push(appRecord)
  }
  setGPURecommendations(processedRecords, maxOpportunity);
  setGlobalReportSummary(processedRecords);
  return processedRecords;
}

function setGlobalReportSummary(processedApps) {
  let totalEstimatedApps = 0;
  let recommendedCnt = 0;
  let tlcCount = 0;
  let totalDurations = 0;
  let totalSqlDFDurations = 0;
  for (let i in processedApps) {
    // check if completedTime is estimated
    if (processedApps[i]["estimated"]) {
      totalEstimatedApps += 1;
    }
    // check if the app is recommended or needs more information
    var recommendedGroup = recommendationContainer.find(grp => grp.isGroupOf(processedApps[i]));
    recommendedCnt = recommendedCnt + (recommendedGroup.id < "C" ? 1 : 0);
    if (recommendedGroup.id === "D") {
      tlcCount += 1;
    }
    totalDurations += processedApps[i].appDuration;
    totalSqlDFDurations += processedApps[i].sqlDFDuration;
  }
  let estimatedPercentage = 0.0;
  let gpuPercent = 0.0;
  let tlcPercent = 0.0;

  if (processedApps.length != 0) {
    // calculate percentage of estimatedEndTime;
    estimatedPercentage = (totalEstimatedApps * 100.0) / processedApps.length;
    // calculate percentage of recommended GPUs
    gpuPercent = (100.0 * recommendedCnt) / processedApps.length;
    // percent of apps missing information
    tlcPercent = (100.0 * tlcCount) / processedApps.length;
  }
  qualReportSummary.totalApps.numeric = processedApps.length;
  qualReportSummary.totalApps.totalAppsDurations = formatDuration(totalDurations);
  qualReportSummary.speedups.totalSqlDFDurations = formatDuration(totalSqlDFDurations);
  qualReportSummary.candidates.numeric = recommendedCnt;
  qualReportSummary.tlc.numeric = tlcCount;
  qualReportSummary.totalApps.statsPercentage =
      twoDecimalFormatter.format(estimatedPercentage)
      + qualReportSummary.totalApps.statsPercentage;
  qualReportSummary.candidates.statsPercentage =
      twoDecimalFormatter.format(gpuPercent)
      + qualReportSummary.candidates.statsPercentage;
  qualReportSummary.tlc.statsPercentage =
      twoDecimalFormatter.format(tlcPercent)
      + qualReportSummary.tlc.statsPercentage;
}
