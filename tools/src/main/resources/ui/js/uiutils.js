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

/* globals $, Mustache, jQuery, qualReportSummary */

class Queue {
  constructor() {
    this.items = [];
  }
  enqueue(element) {
    // adding element to the queue
    this.items.push(element);
  }
  dequeue() {
    if(this.isEmpty())
      return "Underflow";
    return this.items.shift();
  }
  peak() {
    if(this.isEmpty())
      return "No elements in Queue";
    return this.items[0];
  }
  isEmpty() {
    return this.items.length == 0;
  }
}

class AppsToStagesMap {
  constructor() {
    this.appsHashMap = new Map();
  }
  addAppRec(appRecord) {
    if (this.appsHashMap.has(appRecord.appId)) {
      // app already exists
      return this.appsHashMap.get(appRecord.appId);
    }
    let appStages = new Set();
    if (Array.isArray(appRecord.stageInfo)) {
      appRecord.stageInfo.forEach(stageInfoRec => {
        appStages.add(stageInfoRec.stageId);
      });
    }
    appStages.add("N/A");
    this.appsHashMap.set(appRecord.appId, appStages)
    return appRecord.stageInfo;
  }
  getAllStages(appRecordId) {
    return this.appsHashMap.get(appRecordId);
  }
}

let appStagesMap = new AppsToStagesMap();

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
    let dt = new Date(timeMillis);
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
  let seconds = milliseconds * 1.0 / 1000;
  if (seconds < 1) {
    return seconds.toFixed(1) + " s";
  }
  if (seconds < 60) {
    return seconds.toFixed(0) + " s";
  }
  let minutes = seconds / 60;
  if (minutes < 10) {
    return minutes.toFixed(1) + " min";
  } else if (minutes < 60) {
    return minutes.toFixed(0) + " min";
  }
  let hours = minutes / 60;
  return hours.toFixed(1) + " h";
}

function getColumnIndex(columns, columnName) {
  for (let i = 0; i < columns.length; i++) {
    if (columns[i].name === columnName)
      return i;
  }
  return -1;
}

function removeColumnByName(columns, columnName) {
  return columns.filter(function(col) {return col.name != columnName})
}

/** calculations of CPU Processor **/

let CPUPercentThreshold = 40.0;

function totalCPUPercentageStyle(cpuPercent) {
  // Red if GC time over GCTimePercent of total time
  return (cpuPercent < CPUPercentThreshold) ?
      ("hsl(0, 100%, 50%, " + totalCPUPercentageAlpha(CPUPercentThreshold - cpuPercent) + ")") : "";
}

function totalCPUPercentageAlpha(actualCPUPercentage) {
  return actualCPUPercentage >= 0 ?
      (Math.min(actualCPUPercentage / 40.0 + 0.4, 1)) : 1;
}

function totalCPUPercentageColor(cpuPercent) {
  return (cpuPercent < CPUPercentThreshold) ? "white" : "black";
}

function escapeHtml(unsafe) {
  return unsafe
    .replaceAll(/&/g, "&amp;")
    .replaceAll(/</g, "&lt;")
    .replaceAll(/>/g, "&gt;")
    .replaceAll(/"/g, "&quot;")
    .replaceAll(/'/g, "&#039;");
}

function insertSpacePostCommas(arrData) {
  if (arrData.length > 0) {
    return arrData.replaceAll(',', ', ')
  }
  return '';
}

class GpuRecommendationCategory {
  constructor(id, relRate, printName, descr, displayClass, initCollapsed = false) {
    this.id = id;
    this.displayName = printName;
    this.collapsed = initCollapsed;
    this.description = descr;
    this.rate = relRate;
    this.badgeDisplay = displayClass;
  }

  // Method
  isGroupOf(row) {
    return row.recommendation === this.displayName;
  }

  toggleCollapsed() {
    this.collapsed = !this.collapsed;
  }

  getBadgeDisplay(row) {
    return this.badgeDisplay;
  }
}

let recommendationContainer = [
  new GpuRecommendationCategory("A", 5,
      "Strongly Recommended",
      "Spark Rapids is expected to speedup the App",
    "badge badge-pill badge-strong-recommended"),
  new GpuRecommendationCategory("B", 4,
      "Recommended",
      "Using Spark RAPIDS expected to give a moderate speedup.",
    "badge badge-pill badge-recommended"),
  new GpuRecommendationCategory("C", 3,
      "Not Recommended",
      "[Not-Recommended]: It is not likely that GPU Acceleration will be tangible",
    "badge badge-pill badge-not-recommended"),
  new GpuRecommendationCategory("D", 2,
    "Not Applicable",
    "[Not-Applicable]: The application has job or stage failures.",
    "badge badge-pill badge-not-applicable")
];

function createRecommendationGroups(recommendationsArr) {
  let map = new Map()
  recommendationsArr.forEach(object => {
    map.set(object.displayName, object);
  });
  return map;
}

let recommendationsMap = new Map(createRecommendationGroups(recommendationContainer));

let sparkUsers = new Map();
let execNames = new Map();

/* define constants for the tables configurations */
let defaultPageLength = 20;
let defaultLengthMenu = [[20, 40, 60, 100, -1], [20, 40, 60, 100, "All"]];

// bind the raw data top the GPU recommendations
function setGPURecommendations(appsArray) {
  for (let i in appsArray) {
    let appCategory = recommendationContainer.find(grp => grp.isGroupOf(appsArray[i]))
    appsArray[i]["gpuCategory"] = appCategory.displayName;
  }
}

function setAppInfoRecord(appRecord) {
  // set default values
  sparkUsers.set(appRecord["user"], true);
}

function getAppBadgeHTMLWrapper(appRecord) {
  let recommendGroup = recommendationsMap.get(appRecord.gpuCategory);
  return `<span class="` + recommendGroup.getBadgeDisplay(appRecord)
    + `">` + recommendGroup.displayName + `</span>`;
}

function setAppTaskDuration(appRec) {
  // appTaskDuration = nonSql + sqlTask Durations
  appRec["appTaskDuration"] =
    appRec["sqlDataframeTaskDuration"]
    + appRec["nonSqlTaskDurationAndOverhead"]
}

// Quick workaround to map names generated by scala to the name in UI
function mapFieldsToUI(rawAppRecord) {
  rawAppRecord["cpuPercent"] = rawAppRecord["executorCPUPercent"];
  // set default longestSqlDuration for backward compatibility
  if (!rawAppRecord.hasOwnProperty("longestSqlDuration")) {
    rawAppRecord["longestSqlDuration"] = 0;
  }
  rawAppRecord["recommendation"] = rawAppRecord.estimatedInfo.recommendation;
  rawAppRecord["estimatedGPUDuration"] = parseFloat(rawAppRecord.estimatedInfo.estimatedGpuDur);
  rawAppRecord["totalSpeedup"] = rawAppRecord.estimatedInfo.estimatedGpuSpeedup;
  rawAppRecord["appDuration"] = rawAppRecord.estimatedInfo.appDur;
  rawAppRecord["sqlDataFrameDuration"] = rawAppRecord.estimatedInfo.sqlDfDuration;
  rawAppRecord["gpuTimeSaved"] = parseFloat(rawAppRecord.estimatedInfo.estimatedGpuTimeSaved);
  rawAppRecord["gpuOpportunity"] = rawAppRecord.estimatedInfo.gpuOpportunity;
  // escape html characters for data formatted fields
  rawAppRecord["readFileFormats_html_safe"] =
    rawAppRecord.readFileFormats.map(elem => {
      return escapeHtml(elem);
    });
  rawAppRecord["readFileFormatAndTypesNotSupported_html_safe"] =
    rawAppRecord.readFileFormatAndTypesNotSupported.map(elem => {
      return escapeHtml(elem);
    });
  rawAppRecord["writeDataFormat_html_safe"] =
    rawAppRecord.writeDataFormat.map(elem => {
      return escapeHtml(elem);
    });
  rawAppRecord["complexTypes_html_safe"] =
    rawAppRecord.complexTypes.map(elem => {
      return escapeHtml(elem);
    });
  rawAppRecord["nestedComplexTypes_html_safe"] =
    rawAppRecord.nestedComplexTypes.map(elem => {
      return escapeHtml(elem);
    });
  rawAppRecord["writeDataFormat_html_safe"] =
    rawAppRecord.writeDataFormat.map(elem => {
      return escapeHtml(elem).toUpperCase();
    });
  rawAppRecord["potentialProblems_html_safe"] =
    rawAppRecord.potentialProblems.map(elem => {
      return escapeHtml(elem).toUpperCase();
    });
}

function processRawData(rawRecords) {
  let processedRecords = [];
  let maxOpportunity = 0;
  for (let i in rawRecords) {
    let appRecord = JSON.parse(JSON.stringify(rawRecords[i]));
    mapFieldsToUI(appRecord)

    // Set duration fields
    appRecord["durationCollection"] = {
      "appDuration": formatDuration(appRecord["appDuration"]),
      "sqlDFDuration": formatDuration(appRecord["sqlDataFrameDuration"]),
      "sqlDFTaskDuration": formatDuration(appRecord["sqlDataframeTaskDuration"]),
      "sqlDurationProblems": formatDuration(appRecord["sqlDurationForProblematic"]),
      "supportedSQLTaskDuration": formatDuration(appRecord["supportedSQLTaskDuration"]),
      "nonSqlTaskDurationAndOverhead": formatDuration(appRecord["nonSqlTaskDurationAndOverhead"]),
      "estimatedGPUDuration": formatDuration(appRecord["estimatedGPUDuration"]),
      "estimatedDurationWallClock":
        formatDuration(appRecord.estimatedGPUDuration),
      "gpuOpportunity": formatDuration(appRecord.gpuOpportunity),
      "unsupportedSQLTaskDuration": formatDuration(appRecord["unsupportedSQLTaskDuration"]),
      "longestSqlDuration": formatDuration(appRecord["longestSqlDuration"]),
      "gpuTimeSaved": formatDuration(appRecord.gpuTimeSaved),
    }

    // Set numeric fields for display
    appRecord["totalSpeedup_display"] =
      Math.floor(parseFloat(appRecord["totalSpeedup"]) * 10) / 10;
    appRecord["taskSpeedupFactor_display"] =
      Math.floor(parseFloat(appRecord["taskSpeedupFactor"]) * 10) / 10;

    setAppInfoRecord(appRecord);
    maxOpportunity =
        (maxOpportunity < appRecord["gpuRecommendation"])
            ? appRecord["gpuRecommendation"] : maxOpportunity;
    if (UIConfig.fullAppView.enabled) {
      appRecord["attemptDetailsURL"] = "application.html?app_id=" + appRecord.appId;
    } else {
      appRecord["attemptDetailsURL"] = "#!"
    }

    setAppTaskDuration(appRecord);

    processedRecords.push(appRecord)
  }
  setGPURecommendations(processedRecords);
  setGlobalReportSummary(processedRecords);
  return processedRecords;
}

function setGlobalReportSummary(processedApps) {
  let totalEstimatedApps = 0;
  let recommendedCnt = 0;
  let tlcCount = 0;
  let totalDurations = 0;
  let totalSqlDataframeDuration = 0;
  // only count apps that are recommended
  let totalGPUOpportunityDurations = 0;
  for (let i in processedApps) {
    // check if completedTime is estimated
    if (processedApps[i]["endDurationEstimated"]) {
      totalEstimatedApps += 1;
    }
    totalDurations += processedApps[i].appDuration;
    totalSqlDataframeDuration += processedApps[i].sqlDataFrameDuration;
    // check if the app is recommended or needs more information
    let recommendedGroup = recommendationsMap.get(processedApps[i]["gpuCategory"])
    if (recommendedGroup.id < "C") {
      // this is a recommended app
      // aggregate for GPU recommendation box
      recommendedCnt += 1;
      totalGPUOpportunityDurations += processedApps[i]["gpuOpportunity"]
    } else {
      if (recommendedGroup.id === "D") {
        tlcCount += 1;
      }
    }
  }

  let estimatedPercentage = 0.0;
  let gpuPercent = 0.0;
  let tlcPercent = 0.0;
  let speedUpPercent = 0.0;

  if (processedApps.length != 0) {
    // calculate percentage of estimatedEndTime;
    estimatedPercentage = (totalEstimatedApps * 100.0) / processedApps.length;
    // calculate percentage of recommended GPUs
    gpuPercent = (100.0 * recommendedCnt) / processedApps.length;
    // percent of apps missing information
    tlcPercent = (100.0 * tlcCount) / processedApps.length;
    speedUpPercent = (100.0 * totalGPUOpportunityDurations) / totalSqlDataframeDuration;
  }
  qualReportSummary.totalApps.numeric = processedApps.length;
  qualReportSummary.totalApps.totalAppsDurations = formatDuration(totalDurations);
  // speedups
  qualReportSummary.speedups.numeric =
    formatDuration(totalGPUOpportunityDurations);
  qualReportSummary.speedups.totalSqlDataframeTaskDuration =
    formatDuration(totalSqlDataframeDuration);
  qualReportSummary.speedups.statsPercentage = twoDecimalFormatter.format(speedUpPercent)
    + qualReportSummary.speedups.statsPercentage;

  // candidates
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

function createAppIDLinkEnabled(tableViewType) {
  return UIConfig.fullAppView.enabled && tableViewType === "listAppsView"
}

function createAppDetailedTableConf(
    appRecords,
    tableViewType,
    mustacheRecord,
    extraFunctionArgs) {
  let totalSpeedupColumnName = "totalSpeedupFactor"
  let recommendGPUColName = "gpuRecommendation"
  let appDetailsBaseParams = UIConfig.datatables[extraFunctionArgs.tableId];
  let appDetailsCustomParams = appDetailsBaseParams[tableViewType];
  let fileExportName = appDetailsCustomParams.fileExportPrefix;
  if (tableViewType === 'singleAppView') {
    fileExportName =  appDetailsCustomParams.fileExportPrefix + "_" + extraFunctionArgs.appId;
  }
  let rawDataTableConf = {
    paging: (appRecords.length > defaultPageLength),
    pageLength: defaultPageLength,
    lengthMenu: defaultLengthMenu,
    info: true,
    data: appRecords,
    columns: [
      {
        name: "appName",
        data: "appName",
      },
      {
        name: "appId",
        data: "appId",
        className: "all",
        render:  (appId, type, row) => {
          if (type === 'display') {
            if (createAppIDLinkEnabled(tableViewType)) {
              return `<a href="${row.attemptDetailsURL}">${appId}</a>`
            }
          }
          return appId;
        }
      },
      {
        name: "sparkUser",
        data: "user",
      },
      {
        name: "startTime",
        data: "startTime",
        type: 'numeric',
        render: function (data, type, row) {
          if (type === 'display') {
            return formatTimeMillis(data)
          }
          return data;
        },
      },
      {
        name: recommendGPUColName,
        data: 'gpuCategory',
        className: "all",
        render: function (data, type, row) {
          if (type === 'display') {
            return getAppBadgeHTMLWrapper(row);
          }
          return data;
        },
        fnCreatedCell: (nTd, sData, oData, _ignored_iRow, _ignored_iCol) => {
          let recommendGroup = recommendationsMap.get(sData);
          let toolTipVal = recommendGroup.description;
          $(nTd).attr('data-toggle', "tooltip");
          $(nTd).attr('data-placement', "top");
          $(nTd).attr('html', "true");
          $(nTd).attr('data-html', "true");
          $(nTd).attr('title', toolTipVal);
        }
      },
      {
        name: totalSpeedupColumnName,
        data: "totalSpeedup",
        type: 'numeric',
        className: "all",
        render: function (data, type, row) {
          if (type === 'display') {
            return row.totalSpeedup_display
          }
          return data;
        },
      },
      {
        name: 'appDuration',
        data: 'appDuration',
        type: 'numeric',
        render: function (data, type, row) {
          if (type === 'display' || type === 'filter') {
            return formatDuration(data)
          }
          return data;
        },
        fnCreatedCell: (nTd, sData, oData, _ignored_iRow, _ignored_iCol) => {
          if (oData.endDurationEstimated) {
            $(nTd).css('color', 'blue');
          }
        }
      },
      {
        name: "estimatedGPUDuration",
        data: "estimatedGPUDuration",
        type: 'numeric',
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.estimatedDurationWallClock;
          }
          return data;
        },
      },
      {
        name: "gpuTimeSaved",
        data: "gpuTimeSaved",
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.gpuTimeSaved
          }
          return data;
        },
      },
      {
        name: "taskSpeedupFactor",
        data: "taskSpeedupFactor",
        render: function (data, type, row) {
          if (type === 'display') {
            return row.taskSpeedupFactor_display;
          }
          return data;
        },
      },
      {
        name: 'sqlDataFrameDuration',
        data: 'sqlDataFrameDuration',
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.sqlDFDuration
          }
          return data;
        },
      },
      {
        name: 'gpuOpportunity',
        data: "gpuOpportunity",
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.gpuOpportunity
          }
          return data;
        },
      },
      {
        name: 'unsupportedSQLTaskDuration',
        data: "unsupportedSQLTaskDuration",
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.unsupportedSQLTaskDuration
          }
          return data;
        },
      },
      {
        name: 'supportedSQLTaskDuration',
        data: "supportedSQLTaskDuration",
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.supportedSQLTaskDuration
          }
          return data;
        },
      },
      {
        name: 'sqlDataframeTaskDuration',
        data: 'sqlDataframeTaskDuration',
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.sqlDFTaskDuration
          }
          return data;
        },
      },
      {
        name: "executorCpuTimePercent",
        data: "executorCpuTimePercent",
        fnCreatedCell: (nTd, sData, oData, _ignored_iRow, _ignored_iCol) => {
          if (oData.executorCpuTimePercent >= 0) {
            $(nTd).css('color', totalCPUPercentageColor(oData.executorCpuTimePercent));
            $(nTd).css('background', totalCPUPercentageStyle(oData.executorCpuTimePercent));
          }
        }
      },
      {
        name: "longestSqlDuration",
        data: "longestSqlDuration",
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.longestSqlDuration
          }
          return data;
        },
      },
      {
        name: "nonSqlTaskDurationAndOverhead",
        data: "nonSqlTaskDurationAndOverhead",
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.nonSqlTaskDurationAndOverhead
          }
          return data;
        },
      },
      {
        data: "endDurationEstimated",
        name: "endDurationEstimated",
        orderable: false,
      },
      {
        name: "failedSQLIds",
        data: "failedSQLIds[, ]",
      },
      {
        name: "potentialProblems",
        data: "potentialProblems_html_safe[</br>]",
        render: function (data, type, row) {
          if (type === 'display') {
            return insertSpacePostCommas(data);
          }
          return data;
        },
      },
      {
        name: "readFileFormatAndTypesNotSupported",
        data: "readFileFormatAndTypesNotSupported_html_safe[</br>]",
        render: function (data, type, row) {
          if (type === 'display') {
            return insertSpacePostCommas(data);
          }
          return data;
        },
      },
      {
        data: "writeDataFormat_html_safe[, ]",
        name: "writeDataFormat",
        orderable: false,
      },
      {
        data: "complexTypes_html_safe[</br>]",
        name: "complexTypes",
        orderable: false,
        render: function (data, type, row) {
          if (type === 'display') {
            return insertSpacePostCommas(data);
          }
          return data;
        },
      },
      {
        data: "nestedComplexTypes_html_safe[</br>]",
        name: "nestedComplexTypes",
        orderable: false,
        render: function (data, type, row) {
          if (type === 'display') {
            return insertSpacePostCommas(data);
          }
          return data;
        },
      },
      {
        data: "readFileFormats_html_safe[</br>]",
        name: "readFileFormats",
        orderable: false,
        render: function (data, type, row) {
          if (type === 'display') {
            return insertSpacePostCommas(data);
          }
          return data;
        },
      },
    ],
    responsive: {
      details: {
        renderer: function ( api, rowIdx, columns ) {
          let data = $.map( columns, function ( col, i ) {
            let dataTableToolTip = toolTipsValues[appDetailsCustomParams.toolTipID];
            if (!col.hidden) {
              return '';
            }
            let returnStr = '<tr data-dt-row="'+col.rowIndex+'" data-dt-column="'+col.columnIndex+'">'+
              '<th scope="row">';
            if (dataTableToolTip[col.title]) {
              returnStr += '<span data-toggle=\"tooltip\" data-placement=\"top\"' +
                '    title=\"' + dataTableToolTip[col.title] + '\">'+col.title+':'+
                '</span>';
            } else {
              returnStr += col.title;
            }
            returnStr += '</th> <td>'+col.data+'</td> </tr>';
            return returnStr;
          } ).join('');

          return data ?
            $('<table/>').append( data ) :
            false;
        }
      }
    },
    dom: appDetailsCustomParams.Dom,
    buttons: [{
      extend: 'csv',
      title: fileExportName,
      text: 'Export'
    }],
    initComplete: function(settings, json) {
      // Add custom Tool Tip to the headers of the table
      // Add custom Tool Tip to the headers of the table
      let thLabel = extraFunctionArgs.tableDivId + ' thead th';
      let dataTableToolTip = toolTipsValues[appDetailsCustomParams.toolTipID];
      $(thLabel).each(function () {
        let $td = $(this);
        let toolTipVal = dataTableToolTip[$td.text().trim()];
        $td.attr('data-toggle', "tooltip");
        $td.attr('data-placement', "top");
        $td.attr('html', "true");
        $td.attr('data-html', "true");
        $td.attr('title', toolTipVal);
      });
    }
  };

  processDatableColumns(
    rawDataTableConf,
    appDetailsBaseParams,
    appDetailsCustomParams,
    mustacheRecord)

  return rawDataTableConf;
}

function extractExecName(rawExecName) {
  let parenthInd = rawExecName.indexOf(" (");
  if (parenthInd == -1) {
    return rawExecName;
  }
  return rawExecName.substring(0, parenthInd);
}

function createSearchPane(allPanesConfigs, confID, optionsGeneratorFunc = null) {
  let execNamePaneConf = allPanesConfigs[confID];
  if (optionsGeneratorFunc === null) {
    return execNamePaneConf;
  }
  execNamePaneConf.options = optionsGeneratorFunc();
  return execNamePaneConf;
}

function processDatableColumns(
    dataTableConf,
    itemDetailsBaseParams,
    itemDetailsCustomParams,
    dataTableMustacheRecord) {
  // set Columns defaults
  for (let i in dataTableConf.columns) {
    let propName = itemDetailsBaseParams.colEnabledPrefix + dataTableConf.columns[i].name;
    // set all display column to true by default
    dataTableMustacheRecord[propName] = true;
    // disable searchable for each column
    dataTableConf.columns[i].searchable = false;
  }
  // hide columns with classname set to none
  if (itemDetailsCustomParams.hideColumns.length > 0) {
    for (let i in itemDetailsCustomParams.hideColumns) {
      let colInd =
        getColumnIndex(dataTableConf.columns, itemDetailsCustomParams.hideColumns[i]);
      dataTableConf.columns[colInd].className = "none";
    }
  }
  // enable searchable columns
  for (let ind in itemDetailsCustomParams.searchableColumns) {
    let dtColumnInd =
      getColumnIndex(dataTableConf.columns, itemDetailsCustomParams.searchableColumns[ind])
    dataTableConf.columns[dtColumnInd].searchable = true
  }
  // remove skipped columns
  if (itemDetailsCustomParams.skipColumns.length > 0) {
    for (let i in itemDetailsCustomParams.skipColumns) {
      let propName = itemDetailsBaseParams.colEnabledPrefix + itemDetailsCustomParams.skipColumns[i];
      dataTableConf.columns =
        removeColumnByName(dataTableConf.columns, itemDetailsCustomParams.skipColumns[i]);
      dataTableMustacheRecord[propName] = false;
    }
  }
  // order columns
  if (itemDetailsCustomParams.sortTable) {
    dataTableConf.order = [];
    for (let ind in itemDetailsCustomParams.sortColumns) {
      let dtColumnInd =
        getColumnIndex(dataTableConf.columns, itemDetailsCustomParams.sortColumns[ind].colName)
      dataTableConf.order.push([dtColumnInd, itemDetailsCustomParams.sortColumns[ind].order]);
    }
  }
}

function setDataTableButtons(
    dataTableConf,
    itemDetailsBaseParams,
    itemDetailsCustomParams,
    buttonsArgs) {

  // add buttons if enabled in the customView
  if (itemDetailsCustomParams.hasOwnProperty('buttons')) {
    let buttonsConf = itemDetailsCustomParams['buttons'];
    if (buttonsConf["enabled"]) {
      dataTableConf["buttons"] = buttonsConf.buttons
      dataTableConf.dom = 'B' + dataTableConf.dom
    }
    return;
  }
}

function setDataTableSearchPanes(
  dataTableConf,
  itemDetailsBaseParams,
  itemDetailsCustomParams,
  optionGeneratorsFunctionsMap = new Map()) {
  if (itemDetailsBaseParams.hasOwnProperty('searchPanes')) {
    let searchPanesConf = itemDetailsBaseParams['searchPanes'];
    if (searchPanesConf.enabled) {
      // disable searchpanes on default columns
      dataTableConf.columnDefs = [{
        "searchPanes": {
          show: false,
        },
        "targets": ['_all']
      }];
    }
    // add custom panes
    let enabledPanes = [];
    if (itemDetailsCustomParams.hasOwnProperty("enabledPanes")) {
      let panesConfigurations = searchPanesConf["panes"];
      itemDetailsCustomParams.enabledPanes.forEach(searchPaneID => {
        let optionFunc = null;
        if (optionGeneratorsFunctionsMap.has(searchPaneID)) {
          optionFunc = optionGeneratorsFunctionsMap.get(searchPaneID);
        }
        let currentSearchPane = createSearchPane(panesConfigurations, searchPaneID, optionFunc);
        enabledPanes.push(currentSearchPane);
      });
    }

    if (enabledPanes.length > 0) {
      dataTableConf.searchPanes = searchPanesConf["dtConfigurations"];
      // limit the number of filters to 3 by default
      if (enabledPanes.length > 3) {
        dataTableConf.searchPanes.layout = 'columns-3';
      }
      dataTableConf.dom = 'P' + dataTableConf.dom;
      dataTableConf.searchPanes.panes = enabledPanes;
    }
  }
}

function constructDataTableFromHTMLTemplate(
  dataArray,
  viewType,
  confInitializerFunc,
  dataTableArgs = {}) {
  let htmlMustacheRec = {};
  let dataTableConf = confInitializerFunc(dataArray, viewType, htmlMustacheRec, dataTableArgs);
  let dataTableContainerContent = Mustache.render(dataTableArgs.dataTableTemplate, htmlMustacheRec);
  $(dataTableArgs.datatableContainerID).html(jQuery.parseHTML(dataTableContainerContent, false));
  return $(dataTableArgs.tableDivId).DataTable(dataTableConf);
}

function createAppDetailsExecsTableConf(
    execAppRecords,
    tableViewType,
    mustacheRecord,
    extraFunctionArgs) {
  let appExecDetailsBaseParams = UIConfig.datatables[extraFunctionArgs.tableId];
  let appExecDetailsCustomParams = appExecDetailsBaseParams[tableViewType];
  let fileExportName = appExecDetailsCustomParams.fileExportPrefix;
  if (tableViewType === 'singleAppView') {
    fileExportName =  appExecDetailsCustomParams.fileExportPrefix + "_" + extraFunctionArgs.appId;
  }

  let appExecDataTableConf = {
    paging: (execAppRecords.length > defaultPageLength),
    pageLength: defaultPageLength,
    lengthMenu: defaultLengthMenu,
    info: true,
    data: execAppRecords,
    columns: [
      {
        name: "appID",
        data: "appID",
      },
      {
        name: "sqlID",
        data: "sqlID",
        className: "all",
      },
      {
        name: "exec",
        data: "exec",
        className: "all",
      },
      {
        name: "expr",
        data: "expr",
        className: "all",
      },
      {
        name: "isSupported",
        data: "isSupported",
        className: "all",
      },
      {
        name: "speedupFactor",
        data: "speedupFactor",
        render: function (data, type, row) {
          if (data && type === 'display') {
            return twoDecimalFormatter.format(data);
          }
          return data;
        },
      },
      {
        name: "duration",
        data: "duration",
        "defaultContent":"",
        className: "all",
        render: function (data, type, row) {
          if (data && type === 'display') {
            return formatDuration(data);
          }
          return data;
        },
      },
      {
        name: "nodeId",
        data: "nodeId",
      },
      {
        name: "stages",
        data: "stages[, ]",
      },
      {
        name: "children",
        "defaultContent":[],
        data: "children",
        render: "[, ].exec",
      },
      {
        name: "childrenNodeIDs",
        "defaultContent":[],
        data: "children",
        render: "[, ].nodeId",
      },
      {
        name: "isRemoved",
        data: "shouldRemove",
      },
    ],
    responsive: {
      details: {
        renderer: function ( api, rowIdx, columns ) {
          let data = $.map( columns, function ( col, i ) {
            let dataTableToolTip = toolTipsValues[appExecDetailsCustomParams.toolTipID];
            return col.hidden ?
              '<tr data-dt-row="'+col.rowIndex+'" data-dt-column="'+col.columnIndex+'">'+
              '<th scope=\"row\"><span data-toggle=\"tooltip\" data-placement=\"top\"' +
              '    title=\"' + dataTableToolTip[col.title] + '\">'+col.title+':'+
              '</span></th> '+
              '<td>'+col.data+'</td>'+
              '</tr>' :
              '';
          } ).join('');

          return data ?
            $('<table/>').append( data ) :
            false;
        }
      }
    },
    dom: appExecDetailsCustomParams.Dom,
    buttons: [{
      extend: 'csv',
      title: fileExportName,
      text: 'Export'
    }],
    initComplete: function(settings, json) {
      // Add custom Tool Tip to the headers of the table
      // Add custom Tool Tip to the headers of the table
      let thLabel = extraFunctionArgs.tableDivId + ' thead th';
      let dataTableToolTip = toolTipsValues[appExecDetailsCustomParams.toolTipID];
      $(thLabel).each(function () {
        let $td = $(this);
        let toolTipVal = dataTableToolTip[$td.text().trim()];
        $td.attr('data-toggle', "tooltip");
        $td.attr('data-placement', "top");
        $td.attr('html', "true");
        $td.attr('data-html', "true");
        $td.attr('title', toolTipVal);
      });
    }
  };

  processDatableColumns(
    appExecDataTableConf,
    appExecDetailsBaseParams,
    appExecDetailsCustomParams,
    mustacheRecord);

  // set searchpanes
  let optionGeneratorsFunctionsMap = new Map();
  optionGeneratorsFunctionsMap.set("execName", function() {
      let execNameOptions = [];
      execNames.forEach((data, execName) => {
        let currOption = {
          label: execName,
          value: function(rowData, rowIdx) {
            // get spark user
            return (rowData["execName"] === execName);
          },
        }
        execNameOptions.push(currOption);
      });
      return execNameOptions;
    });
  optionGeneratorsFunctionsMap.set("stages", function() {
      let stageIdOptions = [];
      appStagesMap.getAllStages(extraFunctionArgs.appId).forEach(stageID => {
        let currOption = {
          label: stageID,
          value: function(rowData, rowIdx) {
            if (Array.isArray(rowData["stages"]) && rowData["stages"].length > 0) {
              return rowData["stages"].some(stageNum => (stageNum === stageID));
            }
            return stageID === "N/A";
          },
        };
        stageIdOptions.push(currOption);
      });
      return stageIdOptions;
    });

  setDataTableSearchPanes(
    appExecDataTableConf,
    appExecDetailsBaseParams,
    appExecDetailsCustomParams,
    optionGeneratorsFunctionsMap);

  return appExecDataTableConf;
}


function createAppDetailsStagesTableConf(
  execAppRecords,
  tableViewType,
  mustacheRecord,
  extraFunctionArgs) {
  let appStageDetailsBaseParams = UIConfig.datatables[extraFunctionArgs.tableId];
  let appStageDetailsCustomParams = appStageDetailsBaseParams[tableViewType];
  let fileExportName = appStageDetailsCustomParams.fileExportPrefix;
  if (tableViewType === 'singleAppView') {
    fileExportName =  appStageDetailsCustomParams.fileExportPrefix + "_" + extraFunctionArgs.appId;
  }

  let stagesDataTableConf = {
    paging: (execAppRecords.length > defaultPageLength),
    pageLength: defaultPageLength,
    lengthMenu: defaultLengthMenu,
    info: true,
    data: execAppRecords,
    columns: [
      {
        name: "appID",
        data: "appID",
      },
      {
        name: "stageId",
        data: "stageId",
      },
      {
        name: "averageSpeedup",
        data: "averageSpeedup",
        render: function (data, type, row) {
          if (data && type === 'display') {
            return twoDecimalFormatter.format(data);
          }
          return data;
        },
      },
      {
        name: "stageTaskTime",
        data: "stageTaskTime",
        render: function (data, type, row) {
          if (data && type === 'display') {
            return formatDuration(data);
          }
          return data;
        },
      },
      {
        name: "unsupportedTaskDur",
        data: "unsupportedTaskDur",
        render: function (data, type, row) {
          if (data && type === 'display') {
            return formatDuration(data);
          }
          return data;
        },
      },
      {
        name: "estimated",
        data: "estimated",
      },
    ],
    responsive: {
      details: {
        renderer: function ( api, rowIdx, columns ) {
          let data = $.map( columns, function ( col, i ) {
            let dataTableToolTip = toolTipsValues[appStageDetailsCustomParams.toolTipID];
            return col.hidden ?
              '<tr data-dt-row="'+col.rowIndex+'" data-dt-column="'+col.columnIndex+'">'+
              '<th scope=\"row\"><span data-toggle=\"tooltip\" data-placement=\"top\"' +
              '    title=\"' + dataTableToolTip[col.title] + '\">'+col.title+':'+
              '</span></th> '+
              '<td>'+col.data+'</td>'+
              '</tr>' :
              '';
          } ).join('');

          return data ?
            $('<table/>').append( data ) :
            false;
        }
      }
    },
    dom: appStageDetailsCustomParams.Dom,
    buttons: [{
      extend: 'csv',
      title: fileExportName,
      text: 'Export'
    }],
    initComplete: function(settings, json) {
      // Add custom Tool Tip to the headers of the table
      let thLabel = extraFunctionArgs.tableDivId + ' thead th';
      $(thLabel).each(function () {
        let dataTableToolTip = toolTipsValues[appStageDetailsCustomParams.toolTipID];
        let $td = $(this);
        let toolTipVal = dataTableToolTip[$td.text().trim()];
        $td.attr('data-toggle', "tooltip");
        $td.attr('data-placement', "top");
        $td.attr('html', "true");
        $td.attr('data-html', "true");
        $td.attr('title', toolTipVal);
      });
    }
  };

  processDatableColumns(
    stagesDataTableConf,
    appStageDetailsBaseParams,
    appStageDetailsCustomParams,
    mustacheRecord);

  // set searchpanes
  setDataTableSearchPanes(
    stagesDataTableConf,
    appStageDetailsBaseParams,
    appStageDetailsCustomParams);

  return stagesDataTableConf;
}

function getAppDetailsTableTemplate() {
  return `
      <div id="all-apps-raw-data">
        <table id="all-apps-raw-data-table" class="table data-table display wrap" style="width:100%">
          <thead>
          <tr>
            {{#displayCol_appName}}
            <th>App Name</th>
            {{/displayCol_appName}}
            <th>App ID</th>
            <th>User</th>
            <th>Start Time</th>
            <th>Recommendation</th>
            <th>Estimated GPU Speedup</th>
            <th>App Duration</th>
            <th>Estimated GPU Duration</th>
            <th>Estimated GPU Time Saved</th>
            <th>Task Speed-up Factor</th>
            <th>SQL DF Duration</th>
            <th>GPU Opportunity</th>
            <th>Unsupported Task Duration</th>
            <th>Supported SQL DF Task Duration</th>
            <th>SQL Dataframe Task Duration</th>
            <th>Executor CPU Time Percent</th>
            <th>Longest SQL Duration</th>
            <th>NONSQL Task Duration Plus Overhead</th>
            <th>App Duration Estimated</th>
            <th>SQL Ids with Failures</th>
            <th>Potential Problems</th>
            <th>Unsupported Read File Formats and Types</th>
            <th>Unsupported Write Data Format</th>
            <th>Complex Types</th>
            <th>Nested Complex Types</th>
            <th>Read Schema</th>
          </tr>
          </thead>
        </table>
      </div>
  `;
}

function setupNavigation() {
  $(".dash-nav-dropdown-toggle").click(function () {
    $(this).closest(".dash-nav-dropdown")
      .toggleClass("show")
      .find(".dash-nav-dropdown")
      .removeClass("show");

    $(this).parent()
      .siblings()
      .removeClass("show");
  });

  $(".menu-toggle").click(function () {
    $(".dash").toggleClass("dash-compact");
  });
}
