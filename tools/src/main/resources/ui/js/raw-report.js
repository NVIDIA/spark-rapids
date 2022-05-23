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

/* global $, Mustache, formatDuration, jQuery, qualificationRecords */

$(document).ready(function() {
  let attemptArray = processRawData(qualificationRecords);
  let totalSpeedupColumnName = "totalSpeedupFactor"
  let sortColumnForGPURecommend = totalSpeedupColumnName
  let recommendGPUColName = "gpuRecommendation"
  let rawDataTableConf = {
    // TODO: To use horizontal scroll for wide table
    //"scrollX": true,
    responsive: true,
    paging: (attemptArray.length > defaultPageLength),
    pageLength: defaultPageLength,
    lengthMenu: defaultLengthMenu,
    info: true,
    data: attemptArray,
    columns: [
      {
        name: "appName",
        data: "appName",
      },
      {
        data: "appId",
        render:  (appId, type, row) => {
          if (type === 'display' || type === 'filter') {
            return `<a href="${row.attemptDetailsURL}" target="_blank">${appId}</a>`
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
        searchable: false,
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
        render: function (data, type, row) {
          if (type === 'display') {
            let recommendGroup = recommendationsMap.get(data);
            return `<span class="` + recommendGroup.getBadgeDisplay(row)
              + `">` + recommendGroup.displayName + `</span>`;
          }
          return data;
        },
      },
      {
        name: totalSpeedupColumnName,
        data: "totalSpeedup",
        searchable: false,
        type: 'numeric',
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
        searchable: false,
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
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.gpuTimeSaved
          }
          return data;
        },
      },
      {
        name: 'sqlDataFrameDuration',
        data: 'sqlDataFrameDuration',
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.sqlDFDuration
          }
          return data;
        },
      },
      {
        name: 'gpuOpportunity',
        data: "estimatedInfo.gpuOpportunity",
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.gpuOpportunity
          }
          return data;
        },
      },
      {
        name: 'unsupportedTaskDuration',
        data: "unsupportedTaskDuration",
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.unsupportedDuration
          }
          return data;
        },
      },
      {
        name: 'sqlDataframeTaskDuration',
        data: 'sqlDataframeTaskDuration',
        searchable: false,
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
        searchable: false,
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
        searchable: false,
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
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display') {
            return row.durationCollection.nonSqlTaskDurationAndOverhead
          }
          return data;
        },
      },
      {
        name: "failedSQLIds",
        data: "failedSQLIds",
        searchable: false,
      },
      {
        name: "readFileFormatAndTypesNotSupported",
        data: "readFileFormatAndTypesNotSupported",
        searchable: false,
      },
      {
        data: "writeDataFormat",
        name: "writeDataFormat",
        orderable: false,
      },
      {
        data: "complexTypes",
        name: "complexTypes",
        orderable: false,
      },
      {
        data: "nestedComplexTypes",
        name: "nestedComplexTypes",
        orderable: false,
      },
    ],
    dom: 'Bfrtlip',
    buttons: [{
      extend: 'csv',
      text: 'Export'
    }],
    initComplete: function(settings, json) {
      // Add custom Tool Tip to the headers of the table
      $('#all-apps-raw-data-table thead th').each(function () {
        var $td = $(this);
        var toolTipVal = toolTipsValues.rawTable[$td.text().trim()];
        $td.attr('data-toggle', "tooltip");
        $td.attr('data-placement', "top");
        $td.attr('html', "true");
        $td.attr('data-html', "true");
        $td.attr('title', toolTipVal);
      });
    }
  };
  rawDataTableConf.order =
    [[getColumnIndex(rawDataTableConf.columns, sortColumnForGPURecommend), "desc"]];
  var rawAppsTable = $('#all-apps-raw-data-table').DataTable(rawDataTableConf);
  // set the tootTips for the table
  $('#all-apps-raw-data [data-toggle="tooltip"]').tooltip({
    container: 'body',
    html: true,
    animation: true,
    placement:"bottom",
    delay: {show: 0, hide: 10.0}});

  setupNavigation();
});
