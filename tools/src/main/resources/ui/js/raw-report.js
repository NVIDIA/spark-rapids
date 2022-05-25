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
  let totalSpeedupColumnName = "totalSpeedup"
  let sortColumnForGPURecommend = totalSpeedupColumnName
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
      {data: "appName"},
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
        name: 'sqlDataFrameDuration',
        data: 'sqlDataFrameDuration',
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display' || type === 'filter') {
            return formatDuration(data)
          }
          return data;
        },
      },
      {
        name: 'sqlDataframeTaskDuration',
        data: 'sqlDataframeTaskDuration',
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display' || type === 'filter') {
            return formatDuration(data)
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
        data: "durationCollection.estimatedDurationWallClock",
      },
      {
        data: "unsupportedTaskDuration",
      },
      {
        data: "estimatedInfo.gpuOpportunity",
      },
      {
        name: "totalSpeedup",
        data: "estimatedInfo.estimatedGpuSpeedup",
      },
      {
        data: "gpuCategory",
      },
      {
        data: "longestSqlDuration",
      },
      {data: "failedSQLIds"},
      {data: "readFileFormatAndTypesNotSupported"},
      {
        data: "writeDataFormat",
        orderable: false,
      },
      {
        data: "complexTypes",
        orderable: false,
      },
      {
        data: "nestedComplexTypes",
        orderable: false,
      }
    ],
    // dom: "<'row'<'col-sm-12 col-md-6'B><'col-sm-12 col-md-6'>>" +
    //      "<'row'<'col-sm-12 col-md-6'l><'col-sm-12 col-md-6'f>>" +
    //      "<'row'<'col-sm-12'tr>>" +
    //      "<'row'<'col-sm-12 col-md-5'i><'col-sm-12 col-md-7'p>>",
    dom: 'Bfrtlip',
    buttons: [{
      extend: 'csv',
      text: 'Export'
    }]
  };
  rawDataTableConf.order =
    [[getColumnIndex(rawDataTableConf.columns, sortColumnForGPURecommend), "desc"]];
  var rawAppsTable = $('#all-apps-raw-data-table').DataTable(rawDataTableConf);
  $('#all-apps-raw-data [data-toggle="tooltip"]').tooltip();

  setupNavigation();
});
