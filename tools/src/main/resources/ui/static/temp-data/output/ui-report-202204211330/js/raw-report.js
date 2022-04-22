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
  let rawDataTableConf = {
    // TODO: To use horizontal scrol for wide table
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
        name: 'sqlDFDuration',
        data: 'sqlDFDuration',
        searchable: false,
        render: function (data, type, row) {
          if (type === 'display' || type === 'filter') {
            return formatDuration(data)
          }
          return data;
        },
      },
      {
        name: 'sqlDFTaskDuration',
        data: 'sqlDFTaskDuration',
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
          if (oData.estimated) {
            $(nTd).css('color', 'blue');
          }
        }
      },
      {
        data: "cpuPercent",
        searchable: false,
        fnCreatedCell: (nTd, sData, oData, _ignored_iRow, _ignored_iCol) => {
          if (oData.cpuPercent >= 0) {
            $(nTd).css('color', totalCPUPercentageColor(oData.cpuPercent));
            $(nTd).css('background', totalCPUPercentageStyle(oData.cpuPercent));
          }
        }
      },
      {
        data: "sqlDurationProblems",
        searchable: false,
      },
      {data: "sqlIdsFailures"},
      {data: "readScorePercent"},
      {data: "readFileFormatScore"},
      {data: "Unsupported Read File Formats and Types"},
      {
        data: "Unsupported Write Data Format",
        orderable: false,
      },
      {
        data: "complexTypes",
        orderable: false,
      },
      {
        data: "nestedComplexTypes",
        orderable: false,
      },
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
  var rawAppsTable = $('#all-apps-raw-data-table').DataTable(rawDataTableConf);
  $('#all-apps-raw-data [data-toggle="tooltip"]').tooltip({
    // see answer in stackoverflow https://stackoverflow.com/questions/9958825/how-do-i-bind-twitter-bootstrap-tooltips-to-dynamically-created-elements
    //https://stackoverflow.com/questions/39189856/datatables-with-eonasdan-datepicker-doesnt-work/39191075#39191075
    selector: '[rel=tooltip]'
  });

  // TODO: check the displaybuttons page
  // https://datatables.net/forums/discussion/35936/how-to-let-buttons-float-at-right-top
  // rawAppsTable.buttons().container()
  //   .appendTo( $('.col-sm-12:eq(0)', rawAppsTable.table().container() ) );
  // table.buttons().container()
  //           .appendTo('#example_wrapper .col-md-6:eq(0)');
});
