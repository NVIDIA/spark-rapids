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
    paging: (attemptArray.length > defaultPageLength),
    pageLength: defaultPageLength,
    lengthMenu: defaultLengthMenu,
    info: true,
    data: attemptArray,
    columns: [
      {
        name: "appName",
        data: "appName",
        className: "none",
      },
      {
        data: "appId",
        className: "all",
        render:  (appId, type, row) => {
          if (type === 'display') {
            if (UIConfig.fullAppView.enabled) {
              return `<a href="${row.attemptDetailsURL}" target="_blank">${appId}</a>`
            }
          }
          return appId;
        }
      },
      {
        name: "sparkUser",
        data: "user",
        className: "none",
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
        className: "none",
      },
      {
        name: recommendGPUColName,
        data: 'gpuCategory',
        className: "all",
        render: function (data, type, row) {
          if (type === 'display') {
            let recommendGroup = recommendationsMap.get(data);
            return `<span class="` + recommendGroup.getBadgeDisplay(row)
              + `">` + recommendGroup.displayName + `</span>`;
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
        searchable: false,
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
        name: "taskSpeedupFactor",
        data: "taskSpeedupFactor",
        searchable: false,
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
        data: "gpuOpportunity",
        searchable: false,
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
        searchable: false,
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
        searchable: false,
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
        className: "none",
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
        className: "none",
      },
      {
        data: "endDurationEstimated",
        name: "endDurationEstimated",
        orderable: false,
        className: "none",
      },
      {
        name: "failedSQLIds",
        data: "failedSQLIds[, ]",
        searchable: false,
        className: "none",
      },
      {
        name: "potentialProblems",
        data: "potentialProblems_html_safe[</br>]",
        searchable: false,
        className: "none",
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
        searchable: false,
        className: "none",
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
        className: "none",
      },
      {
        data: "complexTypes_html_safe[</br>]",
        name: "complexTypes",
        orderable: false,
        className: "none",
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
        className: "none",
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
        className: "none",
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
            return col.hidden ?
              '<tr data-dt-row="'+col.rowIndex+'" data-dt-column="'+col.columnIndex+'">'+
              '<th scope=\"row\"><span data-toggle=\"tooltip\" data-placement=\"top\"' +
              '    title=\"' + toolTipsValues.rawTable[col.title] + '\">'+col.title+':'+
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
    dom: 'Bfrtlip',
    buttons: [{
      extend: 'csv',
      title: 'rapids_4_spark_qualification_output_ui_raw_data',
      text: 'Export'
    }],
    initComplete: function(settings, json) {
      // Add custom Tool Tip to the headers of the table
      $('#all-apps-raw-data-table thead th').each(function () {
        let $td = $(this);
        let toolTipVal = toolTipsValues.rawTable[$td.text().trim()];
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
  let rawAppsTable = $('#all-apps-raw-data-table').DataTable(rawDataTableConf);

  // set the tootTips for the table
  $('thead th[title]').tooltip({
    container: 'body', "delay":0, "track":true, "fade":250,  "animation": true, "html": true
  });
  setupNavigation();
});
