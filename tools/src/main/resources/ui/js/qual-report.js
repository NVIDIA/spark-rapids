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

/* global $, Mustache, formatDuration, jQuery, qualificationRecords, qualReportSummary */

/*
 * HTML template used to render the application details in the collapsible
 * rows of the GPURecommendationTable.
 */
function getExpandedAppDetails() {
  let fullDetailsContent =
    '<div class=\" mt-3\">' +
    '  <a href=\"{{attemptDetailsURL}}\" target=\"_blank\" class=\"btn btn-secondary btn-lg btn-block mb-1\">Go To Full Details</button>' +
    '</div>';
  let tableContent =
    '<table class=\"table table-striped style=padding-left:50px;\">' +
    '  <col style=\"width:20%\">' +
    '  <col style=\"width:10%\">' +
    '  <col style=\"width:70%\">' +
    '  <thead>' +
    '    <tr>' +
    '      <th scope=\"col\">#</th>' +
    '      <th scope=\"col\">Value</th>' +
    '      <th scope=\"col\">Description</th>' +
    '    </tr>' +
    '  </thead>' +
    '  <tbody>' +
    '    <tr>' +
    '      <th scope=\"row\">Estimated Speed-up</th>' +
    '      <td> {{totalSpeedup_display}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations.details.totalSpeedup + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">App Duration</th>' +
    '      <td> {{durationCollection.appDuration}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations["App Duration"] + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">Estimated GPU Duration</th>' +
    '      <td> {{durationCollection.estimatedDurationWallClock}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations.details.estimatedDuration + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">SQL Duration</th>' +
    '      <td> {{durationCollection.sqlDFDuration}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations.details.sqlDFDuration + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">GPU Opportunity</th>' +
    '      <td> {{durationCollection.gpuOpportunity}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations.details.gpuOpportunity + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">GPU Time Saved</th>' +
    '      <td> {{durationCollection.gpuTimeSaved}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations.details.gpuTimeSaved + '</td>' +
    '    </tr>' +
    '  </tbody>' +
    '</table>';

  if (UIConfig.fullAppView.enabled) {
    return tableContent + fullDetailsContent;
  }
  return tableContent ;
}

function formatAppGPURecommendation ( rowData) {
  return Mustache.render(getExpandedAppDetails(), rowData);
}

let definedDataTables = {};
let gpuRecommendationTableID = "gpuRecommendations";

function expandAllGpuRowEntries() {
  expandAllGpuRows(definedDataTables[gpuRecommendationTableID]);
}

function collapseAllGpuRowEntries() {
  collapseAllGpuRows(definedDataTables[gpuRecommendationTableID]);
}

function expandAllGpuRows(gpuTable) {
  // Enumerate all rows
  gpuTable.rows().every(function(){
    // If row has details collapsed
    if (!this.child.isShown()){
      // Open this row
      this.child(formatAppGPURecommendation(this.data())).show();
      $(this.node()).addClass('shown');
    }
  });
  gpuTable.draw(false);
}

function collapseAllGpuRows(gpuTable) {
  // Enumerate all rows
  gpuTable.rows().every(function(){
    // If row has details expanded
    if(this.child.isShown()){
      // Collapse row details
      this.child.hide();
      $(this.node()).removeClass('shown');
    }
  });
  gpuTable.draw(false);
}

function createGPURecommendationTableConf(
  attemptArray,
  tableViewType,
  mustacheRecord,
  extraFunctionArgs) {
  let initGpuRecommendationConf = UIConfig.datatables[extraFunctionArgs.tableId];
  let initGpuRecommendationCustomConf = initGpuRecommendationConf[tableViewType];
  // Start implementation of GPU Recommendations Apps
  let recommendGPUColName = "recommendation"
  let totalSpeedupColumnName = "totalSpeedup"
  let gpuRecommendationConf = {
    info: true,
    paging: (attemptArray.length > defaultPageLength),
    pageLength: defaultPageLength,
    lengthMenu: defaultLengthMenu,
    stripeClasses: [],
    "data": attemptArray,
    "columns": [
      {
        "className":      'dt-control',
        "orderable":      false,
        "data":           null,
        "defaultContent": ''
      },
      {
        name: "appName",
        data: "appName"
      },
      {
        name: "appId",
        data: "appId",
        render:  (appId, type, row) => {
          if (type === 'display' || type === 'filter') {
            if (UIConfig.fullAppView.enabled) {
              return `<a href="${row.attemptDetailsURL}">${appId}</a>`
            }
          }
          return appId;
        }
      },
      {
        name: recommendGPUColName,
        data: 'gpuCategory',
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
        data: 'totalSpeedup',
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
          if (type === 'display') {
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
    ],
    //dom with search panes
    dom: initGpuRecommendationCustomConf.Dom,
    initComplete: function(settings, json) {
      // Add custom Tool Tip to the headers of the table
      // Add custom Tool Tip to the headers of the table
      let thLabel = extraFunctionArgs.tableDivId + ' thead th';
      let dataTableToolTip = toolTipsValues[initGpuRecommendationCustomConf.toolTipID];
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
    gpuRecommendationConf,
    initGpuRecommendationConf,
    initGpuRecommendationCustomConf,
    mustacheRecord);


  // set searchpanes
  let optionGeneratorsFunctionsMap = new Map();
  optionGeneratorsFunctionsMap.set("recommendation", function() {
    let categoryOptions = [];
    for (let i in recommendationContainer) {
      let currOption = {
        label: recommendationContainer[i].displayName,
        value: function(rowData, rowIdx) {
          return (rowData["gpuCategory"] === recommendationContainer[i].displayName);
        }
      }
      categoryOptions.push(currOption);
    }
    return categoryOptions;
  });

  optionGeneratorsFunctionsMap.set("users", function() {
    let sparkUserOptions = [];
    sparkUsers.forEach((data, userName) => {
      let currOption = {
        label: userName,
        value: function(rowData, rowIdx) {
          // get spark user
          return (rowData["user"] === userName);
        },
      }
      sparkUserOptions.push(currOption);
    });
    return sparkUserOptions;
  });

  setDataTableSearchPanes(
    gpuRecommendationConf,
    initGpuRecommendationConf,
    initGpuRecommendationCustomConf,
    optionGeneratorsFunctionsMap);

  // add buttons if enabled
  setDataTableButtons(
    gpuRecommendationConf,
    initGpuRecommendationConf,
    initGpuRecommendationCustomConf);

  return gpuRecommendationConf;
}

function getGPURecommendationTableTemplate() {
  return `
      <div id=\"apps-recommendations-data\">
        <table id=\"gpu-recommendation-table\" class=\"table display\" style=\"width:100%\">
          <thead>
          <tr>
            <th></th>
            <th>
              App Name
            </th>
            <th>
              App ID
            </th>
            <th>
              Recommendation
            </th>
            <th>
              Estimated Speed-up
            </th>
            <th>
              App Duration
            </th>
          </tr>
          </thead>
        </table>
      </div>
  `;
}

function getGlobalStatisticsTemplate() {
  return `
      <div class="row dash-row">
        <div class="col-xl-4">
          <div class="stats stats-primary">
            <h3 class="stats-title"> {{totalApps.header}} </h3>
            <div class="stats-content">
              <div class="stats-icon">
                <svg width="1.05em"
                     height="1.05em"
                     viewBox="0 0 16 16"
                     fill="none"
                     xmlns="http://www.w3.org/2000/svg">
                  <path d="M6.5 13.5V10.5H3.5V13.5H6.5ZM6.5 13.5H9.5M6.5 13.5V6.5H9.5V13.5M9.5 13.5H12.5V2.5H9.5V13.5Z"
                        stroke="white"/>
                </svg>
              </div>
              <div class="stats-data">
                <div class="stats-number"> {{totalApps.numeric}}</div>
                <div class="stats-change">
                  <span class="stats-percentage"> {{totalApps.totalAppsDurations}} </span>
                  <span class="stats-timeframe"> {{totalApps.totalAppsDurationLabel}} </span>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div class="col-xl-4">
          <div class="stats stats-success ">
            <h3 class="stats-title"> {{candidates.header}} </h3>
            <div class="stats-content">
              <div class="stats-icon">
                <svg width="1.05em"
                     height="1.05em"
                     viewBox="0 0 16 16"
                     fill="none"
                     xmlns="http://www.w3.org/2000/svg">
                  <path d="M1.5 6.5H2V9.5H1.5M6.5 1.5V2H9.5V1.5M14.5 6.5H14V9.5H14.5M6.5 14.5V14H9.5V14.5M14.5 14.5H1.5V1.5H14.5V14.5ZM4.5 4.5H5.5V5.5H4.5V4.5ZM4.5 7.5H5.5V8.5H4.5V7.5ZM4.5 10.5H5.5V11.5H4.5V10.5ZM7.5 4.5H8.5V5.5H7.5V4.5ZM7.5 7.5H8.5V8.5H7.5V7.5ZM7.5 10.5H8.5V11.5H7.5V10.5ZM10.5 4.5H11.5V5.5H10.5V4.5ZM10.5 7.5H11.5V8.5H10.5V7.5ZM10.5 10.5H11.5V11.5H10.5V10.5Z"
                        stroke="white"/>
                </svg>
              </div>
              <div class="stats-data">
                <div class="stats-number"> {{candidates.numeric}} </div>
                <div class="stats-change">
                  <span class="stats-percentage"> {{candidates.statsPercentage}} </span>
                  <span class="stats-timeframe"> {{candidates.statsTimeFrame}} </span>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div class="col-xl-4">
          <div class="stats stats-info">
            <h3 class="stats-title"> {{speedups.header}} </h3>
            <div class="stats-content">
              <div class="stats-icon">
                <svg width="1.05em" height="1.05em"
                viewBox="0 0 16 16"
                     fill="none"
                     xmlns="http://www.w3.org/2000/svg">
                  <g clip-path="url(#clip0_2_1921)">
                    <path d="M8.5 7.99V8L2.5 11.5V4.5L8.5 7.99Z" stroke="white"/>
                    <path d="M14.5 8L8.5 11.5V4.5L14.5 8Z" stroke="white"/>
                  </g>
                  <defs>
                    <clipPath id="clip0_2_1921">
                      <rect width="1.05em" height="1.05em" fill="white"/>
                    </clipPath>
                  </defs>
                </svg>
              </div>
              <div class="stats-data">
                <div class="stats-number"> {{speedups.numeric}} </div>
                <div class="stats-change">
                  <span class="stats-percentage"> {{speedups.totalSqlDataframeTaskDuration}} </span>
                  <span class="stats-timeframe"> {{speedups.totalSqlDFDurationsLabel}} </span>
                </div>
                <div class="stats-change">
                  <span class="stats-percentage"> {{speedups.statsPercentage}} </span>
                  <span class="stats-timeframe"> {{speedups.statsTimeFrame}} </span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
  `;
}

$(document).ready(function(){
  // do the required filtering here
  let attemptArray = processRawData(qualificationRecords);

  let gpuRecommendationTable = constructDataTableFromHTMLTemplate(
    attemptArray,
    "listAppsView",
    createGPURecommendationTableConf,
    {
      tableId: "gpuRecommendations",
      datatableContainerID: '#app-recommendations-data-container',
      dataTableTemplate: getGPURecommendationTableTemplate(),
      tableDivId: '#gpu-recommendation-table',
    }
  );

  definedDataTables[gpuRecommendationTableID] = gpuRecommendationTable;

  // Add event listener for opening and closing details
  $('#gpu-recommendation-table tbody').on('click', 'td.dt-control', function () {
    let tr = $(this).closest('tr');
    let row = gpuRecommendationTable.row( tr );

    if ( row.child.isShown() ) {
      // This row is already open - close it
      row.child.hide();
      tr.removeClass('shown');
    }
    else {
      // Open this row
      row.child( formatAppGPURecommendation(row.data()) ).show();
      tr.addClass('shown');
    }
  });

  // Handle click on "Expand All" button
  $('#btn-show-all-children').on('click', function() {
    expandAllGpuRows(gpuRecommendationTable);
  });

  // Handle click on "Collapse All" button
  $('#btn-hide-all-children').on('click', function() {
    collapseAllGpuRows(gpuRecommendationTable);
  });

  // set the template of the report qualReportSummary
  let text = Mustache.render(getGlobalStatisticsTemplate(), qualReportSummary);
  $("#qual-report-summary").html(jQuery.parseHTML(text, false));

  //
  // Set tooltips for the datatables using jQuery delegated event listener options.
  // Note that we should always use Note that we should always use jQuery delegated event listener
  // options as documented in app-report.js
  //
  $('#gpu-recommendation-table tbody').on('mouseover', 'td, th', function () {
    $('[data-toggle="tooltip"]').tooltip({
      trigger: 'hover',
      html: true
    });
  });

  setupNavigation();
});
