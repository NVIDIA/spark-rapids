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
              return `<a href="${row.attemptDetailsURL}" target="_blank">${appId}</a>`
            }
          }
          return appId;
        }
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
      }
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
      dataTableTemplate: $("#gpu-recommendation-table-template").html(),
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
  let template = $("#qual-report-summary-template").html();
  let text = Mustache.render(template, qualReportSummary);
  $("#qual-report-summary").html(jQuery.parseHTML(text, false));

  // set the tootTips for the table
  $('#gpu-recommendation-card [data-toggle="tooltip"]').tooltip({
    container: 'body',
    html: true,
    animation: true,
    placement:"bottom",
    delay: {show: 0, hide: 10.0}});

  setupNavigation();
});
