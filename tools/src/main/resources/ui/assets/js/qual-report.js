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

function resetCollapsableGrps(groupArr, flag) {
  groupArr.forEach(grpElemnt => grpElemnt.collapsed = flag);
}

let toolTipsValues = {
  "gpuRecommendations": {
    "App Name": "Name of the application",
    "App ID": "An application is referenced by its application ID, [app-id]. \
              When running on YARN, each application may have multiple attempts, but there are attempt IDs only for applications in cluster mode, not applications in client mode. Applications in YARN cluster mode can be identified by their [attempt-id].",
    "App Duration": "Wall-Clock time measured since the application starts till it is completed. If an app is not completed an estimated completion time would be computed.",
    "Acceleration Opportunity": "All applications are ranked based on a score. Todo: add more useful description here",
  }
}

/*
 * HTML template used to render the application details in the collapsible
 * rows of the GPURecommendationTable.
 */
var recommendTblAppDetailsTemplate =
    '<table class=\"table table-striped compact dataTable style=padding-left:50px;\">' +
    '  <thead>' +
    '    <tr>' +
    '      <th scope=\"col\">#</th>' +
    '      <th scope=\"col\">Value</th>' +
    '    </tr>' +
    '  </thead>' +
    '  <tbody>' +
    '    <tr>' +
    '      <th scope=\"row\">Total SpeedUps</th>' +
    '      <td> N/A </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">User</th>' +
    '      <td> N/A </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">Completed</th>' +
    '      <td> N/A </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">App Duration</th>' +
    '      <td> {{durationCollection.appDuration}} </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">Sql DF Duration</th>' +
    '      <td> {{durationCollection.sqlDFDuration}} </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">Sql DF Task Duration</th>' +
    '      <td> {{durationCollection.sqlDFTaskDuration}} </td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">Duration of Problematic Sql</th>' +
    '      <td> {{durationCollection.sqlDurationProblems}} </td>' +
    '    </tr>' +
    '  </tbody>' +
    '</table>' +
    '<div class=\" mt-3\">' +
    '  <a href=\"{{attemptDetailsURL}}\" target=\"_blank\" class=\"btn btn-secondary btn-lg btn-block mb-1\">Go To Full Details</button>' +
    '</div>';

function formatAppGPURecommendation ( rowData) {
  var text = Mustache.render(recommendTblAppDetailsTemplate, rowData);
  return text;
}

var definedDataTables = {};

function expandAllGpuRowEntries() {
  expandAllGpuRows(definedDataTables["gpuRecommendations"]);
}

function collapseAllGpuRowEntries() {
  collapseAllGpuRows(definedDataTables["gpuRecommendations"]);
}

function expandAllGpuRows(gpuTable) {
  resetCollapsableGrps(recommendationContainer, false);

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
  resetCollapsableGrps(recommendationContainer, true);
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


var useStaticToolTip = false;

$(document).ready(function(){
  // do required filtering here
  var attemptArray = processRawData(qualificationRecords);

  // Start implementation of GPU Recommendations Apps
  var sortColumnForGPURecommend = "score"
  var recommendGPUColName = "gpuRecommendation"
  var gpuRecommendationConf = {
    responsive: true,
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
        name: sortColumnForGPURecommend,
        data: "sqlDataframeTaskDuration",
        render: function (data, type, row) {
          if (type === 'display' || type === 'filter') {
            return formatDuration(data)
          }
          return data;
        },
      }
    ],
    //dom with search panes
    //dom: 'Bfrtlip',
    //dom: '<"dtsp-dataTable"Bfrtip>',
    dom: 'Bfrtlip',
    // below if to avoid the buttons messed up with length page
    // dom: "<'row'<'col-sm-12 col-md-10'><'col-sm-12 col-md-2'B>>" +
    //      "<'row'<'col-sm-12 col-md-6'l><'col-sm-12 col-md-6'f>>" +
    //      "<'row'<'col-sm-12'tr>>" +
    //      "<'row'<'col-sm-12 col-md-5'i><'col-sm-12 col-md-7'p>>",
    buttons: [
      {
          extend: 'csv',
          text: 'Export'
      }
    ],
    initComplete: function(settings, json) {
      // Add custom Tool Tip to the headers of the table
      $('#gpu-recommendation-table thead th').each(function () {
         var $td = $(this);
         var toolTipVal = toolTipsValues.gpuRecommendations[$td.text().trim()];
         $td.attr('data-toggle', "tooltip");
         $td.attr('data-placement', "top");
         $td.attr('html', "true");
         $td.attr('data-html', "true");
         $td.attr('title', toolTipVal);
      });
    }
  };

  gpuRecommendationConf.order =
      [[getColumnIndex(gpuRecommendationConf.columns, sortColumnForGPURecommend), "desc"]];
  gpuRecommendationConf.rowGroup = {
    startRender: function (rows, group) {
      // var collapsed = !!(collapsedGroups[group]);
      let collapsedBool = recommendationsMap[group].collapsed;
      rows.nodes().each(function (r) {
        r.style.display = '';
        if (collapsedBool) {
          r.style.display = 'none';
        }
      });
      // Iterate group rows and close open child rows.
      if (collapsedBool) {
        rows.every(function (rowIdx, tableLoop, rowLoop) {
          if (this.child.isShown()) {
            var tr = $(this.node());
            this.child.hide();

            tr.removeClass('shown');
          }
        });
      }
      var arrow = collapsedBool ?
        '<span class="collapse-table-arrow arrow-closed"></span> '
        : ' <span class="collapse-table-arrow arrow-open"></span> ';

      let toolTip = 'data-toggle=\"tooltip\" data-html=\"true\" data-placement=\"top\" '
          +'title=\"' + recommendationsMap[group].description + '\"';
      var addToolTip = true;
      return $('<tr/>')
          .append('<td colspan=\"' + rows.columns()[0].length + '\"'
              + (addToolTip ? toolTip : '') + '>'
              + arrow + '&nbsp;'
              + group
              + ' (' + rows.count() + ')'
              + '</td>')
          .attr('data-name', group)
          .toggleClass('collapsed', collapsedBool);
    },
    dataSrc: function (row) {
      var recommendedGroup = recommendationContainer.find(grp => grp.isGroupOf(row))
      return recommendedGroup.displayName;
    }
  }

  var gpuRecommendationTable = $('#gpu-recommendation-table').DataTable(gpuRecommendationConf);
  definedDataTables["gpuRecommendations"] = gpuRecommendationTable;

  //TODO: we need to expand the rowGroups on search events
  //There is a possible solution https://stackoverflow.com/questions/57692989/datatables-trigger-rowgroup-click-with-search-filter

  $('#gpu-recommendation-table tbody').on('click', 'tr.dtrg-start', function () {
    var name = $(this).data('name');
    // we may need to hide tooltip hangs
    // $('#gpu-recommendation-table [data-toggle="tooltip"]').tooltip('hide');
    recommendationsMap[name].toggleCollapsed();
    gpuRecommendationTable.draw(false);
  });


  // Add event listener for opening and closing details
  $('#gpu-recommendation-table tbody').on('click', 'td.dt-control', function () {
    var tr = $(this).closest('tr');
    var row = gpuRecommendationTable.row( tr );

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
  $('#btn-hide-all-children').on('click', function(){
    collapseAllGpuRows(gpuRecommendationTable);
  });

  // set the template of the report qualReportSummary
  var template = $("#qual-report-summary-template").html();
  var text = Mustache.render(template, qualReportSummary);
  $("#qual-report-summary").html(text);

  // set the template of the Qualification runtimeInformation
  if (false) {
    //TODO: fill the template of the execution: last executed, how long it took..etc
    var template = $("#qual-report-runtime-information-template").html();
    var text = Mustache.render(template, qualReportSummary);
    $("#qual-report-runtime-information").html(text);
  }
});
