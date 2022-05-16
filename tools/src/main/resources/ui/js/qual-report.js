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

/*
 * HTML template used to render the application details in the collapsible
 * rows of the GPURecommendationTable.
 */
function getExpandedAppDetails(rowData) {
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
    '      <td> ' + toolTipsValues.gpuRecommendations.details.mathFormatted.totalSpeedup + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">App Duration</th>' +
    '      <td> {{durationCollection.appDuration}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations["App Duration"] + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">SQL Duration</th>' +
    '      <td> {{durationCollection.sqlDFDuration}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations.details.sqlDFDuration + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">GPU Opportunity</th>' +
    '      <td> {{durationCollection.accelerationOpportunity}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations.details.gpuOpportunity + '</td>' +
    '    </tr>' +
    '    <tr>' +
    '      <th scope=\"row\">GPU Estimated Duration</th>' +
    '      <td> {{durationCollection.estimatedDurationWallClock}} </td>' +
    '      <td> ' + toolTipsValues.gpuRecommendations.details.estimatedDuration + '</td>' +
    '    </tr>' +
    '  </tbody>' +
    '</table>';

  if (UIConfig.fullAppView.enabled) {
    return tableContent + fullDetailsContent;
  }
  return tableContent ;
}


function formatAppGPURecommendation ( rowData) {
  var text = Mustache.render(getExpandedAppDetails(rowData), rowData);
  return text;
}

let definedDataTables = {};
let gpuRecommendationTableID = "datatables.gpuRecommendations";

function expandAllGpuRowEntries() {
  expandAllGpuRows(definedDataTables[gpuRecommendationTableID]);
}

function collapseAllGpuRowEntries() {
  collapseAllGpuRows(definedDataTables[gpuRecommendationTableID]);
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

$(document).ready(function(){
  // do the required filtering here
  let attemptArray = processRawData(qualificationRecords);
  let initGpuRecommendationConf = UIConfig[gpuRecommendationTableID];
  // Start implementation of GPU Recommendations Apps

  let recommendGPUColName = "gpuRecommendation"
  let totalSpeedupColumnName = "totalSpeedup"
  let opportunityColName = "accelerationOpportunity"
  let sortColumnForGPURecommend = totalSpeedupColumnName
  let gpuRecommendationConf = {
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
        name: totalSpeedupColumnName,
        data: 'totalSpeedup_display',
        searchable: false,
        type: 'numeric',
      },
      // {
      //   name: opportunityColName,
      //   data: 'accelerationOpportunity',
      //   render: function (data, type, row) {
      //     if (type === 'display') {
      //       return '<progress title="'
      //         + twoDecimalFormatter.format(data)
      //         + '% '
      //         + toolTipsValues["gpuRecommendations"]["GPU Opportunity"]
      //         + '" value="' + data + '" max="100.0"></progress>';
      //     }
      //     return data;
      //   },
      //   // fnCreatedCell: (nTd, sData, oData, _ignored_iRow, _ignored_iCol) => {
      //   //   let toolTipVal = toolTipsValues['gpuRecommendations']['GPU Opportunity'];
      //   //   $(nTd).attr('data-toggle', "tooltip");
      //   //   $(nTd).attr('data-placement', "top");
      //   //   $(nTd).attr('html', "true");
      //   //   $(nTd).attr('data-html', "true");
      //   //   $(nTd).attr('title', toolTipVal);
      //   // }
      // },
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
    //dom: 'Bfrtlip',
    //dom: '<"dtsp-dataTable"Bfrtip>',
    dom: 'Bfrtlip',
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
  if (initGpuRecommendationConf["rowgroup.enabled"] == true) {
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
            + 'title=\"' + recommendationsMap[group].description + '\"';
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
  } // rowGrouping by recommendations


  // set the dom of the tableConf
  gpuRecommendationConf.dom = initGpuRecommendationConf["Dom"].default;

  if (initGpuRecommendationConf.hasOwnProperty('searchPanes')) {
    let searchPanesConf = initGpuRecommendationConf['searchPanes']
    if (searchPanesConf["enabled"]) {
      // disable searchpanes on default columns
      gpuRecommendationConf.columnDefs = [{
        "searchPanes": {
          show: false,
        },
        "targets": ['_all']
      }];
      // add custom panes
      gpuRecommendationConf.searchPanes = searchPanesConf["dtConfigurations"];

      // add the searchpanes to the dom
      gpuRecommendationConf.dom = 'P' + gpuRecommendationConf.dom;
      // add custom panes to display recommendations
      let panesConfigurations = searchPanesConf["panes"];
      // first define values of the first recommendation Pane
      let gpuCatgeoryOptions = function() {
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
      };
      // define the display options of the recommendation pane
      let gpuRecommendationPane = function() {
        let gpuPaneConfig = panesConfigurations["recommendation"];
        let recommendationPaneConf = {};
        recommendationPaneConf.header = gpuPaneConfig["header"];
        recommendationPaneConf.options = gpuCatgeoryOptions();
        recommendationPaneConf.dtOpts = {
          "searching": gpuPaneConfig["search"],
          "order": gpuPaneConfig["order"],
        }
        recommendationPaneConf.combiner = 'and';
        return recommendationPaneConf;
      }
      // define searchPanes for users
      let sparkUsersOptions = function() {
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
      };
      // define the display options of the user filter pane
      let userPane = function() {
        let userPaneConfig = panesConfigurations["users"];
        let recommendationPaneConf = {};
        recommendationPaneConf.header = userPaneConfig["header"];
        recommendationPaneConf.options = sparkUsersOptions();
        recommendationPaneConf.dtOpts = {
          "searching": userPaneConfig["search"],
          "order": userPaneConfig["order"],
        }
        recommendationPaneConf.combiner = 'and';
        return recommendationPaneConf;
      }
      gpuRecommendationConf.searchPanes.panes = [
        gpuRecommendationPane(), userPane()
      ];
    }
  }

  // add buttons if enabled
  if (initGpuRecommendationConf.hasOwnProperty('buttons')) {
    let buttonsConf = initGpuRecommendationConf['buttons'];
    if (buttonsConf["enabled"]) {
      // below if to avoid the buttons messed up with length page
      // dom: "<'row'<'col-sm-12 col-md-10'><'col-sm-12 col-md-2'B>>" +
      //      "<'row'<'col-sm-12 col-md-6'l><'col-sm-12 col-md-6'f>>" +
      //      "<'row'<'col-sm-12'tr>>" +
      //      "<'row'<'col-sm-12 col-md-5'i><'col-sm-12 col-md-7'p>>",
      gpuRecommendationConf["buttons"] = buttonsConf.buttons
      gpuRecommendationConf.dom = 'B' + gpuRecommendationConf.dom
    }
  }

  var gpuRecommendationTable = $('#gpu-recommendation-table').DataTable(gpuRecommendationConf);

  definedDataTables[gpuRecommendationTableID] = gpuRecommendationTable;

  //TODO: we need to expand the rowGroups on search events
  //There is a possible solution
  // https://stackoverflow.com/questions/57692989/datatables-trigger-rowgroup-click-with-search-filter

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
  $('#btn-hide-all-children').on('click', function() {
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
  // set the tootTips for the table
  $('#gpu-recommendation-card [data-toggle="tooltip"]').tooltip({
    container: 'body',
    html: true,
    animation: true,
    placement:"bottom",
    delay: {show: 0, hide: 10.0}});

  setupNavigation();
});
