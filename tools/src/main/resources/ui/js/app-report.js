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

/* globals $, jQuery, Mustache, qualificationRecords, qualReportSummary */

let applicationUIRecord = null;

function fetchApplicationData(appsArray, id) {
  return appsArray.find(app => app.appId === id);
}

// Uses DFS to get flat representations of the execsInfo
function dpExecInfo(queue, resultArray) {
  while (!queue.isEmpty()) {
    let execInfo = queue.dequeue();
    resultArray.push(execInfo);
    // extract execName into the map to build the search Panes
    let execName = extractExecName(execInfo.exec);
    execInfo["execName"] = execName;
    execNames.set(execName, true);
    for (let sqlInd in execInfo.children) {
      queue.enqueue(execInfo.children[sqlInd]);
    }
  }
}

function getAppExecArray(appRecord) {
  let execInfos = [];
  let execBuffer = new Queue();
  for (let ind in appRecord.planInfo) {
    let currPlan = appRecord.planInfo[ind];

    if (currPlan.execInfo.length > 0) {
      // concatenate all elements adding appID
      for (let sqlInd in currPlan.execInfo) {
        let execInfoRec = currPlan.execInfo[sqlInd];
        execBuffer.enqueue(execInfoRec);
      }
    }
    dpExecInfo(execBuffer, execInfos)
  }
  return execInfos;
}

function getAppStagesArray(appRecord) {
  return appStagesMap.addAppRec(appRecord);
}

$(document).ready(function() {
  const queryString = window.location.search;
  const urlParams = new URLSearchParams(queryString);
  const appID = urlParams.get('app_id')

  // get the appData
  let rawAppRecord = fetchApplicationData(qualificationRecords, appID);
  let attemptsArray = [rawAppRecord];
  applicationUIRecord = processRawData(attemptsArray)[0];

  let execsArray = getAppExecArray(applicationUIRecord);
  //
  // set the statistics cards
  //
  let appHeaderInfoRec = {
    "appInfo": {
      appName: applicationUIRecord.appName,
      timing: {
        appDuration: formatDuration(applicationUIRecord.appDuration),
        startTime: formatTimeMillis(applicationUIRecord.startTime),
        endTime: formatTimeMillis(applicationUIRecord.startTime + applicationUIRecord.appDuration),
        gpuOpportunity: applicationUIRecord.durationCollection.gpuOpportunity,
        sqlDFDuration: applicationUIRecord.durationCollection.sqlDFDuration,
        estimatedGPUDuration: applicationUIRecord.durationCollection.estimatedGPUDuration,
        estimatedGPUTimeSaved: applicationUIRecord.durationCollection.gpuTimeSaved,
      },
      estimatedSpeedup: applicationUIRecord.totalSpeedup_display,
      taskSpeedupFactor: applicationUIRecord.taskSpeedupFactor_display,
      execs: {
        totalExecutors: execsArray.length
      }
    },
    "extension": {
      badgeWrapper: getAppBadgeHTMLWrapper(applicationUIRecord)
    }
  };
  let headerTemplate = $("#app-report-page-header-template").html();
  let headerContent = Mustache.render(headerTemplate, appHeaderInfoRec);
  $("#app-report-page-header").html(jQuery.parseHTML(headerContent, false));

  //
  // set the app details table
  //
  let appDetailsDataTable = constructDataTableFromHTMLTemplate(
    [applicationUIRecord],
    "singleAppView",
    createAppDetailedTableConf,
    {
      tableId: "appDetails",
      appId: appID,
      dataTableTemplate: $("#app-details-table-template").html(),
      datatableContainerID: '#app-details-data-container',
      tableDivId: '#all-apps-raw-data-table',
    }
  );

  //
  // set the stage details table
  //
  let appStagesDataTable = constructDataTableFromHTMLTemplate(
      getAppStagesArray(applicationUIRecord),
      "singleAppView",
      createAppDetailsStagesTableConf,
      {
        tableId: "appStages",
        appId: appID,
        dataTableTemplate: $("#app-stages-details-table-template").html(),
        datatableContainerID: '#app-stages-details-data-container',
        tableDivId: '#app-stages-raw-data-table',
      }
    );

  //
  // set the exec details table
  //
  let appExecDetailsTable = constructDataTableFromHTMLTemplate(
    execsArray,
    "singleAppView",
    createAppDetailsExecsTableConf,
    {
      tableId: "appExecs",
      appId: appID,
      dataTableTemplate: $("#app-execs-details-table-template").html(),
      datatableContainerID: '#app-execs-details-data-container',
      tableDivId: '#app-execs-raw-data-table',
    }
  );

  //
  // Set tooltips for the three tables.
  // Note that we should always use method-2
  //
  // method-1:
  //           using datatables. This method has limitations because datatable removes nodes from
  //           the DOM, therefore events applied with a static event listener might not be able to
  //           bind themselves to all nodes in the table.
  // $('#app-execs-details-data-container [data-toggle="tooltip"]').tooltip({
  //   container: 'body',
  //   html: true,
  //   animation: true,
  //   placement:"bottom",});
  //
  // method-2:
  //          Using jQuery delegated event listener options which overcomes the limitations in method-1
  $('tbody').on('mouseover', 'td, th', function () {
    $('[data-toggle="tooltip"]').tooltip({
      trigger: 'hover',
      html: true
    });
  });

  setupNavigation();
});
