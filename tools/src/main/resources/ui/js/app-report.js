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

function getAppReportPageHeaderTemplate() {
  let content = `
    <h1 class="dash-title">App Details: {{appInfo.appName}} {{{extension.badgeWrapper}}}</h1>
    <div class="row">
      <div class="col-lg-8">
        <div class="alert alert-warning alert-dismissible fade show" role="alert">
          <svg width="1.2em" height="1.2em" viewBox="0 0 16 16"
               fill="none"
               xmlns="http://www.w3.org/2000/svg">
            <path d="M8 6.5V10M8 11.0001V12.0001M8 2L14.5 13.4983H1.5L8 2Z" stroke="black"/>
          </svg>
          <strong>Disclaimer!</strong><ul>
          <li>Estimates provided by the Qualification tool are based on the currently supported "<em>SparkPlan</em>" or "<em>Executor Nodes</em>" used in the application. It currently does not look at the expressions or datatypes used.</li>
          <li>Please refer to the <a href="https://nvidia.github.io/spark-rapids/docs/supported_ops.html">Supported Operators</a> guide to check the types and expressions you are using are supported.</li></ul>
          <button type="button" class="close" data-dismiss="alert" aria-label="Close">
            <span aria-hidden="true">&times;</span>
          </button>
        </div>
      </div>
    </div>
    <div class="row">
      <div class="col-lg-4">
        <div class="stats stats-primary">
          <h3 class="stats-title"> App Duration </h3>
          <div class="stats-content">
            <div class="stats-icon">
              <svg width="1.05em"
                   height="1.05em"
                   viewBox="0 0 16 16"
                   fill="none"
                   xmlns="http://www.w3.org/2000/svg">
                <path d="M6.96447 4.13534L8 8L11.8647 9.03553M14.5 8C14.5 11.5899 11.5899 14.5 8 14.5C4.41015 14.5 1.5 11.5899 1.5 8C1.5 4.41015 4.41015 1.5 8 1.5C11.5899 1.5 14.5 4.41015 14.5 8Z"
                      stroke="white"/>
              </svg>
            </div>
            <div class="stats-data">
              <div class="stats-number">{{appInfo.timing.appDuration}}</div>
              <div class="stats-change">
                <span class="stats-percentage">{{appInfo.timing.startTime}}</span>
                <span class="stats-timeframe">Start</span>
              </div>
              <div class="stats-change">
                <span class="stats-percentage">{{appInfo.timing.endTime}}</span>
                <span class="stats-timeframe">End</span>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="col-lg-4">
        <div class="stats stats-primary">
          <h3 class="stats-title"> GPU Opportunity </h3>
          <div class="stats-content">
            <div class="stats-icon">
              <svg width="1.05em"
                   height="1.05em"
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
              <div class="stats-number">{{appInfo.timing.gpuOpportunity}}</div>
              <div class="stats-change">
                <span class="stats-percentage">{{appInfo.timing.sqlDFDuration}}</span>
                <span class="stats-timeframe">SQL DF Duration</span>
              </div>
              <div class="stats-change">
                <span class="stats-percentage">{{appInfo.taskSpeedupFactor}}</span>
                <span class="stats-timeframe">Task Speed-up Factor</span>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="col-lg-4">
        <div class="stats stats-primary">
          <h3 class="stats-title"> Estimated GPU Duration </h3>
          <div class="stats-content">
            <div class="stats-icon">
              <i>
                <svg width="1.05em" height="1.05em" viewBox="0 0 16 16"
                     fill="none"
                     xmlns="http://www.w3.org/2000/svg">
                  <path d="M12.5 14.5V12.5L9.90192 8L12.5 3.5V1.5H3.5V3.5L6.09808 8L3.5 12.5V14.5H12.5Z"
                        stroke="white"/>
                  <path d="M3.5 12.5H12.5" stroke="white"/>
                  <path d="M12.5 3.5H3.5" stroke="white"/>
                </svg>
              </i>
            </div>
            <div class="stats-data">
              <div class="stats-number">{{appInfo.timing.estimatedGPUDuration}}</div>
              <div class="stats-change">
                <span class="stats-percentage">{{appInfo.timing.estimatedGPUTimeSaved}}</span>
                <span class="stats-timeframe">Time Saved</span>
              </div>
              <div class="stats-change">
                <span class="stats-percentage">{{appInfo.estimatedSpeedup}}</span>
                <span class="stats-timeframe">App Speed-up</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
  return content;
}

function getAppStagesDetailsTableTemplate() {
  let content = `
    <div id="app-stages-raw-data">
      <table id="app-stages-raw-data-table" class="table data-table display wrap" style="width:100%">
        <thead>
        <tr>
          {{#displayCol_appID}}
          <th>App ID</th>
          {{/displayCol_appID}}
          <th>Stage ID</th>
          <th>Average Speedup Factor</th>
          <th>Stage Task Duration</th>
          <th>Unsupported Task Duration</th>
          <th>Stage Estimated</th>
        </tr>
        </thead>
      </table>
    </div>
  `;
  return content;
}

function getAppExecsDetailsTableTemplate() {
  let content = `
    <div id="app-execs-raw-data">
      <table id="app-execs-raw-data-table" class="table data-table display wrap" style="width:100%">
        <thead>
        <tr>
          {{#displayCol_appID}}
          <th>App ID</th>
          {{/displayCol_appID}}
          <th>SQL ID</th>
          <th>Exec Name</th>
          <th>Expression Name</th>
          <th>Exec Is Supported</th>
          <th>Speed-up Factor</th>
          <th>Exec Duration</th>
          <th>SQL Node Id</th>
          <th>Stages</th>
          <th>Exec Children</th>
          <th>Exec Children Node Ids</th>
          <th>Is Removed</th>
        </tr>
        </thead>
      </table>
    </div>
  `;
  return content;
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

  let sqlIDsArray = getAppSqlArray(applicationUIRecord);
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
  let headerContent = Mustache.render(getAppReportPageHeaderTemplate(), appHeaderInfoRec);
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
      dataTableTemplate: getAppDetailsTableTemplate(),
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
        dataTableTemplate: getAppStagesDetailsTableTemplate(),
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
      dataTableTemplate: getAppExecsDetailsTableTemplate(),
      datatableContainerID: '#app-execs-details-data-container',
      tableDivId: '#app-execs-raw-data-table',
    }
  );

  //
  // set the sqlID details table
  //

  let appSQLsDetailsTable = constructDataTableFromHTMLTemplate(
    sqlIDsArray,
    "singleAppView",
    createAppDetailsSQLsTableConf,
    {
      tableId: "appSQLs",
      appId: appID,
      dataTableTemplate: getAppSQLsDetailsTableTemplate(),
      datatableContainerID: '#app-sqls-details-data-container',
      tableDivId: '#app-sqls-raw-data-table',
      replaceTableIfEmptyData: {
        enabled: true,
        text: "No Data to display in the table"
      }
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
