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

/* globals $, Mustache, qualificationRecords, qualReportSummary */

var applicationUIRecord = null;
var applicationInfoRecord = null;
var attemptsArray = null;
let dsInfoRecords = null;

function fetchApplicationData(id) {
  return attemptsArray.find(app => app.appId === id);
}

function fetchApplicationInfoData(id) {
  return appInfoMap.get(id);
}

function getBadgeName(appRecord) {
  let recommendGroup =  recommendationsMap.get(appRecord.gpuCategory);
  return `<span class="` + recommendGroup.getBadgeDisplay(appRecord)
      + `">` + recommendGroup.displayName + `</span>`
}

$(document).ready(function(){
  attemptsArray = processRawData(qualificationRecords, appInfoRecords);
  let dsInfoContainer = processReadFormatSchema(dataSourceInfoRecords);
  dsInfoRecords = dsInfoContainer.records;

  const queryString = window.location.search;
  const urlParams = new URLSearchParams(queryString);
  const appID = urlParams.get('app_id')
  console.log(appID);

  // get the appData
  applicationUIRecord = fetchApplicationData(appID);
  applicationInfoRecord = fetchApplicationInfoData(appID);
  let appDsInfoRec = getDataSourceInfoForApp(dsInfoRecords, appID);
  // set the template of the report header
  let combinedRec = {
    "appInfo":  applicationInfoRecord,
    "qualInfo": applicationUIRecord,
    "extension": {
      "accelerationOpportunity":
        twoDecimalFormatter.format(applicationUIRecord.accelerationOpportunity) + '%',
      "badgeWrapper": getBadgeName(applicationUIRecord),
      "speedUp": twoDecimalFormatter.format(applicationUIRecord.speedupFactor)
    }
  };

  var template = $("#app-report-page-header-template").html();
  var text = Mustache.render(template, combinedRec);

  $("#app-report-page-header").html(text);
  let dataArray = appDsInfoRec["dsData"];
  let dataSrcTableConf = {
    info: true,
    paging: (dataArray.length > defaultPageLength),
    "data": dataArray,
    "columns": [
      {
        data: "sqlID",
      },
      {
        data: "format",
      },
      {
        data: "location",
      },
      {
        data: "pushedFilters",
      },
      {
        data: "schema",
      },
    ],
  };
  let appDSInfoTable = $('#app-details-datasourceinfo-table').DataTable(dataSrcTableConf);
});
