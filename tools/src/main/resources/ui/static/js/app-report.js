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
var attemptsArray = null;

function fetchApplicationData(id) {
  return attemptsArray.find(app => app.appId === id);
}

function getBadgeName(appRecord) {
  var groupBadgeClasses = {
    "A": "badge-success",
    "B": "badge-primary",
    "C": "badge-secondary",
    "D": "badge-light"
  }
  var recommendedGroup = recommendationContainer.find(grp => grp.isGroupOf(appRecord));
  return `<span class=\"badge badge-pill `
    + groupBadgeClasses[recommendedGroup.id]
    + `\"> ` + recommendedGroup.displayName + `</span>`
}

$(document).ready(function(){
  attemptsArray = processRawData(qualificationRecords);
  const queryString = window.location.search;
  const urlParams = new URLSearchParams(queryString);
  const appID = urlParams.get('app_id')
  console.log(appID);

  // get the appData
  applicationUIRecord = fetchApplicationData(appID);
  applicationUIRecord["badgeWrapper"] = getBadgeName(applicationUIRecord);
  // set the template of the report header
  var template = $("#app-report-page-header-template").html();
  var text = Mustache.render(template, applicationUIRecord);
  $("#app-report-page-header").html(text);
});
