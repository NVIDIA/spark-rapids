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
  //
  // set the app details table
  //
  let appDetailsDataTable = constructDataTableFromHTMLTemplate(
    attemptArray,
    "listAppsView",
    createAppDetailedTableConf,
    {
      tableId: "appDetails",
      dataTableTemplate: getAppDetailsTableTemplate(),
      datatableContainerID: '#app-details-data-container',
      tableDivId: '#all-apps-raw-data-table',
    }
  );

  // Set the tootTips for the table header. Enable this when tooltips are limited to the
  // table headers
  // $('thead th[title]').tooltip({
  //   container: 'body', "delay":0, "track":true, "fade":250,  "animation": true, "html": true
  // });

  //
  // Set tooltips for the datatables using jQuery delegated event listener options.
  // Note that we should always use Note that we should always use jQuery delegated event listener
  // options as documented in app-report.js
  //
  $('tbody').on('mouseover', 'td, th', function () {
    $('[data-toggle="tooltip"]').tooltip({
      trigger: 'hover',
      html: true
    });
  });

  setupNavigation();
});
