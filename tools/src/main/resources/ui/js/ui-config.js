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

let qualReportSummary = {
    "config": {
        "showTLCSummary": false
    },
    "totalApps": {
        "numeric": 0,
        "header": "Total Applications",
        "statsPercentage": "%",
        "statsTimeFrame": "Apps with Estimated End Time",
        "totalAppsDurations": "0 ms",
        "totalAppsDurationLabel": "Total Run Durations",
    },
    "candidates": {
        "numeric": 0,
        "header": "RAPIDS Candidates",
        "statsPercentage": "%",
        "statsTimeFrame": "Fit for GPU acceleration",
    },
    "speedups": {
        "numeric": "N/A",
        "header": "GPU Opportunity",
        "statsPercentage": "%",
        "statsTimeFrame": "Supported SQL DF Durations",
        "totalSqlDataframeTaskDuration" : "0 ms",
        "totalSqlDFDurationsLabel" : "Total SqlDF Durations",
    },
    "tlc": {
        "numeric": 0,
        "header": "Apps that need TLC",
        "statsPercentage": "% Needs more information",
        "statsTimeFrame": "We found apps with potential problems",
    },
};

let toolTipsValues = {
    "gpuRecommendations": {
        "App Name": "Name of the application",
        "App ID": "An application is referenced by its application ID, \<em\>app-id\<\/em\>. " +
          "\<br\> When running on YARN, each application may have multiple attempts, but there are " +
          "attempt IDs only for applications in cluster mode, not applications in client mode. " +
          "Applications in YARN cluster mode can be identified by their \<em\>attempt-id\<\/em\>.",
        "App Duration": "Wall-Clock time measured since the application starts till it is completed. " +
          "If an app is not completed an estimated completion time would be computed.",
        "GPU Opportunity": "Wall-Clock time that shows how much of the SQL duration can be accelerated on the GPU.",
        "Recommendation": "Recommendation based on \<em\>Estimated Speed-up Factor\<\/em\>.",
        "Estimated Speed-up": "Speed-up factor estimated for the app. Calculated as the ratio between \<em\>App Duration\<\/em\> and \<em\>Estimated GPU Duration\<\/em\>",
        "details": {
            "mathFormatted": {
                "totalSpeedup":
                  // math tags inside tooltip does not work on Chrome. Using Sup and Sub as a work-around for now.
                  //"Speed-up factor estimated for the app. Calculated as (<math><mfrac><mn>App Duration</mn><mi>GPU Estimated Duration</mi></mfrac></math>)",
                  "Speed-up factor estimated for the app. Calculated as (<sup>App Duration</sup>&frasl;<sub>Estimated GPU Duration</sub>)",
            },
            "totalSpeedup":
              "Speed-up factor estimated for the app. Calculated as (<sup>App Duration</sup>&frasl;<sub>Estimated GPU Duration</sub>)",
            "nonSqlTaskDurationAndOverhead": "total duration of the app not involving SQL",
            "estimatedDuration": "Predicted runtime of the app if it was run on GPU",
            "unsupportedDuration": "An estimate total duration of SQL operations that are not supported on GPU",
            "sqlDFDuration": "Time duration that includes only SQL-Dataframe queries.",
            "gpuOpportunity": "Wall-Clock time that shows how much of the SQL duration can be accelerated on the GPU.",
            "gpuTimeSaved": "Estimated Wall-Clock time saved if it was run on the GPU."
        }
    },
    "rawTable": {
        "App Name": "Name of the application",
        "App ID": "An application is referenced by its application ID, \<em\>app-id\<\/em\>. " +
          "\<br\> When running on YARN, each application may have multiple attempts, but there are " +
          "attempt IDs only for applications in cluster mode, not applications in client mode. " +
          "Applications in YARN cluster mode can be identified by their \<em\>attempt-id\<\/em\>.",
        "App Duration": "Wall-Clock time measured since the application starts till it is completed. " +
          "If an app is not completed an estimated completion time would be computed.",
        "Estimated GPU Duration": "Predicted runtime of the app if it was run on GPU",
        "SQL DF Duration": "Wall-Clock time duration that includes only SQL-Dataframe queries.",
        "SQL Dataframe Task Duration": "Sum of the task time that includes parallel SQL-Dataframe queries.",
        "Executor CPU Time Percent":
          "This is an estimate at how much time the tasks spent doing processing on the CPU vs waiting on IO. Shaded red when it is below 40%",
        "Unsupported Task Duration": "Sum of task durations for any unsupported operators.",
        "GPU Opportunity": "Wall-Clock time that shows how much of the SQL duration can be accelerated on the GPU.",
        "Estimated GPU Speedup":
          "Speed-up factor estimated for the app. Calculated as (<sup>App Duration</sup>&frasl;<sub>GPU Estimated Duration</sub>)",
        "NONSQL Task Duration Plus Overhead": "Time duration that does not span any running SQL task.",
        "Unsupported Read File Formats and Types": "Looks at the Read Schema and reports the file formats along with types " +
          "which may not be fully supported. Example: \<em\>Parquet[decimal], JDBC[*]\<\/em\>. Note that this is based on the current " +
          "version of the plugin and future versions may add support for more file formats and types.",
        "Unsupported Write Data Format": "Reports the data format which we currently don’t support, i.e. if the result " +
          "is written in JSON or CSV format.",
        "Recommendation": "Recommendation based on \<em\>Estimated Speed-up Factor\<\/em\>.",
        "Estimated GPU Time Saved": "Estimated Wall-Clock time saved if it was run on the GPU",
        "Supported SQL DF Task Duration": "Sum of task durations that are supported by RAPIDS GPU acceleration.",
        "App Duration Estimated": "Flag set to true when the application end time was \<em\>estimated\<\/em\> based on the application progress",
        "Task Speed-up Factor": "The average speed-up of all stages."
    },
    "appStages": {
        "Average Speedup Factor": "The average estimated speed-up of all the operators in the given stage",
        "Stage Task Duration": "Amount of time spent in tasks of SQL Dataframe operations for the given stage.",
        "Unsupported Task Duration": "Sum of task durations for the unsupported operators.",
        "Stage Estimated": "True or False indicates if we had to estimate the stage duration.",
    },
    "appExecs": {
        "Speed-up Factor": "It is simply the average acceleration of the operators based on the " +
          "original CPU duration of the operator divided by the GPU duration. The tool uses historical " +
          "queries and benchmarks to estimate a speed-up at an individual operator level to calculate how much a specific " +
          "operator would accelerate on GPU.",
        "Exec Duration": "Wall-Clock time measured since the operator starts till it is completed.",
        "Exec Is Supported": "Whether the Exec is supported by RAPIDS or not.",
    },
}

let UIConfig = {
    "dataProcessing": {
        // name of the column used to decide on the category of the app
        // total SpeedUp is a factor between 1.0 and 10.0
        "gpuRecommendation.appColumn": "estimatedInfo.estimatedGpuSpeedup",
        // when set to true, the JS will generate random value for recommendations
        "simulateRecommendation": false
    },
    "datatables": {
        "gpuRecommendations": {
            "listAppsView": {
                Dom : 'frtlip',
                skipColumns: [],
                sortTable: true,
                sortColumns: [
                    {colName: "recommendation", order: "desc"},
                    {colName: "totalSpeedup", order:"desc"}
                ],
                hideColumns: [],
                searchableColumns: ["appName", "appId", "recommendation"],
                buttons: {
                    enabled: true,
                    buttons: [
                        {
                            extend: 'csv',
                            title: 'rapids_4_spark_qualification_output_ui_apps_recommendations',
                            text: 'Export'
                        }
                    ],
                },
                enabledPanes: ["recommendation", "users"],
                toolTipID: "gpuRecommendations",
            },
            "searchPanes": {
                enabled: true,
                "dtConfigurations": {
                    initCollapsed: true,
                    viewTotal: true,
                    // Note that there is a bug in cascading that breaks paging of the table
                    cascadePanes: true,
                    show: false,
                },
                "panes": {
                    "recommendation": {
                        header: "Recommendations",
                        dtOpts: {
                            searching: true,
                        },
                        order: [[0, 'desc']],
                        combiner: 'and',
                    },
                    "users":{
                        header: "Spark User",
                        dtOpts: {
                            searching: true,
                        },
                        combiner: 'and',
                    }
                }
            },
            "Dom" : {
                default: 'frtlip',
            },
        },
        "appDetails": {
            "colEnabledPrefix": "displayCol_",
            "singleAppView": { // This is a single record. No need to show table information or search
                Dom : 'Brt',
                skipColumns: ["appName"],
                sortTable: false,
                hideColumns: [
                    "appName", "sparkUser", "startTime", "longestSqlDuration",
                    "nonSqlTaskDurationAndOverhead", "endDurationEstimated",
                    "failedSQLIds", "potentialProblems", "readFileFormatAndTypesNotSupported",
                    "writeDataFormat", "complexTypes", "nestedComplexTypes", "readFileFormats"
                ],
                toolTipID: "rawTable"
            },
            "listAppsView": {
                Dom : 'Bfrtlip',
                skipColumns: [],
                sortTable: true,
                sortColumns: [
                    {colName: "gpuRecommendation", order: "desc"},
                    {colName: "totalSpeedupFactor", order:"desc"}
                ],
                searchableColumns: [
                    "appName", "appId", "sparkUser", "gpuRecommendation", "readFileFormats",
                    "nestedComplexTypes", "complexTypes", "readFileFormatAndTypesNotSupported",
                    "writeDataFormat", "potentialProblems"
                ],
                hideColumns: [
                    "appName", "sparkUser", "startTime", "longestSqlDuration",
                    "nonSqlTaskDurationAndOverhead", "endDurationEstimated",
                    "failedSQLIds", "potentialProblems", "readFileFormatAndTypesNotSupported",
                    "writeDataFormat", "complexTypes", "nestedComplexTypes", "readFileFormats"
                ],
                toolTipID: "rawTable",
            }
        },
        "appStages": {
            "colEnabledPrefix": "displayCol_",
            "singleAppView": { // This is a single record. No need to filter because there is no text data
                Dom : 'Brtlip',
                skipColumns: ["appID"],
                sortTable: true,
                sortColumns: [{colName: "stageId", order:"asc"}],
                searchableColumns: [],
                fileExportPrefix: 'rapids_4_spark_qualification_output_ui_stages_data_app',
                hideColumns: [],
                enabledPanes: ["stageEstimated", "speedupFactor", "taskTypes"],
                toolTipID: "appStages",
            },
            "listAppsView": {
                Dom : 'Bfrtlip',
                skipColumns: [],
                sortTable: true,
                sortColumns: [{colName: "stageId", order:"asc"}],
                searchableColumns: [],
                fileExportPrefix: 'rapids_4_spark_qualification_output_ui_stages_data',
                hideColumns: [],
                enabledPanes: ["stageEstimated", "speedupFactor", "taskTypes"],
                toolTipID: "appStages",
            },
            "searchPanes": {
                enabled: true,
                "dtConfigurations": {
                    initCollapsed: true,
                    viewTotal: true,
                    // Note that there is a bug in cascading that breaks paging of the table
                    cascadePanes: true,
                    show: false,
                },
                "panes": {
                    "stageEstimated": {
                        header: "Is Stage Estimated",
                        dtOpts: {
                            searching: false,
                        },
                        combiner: 'and',
                        options: [
                            {
                                label: 'Estimated',
                                value: function(rowData, rowIdx) {
                                    return rowData["estimated"];
                                }
                            },
                            {
                                label: 'Not Estimated',
                                value: function(rowData, rowIdx) {
                                    return !rowData["estimated"];
                                }
                            }
                        ]
                    },
                    "taskTypes": {
                        header: "Are Tasks Supported",
                        dtOpts: {
                            searching: false,
                        },
                        combiner: 'and',
                        options: [
                            {
                                label: 'Supported',
                                value: function(rowData, rowIdx) {
                                    return rowData["unsupportedTaskDur"] <= 0.0;
                                }
                            },
                            {
                                label: 'Unsupported',
                                value: function(rowData, rowIdx) {
                                    return rowData["unsupportedTaskDur"] > 0;
                                }
                            }
                        ]
                    },
                    "speedupFactor":{
                        header: "Speed-up Factor",
                        dtOpts: {
                            searching: false,
                        },
                        combiner: 'and',
                        options: [
                            {
                                label: '1.0 (No Speed-up)',
                                value: function(rowData, rowIdx) {
                                    return rowData["averageSpeedup"] <= 1.0001;
                                }
                            },
                            {
                                label: '1.0 to 1.3',
                                value: function(rowData, rowIdx) {
                                    return rowData["averageSpeedup"] > 1.00 && rowData["averageSpeedup"] < 1.3;
                                }
                            },
                            {
                                label: '1.3 to 2.5',
                                value: function(rowData, rowIdx) {
                                    return rowData["averageSpeedup"] >= 1.3 && rowData["averageSpeedup"] < 2.5;
                                }
                            },
                            {
                                label: '2.5 to 5',
                                value: function(rowData, rowIdx) {
                                    return rowData["averageSpeedup"] >= 2.5 && rowData["averageSpeedup"] < 5;
                                }
                            },
                            {
                                label: '5 or More',
                                value: function(rowData, rowIdx) {
                                    return rowData["averageSpeedup"] >= 5;
                                }
                            },
                        ],
                    },
                }
            }
        },
        "appExecs": {
            "colEnabledPrefix": "displayCol_",
            "singleAppView": {
                Dom : 'Bfrtlip',
                skipColumns: ["appID"],
                sortTable: true,
                sortColumns: [{colName: "sqlID", order:"asc"}],
                fileExportPrefix: 'rapids_4_spark_qualification_output_ui_execs_data_app',
                searchableColumns: ["exec", "expr"],
                hideColumns: ["appID", "isSupported", "shouldRemove"],
                enabledPanes: ["execName", "speedupFactor", "execSupport", "stages", "shouldRemove"],
                toolTipID: "appExecs",
            },
            "listAppsView": {
                Dom : 'Bfrtlip',
                skipColumns: [],
                sortTable: true,
                sortColumns: [
                    {colName: "appID", order: "asc"},
                    {colName: "sqlID", order:"asc"}
                ],
                searchableColumns: ["appID", "exec", "expr"],
                fileExportPrefix: 'rapids_4_spark_qualification_output_ui_execs_data',
                hideColumns: ["appID", "isSupported", "shouldRemove"],
                enabledPanes: ["execName", "speedupFactor", "execSupport", "shouldRemove"],
                toolTipID: "appExecs",
            },
            "searchPanes": {
                enabled: true,
                "dtConfigurations": {
                    initCollapsed: true,
                    viewTotal: true,
                    // Note that there is a bug in cascading that breaks paging of the table
                    cascadePanes: true,
                    show: false,
                },
                "panes": {
                    "execSupport": {
                        header: "Is Exec Supported",
                        dtOpts: {
                            "searching": false
                        },
                        combiner: 'and',
                        options: [
                            {
                                label: 'Supported',
                                value: function(rowData, rowIdx) {
                                    return rowData["isSupported"];
                                }
                            },
                            {
                                label: 'Not Supported',
                                value: function(rowData, rowIdx) {
                                    return !rowData["isSupported"];
                                }
                            }
                        ]
                    },
                    "shouldRemove": {
                        header: "Should Remove Exec",
                        dtOpts: {
                            "searching": false
                        },
                        combiner: 'and',
                        options: [
                            {
                                label: 'Yes',
                                value: function(rowData, rowIdx) {
                                    return rowData["shouldRemove"];
                                }
                            },
                            {
                                label: 'No',
                                value: function(rowData, rowIdx) {
                                    return !rowData["shouldRemove"];
                                }
                            }
                        ]
                    },
                    "execName": {
                        "header": "Exec",
                        dtOpts: {
                            "searching": true
                        },
                        combiner: 'and',
                    },
                    "speedupFactor": {
                        header: "Speed-up Factor",
                        dtOpts: {
                            "searching": false
                        },
                        combiner: 'and',
                        options: [
                            {
                                label: '1.0 (No Speed-up)',
                                value: function(rowData, rowIdx) {
                                    return rowData["speedupFactor"] <= 1.0001;
                                }
                            },
                            {
                                label: '1.0 to 1.3',
                                value: function(rowData, rowIdx) {
                                    return rowData["speedupFactor"] > 1.00 && rowData["speedupFactor"] < 1.3;
                                }
                            },
                            {
                                label: '1.3 to 2.5',
                                value: function(rowData, rowIdx) {
                                    return rowData["speedupFactor"] >= 1.3 && rowData["speedupFactor"] < 2.5;
                                }
                            },
                            {
                                label: '2.5 to 5',
                                value: function(rowData, rowIdx) {
                                    return rowData["speedupFactor"] >= 2.5 && rowData["speedupFactor"] < 5;
                                }
                            },
                            {
                                label: '5 or More',
                                value: function(rowData, rowIdx) {
                                    return rowData["speedupFactor"] >= 5;
                                }
                            },
                        ],
                    },
                    "stages": {
                        header: "By Stage ID",
                        dtOpts: {
                            "searching": true
                        },
                        combiner: 'and',
                    }
                }
            }
        },
    },
    "fullAppView": {
        enabled: true
    }
};
