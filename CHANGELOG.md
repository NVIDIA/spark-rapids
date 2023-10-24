# Change log
Generated on 2023-10-24

## Release 23.10

### Features
|||
|:---|:---|
|[#9220](https://github.com/NVIDIA/spark-rapids/issues/9220)|[FEA] Add GPU support for converting binary data to a hex string in REPL|
|[#9171](https://github.com/NVIDIA/spark-rapids/issues/9171)|[FEA] Add GPU version of ToPrettyString|
|[#5314](https://github.com/NVIDIA/spark-rapids/issues/5314)|[FEA] Support window.rowsBetween(Window.unboundedPreceding, -1) |
|[#9057](https://github.com/NVIDIA/spark-rapids/issues/9057)|[FEA] Add unbounded to unbounded fixers for min and max|
|[#8121](https://github.com/NVIDIA/spark-rapids/issues/8121)|[FEA] Add Spark 3.5.0 shim layer|
|[#9224](https://github.com/NVIDIA/spark-rapids/issues/9224)|[FEA] Allow } and }} to be transpiled to static strings|
|[#8596](https://github.com/NVIDIA/spark-rapids/issues/8596)|[FEA] Support spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY|
|[#8767](https://github.com/NVIDIA/spark-rapids/issues/8767)|[AUDIT][SPARK-43302][SQL] Make Python UDAF an AggregateFunction|
|[#9055](https://github.com/NVIDIA/spark-rapids/issues/9055)|[FEA]  Support Spark 3.3.3 official release|
|[#8672](https://github.com/NVIDIA/spark-rapids/issues/8672)|[FEA] Make GPU readers easier to debug on failure (any failure including OOM)|
|[#8965](https://github.com/NVIDIA/spark-rapids/issues/8965)|[FEA] Enable Bloom filter join acceleration by default|
|[#8625](https://github.com/NVIDIA/spark-rapids/issues/8625)|[FEA] Support outputTimestampType being INT96|

### Performance
|||
|:---|:---|
|[#7803](https://github.com/NVIDIA/spark-rapids/issues/7803)|[FEA] Accelerate Bloom filtered joins |

### Bugs Fixed
|||
|:---|:---|
|[#9485](https://github.com/NVIDIA/spark-rapids/issues/9485)|[BUG] GpuSemaphore can deadlock if there are multiple threads per task|
|[#9498](https://github.com/NVIDIA/spark-rapids/issues/9498)|[BUG] spark 3.5.0 shim spark-shell is broken in spark-rapids 23.10 and 23.12|
|[#9060](https://github.com/NVIDIA/spark-rapids/issues/9060)|[BUG] OOM error in split and retry with multifile coalesce reader with parquet data|
|[#8916](https://github.com/NVIDIA/spark-rapids/issues/8916)|[BUG] Databricks - move init scripts off DBFS|
|[#9416](https://github.com/NVIDIA/spark-rapids/issues/9416)|[BUG] CDH build failed due to missing dependencies|
|[#9357](https://github.com/NVIDIA/spark-rapids/issues/9357)|[BUG] json_test failed on "NameError: name 'TimestampNTZType' is not defined"|
|[#9271](https://github.com/NVIDIA/spark-rapids/issues/9271)|[BUG] ThreadPool size is deduced incorrectly in MultiFileReaderThreadPool on YARN clusters|
|[#9309](https://github.com/NVIDIA/spark-rapids/issues/9309)|[BUG] bround  and round do not return the correct result for some decimal values.|
|[#9153](https://github.com/NVIDIA/spark-rapids/issues/9153)|[BUG] netty OOM with MULTITHREADED shuffle|
|[#9311](https://github.com/NVIDIA/spark-rapids/issues/9311)|[BUG] test_hash_groupby_collect_list fails|
|[#9180](https://github.com/NVIDIA/spark-rapids/issues/9180)|[FEA][AUDIT][SPARK-44641] Incorrect result in certain scenarios when SPJ is not triggered|
|[#9290](https://github.com/NVIDIA/spark-rapids/issues/9290)|[BUG] delta_lake_test FAILED on "column mapping mode id is not supported for this Delta version"|
|[#9255](https://github.com/NVIDIA/spark-rapids/issues/9255)|[BUG] Unable to read DeltaTable with columnMapping.mode = name|
|[#9261](https://github.com/NVIDIA/spark-rapids/issues/9261)|[BUG] Leaks and Double Frees in Unit Tests|
|[#9246](https://github.com/NVIDIA/spark-rapids/issues/9246)|[BUG] `test_predefined_character_classes` failed with seed 4|
|[#9208](https://github.com/NVIDIA/spark-rapids/issues/9208)|[BUG] SplitAndRetryOOM query14_part1 at 100TB with spark.executor.cores=64|
|[#9106](https://github.com/NVIDIA/spark-rapids/issues/9106)|[BUG] Configuring GDS breaks new host spillable buffers and batches|
|[#9131](https://github.com/NVIDIA/spark-rapids/issues/9131)|[BUG] ConcurrentModificationException in ScalableTaskCompletion|
|[#9263](https://github.com/NVIDIA/spark-rapids/issues/9263)|[BUG] Unit test logging is not captured when running against Spark 3.5.0|
|[#9168](https://github.com/NVIDIA/spark-rapids/issues/9168)|[BUG] Calling RmmSpark.getAndResetNumRetryThrow from tests is not working|
|[#8776](https://github.com/NVIDIA/spark-rapids/issues/8776)|[BUG] FileCacheIntegrationSuite intermittent failure|
|[#9223](https://github.com/NVIDIA/spark-rapids/issues/9223)|[BUG] Failed to create memory map on query14_part1 at 100TB with spark.executor.cores=64|
|[#9116](https://github.com/NVIDIA/spark-rapids/issues/9116)|[BUG] spark350 shim build failed in mvn-verify github checks and nightly due to dependencies not released|
|[#8984](https://github.com/NVIDIA/spark-rapids/issues/8984)|[BUG] Check that keys are not null when creating a map|
|[#9233](https://github.com/NVIDIA/spark-rapids/issues/9233)|[BUG] test_parquet_testing_error_files - Failed: DID NOT RAISE <class 'Exception'> in databricks runtime 12.2|
|[#9142](https://github.com/NVIDIA/spark-rapids/issues/9142)|[BUG] AWS EMR 6.12 NDS SF3k query9 Failure on g4dn.4xlarge|
|[#9214](https://github.com/NVIDIA/spark-rapids/issues/9214)|[BUG] mvn resolve dependencies failed missing rapids-4-spark-sql-plugin-api_2.12 of 311 shim|
|[#9204](https://github.com/NVIDIA/spark-rapids/issues/9204)|[BUG] SplitAndRetryOOM query78 at 100TB with spark.executor.cores=64|
|[#9213](https://github.com/NVIDIA/spark-rapids/issues/9213)|[BUG] Missing revision info in databricks shims failed nightly build|
|[#9206](https://github.com/NVIDIA/spark-rapids/issues/9206)|[BUG] test_datetime_roundtrip_with_legacy_rebase failed in databricks runtimes|
|[#9165](https://github.com/NVIDIA/spark-rapids/issues/9165)|[BUG] Data gen for key groups produces type-mismatch columns|
|[#9129](https://github.com/NVIDIA/spark-rapids/issues/9129)|[BUG] Writing Parquet map(map) column can not set the outer key as non-null.|
|[#9194](https://github.com/NVIDIA/spark-rapids/issues/9194)|[BUG] missing sql-plugin-api databricks artifacts in the nightly CI |
|[#9167](https://github.com/NVIDIA/spark-rapids/issues/9167)|[BUG] Ensure no udf-compiler internal nodes escape|
|[#9092](https://github.com/NVIDIA/spark-rapids/issues/9092)|[BUG] NDS query 64 falls back to CPU only for a shuffle|
|[#9071](https://github.com/NVIDIA/spark-rapids/issues/9071)|[BUG] `test_numeric_running_sum_window_no_part_unbounded` failed in MT tests|
|[#9154](https://github.com/NVIDIA/spark-rapids/issues/9154)|[BUG] Spark 3.5.0 nightly build failures (test_parquet_testing_error_files)|
|[#9149](https://github.com/NVIDIA/spark-rapids/issues/9149)|[BUG] compile failed in databricks runtimes due to new added TestReport|
|[#9041](https://github.com/NVIDIA/spark-rapids/issues/9041)|[BUG] Fix regression in Python UDAF support when running against Spark 3.5.0|
|[#9064](https://github.com/NVIDIA/spark-rapids/issues/9064)|[BUG][Spark 3.5.0] Re-enable test_hive_empty_simple_udf when 3.5.0-rc2 is available|
|[#9065](https://github.com/NVIDIA/spark-rapids/issues/9065)|[BUG][Spark 3.5.0] Reinstate cast map/array to string tests when 3.5.0-rc2 is available|
|[#9119](https://github.com/NVIDIA/spark-rapids/issues/9119)|[BUG] Predicate pushdown doesn't work for parquet files written by GPU|
|[#9103](https://github.com/NVIDIA/spark-rapids/issues/9103)|[BUG] test_select_complex_field fails in MT tests|
|[#9086](https://github.com/NVIDIA/spark-rapids/issues/9086)|[BUG] GpuBroadcastNestedLoopJoinExec can assert in doUnconditionalJoin|
|[#8939](https://github.com/NVIDIA/spark-rapids/issues/8939)|[BUG]  q95 odd task failure in query95 at 30TB|
|[#9082](https://github.com/NVIDIA/spark-rapids/issues/9082)|[BUG] Race condition while spilling and aliasing a RapidsBuffer (regression)|
|[#9069](https://github.com/NVIDIA/spark-rapids/issues/9069)|[BUG] ParquetFormatScanSuite does not pass locally|
|[#8980](https://github.com/NVIDIA/spark-rapids/issues/8980)|[BUG] invalid escape sequences in pytests|
|[#7807](https://github.com/NVIDIA/spark-rapids/issues/7807)|[BUG] Round robin partitioning sort check falls back to CPU for cases that can be supported|
|[#8482](https://github.com/NVIDIA/spark-rapids/issues/8482)|[BUG] Potential leak on SplitAndRetry when iterator not fully drained|
|[#8942](https://github.com/NVIDIA/spark-rapids/issues/8942)|[BUG] NDS query 14 parts 1 and 2 both fail at SF100K|
|[#8778](https://github.com/NVIDIA/spark-rapids/issues/8778)|[BUG] GPU Parquet output for TIMESTAMP_MICROS is misinteterpreted by fastparquet as nanos|

### PRs
|||
|:---|:---|
|[#9501](https://github.com/NVIDIA/spark-rapids/pull/9501)|Fix GpuSemaphore to support multiple threads per task|
|[#9500](https://github.com/NVIDIA/spark-rapids/pull/9500)|Fix Spark 3.5.0 shell classloader issue with the plugin|
|[#9427](https://github.com/NVIDIA/spark-rapids/pull/9427)|[DOC] Update docs for 23.10.0 release [skip ci]|
|[#9421](https://github.com/NVIDIA/spark-rapids/pull/9421)|Init changelog of 23.10 [skip ci]|
|[#9445](https://github.com/NVIDIA/spark-rapids/pull/9445)|Only run test_csv_infer_schema_timestamp_ntz tests with PySpark >= 3.4.1|
|[#9420](https://github.com/NVIDIA/spark-rapids/pull/9420)|Update private and jni dep version to released 23.10.0|
|[#9415](https://github.com/NVIDIA/spark-rapids/pull/9415)|[BUG] fix docker modified check in premerge [skip ci]|
|[#9392](https://github.com/NVIDIA/spark-rapids/pull/9392)|Only run test_json_ts_formats_round_trip_ntz tests with PySpark >= 3.4.1|
|[#9401](https://github.com/NVIDIA/spark-rapids/pull/9401)|Remove using mamba before they fix the incompatibility issue [skip ci]|
|[#9381](https://github.com/NVIDIA/spark-rapids/pull/9381)|Change the executor core calculation to take into account the cluster manager|
|[#9351](https://github.com/NVIDIA/spark-rapids/pull/9351)|Put back in full decimal support for format_number|
|[#9374](https://github.com/NVIDIA/spark-rapids/pull/9374)|GpuCoalesceBatches should throw SplitAndRetyOOM on GPU OOM error|
|[#9238](https://github.com/NVIDIA/spark-rapids/pull/9238)|Simplified handling of GPU core dumps|
|[#9362](https://github.com/NVIDIA/spark-rapids/pull/9362)|[DOC] Removing User Guide pages that will be source of truth on docs.nvidia…|
|[#9365](https://github.com/NVIDIA/spark-rapids/pull/9365)|Update DataWriteCommandExec docs to reflect ORC support for nested types|
|[#9277](https://github.com/NVIDIA/spark-rapids/pull/9277)|[Doc]Remove CUDA related requirement from download page.[Skip CI]|
|[#9352](https://github.com/NVIDIA/spark-rapids/pull/9352)|Refine rules for skipping `test_csv_infer_schema_timestamp_ntz_*` tests|
|[#9334](https://github.com/NVIDIA/spark-rapids/pull/9334)|Add NaNs to Data Generators In Floating-Point Testing|
|[#9344](https://github.com/NVIDIA/spark-rapids/pull/9344)|Update MULTITHREADED shuffle maxBytesInFlight default to 128MB|
|[#9330](https://github.com/NVIDIA/spark-rapids/pull/9330)|Add Hao to blossom-ci whitelist|
|[#9328](https://github.com/NVIDIA/spark-rapids/pull/9328)|Building different Cuda versions section profile does not take effect [skip ci]|
|[#9329](https://github.com/NVIDIA/spark-rapids/pull/9329)|Add kuhushukla to blossom ci yml|
|[#9281](https://github.com/NVIDIA/spark-rapids/pull/9281)|Support `format_number`|
|[#9335](https://github.com/NVIDIA/spark-rapids/pull/9335)|Temporarily skip failing tests test_csv_infer_schema_timestamp_ntz*|
|[#9318](https://github.com/NVIDIA/spark-rapids/pull/9318)|Update authorized user in blossom-ci whitelist [skip ci]|
|[#9221](https://github.com/NVIDIA/spark-rapids/pull/9221)|Add GPU version of ToPrettyString|
|[#9321](https://github.com/NVIDIA/spark-rapids/pull/9321)|[DOC] Fix some incorrect config links in doc [skip ci]|
|[#9314](https://github.com/NVIDIA/spark-rapids/pull/9314)|Fix RMM crash in FileCacheIntegrationSuite with ARENA memory allocator|
|[#9287](https://github.com/NVIDIA/spark-rapids/pull/9287)|Allow checkpoint and restore on non-deterministic expressions in GpuFilter and GpuProject|
|[#9146](https://github.com/NVIDIA/spark-rapids/pull/9146)|Improve some CSV integration tests|
|[#9159](https://github.com/NVIDIA/spark-rapids/pull/9159)|Update tests and documentation for `spark.sql.timestampType` when reading CSV/JSON|
|[#9313](https://github.com/NVIDIA/spark-rapids/pull/9313)|Sort results of collect_list test before comparing since it is not guaranteed|
|[#9286](https://github.com/NVIDIA/spark-rapids/pull/9286)|[FEA][AUDIT][SPARK-44641] Incorrect result in certain scenarios when SPJ is not triggered|
|[#9229](https://github.com/NVIDIA/spark-rapids/pull/9229)|Support negative preceding/following for ROW-based window functions|
|[#9297](https://github.com/NVIDIA/spark-rapids/pull/9297)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#9294](https://github.com/NVIDIA/spark-rapids/pull/9294)|Fix test_delta_read_column_mapping test failures on Spark 3.2.x and 3.3.x|
|[#9285](https://github.com/NVIDIA/spark-rapids/pull/9285)|Add CastOptions to make GpuCast extendible to handle more options|
|[#9279](https://github.com/NVIDIA/spark-rapids/pull/9279)|Fix file format checks to be exact and handle Delta Lake column mapping|
|[#9283](https://github.com/NVIDIA/spark-rapids/pull/9283)|Refactor ExternalSource to move some APIs to converted GPU format or scan|
|[#9264](https://github.com/NVIDIA/spark-rapids/pull/9264)|Fix leak in test and double free in corner case|
|[#9280](https://github.com/NVIDIA/spark-rapids/pull/9280)|Fix some issues found with different seeds in integration tests|
|[#9257](https://github.com/NVIDIA/spark-rapids/pull/9257)|Have host spill use the new HostAlloc API|
|[#9253](https://github.com/NVIDIA/spark-rapids/pull/9253)|Enforce Scala method syntax over deprecated procedure syntax|
|[#9273](https://github.com/NVIDIA/spark-rapids/pull/9273)|Add arm64 profile to build arm artifacts|
|[#9270](https://github.com/NVIDIA/spark-rapids/pull/9270)|Remove GDS spilling|
|[#9267](https://github.com/NVIDIA/spark-rapids/pull/9267)|Roll our own BufferedIterator so we can close cleanly|
|[#9266](https://github.com/NVIDIA/spark-rapids/pull/9266)|Specify correct dependency versions for 350 build|
|[#9262](https://github.com/NVIDIA/spark-rapids/pull/9262)|Add Delta Lake support for Spark 3.4.1 and Delta Lake tests on Spark 3.4.x|
|[#9256](https://github.com/NVIDIA/spark-rapids/pull/9256)|Test Parquet double column stat without NaN|
|[#9254](https://github.com/NVIDIA/spark-rapids/pull/9254)|[Doc]update the emr getting started doc for emr-6130 release[skip ci]|
|[#9228](https://github.com/NVIDIA/spark-rapids/pull/9228)|Add in unbounded to unbounded optimization for min/max|
|[#9252](https://github.com/NVIDIA/spark-rapids/pull/9252)|Add Spark 3.5.0 to list of supported Spark versions [skip ci]|
|[#9251](https://github.com/NVIDIA/spark-rapids/pull/9251)|Enable a couple of retry asserts in internal row to cudf row iterator suite|
|[#9239](https://github.com/NVIDIA/spark-rapids/pull/9239)|Handle escaping the dangling right ] and right } in the regexp transpiler|
|[#9090](https://github.com/NVIDIA/spark-rapids/pull/9090)|Add test cases for Parquet statistics|
|[#9240](https://github.com/NVIDIA/spark-rapids/pull/9240)|Fix flaky ORC filecache test|
|[#9053](https://github.com/NVIDIA/spark-rapids/pull/9053)|[DOC] update the turning guide document issues [skip ci]|
|[#9211](https://github.com/NVIDIA/spark-rapids/pull/9211)|Allow skipping host spill for a direct device->disk spill|
|[#9234](https://github.com/NVIDIA/spark-rapids/pull/9234)|Enable Spark 350 builds|
|[#9237](https://github.com/NVIDIA/spark-rapids/pull/9237)|Check for null keys when creating map|
|[#9235](https://github.com/NVIDIA/spark-rapids/pull/9235)|xfail fixed_length_byte_array.parquet test due to rapidsai/cudf#14104|
|[#9231](https://github.com/NVIDIA/spark-rapids/pull/9231)|Use conda libmamba solver to resolve intermittent libarchive issue [skip ci]|
|[#8404](https://github.com/NVIDIA/spark-rapids/pull/8404)|Add in support for FIXED_LEN_BYTE_ARRAY as binary|
|[#9225](https://github.com/NVIDIA/spark-rapids/pull/9225)|Add in a HostAlloc API for high priority and add in spilling|
|[#9207](https://github.com/NVIDIA/spark-rapids/pull/9207)|Support SplitAndRetry for GpuRangeExec|
|[#9217](https://github.com/NVIDIA/spark-rapids/pull/9217)|Fix leak in aggregate when there are retries|
|[#9200](https://github.com/NVIDIA/spark-rapids/pull/9200)|Fix a few minor things with scale test|
|[#9222](https://github.com/NVIDIA/spark-rapids/pull/9222)|Deploy classified aggregator for Databricks [skip ci]|
|[#9209](https://github.com/NVIDIA/spark-rapids/pull/9209)|Fix tests for datetime rebase in Databricks|
|[#9181](https://github.com/NVIDIA/spark-rapids/pull/9181)|[DOC] address document issues [skip ci]|
|[#9132](https://github.com/NVIDIA/spark-rapids/pull/9132)|Support `spark.sql.parquet.datetimeRebaseModeInWrite=LEGACY`|
|[#9196](https://github.com/NVIDIA/spark-rapids/pull/9196)|Fix host memory leak for R2C|
|[#9192](https://github.com/NVIDIA/spark-rapids/pull/9192)|Throw overflow exception when interval seconds are outside of range [0, 59]|
|[#9150](https://github.com/NVIDIA/spark-rapids/pull/9150)|add error section in report and the rest queries|
|[#9189](https://github.com/NVIDIA/spark-rapids/pull/9189)|Expose host store spill|
|[#9147](https://github.com/NVIDIA/spark-rapids/pull/9147)|Make map column non-nullable when it's a key in another map.|
|[#9193](https://github.com/NVIDIA/spark-rapids/pull/9193)|Support Retry for GpuLocalLimitExec and GpuGlobalLimitExec|
|[#9183](https://github.com/NVIDIA/spark-rapids/pull/9183)|Add test to verify UDT fallback for parquet|
|[#9195](https://github.com/NVIDIA/spark-rapids/pull/9195)|Deploy sql-plugin-api artifact in DBR CI pipelines [skip ci]|
|[#9170](https://github.com/NVIDIA/spark-rapids/pull/9170)|Add in new HostAlloc API|
|[#9182](https://github.com/NVIDIA/spark-rapids/pull/9182)|Consolidate Spark vendor shim dependency management|
|[#9190](https://github.com/NVIDIA/spark-rapids/pull/9190)|Prevent returning internal compiler expressions when compiling UDFs|
|[#9164](https://github.com/NVIDIA/spark-rapids/pull/9164)|Support Retry for GpuTopN and GpuSortEachBatchIterator|
|[#9134](https://github.com/NVIDIA/spark-rapids/pull/9134)|Fix shuffle fallback due to AQE on AWS EMR|
|[#9188](https://github.com/NVIDIA/spark-rapids/pull/9188)|Fix flaky tests in FileCacheIntegrationSuite|
|[#9148](https://github.com/NVIDIA/spark-rapids/pull/9148)|Add minimum Maven module eventually containing all non-shimmable source code|
|[#9169](https://github.com/NVIDIA/spark-rapids/pull/9169)|Add retry-without-split in InternalRowToColumnarBatchIterator|
|[#9172](https://github.com/NVIDIA/spark-rapids/pull/9172)|Remove doSetSpillable in favor of setSpillable|
|[#9152](https://github.com/NVIDIA/spark-rapids/pull/9152)|Add test cases for testing Parquet compression types|
|[#9157](https://github.com/NVIDIA/spark-rapids/pull/9157)|XFAIL parquet `lz4_raw` tests for Spark 3.5.0 or later|
|[#9128](https://github.com/NVIDIA/spark-rapids/pull/9128)|Test parquet predicate pushdown for basic types and fields having dots in names|
|[#9158](https://github.com/NVIDIA/spark-rapids/pull/9158)|Add json4s dependencies for Databricks integration_tests build|
|[#9102](https://github.com/NVIDIA/spark-rapids/pull/9102)|Add retry support to GpuOutOfCoreSortIterator.mergeSortEnoughToOutput |
|[#9089](https://github.com/NVIDIA/spark-rapids/pull/9089)|Add application to run Scale Test|
|[#9143](https://github.com/NVIDIA/spark-rapids/pull/9143)|[DOC] update spark.rapids.sql.concurrentGpuTasks default value in tuning guide [skip ci]|
|[#9141](https://github.com/NVIDIA/spark-rapids/pull/9141)|Removed resultDecimalType in GpuIntegralDecimalDivide|
|[#9099](https://github.com/NVIDIA/spark-rapids/pull/9099)|Spark 3.5.0 follow-on work (rc2 support + Python UDAF)|
|[#9140](https://github.com/NVIDIA/spark-rapids/pull/9140)|Bump Jython to 2.7.3|
|[#9136](https://github.com/NVIDIA/spark-rapids/pull/9136)|Moving row column conversion code from cudf to jni|
|[#9133](https://github.com/NVIDIA/spark-rapids/pull/9133)|Add 350 tag to InSubqueryShims|
|[#9124](https://github.com/NVIDIA/spark-rapids/pull/9124)|Import `scala.collection` intead of `collection`|
|[#9122](https://github.com/NVIDIA/spark-rapids/pull/9122)|Fall back to CPU if `spark.sql.execution.arrow.useLargeVarTypes` is true|
|[#9115](https://github.com/NVIDIA/spark-rapids/pull/9115)|[DOC] updates documentation related to java compatibility [skip ci]|
|[#9098](https://github.com/NVIDIA/spark-rapids/pull/9098)|Add SpillableHostColumnarBatch|
|[#9091](https://github.com/NVIDIA/spark-rapids/pull/9091)|GPU support for DynamicPruningExpression and InSubqueryExec|
|[#9117](https://github.com/NVIDIA/spark-rapids/pull/9117)|Temply disable spark 350 shim build in nightly [skip ci]|
|[#9113](https://github.com/NVIDIA/spark-rapids/pull/9113)|Instantiate execution plan capture callback via shim loader|
|[#8969](https://github.com/NVIDIA/spark-rapids/pull/8969)|Initial support for Spark 3.5.0-rc1|
|[#9100](https://github.com/NVIDIA/spark-rapids/pull/9100)|Support broadcast nested loop existence joins with no condition|
|[#8925](https://github.com/NVIDIA/spark-rapids/pull/8925)|Add GpuConv operator for the `conv` 10<->16 expression|
|[#9109](https://github.com/NVIDIA/spark-rapids/pull/9109)|[DOC] adding java 11 to download docs [skip ci]|
|[#9085](https://github.com/NVIDIA/spark-rapids/pull/9085)|Retry with smaller split on `CudfColumnSizeOverflowException`|
|[#8961](https://github.com/NVIDIA/spark-rapids/pull/8961)|Save Databricks init scripts in the workspace|
|[#9088](https://github.com/NVIDIA/spark-rapids/pull/9088)|Add retry and SplitAndRetry support to AcceleratedColumnarToRowIterator|
|[#9095](https://github.com/NVIDIA/spark-rapids/pull/9095)|Support released spark 3.3.3|
|[#9084](https://github.com/NVIDIA/spark-rapids/pull/9084)|Fix race when a rapids buffer is aliased while it is spilled|
|[#9093](https://github.com/NVIDIA/spark-rapids/pull/9093)|Update ParquetFormatScanSuite to not call CUDF directly|
|[#9068](https://github.com/NVIDIA/spark-rapids/pull/9068)|Test ORC predicate pushdown (PPD) with timestamps decimals booleans|
|[#9054](https://github.com/NVIDIA/spark-rapids/pull/9054)|Initial entry point to data generation for scale test|
|[#9070](https://github.com/NVIDIA/spark-rapids/pull/9070)|Spillable host buffer|
|[#9066](https://github.com/NVIDIA/spark-rapids/pull/9066)|Add retry support to RowToColumnarIterator|
|[#9073](https://github.com/NVIDIA/spark-rapids/pull/9073)|Stop using invalid escape sequences|
|[#9018](https://github.com/NVIDIA/spark-rapids/pull/9018)|Add test for selecting a single complex field array and its parent struct array|
|[#9067](https://github.com/NVIDIA/spark-rapids/pull/9067)|Add array support for round robin partition; Refactor pluginSupportedOrderableSig|
|[#9072](https://github.com/NVIDIA/spark-rapids/pull/9072)|Revert "Implement SumUnboundedToUnboundedFixer (#8934)"|
|[#9056](https://github.com/NVIDIA/spark-rapids/pull/9056)|Add in configs for host memory limits|
|[#9061](https://github.com/NVIDIA/spark-rapids/pull/9061)|Fix import order|
|[#8934](https://github.com/NVIDIA/spark-rapids/pull/8934)|Implement SumUnboundedToUnboundedFixer|
|[#9051](https://github.com/NVIDIA/spark-rapids/pull/9051)|Use number of threads on executor instead of driver to set core count|
|[#9040](https://github.com/NVIDIA/spark-rapids/pull/9040)|Fix issues from 23.08 merge in join_test|
|[#9045](https://github.com/NVIDIA/spark-rapids/pull/9045)|Fix auto merge conflict 9043 [skip ci]|
|[#9009](https://github.com/NVIDIA/spark-rapids/pull/9009)|Add in a layer of indirection for task completion callbacks|
|[#9013](https://github.com/NVIDIA/spark-rapids/pull/9013)|Create a two-shim jar by default on Databricks|
|[#8995](https://github.com/NVIDIA/spark-rapids/pull/8995)|Add test case for ORC statistics test|
|[#8970](https://github.com/NVIDIA/spark-rapids/pull/8970)|Add ability to debug dump input data only on errors|
|[#9003](https://github.com/NVIDIA/spark-rapids/pull/9003)|Fix auto merge conflict 9002 [skip ci]|
|[#8989](https://github.com/NVIDIA/spark-rapids/pull/8989)|Mark lazy spillables as allowSpillable in during gatherer construction|
|[#8988](https://github.com/NVIDIA/spark-rapids/pull/8988)|Move big data generator to a separate module|
|[#8987](https://github.com/NVIDIA/spark-rapids/pull/8987)|Fix host memory buffer leaks in SerializationSuite|
|[#8968](https://github.com/NVIDIA/spark-rapids/pull/8968)|Enable GPU acceleration of Bloom filter join expressions by default|
|[#8947](https://github.com/NVIDIA/spark-rapids/pull/8947)|Add ArrowUtilsShims in preparation for Spark 3.5.0|
|[#8946](https://github.com/NVIDIA/spark-rapids/pull/8946)|[Spark 3.5.0] Shim access to StructType.fromAttributes|
|[#8824](https://github.com/NVIDIA/spark-rapids/pull/8824)|Drop the in-range check at INT96 output path|
|[#8924](https://github.com/NVIDIA/spark-rapids/pull/8924)|Deprecate and delegate GpuCV.debug to cudf TableDebug|
|[#8915](https://github.com/NVIDIA/spark-rapids/pull/8915)|Move LegacyBehaviorPolicy references to shim layer|
|[#8918](https://github.com/NVIDIA/spark-rapids/pull/8918)|Output unified diff when GPU output deviates|
|[#8857](https://github.com/NVIDIA/spark-rapids/pull/8857)|Remove the pageable pool|
|[#8854](https://github.com/NVIDIA/spark-rapids/pull/8854)|Fix auto merge conflict 8853 [skip ci]|
|[#8805](https://github.com/NVIDIA/spark-rapids/pull/8805)|Bump up dep versions to 23.10.0-SNAPSHOT|
|[#8796](https://github.com/NVIDIA/spark-rapids/pull/8796)|Init version 23.10.0-SNAPSHOT|

## Release 23.08

### Features
|||
|:---|:---|
|[#5509](https://github.com/NVIDIA/spark-rapids/issues/5509)|[FEA] Support order-by on Array|
|[#7876](https://github.com/NVIDIA/spark-rapids/issues/7876)|[FEA] Add initial support for Databricks 12.2 ML LTS|
|[#8547](https://github.com/NVIDIA/spark-rapids/issues/8547)|[FEA] Add support for Delta Lake 2.4 with Spark 3.4|
|[#8633](https://github.com/NVIDIA/spark-rapids/issues/8633)|[FEA] Add support for xxHash64 function|
|[#4929](https://github.com/NVIDIA/spark-rapids/issues/4929)|[FEA] Support min/max aggregation/reduction for arrays of structs and arrays of strings|
|[#8668](https://github.com/NVIDIA/spark-rapids/issues/8668)|[FEA] Support min and max for arrays|
|[#4887](https://github.com/NVIDIA/spark-rapids/issues/4887)|[FEA] Hash partitioning on ArrayType|
|[#6680](https://github.com/NVIDIA/spark-rapids/issues/6680)|[FEA] Support hashaggregate for Array[Any]|
|[#8085](https://github.com/NVIDIA/spark-rapids/issues/8085)|[FEA] Add support for MillisToTimestamp|
|[#7801](https://github.com/NVIDIA/spark-rapids/issues/7801)|[FEA] Window Expression orderBy column is not supported in a window range function, found  DoubleType|
|[#8556](https://github.com/NVIDIA/spark-rapids/issues/8556)|[FEA] [Delta Lake] Add support for new metrics in MERGE|
|[#308](https://github.com/NVIDIA/spark-rapids/issues/308)|[FEA] Spark 3.1 adding support for  TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS functions|
|[#8122](https://github.com/NVIDIA/spark-rapids/issues/8122)|[FEA] Add spark 3.4.1 snapshot shim|
|[#8525](https://github.com/NVIDIA/spark-rapids/issues/8525)|[FEA] Add support for org.apache.spark.sql.functions.flatten|
|[#8202](https://github.com/NVIDIA/spark-rapids/issues/8202)|[FEA] List supported Spark builds when the Shim is not found|

### Performance
|||
|:---|:---|
|[#8231](https://github.com/NVIDIA/spark-rapids/issues/8231)|[FEA] Add filecache support to ORC scans|
|[#8141](https://github.com/NVIDIA/spark-rapids/issues/8141)|[FEA] Explore how to best deal with large numbers of aggregations in the short term|

### Bugs Fixed
|||
|:---|:---|
|[#9034](https://github.com/NVIDIA/spark-rapids/issues/9034)|[BUG] java.lang.ClassCastException: com.nvidia.spark.rapids.RuleNotFoundExprMeta cannot be cast to com.nvidia.spark.rapids.GeneratorExprMeta|
|[#9032](https://github.com/NVIDIA/spark-rapids/issues/9032)|[BUG] Multiple NDS queries fail with Spark-3.4.1 with bloom filter exception|
|[#8962](https://github.com/NVIDIA/spark-rapids/issues/8962)|[BUG] Nightly build failed: ExecutionPlanCaptureCallback$.class is not bitwise-identical across shims|
|[#9021](https://github.com/NVIDIA/spark-rapids/issues/9021)|[BUG] test_map_scalars_supported_key_types failed in dataproc 2.1|
|[#9020](https://github.com/NVIDIA/spark-rapids/issues/9020)|[BUG] auto-disable snapshot shims test in github action for pre-release branch|
|[#9010](https://github.com/NVIDIA/spark-rapids/issues/9010)|[BUG] Customer failure 23.08: Cannot compute hash of a table with a LIST of STRUCT columns.|
|[#8922](https://github.com/NVIDIA/spark-rapids/issues/8922)|[BUG] integration map_test:test_map_scalars_supported_key_types failures|
|[#8982](https://github.com/NVIDIA/spark-rapids/issues/8982)|[BUG] Nightly prerelease failures - OrcSuite|
|[#8978](https://github.com/NVIDIA/spark-rapids/issues/8978)|[BUG] compiling error due to OrcSuite&OrcStatisticShim in databricks runtimes|
|[#8610](https://github.com/NVIDIA/spark-rapids/issues/8610)|[BUG] query 95 @ SF30K fails with OOM exception|
|[#8955](https://github.com/NVIDIA/spark-rapids/issues/8955)|[BUG] Bloom filter join tests can fail with multiple join columns|
|[#45](https://github.com/NVIDIA/spark-rapids/issues/45)|[BUG] very large shuffles can fail|
|[#8779](https://github.com/NVIDIA/spark-rapids/issues/8779)|[BUG] Put shared Databricks test script together for ease of maintenance|
|[#8930](https://github.com/NVIDIA/spark-rapids/issues/8930)|[BUG] checkoutSCM plugin is unstable for pre-merge CI, it is often unable to clone submodules|
|[#8923](https://github.com/NVIDIA/spark-rapids/issues/8923)|[BUG] Mortgage test failing with 'JavaPackage' error on AWS Databricks|
|[#8303](https://github.com/NVIDIA/spark-rapids/issues/8303)|[BUG] GpuExpression columnarEval can return scalars from subqueries that may be unhandled|
|[#8318](https://github.com/NVIDIA/spark-rapids/issues/8318)|[BUG][Databricks 12.2] GpuRowBasedHiveGenericUDF ClassCastException|
|[#8822](https://github.com/NVIDIA/spark-rapids/issues/8822)|[BUG] Early terminate CI if submodule init failed|
|[#8847](https://github.com/NVIDIA/spark-rapids/issues/8847)|[BUG] github actions CI messed up w/ JDK versions intermittently|
|[#8716](https://github.com/NVIDIA/spark-rapids/issues/8716)|[BUG] `test_hash_groupby_collect_set_on_nested_type` and `test_hash_reduction_collect_set_on_nested_type` failed|
|[#8827](https://github.com/NVIDIA/spark-rapids/issues/8827)|[BUG] databricks cudf_udf night build failing with pool size exceeded errors|
|[#8630](https://github.com/NVIDIA/spark-rapids/issues/8630)|[BUG] Parquet with RLE encoded booleans loads corrupted data|
|[#8735](https://github.com/NVIDIA/spark-rapids/issues/8735)|[BUG] test_orc_column_name_with_dots fails in nightly EGX tests|
|[#6980](https://github.com/NVIDIA/spark-rapids/issues/6980)|[BUG] Partitioned writes release GPU semaphore with unspillable GPU memory|
|[#8784](https://github.com/NVIDIA/spark-rapids/issues/8784)|[BUG] hash_aggregate_test.py::test_min_max_in_groupby_and_reduction failed on "TypeError: object of type 'NoneType' has no len()"|
|[#8756](https://github.com/NVIDIA/spark-rapids/issues/8756)|[BUG] [Databricks 12.2] RapidsDeltaWrite queries that reference internal metadata fail to run|
|[#8636](https://github.com/NVIDIA/spark-rapids/issues/8636)|[BUG] AWS Databricks 12.2 integration tests failed due to Iceberg check|
|[#8754](https://github.com/NVIDIA/spark-rapids/issues/8754)|[BUG] databricks build broke after adding bigDataGen|
|[#8726](https://github.com/NVIDIA/spark-rapids/issues/8726)|[BUG] Test "parquet_write_test.py::test_hive_timestamp_value[INJECT_OOM]" failed on Databricks |
|[#8690](https://github.com/NVIDIA/spark-rapids/issues/8690)|[BUG buildall script does not support JDK11 profile|
|[#8702](https://github.com/NVIDIA/spark-rapids/issues/8702)|[BUG] test_min_max_for_single_level_struct failed|
|[#8727](https://github.com/NVIDIA/spark-rapids/issues/8727)|[BUG] test_column_add_after_partition failed in databricks 10.4 runtime|
|[#8669](https://github.com/NVIDIA/spark-rapids/issues/8669)|[BUG] SpillableColumnarBatch doesn't always take ownership|
|[#8655](https://github.com/NVIDIA/spark-rapids/issues/8655)|[BUG] There are some potential device memory leaks in `AbstractGpuCoalesceIterator`|
|[#8685](https://github.com/NVIDIA/spark-rapids/issues/8685)|[BUG] install build fails with Maven 3.9.3|
|[#8156](https://github.com/NVIDIA/spark-rapids/issues/8156)|[BUG] Install phase for modules with Spark build classifier fails for install plugin versions 3.0.0+|
|[#1130](https://github.com/NVIDIA/spark-rapids/issues/1130)|[BUG] TIMESTAMP_MILLIS not handled in isDateTimeRebaseNeeded|
|[#7676](https://github.com/NVIDIA/spark-rapids/issues/7676)|[BUG] SparkShimsImpl  class initialization in SparkShimsSuite for 340 too eager|
|[#8278](https://github.com/NVIDIA/spark-rapids/issues/8278)|[BUG] NDS query 16 hangs at SF30K|
|[#8665](https://github.com/NVIDIA/spark-rapids/issues/8665)|[BUG] EGX nightly tests fail to detect Spark version on startup|
|[#8647](https://github.com/NVIDIA/spark-rapids/issues/8647)|[BUG] array_test.py::test_array_min_max[Float][INJECT_OOM] failed mismatched CPU and GPU output in nightly|
|[#8640](https://github.com/NVIDIA/spark-rapids/issues/8640)|[BUG] Optimize Databricks pre-merge scripts, move it out into a new CI file|
|[#8308](https://github.com/NVIDIA/spark-rapids/issues/8308)|[BUG] Device Memory leak seen in integration_tests when AssertEmptyNulls are enabled|
|[#8602](https://github.com/NVIDIA/spark-rapids/issues/8602)|[BUG] AutoCloseable Broadcast results are getting closed by Spark|
|[#8603](https://github.com/NVIDIA/spark-rapids/issues/8603)|[BUG] SerializeConcatHostBuffersDeserializeBatch.writeObject fails with ArrayIndexOutOfBoundsException on rows-only table|
|[#8615](https://github.com/NVIDIA/spark-rapids/issues/8615)|[BUG] RapidsShuffleThreadedWriterSuite temp shuffle file test failure|
|[#6872](https://github.com/NVIDIA/spark-rapids/issues/6872)|[BUG] awk: cmd. line:1: warning: regexp escape sequence `\ ' is not a known regexp operator|
|[#8588](https://github.com/NVIDIA/spark-rapids/issues/8588)|[BUG] Spark 3.3.x integration tests failed due to missing jars|
|[#7775](https://github.com/NVIDIA/spark-rapids/issues/7775)|[BUG] scala version hardcoded irrespective of Spark dependency|
|[#8548](https://github.com/NVIDIA/spark-rapids/issues/8548)|[BUG] cache_test:test_batch_no_cols test FAILED on spark-3.3.0+|
|[#8579](https://github.com/NVIDIA/spark-rapids/issues/8579)|[BUG] build failed on Databricks clusters "GpuDeleteCommand.scala:104: type mismatch" |
|[#8187](https://github.com/NVIDIA/spark-rapids/issues/8187)|[BUG] Integration test test_window_running_no_part can produce non-empty nulls (cudf scan)|
|[#8493](https://github.com/NVIDIA/spark-rapids/issues/8493)|[BUG] branch-23.08 fails to build on Databricks 12.2|

### PRs
|||
|:---|:---|
|[#9407](https://github.com/NVIDIA/spark-rapids/pull/9407)|[Doc]Update docs for 23.08.2 version[skip ci]|
|[#9382](https://github.com/NVIDIA/spark-rapids/pull/9382)|Bump up project version to 23.08.2|
|[#8476](https://github.com/NVIDIA/spark-rapids/pull/8476)|Use retry with split in GpuCachedDoublePassWindowIterator|
|[#9048](https://github.com/NVIDIA/spark-rapids/pull/9048)|Update 23.08 changelog 23/08/15 [skip ci]|
|[#9044](https://github.com/NVIDIA/spark-rapids/pull/9044)|[DOC] update release version from v2308.0 to 2308.1 [skip ci]|
|[#9036](https://github.com/NVIDIA/spark-rapids/pull/9036)|Fix meta class cast exception when generator not supported|
|[#9042](https://github.com/NVIDIA/spark-rapids/pull/9042)|Bump up project version to 23.08.1-SNAPSHOT|
|[#9035](https://github.com/NVIDIA/spark-rapids/pull/9035)|Handle null values when merging Bloom filters|
|[#9029](https://github.com/NVIDIA/spark-rapids/pull/9029)|Update 23.08 changelog to latest [skip ci]|
|[#9023](https://github.com/NVIDIA/spark-rapids/pull/9023)|Allow WindowLocalExec to run on CPU for a map test.|
|[#9024](https://github.com/NVIDIA/spark-rapids/pull/9024)|Do not trigger snapshot spark version test in pre-release maven-verify checks [skip ci]|
|[#8975](https://github.com/NVIDIA/spark-rapids/pull/8975)|Init 23.08 changelog [skip ci]|
|[#9016](https://github.com/NVIDIA/spark-rapids/pull/9016)|Fix issue where murmur3 tried to work on array of structs|
|[#9014](https://github.com/NVIDIA/spark-rapids/pull/9014)|Updating link to download jar [skip ci]|
|[#9006](https://github.com/NVIDIA/spark-rapids/pull/9006)|Revert test changes to fix binary dedup error|
|[#9001](https://github.com/NVIDIA/spark-rapids/pull/9001)|[Doc]update the emr getting started doc for emr-6120 release[skip ci]|
|[#8949](https://github.com/NVIDIA/spark-rapids/pull/8949)|Update JNI and private version to released 23.08.0|
|[#8977](https://github.com/NVIDIA/spark-rapids/pull/8977)|Create an anonymous subclass of AdaptiveSparkPlanHelper in ExecutionPlanCaptureCallback.scala|
|[#8972](https://github.com/NVIDIA/spark-rapids/pull/8972)|[Doc]Add best practice doc[skip ci]|
|[#8948](https://github.com/NVIDIA/spark-rapids/pull/8948)|[Doc]update download docs for 2308 version[skip ci]|
|[#8971](https://github.com/NVIDIA/spark-rapids/pull/8971)|Fix test_map_scalars_supported_key_types|
|[#8990](https://github.com/NVIDIA/spark-rapids/pull/8990)|Remove doc references to 312db [skip ci]|
|[#8960](https://github.com/NVIDIA/spark-rapids/pull/8960)|[Doc] address profiling tool formatted issue [skip ci]|
|[#8983](https://github.com/NVIDIA/spark-rapids/pull/8983)|Revert OrcSuite to fix deployment build|
|[#8979](https://github.com/NVIDIA/spark-rapids/pull/8979)|Fix Databricks build error for new added ORC test cases|
|[#8920](https://github.com/NVIDIA/spark-rapids/pull/8920)|Add test case to test orc dictionary encoding with lots of rows for nested types|
|[#8940](https://github.com/NVIDIA/spark-rapids/pull/8940)|Add test case for ORC statistics test|
|[#8909](https://github.com/NVIDIA/spark-rapids/pull/8909)|Match Spark's NaN handling in collect_set|
|[#8892](https://github.com/NVIDIA/spark-rapids/pull/8892)|Experimental support for BloomFilterAggregate expression in a reduction context|
|[#8957](https://github.com/NVIDIA/spark-rapids/pull/8957)|Fix building dockerfile.cuda hanging at tzdata installation [skip ci]|
|[#8944](https://github.com/NVIDIA/spark-rapids/pull/8944)|Fix issues around bloom filter with multple columns|
|[#8744](https://github.com/NVIDIA/spark-rapids/pull/8744)|Add test for selecting a single complex field array and its parent struct array|
|[#8936](https://github.com/NVIDIA/spark-rapids/pull/8936)|Device synchronize prior to freeing a set of RapidsBuffer|
|[#8935](https://github.com/NVIDIA/spark-rapids/pull/8935)|Don't go over shuffle limits on CPU|
|[#8927](https://github.com/NVIDIA/spark-rapids/pull/8927)|Skipping test_map_scalars_supported_key_types because of distributed …|
|[#8931](https://github.com/NVIDIA/spark-rapids/pull/8931)|Clone submodule using git command instead of checkoutSCM plugin|
|[#8917](https://github.com/NVIDIA/spark-rapids/pull/8917)|Databricks shim version for integration test|
|[#8775](https://github.com/NVIDIA/spark-rapids/pull/8775)|Support BloomFilterMightContain expression|
|[#8833](https://github.com/NVIDIA/spark-rapids/pull/8833)|Binary and ternary handling of scalar audit and some fixes|
|[#7233](https://github.com/NVIDIA/spark-rapids/pull/7233)|[FEA] Support `order by` on single-level array|
|[#8893](https://github.com/NVIDIA/spark-rapids/pull/8893)|Fix regression in Hive Generic UDF support on Databricks 12.2|
|[#8828](https://github.com/NVIDIA/spark-rapids/pull/8828)|Put shared part together for Databricks test scripts|
|[#8872](https://github.com/NVIDIA/spark-rapids/pull/8872)|Terminate CI if fail to clone submodule|
|[#8787](https://github.com/NVIDIA/spark-rapids/pull/8787)|Add in support for ExponentialDistribution|
|[#8868](https://github.com/NVIDIA/spark-rapids/pull/8868)|Add a test case for testing ORC version V_0_11 and V_0_12|
|[#8795](https://github.com/NVIDIA/spark-rapids/pull/8795)|Add ORC writing test cases for not implicitly lowercase columns|
|[#8871](https://github.com/NVIDIA/spark-rapids/pull/8871)|Adjust parallelism in spark-tests script to reduce memory footprint [skip ci]|
|[#8869](https://github.com/NVIDIA/spark-rapids/pull/8869)|Specify expected JAVA_HOME and bin for mvn-verify-check [skip ci]|
|[#8785](https://github.com/NVIDIA/spark-rapids/pull/8785)|Add test cases for ORC writing according to options orc.compress and compression|
|[#8810](https://github.com/NVIDIA/spark-rapids/pull/8810)|Fall back to CPU for deletion vectors writes on Databricks|
|[#8830](https://github.com/NVIDIA/spark-rapids/pull/8830)|Update documentation to add Databricks 12.2 as a supported platform [skip ci]|
|[#8799](https://github.com/NVIDIA/spark-rapids/pull/8799)|Add tests to cover some odd corner cases with nulls and empty arrays|
|[#8783](https://github.com/NVIDIA/spark-rapids/pull/8783)|Fix collect_set_on_nested_type tests failed|
|[#8855](https://github.com/NVIDIA/spark-rapids/pull/8855)|Fix bug: Check GPU file instead of CPU file [skip ci]|
|[#8852](https://github.com/NVIDIA/spark-rapids/pull/8852)|Update test scripts and dockerfiles to match cudf conda pkg change [skip ci]|
|[#8848](https://github.com/NVIDIA/spark-rapids/pull/8848)|Try mitigate mismatched JDK versions in mvn-verify checks [skip ci]|
|[#8825](https://github.com/NVIDIA/spark-rapids/pull/8825)|Add a case to test ORC writing/reading with lots of nulls|
|[#8802](https://github.com/NVIDIA/spark-rapids/pull/8802)|Treat unbounded windows as truly non-finite.|
|[#8798](https://github.com/NVIDIA/spark-rapids/pull/8798)|Add ORC writing test cases for dictionary compression|
|[#8829](https://github.com/NVIDIA/spark-rapids/pull/8829)|Enable rle_boolean_encoding.parquet test|
|[#8667](https://github.com/NVIDIA/spark-rapids/pull/8667)|Make state spillable in partitioned writer|
|[#8801](https://github.com/NVIDIA/spark-rapids/pull/8801)|Fix shuffling an empty Struct() column with UCX|
|[#8748](https://github.com/NVIDIA/spark-rapids/pull/8748)|Add driver log warning when GPU is limiting scheduling resource|
|[#8786](https://github.com/NVIDIA/spark-rapids/pull/8786)|Add support for row-based execution in RapidsDeltaWrite|
|[#8791](https://github.com/NVIDIA/spark-rapids/pull/8791)|Auto merge to branch-23.10 from branch-23.08[skip ci]|
|[#8790](https://github.com/NVIDIA/spark-rapids/pull/8790)|Update ubuntu dockerfiles default to 20.04 and deprecating centos one [skip ci]|
|[#8777](https://github.com/NVIDIA/spark-rapids/pull/8777)|Install python packages with shared scripts on Databricks|
|[#8772](https://github.com/NVIDIA/spark-rapids/pull/8772)|Test concurrent writer update file metrics|
|[#8646](https://github.com/NVIDIA/spark-rapids/pull/8646)|Add testing of Parquet files from apache/parquet-testing|
|[#8684](https://github.com/NVIDIA/spark-rapids/pull/8684)|Add 'submodule update --init' when build spark-rapids|
|[#8769](https://github.com/NVIDIA/spark-rapids/pull/8769)|Remove iceberg scripts from Databricks test scripts|
|[#8773](https://github.com/NVIDIA/spark-rapids/pull/8773)|Add a test case for reading/write null to ORC|
|[#8749](https://github.com/NVIDIA/spark-rapids/pull/8749)|Add test cases for read/write User Defined Type (UDT) to ORC|
|[#8768](https://github.com/NVIDIA/spark-rapids/pull/8768)|Add support for xxhash64|
|[#8751](https://github.com/NVIDIA/spark-rapids/pull/8751)|Ensure columnarEval always returns a GpuColumnVector|
|[#8765](https://github.com/NVIDIA/spark-rapids/pull/8765)|Add in support for maps to big data gen|
|[#8758](https://github.com/NVIDIA/spark-rapids/pull/8758)|Normal and Multi Distributions for BigDataGen|
|[#8755](https://github.com/NVIDIA/spark-rapids/pull/8755)|Add in dependency for databricks on integration tests|
|[#8737](https://github.com/NVIDIA/spark-rapids/pull/8737)|Fix parquet_write_test.py::test_hive_timestamp_value failure for Databricks|
|[#8745](https://github.com/NVIDIA/spark-rapids/pull/8745)|Conventional jar layout is not required for JDK9+|
|[#8706](https://github.com/NVIDIA/spark-rapids/pull/8706)|Add a tool to support generating large amounts of data|
|[#8747](https://github.com/NVIDIA/spark-rapids/pull/8747)|xfail hash_groupby_collect_set and hash_reduction_collect_set on nested type cases|
|[#8689](https://github.com/NVIDIA/spark-rapids/pull/8689)|Support nested arrays for `min`/`max` aggregations in groupby and reduction|
|[#8699](https://github.com/NVIDIA/spark-rapids/pull/8699)|Regression test for array of struct with a single field name "element" in Parquet|
|[#8733](https://github.com/NVIDIA/spark-rapids/pull/8733)|Avoid generating numeric null partition values on Databricks 10.4|
|[#8728](https://github.com/NVIDIA/spark-rapids/pull/8728)|Use specific mamba version and install libarchive explictly [skip ci]|
|[#8594](https://github.com/NVIDIA/spark-rapids/pull/8594)|String generation from complex regex in integration tests|
|[#8700](https://github.com/NVIDIA/spark-rapids/pull/8700)|Add regression test to ensure Parquet doesn't interpret timestamp values differently from Hive 0.14.0+|
|[#8711](https://github.com/NVIDIA/spark-rapids/pull/8711)|Factor out modules shared among shim profiles|
|[#8697](https://github.com/NVIDIA/spark-rapids/pull/8697)|Spillable columnar batch takes ownership and improve code coverage|
|[#8705](https://github.com/NVIDIA/spark-rapids/pull/8705)|Add schema evolution integration tests for partitioned data|
|[#8673](https://github.com/NVIDIA/spark-rapids/pull/8673)|Fix some potential memory leaks|
|[#8707](https://github.com/NVIDIA/spark-rapids/pull/8707)|Update config docs for new filecache configs [skip ci]|
|[#8695](https://github.com/NVIDIA/spark-rapids/pull/8695)|Always create the main artifact along with a shim-classifier artifact|
|[#8704](https://github.com/NVIDIA/spark-rapids/pull/8704)|Add tests for column names with dots|
|[#8703](https://github.com/NVIDIA/spark-rapids/pull/8703)|Comment out min/max agg test for nested structs to unblock CI|
|[#8698](https://github.com/NVIDIA/spark-rapids/pull/8698)|Cache last ORC stripe footer to avoid redundant remote reads|
|[#8687](https://github.com/NVIDIA/spark-rapids/pull/8687)|Handle TIMESTAMP_MILLIS for rebase check|
|[#8688](https://github.com/NVIDIA/spark-rapids/pull/8688)|Enable the 340 shim test|
|[#8656](https://github.com/NVIDIA/spark-rapids/pull/8656)|Return result from filecache message instead of null|
|[#8659](https://github.com/NVIDIA/spark-rapids/pull/8659)|Filter out nulls for build batches when needed in hash joins|
|[#8682](https://github.com/NVIDIA/spark-rapids/pull/8682)|[DOC] Update CUDA requirements in documentation and Dockerfiles[skip ci]|
|[#8637](https://github.com/NVIDIA/spark-rapids/pull/8637)|Support Float order-by columns for RANGE window functions|
|[#8681](https://github.com/NVIDIA/spark-rapids/pull/8681)|changed container name to adapt to blossom-lib refactor [skip ci]|
|[#8573](https://github.com/NVIDIA/spark-rapids/pull/8573)|Add support for Delta Lake 2.4.0|
|[#8671](https://github.com/NVIDIA/spark-rapids/pull/8671)|Fix use-after-freed bug in `GpuFloatArrayMin`|
|[#8650](https://github.com/NVIDIA/spark-rapids/pull/8650)|Support TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS|
|[#8495](https://github.com/NVIDIA/spark-rapids/pull/8495)|Speed up PCBS CPU read path by not recalculating as much|
|[#8389](https://github.com/NVIDIA/spark-rapids/pull/8389)|Add filecache support for ORC|
|[#8658](https://github.com/NVIDIA/spark-rapids/pull/8658)|Check if need to run Databricks pre-merge|
|[#8649](https://github.com/NVIDIA/spark-rapids/pull/8649)|Add Spark 3.4.1 shim|
|[#8624](https://github.com/NVIDIA/spark-rapids/pull/8624)|Rename numBytesAdded/Removed metrics and add deletion vector metrics in Databricks 12.2 shims|
|[#8645](https://github.com/NVIDIA/spark-rapids/pull/8645)|Fix "PytestUnknownMarkWarning: Unknown pytest.mark.inject_oom" warning|
|[#8608](https://github.com/NVIDIA/spark-rapids/pull/8608)|Matrix stages to dynamically build Databricks shims|
|[#8517](https://github.com/NVIDIA/spark-rapids/pull/8517)|Revert "Disable asserts for non-empty nulls (#8183)"|
|[#8628](https://github.com/NVIDIA/spark-rapids/pull/8628)|Enable Delta Write fallback tests on Databricks 12.2|
|[#8632](https://github.com/NVIDIA/spark-rapids/pull/8632)|Fix GCP examples and getting started guide [skip ci]|
|[#8638](https://github.com/NVIDIA/spark-rapids/pull/8638)|Support nested structs for `min`/`max` aggregations in groupby and reduction|
|[#8639](https://github.com/NVIDIA/spark-rapids/pull/8639)|Add iceberg test for nightly DB12.2 IT pipeline[skip ci]|
|[#8618](https://github.com/NVIDIA/spark-rapids/pull/8618)|Heuristic to speed up partial aggregates that get larger|
|[#8605](https://github.com/NVIDIA/spark-rapids/pull/8605)|[Doc] Fix demo link in index.md [skip ci]|
|[#8619](https://github.com/NVIDIA/spark-rapids/pull/8619)|Enable output batches metric for GpuShuffleCoalesceExec by default|
|[#8617](https://github.com/NVIDIA/spark-rapids/pull/8617)|Fixes broadcast spill serialization/deserialization|
|[#8531](https://github.com/NVIDIA/spark-rapids/pull/8531)|filecache: Modify FileCacheLocalityManager.init to pass in Spark context|
|[#8613](https://github.com/NVIDIA/spark-rapids/pull/8613)|Try print JVM core dump files if any test failures in CI|
|[#8616](https://github.com/NVIDIA/spark-rapids/pull/8616)|Wait for futures in multi-threaded writers even on exception|
|[#8578](https://github.com/NVIDIA/spark-rapids/pull/8578)|Add in metric to see how much computation time is lost due to retry|
|[#8590](https://github.com/NVIDIA/spark-rapids/pull/8590)|Drop ".dev0" suffix from Spark SNASHOT distro builds|
|[#8604](https://github.com/NVIDIA/spark-rapids/pull/8604)|Upgrade scalatest version to 3.2.16|
|[#8555](https://github.com/NVIDIA/spark-rapids/pull/8555)|Support `flatten`  SQL function|
|[#8599](https://github.com/NVIDIA/spark-rapids/pull/8599)|Fix broken links in advanced_configs.md|
|[#8589](https://github.com/NVIDIA/spark-rapids/pull/8589)|Revert to the JVM-based Spark version extraction in pytests|
|[#8582](https://github.com/NVIDIA/spark-rapids/pull/8582)|Fix databricks shims build errors caused by DB updates|
|[#8564](https://github.com/NVIDIA/spark-rapids/pull/8564)|Fold `verify-all-modules-with-headSparkVersion` into `verify-all-modules` [skip ci]|
|[#8553](https://github.com/NVIDIA/spark-rapids/pull/8553)|Handle empty batch in ParquetCachedBatchSerializer|
|[#8575](https://github.com/NVIDIA/spark-rapids/pull/8575)|Corrected typos in CONTRIBUTING.md [skip ci]|
|[#8574](https://github.com/NVIDIA/spark-rapids/pull/8574)|Remove maxTaskFailures=4 for pre-3.1.1 Spark|
|[#8503](https://github.com/NVIDIA/spark-rapids/pull/8503)|Remove hard-coded version numbers for dependencies when building on|
|[#8544](https://github.com/NVIDIA/spark-rapids/pull/8544)|Fix auto merge conflict 8543 [skip ci]|
|[#8521](https://github.com/NVIDIA/spark-rapids/pull/8521)|List supported Spark versions when no shim found|
|[#8520](https://github.com/NVIDIA/spark-rapids/pull/8520)|Add support for first, last, nth, and collect_list aggregations for BinaryType|
|[#8509](https://github.com/NVIDIA/spark-rapids/pull/8509)|Remove legacy spark version check|
|[#8494](https://github.com/NVIDIA/spark-rapids/pull/8494)|Fix 23.08 build on Databricks 12.2|
|[#8487](https://github.com/NVIDIA/spark-rapids/pull/8487)|Move MockTaskContext to tests project|
|[#8426](https://github.com/NVIDIA/spark-rapids/pull/8426)|Pre-merge CI to support Databricks 12.2|
|[#8282](https://github.com/NVIDIA/spark-rapids/pull/8282)|Databricks 12.2 Support|
|[#8407](https://github.com/NVIDIA/spark-rapids/pull/8407)|Bump up dep version to 23.08.0-SNAPSHOT|
|[#8359](https://github.com/NVIDIA/spark-rapids/pull/8359)|Init version 23.08.0-SNAPSHOT|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
