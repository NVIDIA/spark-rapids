# Change log
Generated on 2023-12-12

## Release 23.12

### Features
|||
|:---|:---|
|[#6832](https://github.com/NVIDIA/spark-rapids/issues/6832)|[FEA] Convert Timestamp/Timezone tests/checks to be per operator instead of generic |
|[#9805](https://github.com/NVIDIA/spark-rapids/issues/9805)|[FEA] Support ```current_date``` expression function with CST (UTC + 8) timezone support|
|[#9515](https://github.com/NVIDIA/spark-rapids/issues/9515)|[FEA] Support temporal types in to_json|
|[#9872](https://github.com/NVIDIA/spark-rapids/issues/9872)|[FEA][JSON] Support Decimal type in `to_json`|
|[#9802](https://github.com/NVIDIA/spark-rapids/issues/9802)|[FEA] Support FromUTCTimestamp on the GPU with a non-UTC timezone|
|[#6831](https://github.com/NVIDIA/spark-rapids/issues/6831)|[FEA] Support timestamp transitions to and from UTC for single time zones with no repeating rules|
|[#9590](https://github.com/NVIDIA/spark-rapids/issues/9590)|[FEA][JSON] Support temporal types in `from_json`|
|[#9804](https://github.com/NVIDIA/spark-rapids/issues/9804)|[FEA] Support CPU path for from_utc_timestamp function with timezone|
|[#9461](https://github.com/NVIDIA/spark-rapids/issues/9461)|[FEA] Validate nvcomp-3.0 with spark rapids plugin|
|[#8832](https://github.com/NVIDIA/spark-rapids/issues/8832)|[FEA] rewrite join conditions where only part of it can fit on the AST|
|[#9059](https://github.com/NVIDIA/spark-rapids/issues/9059)|[FEA] Support spark.sql.parquet.datetimeRebaseModeInRead=LEGACY|
|[#9037](https://github.com/NVIDIA/spark-rapids/issues/9037)|[FEA] Support spark.sql.parquet.int96RebaseModeInWrite= LEGACY|
|[#9632](https://github.com/NVIDIA/spark-rapids/issues/9632)|[FEA] Take into account `org.apache.spark.timeZone` in Parquet/Avro from Spark 3.2|
|[#8770](https://github.com/NVIDIA/spark-rapids/issues/8770)|[FEA] add more metrics to Eventlogs or Executor logs|
|[#9597](https://github.com/NVIDIA/spark-rapids/issues/9597)|[FEA][JSON] Support boolean type in `from_json`|
|[#9516](https://github.com/NVIDIA/spark-rapids/issues/9516)|[FEA] Add support for JSON data source option `ignoreNullFields=false` in `to_json`|
|[#9520](https://github.com/NVIDIA/spark-rapids/issues/9520)|[FEA] Add support for `LAST()` as running window function|
|[#9518](https://github.com/NVIDIA/spark-rapids/issues/9518)|[FEA] Add support for relevant JSON data source options in `to_json`|
|[#9218](https://github.com/NVIDIA/spark-rapids/issues/9218)|[FEA] Support stack function|
|[#9532](https://github.com/NVIDIA/spark-rapids/issues/9532)|[FEA] Support Delta Lake 2.3.0|
|[#1525](https://github.com/NVIDIA/spark-rapids/issues/1525)|[FEA] Support Scala 2.13|
|[#7279](https://github.com/NVIDIA/spark-rapids/issues/7279)|[FEA] Support OverwriteByExpressionExecV1 for Delta Lake|
|[#9326](https://github.com/NVIDIA/spark-rapids/issues/9326)|[FEA] Specify `recover_with_null` when reading JSON files|
|[#8780](https://github.com/NVIDIA/spark-rapids/issues/8780)|[FEA] Support to_json function|
|[#7278](https://github.com/NVIDIA/spark-rapids/issues/7278)|[FEA] Support AppendDataExecV1 for Delta Lake|
|[#6266](https://github.com/NVIDIA/spark-rapids/issues/6266)|[FEA] Support Percentile|
|[#7277](https://github.com/NVIDIA/spark-rapids/issues/7277)|[FEA] Support AtomicReplaceTableAsSelect for Delta Lake|
|[#7276](https://github.com/NVIDIA/spark-rapids/issues/7276)|[FEA] Support AtomicCreateTableAsSelect for Delta Lake|

### Performance
|||
|:---|:---|
|[#8137](https://github.com/NVIDIA/spark-rapids/issues/8137)|[FEA] Upgrade to UCX 1.15|
|[#8157](https://github.com/NVIDIA/spark-rapids/issues/8157)|[FEA] Add string comparison to AST expressions|
|[#9398](https://github.com/NVIDIA/spark-rapids/issues/9398)|[FEA] Compress/encrypt spill to disk|

### Bugs Fixed
|||
|:---|:---|
|[#9687](https://github.com/NVIDIA/spark-rapids/issues/9687)|[BUG] `test_in_set` fails when DATAGEN_SEED=1698940723|
|[#9659](https://github.com/NVIDIA/spark-rapids/issues/9659)|[BUG] executor crash intermittantly in scala2.13-built spark332 integration tests|
|[#9923](https://github.com/NVIDIA/spark-rapids/issues/9923)|[BUG] Failed case about ```test_timestamp_seconds_rounding_necessary[Decimal(20,7)][DATAGEN_SEED=1701412018] – src.main.python.date_time_test```|
|[#9982](https://github.com/NVIDIA/spark-rapids/issues/9982)|[BUG] test "convert large InternalRow iterator to cached batch single col" failed with arena pool|
|[#9683](https://github.com/NVIDIA/spark-rapids/issues/9683)|[BUG] test_map_scalars_supported_key_types fails with DATAGEN_SEED=1698940723|
|[#9976](https://github.com/NVIDIA/spark-rapids/issues/9976)|[BUG] test_part_write_round_trip[Float] Failed on -0.0 partition|
|[#9948](https://github.com/NVIDIA/spark-rapids/issues/9948)|[BUG] parquet reader data corruption in nested schema after https://github.com/rapidsai/cudf/pull/13302|
|[#9867](https://github.com/NVIDIA/spark-rapids/issues/9867)|[BUG] Unable to use Spark Rapids with Spark Thrift Server|
|[#9934](https://github.com/NVIDIA/spark-rapids/issues/9934)|[BUG] test_delta_multi_part_write_round_trip_unmanaged and test_delta_part_write_round_trip_unmanaged failed DATA_SEED=1701608331 |
|[#9933](https://github.com/NVIDIA/spark-rapids/issues/9933)|[BUG] collection_ops_test.py::test_sequence_too_long_sequence[Long(not_null)][DATAGEN_SEED=1701553915, INJECT_OOM]|
|[#9837](https://github.com/NVIDIA/spark-rapids/issues/9837)|[BUG] test_part_write_round_trip failed|
|[#9932](https://github.com/NVIDIA/spark-rapids/issues/9932)|[BUG] Failed test_multi_tier_ast[DATAGEN_SEED=1701445668] on CI|
|[#9829](https://github.com/NVIDIA/spark-rapids/issues/9829)|[BUG] Java OOM when testing non-UTC time zone with lots of cases fallback.|
|[#9403](https://github.com/NVIDIA/spark-rapids/issues/9403)|[BUG] test_cogroup_apply_udf[Short(not_null)] failed with pandas 2.1.X|
|[#9684](https://github.com/NVIDIA/spark-rapids/issues/9684)|[BUG] test_coalesce fails with DATAGEN_SEED=1698940723|
|[#9685](https://github.com/NVIDIA/spark-rapids/issues/9685)|[BUG] test_case_when fails with DATAGEN_SEED=1698940723|
|[#9776](https://github.com/NVIDIA/spark-rapids/issues/9776)|[BUG] fastparquet compatibility tests fail with data mismatch if TZ is not set and system timezone is not UTC|
|[#9733](https://github.com/NVIDIA/spark-rapids/issues/9733)|[BUG] Complex AST expressions can crash with non-matching operand type error|
|[#9877](https://github.com/NVIDIA/spark-rapids/issues/9877)|[BUG] Fix resource leak in to_json|
|[#9722](https://github.com/NVIDIA/spark-rapids/issues/9722)|[BUG] test_floor_scale_zero fails with DATAGEN_SEED=1700009407|
|[#9846](https://github.com/NVIDIA/spark-rapids/issues/9846)|[BUG] test_ceil_scale_zero may fail with different datagen_seed|
|[#9781](https://github.com/NVIDIA/spark-rapids/issues/9781)|[BUG] test_cast_string_date_valid_format fails on DATAGEN_SEED=1700250017|
|[#9714](https://github.com/NVIDIA/spark-rapids/issues/9714)|Scala Map class not found when executing the benchmark on Spark 3.5.0 with Scala 2.13|
|[#9856](https://github.com/NVIDIA/spark-rapids/issues/9856)|collection_ops_test.py failed on Dataproc-2.1 with: Column 'None' does not exist|
|[#9397](https://github.com/NVIDIA/spark-rapids/issues/9397)|[BUG] RapidsShuffleManager MULTITHREADED on Databricks, we see loss of executors due to Rpc issues|
|[#9738](https://github.com/NVIDIA/spark-rapids/issues/9738)|[BUG] `test_delta_part_write_round_trip_unmanaged` and `test_delta_multi_part_write_round_trip_unmanaged` fail with `DATAGEN_SEED=1700105176`|
|[#9771](https://github.com/NVIDIA/spark-rapids/issues/9771)|[BUG] ast_test.py::test_X[(String, True)][DATAGEN_SEED=1700205785] failed|
|[#9782](https://github.com/NVIDIA/spark-rapids/issues/9782)|[BUG] Error messages appear in a clean build|
|[#9798](https://github.com/NVIDIA/spark-rapids/issues/9798)|[BUG] GpuCheckOverflowInTableInsert should be added to databricks shim|
|[#9820](https://github.com/NVIDIA/spark-rapids/issues/9820)|[BUG] test_parquet_write_roundtrip_datetime_with_legacy_rebase fails with "year 0 is out of range"|
|[#9817](https://github.com/NVIDIA/spark-rapids/issues/9817)|[BUG] FAILED dpp_test.py::test_dpp_reuse_broadcast_exchange[false-0-parquet][DATAGEN_SEED=1700572856, IGNORE_ORDER]|
|[#9768](https://github.com/NVIDIA/spark-rapids/issues/9768)|[BUG] `cast decimal to string` ScalaTest relies on a side effects |
|[#9711](https://github.com/NVIDIA/spark-rapids/issues/9711)|[BUG] test_lte fails with DATAGEN_SEED=1699987762|
|[#9751](https://github.com/NVIDIA/spark-rapids/issues/9751)|[BUG] cmp_test test_gte failed with DATAGEN_SEED=1700149611|
|[#9469](https://github.com/NVIDIA/spark-rapids/issues/9469)|[BUG] [main] ERROR com.nvidia.spark.rapids.GpuOverrideUtil - Encountered an exception applying GPU overrides java.lang.IllegalStateException: the broadcast must be on the GPU too|
|[#9648](https://github.com/NVIDIA/spark-rapids/issues/9648)|[BUG] Existence default values in schema are not being honored|
|[#9676](https://github.com/NVIDIA/spark-rapids/issues/9676)|Fix Delta Lake Integration tests; `test_delta_atomic_create_table_as_select` and `test_delta_atomic_replace_table_as_select`|
|[#9701](https://github.com/NVIDIA/spark-rapids/issues/9701)|[BUG] test_ts_formats_round_trip and test_datetime_roundtrip_with_legacy_rebase fail with DATAGEN_SEED=1699915317|
|[#9691](https://github.com/NVIDIA/spark-rapids/issues/9691)|[BUG] Repeated Maven invocations w/o changes recompile too many Scala sources despite recompileMode=incremental |
|[#9547](https://github.com/NVIDIA/spark-rapids/issues/9547)|Update buildall and doc to generate bloop projects for test debugging|
|[#9697](https://github.com/NVIDIA/spark-rapids/issues/9697)|[BUG] Iceberg multiple file readers can not read files if the file paths contain encoded URL unsafe chars|
|[#9681](https://github.com/NVIDIA/spark-rapids/issues/9681)|Databricks Build Failing For 330db+|
|[#9521](https://github.com/NVIDIA/spark-rapids/issues/9521)|[BUG] Multi Threaded Shuffle Writer needs flow control|
|[#9675](https://github.com/NVIDIA/spark-rapids/issues/9675)|Failing Delta Lake Tests for Databricks 13.3 Due to WriteIntoDeltaCommand|
|[#9669](https://github.com/NVIDIA/spark-rapids/issues/9669)|[BUG] Rebase exception states not in UTC but timezone is Etc/UTC|
|[#7940](https://github.com/NVIDIA/spark-rapids/issues/7940)|[BUG] UCX peer connection issue in multi-nic single node cluster|
|[#9650](https://github.com/NVIDIA/spark-rapids/issues/9650)|[BUG] Github workflow for missing scala2.13 updates fails to detect when pom is new|
|[#9621](https://github.com/NVIDIA/spark-rapids/issues/9621)|[BUG] Scala 2.13 with-classifier profile is picking up Scala2.12 spark.version|
|[#9636](https://github.com/NVIDIA/spark-rapids/issues/9636)|[BUG] All parquet integration tests failed "Part of the plan is not columnar class" in databricks runtimes|
|[#9108](https://github.com/NVIDIA/spark-rapids/issues/9108)|[BUG] nullability on some decimal operations is wrong|
|[#9625](https://github.com/NVIDIA/spark-rapids/issues/9625)|[BUG] Typo in github Maven check install-modules |
|[#9603](https://github.com/NVIDIA/spark-rapids/issues/9603)|[BUG] fastparquet_compatibility_test fails on dataproc|
|[#8729](https://github.com/NVIDIA/spark-rapids/issues/8729)|[BUG] nightly integration test failed OOM kill in JDK11 ENV|
|[#9589](https://github.com/NVIDIA/spark-rapids/issues/9589)|[BUG] Scala 2.13 build hard-codes Java 8 target |
|[#9581](https://github.com/NVIDIA/spark-rapids/issues/9581)|Delta Lake 2.4 missing equals/hashCode override for file format and some metrics for merge|
|[#9507](https://github.com/NVIDIA/spark-rapids/issues/9507)|[BUG] Spark 3.2+/ParquetFilterSuite/Parquet filter pushdown - timestamp/ FAILED  |
|[#9540](https://github.com/NVIDIA/spark-rapids/issues/9540)|[BUG] Job failed with SparkUpgradeException no matter which value are set for spark.sql.parquet.datetimeRebaseModeInRead|
|[#9545](https://github.com/NVIDIA/spark-rapids/issues/9545)|[BUG] Dataproc 2.0 test_reading_file_rewritten_with_fastparquet tests failing|
|[#9552](https://github.com/NVIDIA/spark-rapids/issues/9552)|[BUG] Inconsistent CDH dependency overrides across submodules|
|[#9571](https://github.com/NVIDIA/spark-rapids/issues/9571)|[BUG] non-deterministic compiled SQLExecPlugin.class with scala 2.13 deployment|
|[#9569](https://github.com/NVIDIA/spark-rapids/issues/9569)|[BUG] test_window_running failed in 3.1.2+3.1.3|
|[#9480](https://github.com/NVIDIA/spark-rapids/issues/9480)|[BUG] mapInPandas doesn't invoke udf on empty partitions|
|[#8644](https://github.com/NVIDIA/spark-rapids/issues/8644)|[BUG] Parquet file with malformed dictionary does not error when loaded|
|[#9310](https://github.com/NVIDIA/spark-rapids/issues/9310)|[BUG] Improve support for reading JSON files with malformed rows|
|[#9457](https://github.com/NVIDIA/spark-rapids/issues/9457)|[BUG] CDH 332 unit tests failing|
|[#9404](https://github.com/NVIDIA/spark-rapids/issues/9404)|[BUG] Spark reports a decimal error when create lit scalar when generate Decimal(34, -5) data.|
|[#9110](https://github.com/NVIDIA/spark-rapids/issues/9110)|[BUG] GPU Reader fails due to partition column creating column larger then cudf column size limit|
|[#8631](https://github.com/NVIDIA/spark-rapids/issues/8631)|[BUG] Parquet load failure on repeated_no_annotation.parquet|
|[#9364](https://github.com/NVIDIA/spark-rapids/issues/9364)|[BUG] CUDA illegal access error is triggering split and retry logic|

### PRs
|||
|:---|:---|
|[#9943](https://github.com/NVIDIA/spark-rapids/pull/9943)|[DOC] Update docs for 23.12.0 release [skip ci]|
|[#10014](https://github.com/NVIDIA/spark-rapids/pull/10014)|Add documentation for how to run tests with a fixed datagen seed [skip ci]|
|[#9954](https://github.com/NVIDIA/spark-rapids/pull/9954)|Update private and JNI version to released 23.12.0|
|[#10009](https://github.com/NVIDIA/spark-rapids/pull/10009)|Using fix seed to unblock 23.12 release; Move the blocked issues to 24.02|
|[#10007](https://github.com/NVIDIA/spark-rapids/pull/10007)|Fix Java OOM in non-UTC case with lots of xfail (#9944)|
|[#9985](https://github.com/NVIDIA/spark-rapids/pull/9985)|Avoid allocating GPU memory out of RMM managed pool in test|
|[#9970](https://github.com/NVIDIA/spark-rapids/pull/9970)|Avoid leading and trailing zeros in test_timestamp_seconds_rounding_necessary|
|[#9978](https://github.com/NVIDIA/spark-rapids/pull/9978)|Avoid using floating point values as partition values in tests|
|[#9979](https://github.com/NVIDIA/spark-rapids/pull/9979)|Add compatibility notes for writing ORC with lost Gregorian days [skip ci]|
|[#9949](https://github.com/NVIDIA/spark-rapids/pull/9949)|Override the seed for `test_map_scalars_supported_key_types ` for version of Spark before 3.4.0 [Databricks]|
|[#9961](https://github.com/NVIDIA/spark-rapids/pull/9961)|Avoid using floating point for partition values in Delta Lake tests|
|[#9960](https://github.com/NVIDIA/spark-rapids/pull/9960)|Fix LongGen accidentally using special cases when none are desired|
|[#9950](https://github.com/NVIDIA/spark-rapids/pull/9950)|Avoid generating NaNs as partition values in test_part_write_round_trip|
|[#9940](https://github.com/NVIDIA/spark-rapids/pull/9940)|Fix 'year 0 is out of range' by setting a fix seed|
|[#9946](https://github.com/NVIDIA/spark-rapids/pull/9946)|Fix test_multi_tier_ast to ignore ordering of output rows|
|[#9928](https://github.com/NVIDIA/spark-rapids/pull/9928)|Test `inset` with `NaN` only for Spark from 3.1.3|
|[#9906](https://github.com/NVIDIA/spark-rapids/pull/9906)|Fix test_initcap to use the intended limited character set|
|[#9831](https://github.com/NVIDIA/spark-rapids/pull/9831)|Skip fastparquet timestamp tests when plugin cannot read/write timestamps|
|[#9893](https://github.com/NVIDIA/spark-rapids/pull/9893)|Add multiple expression tier regression test for AST|
|[#9873](https://github.com/NVIDIA/spark-rapids/pull/9873)|Add support for decimal in `to_json`|
|[#9890](https://github.com/NVIDIA/spark-rapids/pull/9890)|Remove Databricks 13.3 from release 23.12|
|[#9874](https://github.com/NVIDIA/spark-rapids/pull/9874)|Fix zero-scale floor and ceil tests|
|[#9879](https://github.com/NVIDIA/spark-rapids/pull/9879)|Fix resource leak in to_json|
|[#9600](https://github.com/NVIDIA/spark-rapids/pull/9600)|Add date and timestamp support to to_json|
|[#9871](https://github.com/NVIDIA/spark-rapids/pull/9871)|Fix test_cast_string_date_valid_format generating year 0|
|[#9885](https://github.com/NVIDIA/spark-rapids/pull/9885)|Preparation for non-UTC nightly CI [skip ci]|
|[#9810](https://github.com/NVIDIA/spark-rapids/pull/9810)|Support from_utc_timestamp on the GPU for non-UTC timezones (non-DST)|
|[#9865](https://github.com/NVIDIA/spark-rapids/pull/9865)|Fix problems with nulls in sequence tests|
|[#9864](https://github.com/NVIDIA/spark-rapids/pull/9864)|Add compatibility documentation with respect to decimal overflow detection [skip ci]|
|[#9860](https://github.com/NVIDIA/spark-rapids/pull/9860)|Fixing FAQ deadlink in plugin code [skip ci]|
|[#9840](https://github.com/NVIDIA/spark-rapids/pull/9840)|Avoid using NaNs as Delta Lake partition values|
|[#9773](https://github.com/NVIDIA/spark-rapids/pull/9773)|xfail all the impacted cases when using non-UTC time zone|
|[#9849](https://github.com/NVIDIA/spark-rapids/pull/9849)|Instantly Delete pre-merge content of stage workspace if success|
|[#9848](https://github.com/NVIDIA/spark-rapids/pull/9848)|Force datagen_seed for test_ceil_scale_zero and test_decimal_round|
|[#9677](https://github.com/NVIDIA/spark-rapids/pull/9677)|Enable build for Databricks 13.3|
|[#9809](https://github.com/NVIDIA/spark-rapids/pull/9809)|Re-enable AST string integration cases|
|[#9835](https://github.com/NVIDIA/spark-rapids/pull/9835)|Avoid pre-Gregorian dates in schema_evolution_test|
|[#9786](https://github.com/NVIDIA/spark-rapids/pull/9786)|Check paths for existence to prevent ignorable error messages during build|
|[#9824](https://github.com/NVIDIA/spark-rapids/pull/9824)|UCX 1.15 upgrade|
|[#9800](https://github.com/NVIDIA/spark-rapids/pull/9800)|Add GpuCheckOverflowInTableInsert to Databricks 11.3+|
|[#9821](https://github.com/NVIDIA/spark-rapids/pull/9821)|Update timestamp gens to avoid "year 0 is out of range" errors|
|[#9826](https://github.com/NVIDIA/spark-rapids/pull/9826)|Set seed to 0 for test_hash_reduction_sum|
|[#9720](https://github.com/NVIDIA/spark-rapids/pull/9720)|Support timestamp in `from_json`|
|[#9818](https://github.com/NVIDIA/spark-rapids/pull/9818)|Specify nullable=False when generating filter values in dpp tests|
|[#9689](https://github.com/NVIDIA/spark-rapids/pull/9689)|Support CPU path for from_utc_timestamp function with timezone |
|[#9769](https://github.com/NVIDIA/spark-rapids/pull/9769)|Use withGpuSparkSession to customize SparkConf|
|[#9780](https://github.com/NVIDIA/spark-rapids/pull/9780)|Fix NaN handling in GpuLessThanOrEqual and GpuGreaterThanOrEqual|
|[#9795](https://github.com/NVIDIA/spark-rapids/pull/9795)|xfail AST string tests|
|[#9666](https://github.com/NVIDIA/spark-rapids/pull/9666)|Add support for parsing strings as dates in `from_json`|
|[#9673](https://github.com/NVIDIA/spark-rapids/pull/9673)|Fix the broadcast joins issues caused by InputFileBlockRule|
|[#9785](https://github.com/NVIDIA/spark-rapids/pull/9785)|Force datagen_seed for 9781 and 9784 [skip ci]|
|[#9765](https://github.com/NVIDIA/spark-rapids/pull/9765)|Let GPU scans fall back when default values exist in schema|
|[#9729](https://github.com/NVIDIA/spark-rapids/pull/9729)|Fix Delta Lake atomic table operations on spark341db|
|[#9770](https://github.com/NVIDIA/spark-rapids/pull/9770)|[BUG] Fix the doc for Maven and Scala 2.13 test example [skip ci]|
|[#9761](https://github.com/NVIDIA/spark-rapids/pull/9761)|Fix bug in tagging of JsonToStructs|
|[#9758](https://github.com/NVIDIA/spark-rapids/pull/9758)|Remove forced seed from Delta Lake part_write_round_trip_unmanaged tests|
|[#9652](https://github.com/NVIDIA/spark-rapids/pull/9652)|Add time zone config to set non-UTC|
|[#9736](https://github.com/NVIDIA/spark-rapids/pull/9736)|Fix `TimestampGen` to generate value not too close to the minimum allowed timestamp|
|[#9698](https://github.com/NVIDIA/spark-rapids/pull/9698)|Speed up build: unnecessary invalidation in the incremental recompile mode|
|[#9748](https://github.com/NVIDIA/spark-rapids/pull/9748)|Fix Delta Lake part_write_round_trip_unmanaged tests with floating point|
|[#9702](https://github.com/NVIDIA/spark-rapids/pull/9702)|Support split BroadcastNestedLoopJoin condition for AST and non-AST|
|[#9746](https://github.com/NVIDIA/spark-rapids/pull/9746)|Force test_hypot to be single seed for now|
|[#9745](https://github.com/NVIDIA/spark-rapids/pull/9745)|Avoid generating null filter values in test_delta_dfp_reuse_broadcast_exchange|
|[#9741](https://github.com/NVIDIA/spark-rapids/pull/9741)|Set seed=0 for the delta lake part roundtrip tests|
|[#9660](https://github.com/NVIDIA/spark-rapids/pull/9660)|Fully support date/time legacy rebase for nested input|
|[#9672](https://github.com/NVIDIA/spark-rapids/pull/9672)|Support String type for AST|
|[#9732](https://github.com/NVIDIA/spark-rapids/pull/9732)|Temporarily force `datagen_seed=0` for `test_re_replace_all` to unblock CI|
|[#9726](https://github.com/NVIDIA/spark-rapids/pull/9726)|Fix leak in BatchWithPartitionData|
|[#9717](https://github.com/NVIDIA/spark-rapids/pull/9717)|Encode the file path from Iceberg when converting to a PartitionedFile|
|[#9441](https://github.com/NVIDIA/spark-rapids/pull/9441)|Add a random seed specific to datagen cases|
|[#9649](https://github.com/NVIDIA/spark-rapids/pull/9649)|Support `spark.sql.parquet.datetimeRebaseModeInRead=LEGACY` and `spark.sql.parquet.int96RebaseModeInRead=LEGACY`|
|[#9612](https://github.com/NVIDIA/spark-rapids/pull/9612)|Escape quotes and newlines when converting strings to json format in to_json|
|[#9644](https://github.com/NVIDIA/spark-rapids/pull/9644)|Add Partial Delta Lake Support for Databricks 13.3|
|[#9690](https://github.com/NVIDIA/spark-rapids/pull/9690)|Changed `extractExecutedPlan` to consider ResultQueryStageExec for Databricks 13.3|
|[#9686](https://github.com/NVIDIA/spark-rapids/pull/9686)|Removed Maven Profiles From `tests/pom.xml`|
|[#9509](https://github.com/NVIDIA/spark-rapids/pull/9509)|Fine-grained spill metrics|
|[#9658](https://github.com/NVIDIA/spark-rapids/pull/9658)|Support `spark.sql.parquet.int96RebaseModeInWrite=LEGACY`|
|[#9695](https://github.com/NVIDIA/spark-rapids/pull/9695)|Revert "Support split non-AST-able join condition for BroadcastNested…|
|[#9693](https://github.com/NVIDIA/spark-rapids/pull/9693)|Enable automerge from 23.12 to 24.02 [skip ci]|
|[#9679](https://github.com/NVIDIA/spark-rapids/pull/9679)|[Doc] update the dead link in download page [skip ci]|
|[#9678](https://github.com/NVIDIA/spark-rapids/pull/9678)|Add flow control for multithreaded shuffle writer|
|[#9635](https://github.com/NVIDIA/spark-rapids/pull/9635)|Support split non-AST-able join condition for BroadcastNestedLoopJoin|
|[#9646](https://github.com/NVIDIA/spark-rapids/pull/9646)|Fix Integration Test Failures for Databricks 13.3 Support|
|[#9670](https://github.com/NVIDIA/spark-rapids/pull/9670)|Normalize file timezone and handle missing file timezone in datetimeRebaseUtils|
|[#9657](https://github.com/NVIDIA/spark-rapids/pull/9657)|Update verify check to handle new pom files [skip ci]|
|[#9663](https://github.com/NVIDIA/spark-rapids/pull/9663)|Making User Guide info in bold and adding it as top right link in github.io [skip ci]|
|[#9609](https://github.com/NVIDIA/spark-rapids/pull/9609)|Add valid retry solution to mvn-verify [skip ci]|
|[#9655](https://github.com/NVIDIA/spark-rapids/pull/9655)|Document problem with handling of invalid characters in CSV reader|
|[#9620](https://github.com/NVIDIA/spark-rapids/pull/9620)|Add support for parsing boolean values in `from_json`|
|[#9615](https://github.com/NVIDIA/spark-rapids/pull/9615)|Bloop updates - require JDK11 in buildall + docs, build bloop for all targets.|
|[#9631](https://github.com/NVIDIA/spark-rapids/pull/9631)|Refactor Parquet readers|
|[#9637](https://github.com/NVIDIA/spark-rapids/pull/9637)|Added Support For Various Execs for Databricks 13.3 |
|[#9640](https://github.com/NVIDIA/spark-rapids/pull/9640)|Add support for `ignoreNullFields=false` in `to_json`|
|[#9623](https://github.com/NVIDIA/spark-rapids/pull/9623)|Running window optimization for `LAST()`|
|[#9641](https://github.com/NVIDIA/spark-rapids/pull/9641)|Revert "Support rebase checking for nested dates and timestamps (#9617)"|
|[#9423](https://github.com/NVIDIA/spark-rapids/pull/9423)|Re-enable `from_json` / `JsonToStructs`|
|[#9624](https://github.com/NVIDIA/spark-rapids/pull/9624)|Add jenkins-level retry for pre-merge build in databricks runtimes|
|[#9608](https://github.com/NVIDIA/spark-rapids/pull/9608)|Fix nullability issues for some decimal operations|
|[#9617](https://github.com/NVIDIA/spark-rapids/pull/9617)|Support rebase checking for nested dates and timestamps|
|[#9611](https://github.com/NVIDIA/spark-rapids/pull/9611)|Move simple classes after refactoring to sql-plugin-api|
|[#9618](https://github.com/NVIDIA/spark-rapids/pull/9618)|Remove unused dataTypes argument from HostShuffleCoalesceIterator|
|[#9626](https://github.com/NVIDIA/spark-rapids/pull/9626)|Fix ENV typo in pre-merge github actions [skip ci]|
|[#9593](https://github.com/NVIDIA/spark-rapids/pull/9593)|PythonRunner and RapidsErrorUtils Changes For Databricks 13.3|
|[#9607](https://github.com/NVIDIA/spark-rapids/pull/9607)|Integration tests: Install specific fastparquet version.|
|[#9610](https://github.com/NVIDIA/spark-rapids/pull/9610)|Propagate local properties to broadcast execs|
|[#9544](https://github.com/NVIDIA/spark-rapids/pull/9544)|Support batching for `RANGE` running window aggregations. Including on|
|[#9601](https://github.com/NVIDIA/spark-rapids/pull/9601)|Remove usage of deprecated scala.Proxy|
|[#9591](https://github.com/NVIDIA/spark-rapids/pull/9591)|Enable implicit JDK profile activation|
|[#9586](https://github.com/NVIDIA/spark-rapids/pull/9586)|Merge metrics and file format fixes to Delta 2.4 support|
|[#9594](https://github.com/NVIDIA/spark-rapids/pull/9594)|Revert "Ignore failing Parquet filter test to unblock CI (#9519)"|
|[#9454](https://github.com/NVIDIA/spark-rapids/pull/9454)|Support encryption and compression in disk store|
|[#9439](https://github.com/NVIDIA/spark-rapids/pull/9439)|Support stack function|
|[#9583](https://github.com/NVIDIA/spark-rapids/pull/9583)|Fix fastparquet tests to work with HDFS|
|[#9508](https://github.com/NVIDIA/spark-rapids/pull/9508)|Consolidate deps switching in an intermediate pom|
|[#9562](https://github.com/NVIDIA/spark-rapids/pull/9562)|Delta Lake 2.3.0 support|
|[#9576](https://github.com/NVIDIA/spark-rapids/pull/9576)|Move Stack classes to wrapper classes to fix non-deterministic build issue|
|[#9572](https://github.com/NVIDIA/spark-rapids/pull/9572)|Add retry for CrossJoinIterator and ConditionalNestedLoopJoinIterator|
|[#9575](https://github.com/NVIDIA/spark-rapids/pull/9575)|Fix `test_window_running*()` for `NTH_VALUE IGNORE NULLS`.|
|[#9574](https://github.com/NVIDIA/spark-rapids/pull/9574)|Fix broken #endif scala comments [skip ci]|
|[#9568](https://github.com/NVIDIA/spark-rapids/pull/9568)|Enforce Apache 3.3.0+ for Scala 2.13|
|[#9557](https://github.com/NVIDIA/spark-rapids/pull/9557)|Support launching Map Pandas UDF on empty partitions|
|[#9489](https://github.com/NVIDIA/spark-rapids/pull/9489)|Batching support for ROW-based `FIRST()` window function|
|[#9510](https://github.com/NVIDIA/spark-rapids/pull/9510)|Add Databricks 13.3 shim boilerplate code and refactor Databricks 12.2 shim|
|[#9554](https://github.com/NVIDIA/spark-rapids/pull/9554)|Fix fastparquet installation for|
|[#9536](https://github.com/NVIDIA/spark-rapids/pull/9536)|Add CPU POC of TimeZoneDB; Test some time zones by comparing CPU POC and Spark|
|[#9558](https://github.com/NVIDIA/spark-rapids/pull/9558)|Support integration test against scala2.13 spark binaries[skip ci]|
|[#8592](https://github.com/NVIDIA/spark-rapids/pull/8592)|Scala 2.13 Support|
|[#9551](https://github.com/NVIDIA/spark-rapids/pull/9551)|Enable malformed Parquet failure test|
|[#9546](https://github.com/NVIDIA/spark-rapids/pull/9546)|Support OverwriteByExpressionExecV1 for Delta Lake tables|
|[#9527](https://github.com/NVIDIA/spark-rapids/pull/9527)|Support Split And Retry for GpuProjectAstExec|
|[#9541](https://github.com/NVIDIA/spark-rapids/pull/9541)|Move simple classes to  API|
|[#9548](https://github.com/NVIDIA/spark-rapids/pull/9548)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#9418](https://github.com/NVIDIA/spark-rapids/pull/9418)|Fix STRUCT comparison between Pandas and Spark dataframes in fastparquet tests|
|[#9468](https://github.com/NVIDIA/spark-rapids/pull/9468)|Add SplitAndRetry to GpuRunningWindowIterator|
|[#9486](https://github.com/NVIDIA/spark-rapids/pull/9486)|Add partial support for `to_json`|
|[#9538](https://github.com/NVIDIA/spark-rapids/pull/9538)|Fix tiered project breaking higher order functions|
|[#9539](https://github.com/NVIDIA/spark-rapids/pull/9539)|Add delta-24x to delta-lake/README.md [skip ci]|
|[#9534](https://github.com/NVIDIA/spark-rapids/pull/9534)|Add pyarrow tests for Databricks runtime|
|[#9444](https://github.com/NVIDIA/spark-rapids/pull/9444)|Remove redundant pass-through shuffle manager classes|
|[#9531](https://github.com/NVIDIA/spark-rapids/pull/9531)|Fix relative path for spark-shell nightly test [skip ci]|
|[#9525](https://github.com/NVIDIA/spark-rapids/pull/9525)|Follow-up to dbdeps consolidation|
|[#9506](https://github.com/NVIDIA/spark-rapids/pull/9506)|Move ProxyShuffleInternalManagerBase to api|
|[#9504](https://github.com/NVIDIA/spark-rapids/pull/9504)|Add a spark-shell smoke test to premerge and nightly|
|[#9519](https://github.com/NVIDIA/spark-rapids/pull/9519)|Ignore failing Parquet filter test to unblock CI|
|[#9478](https://github.com/NVIDIA/spark-rapids/pull/9478)|Support AppendDataExecV1 for Delta Lake tables|
|[#9366](https://github.com/NVIDIA/spark-rapids/pull/9366)|Add tests to check compatibility with `fastparquet`|
|[#9419](https://github.com/NVIDIA/spark-rapids/pull/9419)|Add retry to RoundRobin Partitioner and Range Partitioner|
|[#9502](https://github.com/NVIDIA/spark-rapids/pull/9502)|Install Dependencies Needed For Databricks 13.3|
|[#9296](https://github.com/NVIDIA/spark-rapids/pull/9296)|Implement `percentile` aggregation|
|[#9488](https://github.com/NVIDIA/spark-rapids/pull/9488)|Add Shim JSON Headers for Databricks 13.3|
|[#9443](https://github.com/NVIDIA/spark-rapids/pull/9443)|Add AtomicReplaceTableAsSelectExec support for Delta Lake|
|[#9476](https://github.com/NVIDIA/spark-rapids/pull/9476)|Refactor common Delta Lake test code|
|[#9463](https://github.com/NVIDIA/spark-rapids/pull/9463)|Fix Cloudera 3.3.2 shim for handling CheckOverflowInTableInsert and orc zstd support|
|[#9460](https://github.com/NVIDIA/spark-rapids/pull/9460)|Update links in old release notes to new doc locations [skip ci]|
|[#9405](https://github.com/NVIDIA/spark-rapids/pull/9405)|Wrap scalar generation into spark session in integration test|
|[#9459](https://github.com/NVIDIA/spark-rapids/pull/9459)|Fix 332cdh build [skip ci]|
|[#9425](https://github.com/NVIDIA/spark-rapids/pull/9425)|Add support for AtomicCreateTableAsSelect with Delta Lake|
|[#9434](https://github.com/NVIDIA/spark-rapids/pull/9434)|Add retry support to `HostToGpuCoalesceIterator.concatAllAndPutOnGPU`|
|[#9453](https://github.com/NVIDIA/spark-rapids/pull/9453)|Update codeowner and blossom-ci ACL [skip ci]|
|[#9396](https://github.com/NVIDIA/spark-rapids/pull/9396)|Add support for Cloudera CDS-3.3.2|
|[#9380](https://github.com/NVIDIA/spark-rapids/pull/9380)|Fix parsing of Parquet legacy list-of-struct format|
|[#9438](https://github.com/NVIDIA/spark-rapids/pull/9438)|Fix auto merge conflict 9437 [skip ci]|
|[#9424](https://github.com/NVIDIA/spark-rapids/pull/9424)|Refactor aggregate functions|
|[#9414](https://github.com/NVIDIA/spark-rapids/pull/9414)|Add retry to GpuHashJoin.filterNulls|
|[#9388](https://github.com/NVIDIA/spark-rapids/pull/9388)|Add developer documentation about working with data sources [skip ci]|
|[#9369](https://github.com/NVIDIA/spark-rapids/pull/9369)|Improve JSON empty row fix to use less memory|
|[#9373](https://github.com/NVIDIA/spark-rapids/pull/9373)|Fix auto merge conflict 9372|
|[#9308](https://github.com/NVIDIA/spark-rapids/pull/9308)|Initiate arm64 CI support [skip ci]|
|[#9292](https://github.com/NVIDIA/spark-rapids/pull/9292)|Init project version 23.12.0-SNAPSHOT|
|[#9291](https://github.com/NVIDIA/spark-rapids/pull/9291)|Automerge from 23.10 to 23.12 [skip ci]|

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
|[#9512](https://github.com/NVIDIA/spark-rapids/issues/9512)|[DOC] Multi-Threaded shuffle documentation is not accurate on the read side|
|[#7803](https://github.com/NVIDIA/spark-rapids/issues/7803)|[FEA] Accelerate Bloom filtered joins |

### Bugs Fixed
|||
|:---|:---|
|[#8662](https://github.com/NVIDIA/spark-rapids/issues/8662)|[BUG] Dataproc spark-rapids.sh fails due to cuda driver version issue|
|[#9428](https://github.com/NVIDIA/spark-rapids/issues/9428)|[Audit] SPARK-44448 Wrong results for dense_rank() <= k|
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
|[#9304](https://github.com/NVIDIA/spark-rapids/pull/9304)|Specify recoverWithNull when reading JSON files|
|[#9474](https://github.com/NVIDIA/spark-rapids/pull/9474)| Improve configuration handling in BatchWithPartitionData|
|[#9289](https://github.com/NVIDIA/spark-rapids/pull/9289)|Add tests to check compatibility with pyarrow|
|[#9522](https://github.com/NVIDIA/spark-rapids/pull/9522)|Update 23.10 changelog [skip ci]|
|[#9501](https://github.com/NVIDIA/spark-rapids/pull/9501)|Fix GpuSemaphore to support multiple threads per task|
|[#9500](https://github.com/NVIDIA/spark-rapids/pull/9500)|Fix Spark 3.5.0 shell classloader issue with the plugin|
|[#9230](https://github.com/NVIDIA/spark-rapids/pull/9230)|Fix reading partition value columns larger than cudf column size limit|
|[#9427](https://github.com/NVIDIA/spark-rapids/pull/9427)|[DOC] Update docs for 23.10.0 release [skip ci]|
|[#9421](https://github.com/NVIDIA/spark-rapids/pull/9421)|Init changelog of 23.10 [skip ci]|
|[#9445](https://github.com/NVIDIA/spark-rapids/pull/9445)|Only run test_csv_infer_schema_timestamp_ntz tests with PySpark >= 3.4.1|
|[#9420](https://github.com/NVIDIA/spark-rapids/pull/9420)|Update private and jni dep version to released 23.10.0|
|[#9415](https://github.com/NVIDIA/spark-rapids/pull/9415)|[BUG] fix docker modified check in premerge [skip ci]|
|[#9407](https://github.com/NVIDIA/spark-rapids/pull/9407)|[Doc]Update docs for 23.08.2 version[skip ci]|
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
|[#8476](https://github.com/NVIDIA/spark-rapids/pull/8476)|Use retry with split in GpuCachedDoublePassWindowIterator|
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

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
