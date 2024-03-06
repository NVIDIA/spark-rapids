# Change log
Generated on 2024-03-06

## Release 24.02

### Features
|||
|:---|:---|
|[#9926](https://github.com/NVIDIA/spark-rapids/issues/9926)|[FEA] Add config option for the parquet reader input read limit.|
|[#10270](https://github.com/NVIDIA/spark-rapids/issues/10270)|[FEA] Add support for single quotes when reading JSON|
|[#10253](https://github.com/NVIDIA/spark-rapids/issues/10253)|[FEA] Enable mixed types as string in GpuJsonToStruct|
|[#9692](https://github.com/NVIDIA/spark-rapids/issues/9692)|[FEA] Remove Pascal support|
|[#8806](https://github.com/NVIDIA/spark-rapids/issues/8806)|[FEA] Support lazy quantifier and specified group index in regexp_extract function|
|[#10079](https://github.com/NVIDIA/spark-rapids/issues/10079)|[FEA] Add string parameter support for `unix_timestamp` for non-UTC time zones|
|[#9667](https://github.com/NVIDIA/spark-rapids/issues/9667)|[FEA][JSON] Add support for non default `dateFormat` in `from_json`|
|[#9173](https://github.com/NVIDIA/spark-rapids/issues/9173)|[FEA] Support format_number |
|[#10145](https://github.com/NVIDIA/spark-rapids/issues/10145)|[FEA] Support to_utc_timestamp|
|[#9927](https://github.com/NVIDIA/spark-rapids/issues/9927)|[FEA] Support to_date with non-UTC timezones without DST|
|[#10006](https://github.com/NVIDIA/spark-rapids/issues/10006)|[FEA] Support ```ParseToTimestamp``` for non-UTC time zones|
|[#9096](https://github.com/NVIDIA/spark-rapids/issues/9096)|[FEA] Add Spark 3.3.4 support|
|[#9585](https://github.com/NVIDIA/spark-rapids/issues/9585)|[FEA] support ascii function|
|[#9260](https://github.com/NVIDIA/spark-rapids/issues/9260)|[FEA] Create Spark 3.4.2 shim and build env|
|[#10076](https://github.com/NVIDIA/spark-rapids/issues/10076)|[FEA] Add performance test framework for non-UTC time zone features.|
|[#9881](https://github.com/NVIDIA/spark-rapids/issues/9881)|[TASK] Remove `spark.rapids.sql.nonUTC.enabled` configuration option|
|[#9801](https://github.com/NVIDIA/spark-rapids/issues/9801)|[FEA] Support DateFormat on GPU with a non-UTC timezone|
|[#6834](https://github.com/NVIDIA/spark-rapids/issues/6834)|[FEA] Support GpuHour expression for timezones other than UTC|
|[#6842](https://github.com/NVIDIA/spark-rapids/issues/6842)|[FEA] Support TimeZone aware operations for value extraction|
|[#1860](https://github.com/NVIDIA/spark-rapids/issues/1860)|[FEA] Optimize row based window operations for BOUNDED ranges|
|[#9606](https://github.com/NVIDIA/spark-rapids/issues/9606)|[FEA] Support unix_timestamp with CST(China Time Zone) support|
|[#9815](https://github.com/NVIDIA/spark-rapids/issues/9815)|[FEA] Support ```unix_timestamp``` for non-DST timezones|
|[#8807](https://github.com/NVIDIA/spark-rapids/issues/8807)|[FEA] support ‘yyyyMMdd’ format in from_unixtime function|
|[#9605](https://github.com/NVIDIA/spark-rapids/issues/9605)|[FEA] Support from_unixtime with CST(China Time Zone) support|
|[#6836](https://github.com/NVIDIA/spark-rapids/issues/6836)|[FEA] Support FromUnixTime for non UTC timezones|
|[#9175](https://github.com/NVIDIA/spark-rapids/issues/9175)|[FEA] Support Databricks 13.3|
|[#6881](https://github.com/NVIDIA/spark-rapids/issues/6881)|[FEA] Support RAPIDS Spark plugin on ARM|
|[#9274](https://github.com/NVIDIA/spark-rapids/issues/9274)|[FEA] Regular deploy process to include arm artifacts|
|[#9844](https://github.com/NVIDIA/spark-rapids/issues/9844)|[FEA] Let Gpu arrow python runners support writing one batch one time for the single threaded model.|
|[#7309](https://github.com/NVIDIA/spark-rapids/issues/7309)|[FEA] Detect multiple versions of the RAPIDS jar on the classpath at the same time|

### Performance
|||
|:---|:---|
|[#9442](https://github.com/NVIDIA/spark-rapids/issues/9442)|[FEA] For hash joins where the build side can change use the smaller table for the build side|
|[#10142](https://github.com/NVIDIA/spark-rapids/issues/10142)|[TASK] Benchmark existing timestamp functions that work in non-UTC time zone (non-DST)|

### Bugs Fixed
|||
|:---|:---|
|[#10548](https://github.com/NVIDIA/spark-rapids/issues/10548)|[BUG] test_dpp_bypass / test_dpp_via_aggregate_subquery failures in CI Databricks 13.3|
|[#10530](https://github.com/NVIDIA/spark-rapids/issues/10530)|test_delta_merge_match_delete_only java.lang.OutOfMemoryError: GC overhead limit exceeded|
|[#10464](https://github.com/NVIDIA/spark-rapids/issues/10464)|[BUG] spark334 and spark342 shims missed in scala2.13 dist jar|
|[#10473](https://github.com/NVIDIA/spark-rapids/issues/10473)|[BUG] Leak when running RANK query|
|[#10432](https://github.com/NVIDIA/spark-rapids/issues/10432)|Plug-in Build Failing for Databricks 11.3 |
|[#9974](https://github.com/NVIDIA/spark-rapids/issues/9974)|[BUG] host memory Leak in MultiFileCoalescingPartitionReaderBase in UTC time zone|
|[#10359](https://github.com/NVIDIA/spark-rapids/issues/10359)|[BUG] Build failure on Databricks nightly run with `GpuMapInPandasExecMeta`|
|[#10327](https://github.com/NVIDIA/spark-rapids/issues/10327)|[BUG] Unit test FAILED against : SPARK-24957: average with decimal followed by aggregation returning wrong result |
|[#10324](https://github.com/NVIDIA/spark-rapids/issues/10324)|[BUG] hash_aggregate_test.py test FAILED:  Type conversion is not allowed from Table {...}|
|[#10291](https://github.com/NVIDIA/spark-rapids/issues/10291)|[BUG] SIGSEGV in libucp.so|
|[#9212](https://github.com/NVIDIA/spark-rapids/issues/9212)|[BUG] `from_json` fails with cuDF error `Invalid list size computation error`|
|[#10264](https://github.com/NVIDIA/spark-rapids/issues/10264)|[BUG] hash aggregate test failures due to type conversion errors|
|[#10262](https://github.com/NVIDIA/spark-rapids/issues/10262)|[BUG] Test "SPARK-24957: average with decimal followed by aggregation returning wrong result" failed.|
|[#9353](https://github.com/NVIDIA/spark-rapids/issues/9353)|[BUG] [JSON] A mix of lists and structs within the same column is not supported|
|[#10099](https://github.com/NVIDIA/spark-rapids/issues/10099)|[BUG] orc_test.py::test_orc_scan_with_aggregate_pushdown fails with a standalone cluster on spark 3.3.0|
|[#10047](https://github.com/NVIDIA/spark-rapids/issues/10047)|[BUG] CudfException during conditional hash join while running nds query64|
|[#9779](https://github.com/NVIDIA/spark-rapids/issues/9779)|[BUG] 330cdh failed test_hash_reduction_sum_full_decimal on CI|
|[#10197](https://github.com/NVIDIA/spark-rapids/issues/10197)|[BUG] Disable GetJsonObject by default and update docs|
|[#10165](https://github.com/NVIDIA/spark-rapids/issues/10165)|[BUG] Databricks 13.3 executor side broadcast failure|
|[#10224](https://github.com/NVIDIA/spark-rapids/issues/10224)|[BUG] DBR builds fails when installing Maven|
|[#10222](https://github.com/NVIDIA/spark-rapids/issues/10222)|[BUG] to_utc_timestamp and from_utc_timestamp fallback when TZ is supported time zone|
|[#10195](https://github.com/NVIDIA/spark-rapids/issues/10195)|[BUG] test_window_aggs_for_negative_rows_partitioned failure in CI|
|[#10182](https://github.com/NVIDIA/spark-rapids/issues/10182)|[BUG] test_dpp_bypass / test_dpp_via_aggregate_subquery failures in CI (databricks)|
|[#10169](https://github.com/NVIDIA/spark-rapids/issues/10169)|[BUG] Host column vector leaks when running `test_cast_timestamp_to_date`|
|[#10050](https://github.com/NVIDIA/spark-rapids/issues/10050)|[BUG] test_cast_decimal_to_decimal[to:DecimalType(1,-1)-from:Decimal(5,-3)] fails with DATAGEN_SEED=1702439569|
|[#10088](https://github.com/NVIDIA/spark-rapids/issues/10088)|[BUG] GpuExplode single row split to fit cuDF limits|
|[#10174](https://github.com/NVIDIA/spark-rapids/issues/10174)|[BUG]  json_test.py::test_from_json_struct_timestamp failed on: Part of the plan is not columnar |
|[#10186](https://github.com/NVIDIA/spark-rapids/issues/10186)|[BUG] test_to_date_with_window_functions failed in non-UTC nightly CI|
|[#10154](https://github.com/NVIDIA/spark-rapids/issues/10154)|[BUG] 'spark-test.sh' integration tests FAILED on 'ps: command not found" in Rocky Docker environment|
|[#10175](https://github.com/NVIDIA/spark-rapids/issues/10175)|[BUG] string_test.py::test_format_number_float_special FAILED : AssertionError 'NaN' == |
|[#10166](https://github.com/NVIDIA/spark-rapids/issues/10166)|Detect Undeclared Shim in POM.xml|
|[#10170](https://github.com/NVIDIA/spark-rapids/issues/10170)|[BUG] `test_cast_timestamp_to_date` fails with `TZ=Asia/Hebron`|
|[#10149](https://github.com/NVIDIA/spark-rapids/issues/10149)|[BUG] GPU illegal access detected during delta_byte_array.parquet read|
|[#9905](https://github.com/NVIDIA/spark-rapids/issues/9905)|[BUG] GpuJsonScan incorrect behavior when parsing dates|
|[#10163](https://github.com/NVIDIA/spark-rapids/issues/10163)|Spark 3.3.4 Shim Build Failure|
|[#10105](https://github.com/NVIDIA/spark-rapids/issues/10105)|[BUG] scala:compile is not thread safe unless compiler bridge already exists |
|[#10026](https://github.com/NVIDIA/spark-rapids/issues/10026)|[BUG] test_hash_agg_with_nan_keys failed with a DATAGEN_SEED=1702335559|
|[#10075](https://github.com/NVIDIA/spark-rapids/issues/10075)|[BUG] `non-pinned blocking alloc with spill` unit test failed in HostAllocSuite|
|[#10134](https://github.com/NVIDIA/spark-rapids/issues/10134)|[BUG] test_window_aggs_for_batched_finite_row_windows_partitioned failed on Scala 2.13 with DATAGEN_SEED=1704033145|
|[#10118](https://github.com/NVIDIA/spark-rapids/issues/10118)|[BUG] non-UTC Nightly CI failed|
|[#10136](https://github.com/NVIDIA/spark-rapids/issues/10136)|[BUG] The canonicalized version of `GpuFileSourceScanExec`s that suppose to be semantic-equal can be different |
|[#10110](https://github.com/NVIDIA/spark-rapids/issues/10110)|[BUG] disable collect_list and collect_set for window operations by default.|
|[#10129](https://github.com/NVIDIA/spark-rapids/issues/10129)|[BUG] Unit test suite fails with `Null data pointer` in GpuTimeZoneDB|
|[#10089](https://github.com/NVIDIA/spark-rapids/issues/10089)|[BUG] DATAGEN_SEED=<seed> environment does not override the marker datagen_overrides|
|[#10108](https://github.com/NVIDIA/spark-rapids/issues/10108)|[BUG] @datagen_overrides seed is sticky when it shouldn't be|
|[#10064](https://github.com/NVIDIA/spark-rapids/issues/10064)|[BUG] test_unsupported_fallback_regexp_replace failed with DATAGEN_SEED=1702662063|
|[#10117](https://github.com/NVIDIA/spark-rapids/issues/10117)|[BUG] test_from_utc_timestamp failed on Cloudera Env when TZ is Iran|
|[#9914](https://github.com/NVIDIA/spark-rapids/issues/9914)|[BUG] Report GPU OOM on recent passed CI premerges.|
|[#10094](https://github.com/NVIDIA/spark-rapids/issues/10094)|[BUG] spark351 PR check failure MockTaskContext method isFailed in class TaskContext of type ()Boolean is not defined|
|[#10017](https://github.com/NVIDIA/spark-rapids/issues/10017)|[BUG] test_casting_from_double_to_timestamp failed for DATAGEN_SEED=1702329497|
|[#9992](https://github.com/NVIDIA/spark-rapids/issues/9992)|[BUG] conditionals_test.py::test_conditional_with_side_effects_cast[String] failed with DATAGEN_SEED=1701976979|
|[#9743](https://github.com/NVIDIA/spark-rapids/issues/9743)|[BUG][AUDIT] SPARK-45652 - SPJ: Handle empty input partitions after dynamic filtering|
|[#9859](https://github.com/NVIDIA/spark-rapids/issues/9859)|[AUDIT] [SPARK-45786] Inaccurate Decimal multiplication and division results|
|[#9555](https://github.com/NVIDIA/spark-rapids/issues/9555)|[BUG] Scala 2.13 build with JDK 11 or 17 fails OpcodeSuite tests|
|[#10073](https://github.com/NVIDIA/spark-rapids/issues/10073)|[BUG] test_csv_prefer_date_with_infer_schema failed with DATAGEN_SEED=1702847907|
|[#10004](https://github.com/NVIDIA/spark-rapids/issues/10004)|[BUG] If a host memory buffer is spilled, it cannot be unspilled|
|[#10063](https://github.com/NVIDIA/spark-rapids/issues/10063)|[BUG] CI build failure with 341db: method getKillReason has weaker access privileges; it should be public|
|[#10055](https://github.com/NVIDIA/spark-rapids/issues/10055)|[BUG]  array_test.py::test_array_transform_non_deterministic failed with non-UTC time zone|
|[#10056](https://github.com/NVIDIA/spark-rapids/issues/10056)|[BUG] Unit tests ToPrettyStringSuite FAILED on spark-3.5.0|
|[#10048](https://github.com/NVIDIA/spark-rapids/issues/10048)|[BUG] Fix ```out of range``` error from ```pySpark``` in ```test_timestamp_millis``` and other two integration test cases|
|[#4204](https://github.com/NVIDIA/spark-rapids/issues/4204)|casting double to string does not match Spark|
|[#9938](https://github.com/NVIDIA/spark-rapids/issues/9938)|Better to do some refactor for the Python UDF code|
|[#10018](https://github.com/NVIDIA/spark-rapids/issues/10018)|[BUG] `GpuToUnixTimestampImproved` off by 1 on GPU when handling timestamp before epoch|
|[#10012](https://github.com/NVIDIA/spark-rapids/issues/10012)|[BUG] test_str_to_map_expr_random_delimiters with DATAGEN_SEED=1702166057 hangs|
|[#10029](https://github.com/NVIDIA/spark-rapids/issues/10029)|[BUG] doc links fail with 404 for shims.md|
|[#9472](https://github.com/NVIDIA/spark-rapids/issues/9472)|[BUG] Non-Deterministic expressions in an array_transform can cause errors|
|[#9884](https://github.com/NVIDIA/spark-rapids/issues/9884)|[BUG] delta_lake_delete_test.py failed assertion [DATAGEN_SEED=1701225104, IGNORE_ORDER...|
|[#9977](https://github.com/NVIDIA/spark-rapids/issues/9977)|[BUG] test_cast_date_integral fails on databricks 3.4.1|
|[#9936](https://github.com/NVIDIA/spark-rapids/issues/9936)|[BUG] Nightly CI of non-UTC time zone reports 'year 0 is out of range' error|
|[#9941](https://github.com/NVIDIA/spark-rapids/issues/9941)|[BUG] A potential data corruption in Pandas UDFs|
|[#9897](https://github.com/NVIDIA/spark-rapids/issues/9897)|[BUG] Error message for multiple jars on classpath is wrong|
|[#9916](https://github.com/NVIDIA/spark-rapids/issues/9916)|[BUG] ```test_cast_string_ts_valid_format``` failed at ```seed = 1701362564```|
|[#9559](https://github.com/NVIDIA/spark-rapids/issues/9559)|[BUG] precommit regularly fails with error trying to download a dependency|
|[#9708](https://github.com/NVIDIA/spark-rapids/issues/9708)|[BUG] test_cast_string_ts_valid_format fails with DATAGEN_SEED=1699978422|

### PRs
|||
|:---|:---|
|[#10551](https://github.com/NVIDIA/spark-rapids/pull/10551)|Try to make degenerative joins here impossible for these tests|
|[#10546](https://github.com/NVIDIA/spark-rapids/pull/10546)|Update changelog [skip ci]|
|[#10541](https://github.com/NVIDIA/spark-rapids/pull/10541)|Fix Delta log cache size settings during integration tests|
|[#10525](https://github.com/NVIDIA/spark-rapids/pull/10525)|Update changelog for v24.02.0 release [skip ci]|
|[#10465](https://github.com/NVIDIA/spark-rapids/pull/10465)|Add missed shims for scala2.13|
|[#10511](https://github.com/NVIDIA/spark-rapids/pull/10511)|Update rapids jni and private dependency version to 24.02.1|
|[#10513](https://github.com/NVIDIA/spark-rapids/pull/10513)|Fix scalar leak in SumBinaryFixer (#10510)|
|[#10475](https://github.com/NVIDIA/spark-rapids/pull/10475)|Fix scalar leak in RankFixer|
|[#10461](https://github.com/NVIDIA/spark-rapids/pull/10461)|Preserve tags on FileSourceScanExec|
|[#10459](https://github.com/NVIDIA/spark-rapids/pull/10459)|[DOC] Fix table rendering issue in github.io download UI page on branch-24.02 [skip ci] |
|[#10443](https://github.com/NVIDIA/spark-rapids/pull/10443)|Update change log for v24.02.0 release [skip ci]|
|[#10439](https://github.com/NVIDIA/spark-rapids/pull/10439)|Reverts NVIDIA/spark-rapids#10232 and fixes the plugin build on Databricks 11.3|
|[#10380](https://github.com/NVIDIA/spark-rapids/pull/10380)|Init changelog 24.02 [skip ci]|
|[#10367](https://github.com/NVIDIA/spark-rapids/pull/10367)|Update rapids JNI and private version to release 24.02.0|
|[#10414](https://github.com/NVIDIA/spark-rapids/pull/10414)|[DOC] Fix 24.02.0 documentation errors [skip ci]|
|[#10403](https://github.com/NVIDIA/spark-rapids/pull/10403)|Cherry-pick: Fix a memory leak in json tuple (#10360)|
|[#10387](https://github.com/NVIDIA/spark-rapids/pull/10387)|[DOC] Update docs for 24.02.0 release [skip ci]|
|[#10399](https://github.com/NVIDIA/spark-rapids/pull/10399)|Update NOTICE-binary|
|[#10389](https://github.com/NVIDIA/spark-rapids/pull/10389)|Change version and branch to 24.02 in docs [skip ci]|
|[#10309](https://github.com/NVIDIA/spark-rapids/pull/10309)|[DOC] add custom 404 page and fix some document issue [skip ci]|
|[#10352](https://github.com/NVIDIA/spark-rapids/pull/10352)|xfail mixed type test|
|[#10355](https://github.com/NVIDIA/spark-rapids/pull/10355)|Revert "Support barrier mode for mapInPandas/mapInArrow (#10343)"|
|[#10353](https://github.com/NVIDIA/spark-rapids/pull/10353)|Use fixed seed for test_from_json_struct_decimal|
|[#10343](https://github.com/NVIDIA/spark-rapids/pull/10343)|Support barrier mode for mapInPandas/mapInArrow|
|[#10345](https://github.com/NVIDIA/spark-rapids/pull/10345)|Fix auto merge conflict 10339 [skip ci]|
|[#9991](https://github.com/NVIDIA/spark-rapids/pull/9991)|Start to use explicit memory limits in the parquet chunked reader|
|[#10328](https://github.com/NVIDIA/spark-rapids/pull/10328)|Fix typo in spark-tests.sh [skip ci]|
|[#10279](https://github.com/NVIDIA/spark-rapids/pull/10279)|Run '--packages' only with default cuda11 jar|
|[#10273](https://github.com/NVIDIA/spark-rapids/pull/10273)|Support reading JSON data with single quotes around attribute names and values|
|[#10306](https://github.com/NVIDIA/spark-rapids/pull/10306)|Fix performance regression in from_json|
|[#10272](https://github.com/NVIDIA/spark-rapids/pull/10272)|Add FullOuter support to GpuShuffledSymmetricHashJoinExec|
|[#10260](https://github.com/NVIDIA/spark-rapids/pull/10260)|Add perf test for time zone operators|
|[#10275](https://github.com/NVIDIA/spark-rapids/pull/10275)|Add tests for window Python udf with array input|
|[#10278](https://github.com/NVIDIA/spark-rapids/pull/10278)|Clean up $M2_CACHE to avoid side-effect of previous dependency:get [skip ci]|
|[#10268](https://github.com/NVIDIA/spark-rapids/pull/10268)|Add config to enable mixed types as string in GpuJsonToStruct & GpuJsonScan|
|[#10297](https://github.com/NVIDIA/spark-rapids/pull/10297)|Revert "UCX 1.16.0 upgrade (#10190)"|
|[#10289](https://github.com/NVIDIA/spark-rapids/pull/10289)|Add gerashegalov to CODEOWNERS [skip ci]|
|[#10290](https://github.com/NVIDIA/spark-rapids/pull/10290)|Fix merge conflict with 23.12 [skip ci]|
|[#10190](https://github.com/NVIDIA/spark-rapids/pull/10190)|UCX 1.16.0 upgrade|
|[#10211](https://github.com/NVIDIA/spark-rapids/pull/10211)|Use parse_url kernel for QUERY literal and column key|
|[#10267](https://github.com/NVIDIA/spark-rapids/pull/10267)|Update to libcudf unsigned sum aggregation types change|
|[#10208](https://github.com/NVIDIA/spark-rapids/pull/10208)|Added Support for Lazy Quantifier|
|[#9993](https://github.com/NVIDIA/spark-rapids/pull/9993)|Enable mixed types as string in GpuJsonScan|
|[#10246](https://github.com/NVIDIA/spark-rapids/pull/10246)|Refactor full join iterator to allow access to build tracker|
|[#10257](https://github.com/NVIDIA/spark-rapids/pull/10257)|Enable auto-merge from branch-24.02 to branch-24.04 [skip CI]|
|[#10178](https://github.com/NVIDIA/spark-rapids/pull/10178)|Mark hash reduction decimal overflow test as a permanent seed override|
|[#10244](https://github.com/NVIDIA/spark-rapids/pull/10244)|Use POSIX mode in assembly plugin to avoid issues with large UID/GID|
|[#10238](https://github.com/NVIDIA/spark-rapids/pull/10238)|Smoke test with '--package' to fetch the plugin jar|
|[#10201](https://github.com/NVIDIA/spark-rapids/pull/10201)|Deploy release candidates to local maven repo for dependency check[skip ci]|
|[#10240](https://github.com/NVIDIA/spark-rapids/pull/10240)|Improved inner joins with large build side|
|[#10220](https://github.com/NVIDIA/spark-rapids/pull/10220)|Disable GetJsonObject by default and add tests for as many issues with it as possible|
|[#10230](https://github.com/NVIDIA/spark-rapids/pull/10230)|Fix Databricks 13.3 BroadcastHashJoin using executor side broadcast fed by ColumnarToRow [Databricks]|
|[#10232](https://github.com/NVIDIA/spark-rapids/pull/10232)|Fixed 330db Shims to Adopt the PythonRunner Changes|
|[#10225](https://github.com/NVIDIA/spark-rapids/pull/10225)|Download Maven from apache.org archives [skip ci]|
|[#10210](https://github.com/NVIDIA/spark-rapids/pull/10210)|Add string parameter support for unix_timestamp for non-UTC time zones|
|[#10223](https://github.com/NVIDIA/spark-rapids/pull/10223)|Fix to_utc_timestamp and from_utc_timestamp fallback when TZ is supported time zone|
|[#10205](https://github.com/NVIDIA/spark-rapids/pull/10205)|Deterministic ordering in window tests|
|[#10204](https://github.com/NVIDIA/spark-rapids/pull/10204)|Further prevent degenerative joins in dpp_test|
|[#10156](https://github.com/NVIDIA/spark-rapids/pull/10156)|Update string to float compatibility doc[skip ci]|
|[#10193](https://github.com/NVIDIA/spark-rapids/pull/10193)|Fix explode with carry-along columns on GpuExplode single row retry handling|
|[#10191](https://github.com/NVIDIA/spark-rapids/pull/10191)|Updating the config documentation for filecache configs [skip ci]|
|[#10131](https://github.com/NVIDIA/spark-rapids/pull/10131)|With a single row GpuExplode tries to split the generator array|
|[#10179](https://github.com/NVIDIA/spark-rapids/pull/10179)|Fix build regression against Spark 3.2.x|
|[#10189](https://github.com/NVIDIA/spark-rapids/pull/10189)|test needs marks for non-UTC and for non_supported timezones|
|[#10176](https://github.com/NVIDIA/spark-rapids/pull/10176)|Fix format_number NaN symbol in high jdk version|
|[#10074](https://github.com/NVIDIA/spark-rapids/pull/10074)|Update the legacy mode check: only take effect when reading date/timestamp column|
|[#10167](https://github.com/NVIDIA/spark-rapids/pull/10167)|Defined Shims Should Be Declared In POM |
|[#10168](https://github.com/NVIDIA/spark-rapids/pull/10168)|Prevent a degenerative join in test_dpp_reuse_broadcast_exchange|
|[#10171](https://github.com/NVIDIA/spark-rapids/pull/10171)|Fix `test_cast_timestamp_to_date` when running in a DST time zone|
|[#9975](https://github.com/NVIDIA/spark-rapids/pull/9975)|Improve dateFormat support in GpuJsonScan and make tests consistent with GpuStructsToJson|
|[#9790](https://github.com/NVIDIA/spark-rapids/pull/9790)|Support float case of format_number with format_float kernel|
|[#10144](https://github.com/NVIDIA/spark-rapids/pull/10144)|Support to_utc_timestamp|
|[#10162](https://github.com/NVIDIA/spark-rapids/pull/10162)|Fix Spark 334 Build|
|[#10146](https://github.com/NVIDIA/spark-rapids/pull/10146)|Refactor the window code so it is not mostly kept in a few very large files|
|[#10155](https://github.com/NVIDIA/spark-rapids/pull/10155)|Install procps tools for rocky docker images [skip ci]|
|[#10153](https://github.com/NVIDIA/spark-rapids/pull/10153)|Disable multi-threaded Maven |
|[#10100](https://github.com/NVIDIA/spark-rapids/pull/10100)|Enable to_date (via gettimestamp and casting timestamp to date) for non-UTC time zones|
|[#10140](https://github.com/NVIDIA/spark-rapids/pull/10140)|Removed Unnecessary Whitespaces From Spark 3.3.4 Shim [skip ci]|
|[#10148](https://github.com/NVIDIA/spark-rapids/pull/10148)|fix test_hash_agg_with_nan_keys floating point sum failure|
|[#10150](https://github.com/NVIDIA/spark-rapids/pull/10150)|Increase timeouts in HostAllocSuite to avoid timeout failures on slow machines|
|[#10143](https://github.com/NVIDIA/spark-rapids/pull/10143)|Fix `test_window_aggs_for_batched_finite_row_windows_partitioned` fail|
|[#9887](https://github.com/NVIDIA/spark-rapids/pull/9887)|Reduce time-consuming of pre-merge|
|[#10130](https://github.com/NVIDIA/spark-rapids/pull/10130)|Change unit tests that force ooms to specify the oom type (gpu|cpu)|
|[#10138](https://github.com/NVIDIA/spark-rapids/pull/10138)|Update copyright dates in NOTICE files [skip ci]|
|[#10139](https://github.com/NVIDIA/spark-rapids/pull/10139)|Add Delta Lake 2.3.0 to list of versions to test for Spark 3.3.x|
|[#10135](https://github.com/NVIDIA/spark-rapids/pull/10135)|Fix CI: can't find script when there is pushd in script [skip ci]|
|[#10137](https://github.com/NVIDIA/spark-rapids/pull/10137)|Fix the canonicalizing for GPU file scan|
|[#10132](https://github.com/NVIDIA/spark-rapids/pull/10132)|Disable collect_list and collect_set for window by default|
|[#10084](https://github.com/NVIDIA/spark-rapids/pull/10084)|Refactor GpuJsonToStruct to reduce code duplication and manage resources more efficiently|
|[#10087](https://github.com/NVIDIA/spark-rapids/pull/10087)|Additional unit tests for GeneratedInternalRowToCudfRowIterator|
|[#10082](https://github.com/NVIDIA/spark-rapids/pull/10082)|Add Spark 3.3.4 Shim|
|[#10054](https://github.com/NVIDIA/spark-rapids/pull/10054)|Support Ascii function for ascii and latin-1|
|[#10127](https://github.com/NVIDIA/spark-rapids/pull/10127)|Fix merge conflict with branch-23.12|
|[#10097](https://github.com/NVIDIA/spark-rapids/pull/10097)|[DOC] Update docs for 23.12.1 release [skip ci]|
|[#10109](https://github.com/NVIDIA/spark-rapids/pull/10109)|Fixes a bug where datagen seed overrides were sticky and adds datagen_seed_override_disabled|
|[#10093](https://github.com/NVIDIA/spark-rapids/pull/10093)|Fix test_unsupported_fallback_regexp_replace|
|[#10119](https://github.com/NVIDIA/spark-rapids/pull/10119)|Fix from_utc_timestamp case failure on Cloudera when TZ is Iran|
|[#10106](https://github.com/NVIDIA/spark-rapids/pull/10106)|Add `isFailed()` to MockTaskContext and Remove MockTaskContextBase.scala|
|[#10112](https://github.com/NVIDIA/spark-rapids/pull/10112)|Remove datagen seed override for test_conditional_with_side_effects_cast|
|[#10104](https://github.com/NVIDIA/spark-rapids/pull/10104)|[DOC] Add in docs about memory debugging [skip ci]|
|[#9925](https://github.com/NVIDIA/spark-rapids/pull/9925)|Use threads, cache Scala compiler in GH mvn workflow|
|[#9967](https://github.com/NVIDIA/spark-rapids/pull/9967)|Added Spark-3.4.2 Shims|
|[#10061](https://github.com/NVIDIA/spark-rapids/pull/10061)|Use parse_url kernel for QUERY parsing|
|[#10101](https://github.com/NVIDIA/spark-rapids/pull/10101)|[DOC] Add column order error docs [skip ci]|
|[#10078](https://github.com/NVIDIA/spark-rapids/pull/10078)|Add perf test for non-UTC operators|
|[#10096](https://github.com/NVIDIA/spark-rapids/pull/10096)|Shim MockTaskContext to fix Spark 3.5.1 build|
|[#10092](https://github.com/NVIDIA/spark-rapids/pull/10092)|Implement Math.round using floor on GPU|
|[#10085](https://github.com/NVIDIA/spark-rapids/pull/10085)|Update tests that originally restricted the Spark timestamp range|
|[#10090](https://github.com/NVIDIA/spark-rapids/pull/10090)|Replace GPU-unsupported `\z` with an alternative RLIKE expression|
|[#10095](https://github.com/NVIDIA/spark-rapids/pull/10095)|Temporarily fix date format failed cases for non-UTC time zone.|
|[#9999](https://github.com/NVIDIA/spark-rapids/pull/9999)|Add some odd time zones for timezone transition tests|
|[#9962](https://github.com/NVIDIA/spark-rapids/pull/9962)|Add 3.5.1-SNAPSHOT Shim|
|[#10071](https://github.com/NVIDIA/spark-rapids/pull/10071)|Cleanup usage of non-utc configuration here|
|[#10057](https://github.com/NVIDIA/spark-rapids/pull/10057)|Add support for StringConcatFactory.makeConcatWithConstants (#9555)|
|[#9996](https://github.com/NVIDIA/spark-rapids/pull/9996)|Test full timestamp output range in PySpark|
|[#10081](https://github.com/NVIDIA/spark-rapids/pull/10081)|Add a fallback Cloudera Maven repo URL [skip ci]|
|[#10065](https://github.com/NVIDIA/spark-rapids/pull/10065)|Improve host memory spill interfaces|
|[#10070](https://github.com/NVIDIA/spark-rapids/pull/10070)|Fix 332db build failure|
|[#10060](https://github.com/NVIDIA/spark-rapids/pull/10060)|Fix failed cases for non-utc time zone|
|[#10038](https://github.com/NVIDIA/spark-rapids/pull/10038)|Remove spark.rapids.sql.nonUTC.enabled configuration option|
|[#10059](https://github.com/NVIDIA/spark-rapids/pull/10059)|Fixed Failing ToPrettyStringSuite Test for 3.5.0|
|[#10013](https://github.com/NVIDIA/spark-rapids/pull/10013)|Extended configuration of OOM injection mode|
|[#10052](https://github.com/NVIDIA/spark-rapids/pull/10052)|Set seed=0 for some integration test cases|
|[#10053](https://github.com/NVIDIA/spark-rapids/pull/10053)|Remove invalid user from CODEOWNER file [skip ci]|
|[#10049](https://github.com/NVIDIA/spark-rapids/pull/10049)|Fix out of range error from pySpark in test_timestamp_millis and other two integration test cases|
|[#9721](https://github.com/NVIDIA/spark-rapids/pull/9721)|Support date_format via Gpu for non-UTC time zone|
|[#9845](https://github.com/NVIDIA/spark-rapids/pull/9845)|Use parse_url kernel for HOST parsing|
|[#10024](https://github.com/NVIDIA/spark-rapids/pull/10024)|Support hour minute second for non-UTC time zone|
|[#9973](https://github.com/NVIDIA/spark-rapids/pull/9973)|Batching support for row-based bounded window functions |
|[#10042](https://github.com/NVIDIA/spark-rapids/pull/10042)|Update tests to not have hard coded fallback when not needed|
|[#9816](https://github.com/NVIDIA/spark-rapids/pull/9816)|Support unix_timestamp and to_unix_timestamp with non-UTC timezones (non-DST)|
|[#9902](https://github.com/NVIDIA/spark-rapids/pull/9902)|Some refactor for the Python UDF code|
|[#10023](https://github.com/NVIDIA/spark-rapids/pull/10023)|GPU supports `yyyyMMdd` format by post process for the `from_unixtime` function|
|[#10033](https://github.com/NVIDIA/spark-rapids/pull/10033)|Remove GpuToTimestampImproved and spark.rapids.sql.improvedTimeOps.enabled|
|[#10016](https://github.com/NVIDIA/spark-rapids/pull/10016)|Fix infinite loop in test_str_to_map_expr_random_delimiters|
|[#10030](https://github.com/NVIDIA/spark-rapids/pull/10030)|Update links in shims.md|
|[#10015](https://github.com/NVIDIA/spark-rapids/pull/10015)|Fix array_transform to not recompute the argument|
|[#10011](https://github.com/NVIDIA/spark-rapids/pull/10011)|Add cpu oom retry split handling to InternalRowToColumnarBatchIterator|
|[#10019](https://github.com/NVIDIA/spark-rapids/pull/10019)|Fix auto merge conflict 10010 [skip ci]|
|[#9760](https://github.com/NVIDIA/spark-rapids/pull/9760)|Support split broadcast join condition into ast and non-ast|
|[#9827](https://github.com/NVIDIA/spark-rapids/pull/9827)|Enable ORC timestamp and decimal predicate push down tests|
|[#10002](https://github.com/NVIDIA/spark-rapids/pull/10002)|Use Spark 3.3.3 instead of 3.3.2 for Scala 2.13 premerge builds|
|[#10000](https://github.com/NVIDIA/spark-rapids/pull/10000)|Optimize from_unixtime|
|[#10003](https://github.com/NVIDIA/spark-rapids/pull/10003)|Fix merge conflict with branch-23.12|
|[#9984](https://github.com/NVIDIA/spark-rapids/pull/9984)|Fix 340+(including DB341+) does not support casting date to integral/float|
|[#9972](https://github.com/NVIDIA/spark-rapids/pull/9972)|Fix year 0 is out of range in test_from_json_struct_timestamp |
|[#9814](https://github.com/NVIDIA/spark-rapids/pull/9814)|Support from_unixtime via Gpu for non-UTC time zone|
|[#9929](https://github.com/NVIDIA/spark-rapids/pull/9929)|Add host memory retries for GeneratedInternalRowToCudfRowIterator|
|[#9957](https://github.com/NVIDIA/spark-rapids/pull/9957)|Update cases for cast between integral and (date/time)|
|[#9959](https://github.com/NVIDIA/spark-rapids/pull/9959)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#9942](https://github.com/NVIDIA/spark-rapids/pull/9942)|Fix a potential data corruption for Pandas UDF|
|[#9922](https://github.com/NVIDIA/spark-rapids/pull/9922)|Fix `allowMultipleJars` recommend setting message|
|[#9947](https://github.com/NVIDIA/spark-rapids/pull/9947)|Fix merge conflict with branch-23.12|
|[#9908](https://github.com/NVIDIA/spark-rapids/pull/9908)|Register default allocator for host memory|
|[#9944](https://github.com/NVIDIA/spark-rapids/pull/9944)|Fix Java OOM caused by incorrect state of shouldCapture when exception occurred|
|[#9937](https://github.com/NVIDIA/spark-rapids/pull/9937)|Refactor to use CLASSIFIER instead of CUDA_CLASSIFIER [skip ci]|
|[#9904](https://github.com/NVIDIA/spark-rapids/pull/9904)|Params for build and test CI scripts on Databricks|
|[#9719](https://github.com/NVIDIA/spark-rapids/pull/9719)|Support fine grained timezone checker instead of type based|
|[#9918](https://github.com/NVIDIA/spark-rapids/pull/9918)|Prevent generation of 'year 0 is out of range' strings in IT|
|[#9852](https://github.com/NVIDIA/spark-rapids/pull/9852)|Avoid generating duplicate nan keys with MapGen(FloatGen)|
|[#9674](https://github.com/NVIDIA/spark-rapids/pull/9674)|Add cache action to speed up mvn workflow [skip ci]|
|[#9900](https://github.com/NVIDIA/spark-rapids/pull/9900)|Revert "Remove Databricks 13.3 from release 23.12  (#9890)"|
|[#9888](https://github.com/NVIDIA/spark-rapids/pull/9888)|Update nightly build and deploy script for arm artifacts [skip ci]|
|[#9656](https://github.com/NVIDIA/spark-rapids/pull/9656)|Update for new retry state machine JNI APIs|
|[#9654](https://github.com/NVIDIA/spark-rapids/pull/9654)|Detect multiple jars on the classpath when init plugin|
|[#9857](https://github.com/NVIDIA/spark-rapids/pull/9857)|Skip redundant steps in nightly build [skip ci]|
|[#9812](https://github.com/NVIDIA/spark-rapids/pull/9812)|Update JNI and private dep version to 24.02.0-SNAPSHOT|

## Release 23.12

### Features
|||
|:---|:---|
|[#6832](https://github.com/NVIDIA/spark-rapids/issues/6832)|[FEA] Convert Timestamp/Timezone tests/checks to be per operator instead of generic |
|[#9805](https://github.com/NVIDIA/spark-rapids/issues/9805)|[FEA] Support ```current_date``` expression function with CST (UTC + 8) timezone support|
|[#9515](https://github.com/NVIDIA/spark-rapids/issues/9515)|[FEA] Support temporal types in to_json|
|[#9872](https://github.com/NVIDIA/spark-rapids/issues/9872)|[FEA][JSON] Support Decimal type in `to_json`|
|[#9802](https://github.com/NVIDIA/spark-rapids/issues/9802)|[FEA] Support FromUTCTimestamp on the GPU with a non-UTC time zone|
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
|[#10384](https://github.com/NVIDIA/spark-rapids/pull/10384)|[DOC] Update docs for 23.12.2 release [skip ci] |
|[#10341](https://github.com/NVIDIA/spark-rapids/pull/10341)|Update changelog for v23.12.2 [skip ci]|
|[#10340](https://github.com/NVIDIA/spark-rapids/pull/10340)|Copyright to 2024 [skip ci]|
|[#10323](https://github.com/NVIDIA/spark-rapids/pull/10323)|Upgrade version to 23.12.2-SNAPSHOT|
|[#10329](https://github.com/NVIDIA/spark-rapids/pull/10329)|update download page for v23.12.2 release [skip ci]|
|[#10274](https://github.com/NVIDIA/spark-rapids/pull/10274)|PythonRunner Changes|
|[#10124](https://github.com/NVIDIA/spark-rapids/pull/10124)|Update changelog for v23.12.1 [skip ci]|
|[#10123](https://github.com/NVIDIA/spark-rapids/pull/10123)|Change version to v23.12.1 [skip ci]|
|[#10122](https://github.com/NVIDIA/spark-rapids/pull/10122)|Init changelog for v23.12.1 [skip ci]|
|[#10121](https://github.com/NVIDIA/spark-rapids/pull/10121)|[DOC] update download page for db hot fix  [skip ci]|
|[#10116](https://github.com/NVIDIA/spark-rapids/pull/10116)|Upgrade to 23.12.1-SNAPSHOT|
|[#10069](https://github.com/NVIDIA/spark-rapids/pull/10069)|Revert "Support split broadcast join condition into ast and non-ast […|
|[#9470](https://github.com/NVIDIA/spark-rapids/pull/9470)|Use float to string kernel|
|[#9481](https://github.com/NVIDIA/spark-rapids/pull/9481)|Use parse_url kernel for PROTOCOL parsing|
|[#9935](https://github.com/NVIDIA/spark-rapids/pull/9935)|Init 23.12 changelog [skip ci]|
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
|[#9889](https://github.com/NVIDIA/spark-rapids/pull/9889)|Fix test_cast_string_ts_valid_format test|
|[#9833](https://github.com/NVIDIA/spark-rapids/pull/9833)|Fix a hang for Pandas UDFs on DB 13.3|
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
|[#9716](https://github.com/NVIDIA/spark-rapids/pull/9716)|Initiate project version 24.02.0-SNAPSHOT|
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

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
