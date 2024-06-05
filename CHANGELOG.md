# Change log
Generated on 2024-06-05

## Release 24.06

### Features
|||
|:---|:---|
|[#10744](https://github.com/NVIDIA/spark-rapids/issues/10744)|[FEA] Run Spark UT with spark-rapids|
|[#10850](https://github.com/NVIDIA/spark-rapids/issues/10850)|[FEA] Refine the test framework introduced in #10745|
|[#8963](https://github.com/NVIDIA/spark-rapids/issues/8963)|[FEA] Use custom kernel for parse_url|
|[#6969](https://github.com/NVIDIA/spark-rapids/issues/6969)|[FEA] Support parse_url |
|[#10496](https://github.com/NVIDIA/spark-rapids/issues/10496)|[FEA] Drop support for CentOS7|
|[#10760](https://github.com/NVIDIA/spark-rapids/issues/10760)|[FEA]Support ArrayFilter|
|[#10721](https://github.com/NVIDIA/spark-rapids/issues/10721)|[FEA] Dump the complete set of build-info properties to the Spark eventLog|
|[#10666](https://github.com/NVIDIA/spark-rapids/issues/10666)|[FEA]  Create Spark 3.4.3 shim|

### Performance
|||
|:---|:---|
|[#10817](https://github.com/NVIDIA/spark-rapids/issues/10817)|[FOLLOW ON] Combining regex parsing in transpiling and regex rewrite in `rlike`|
|[#10821](https://github.com/NVIDIA/spark-rapids/issues/10821)|Rewrite `pattern[A-B]{X,Y}` (a pattern string followed by X to Y chars in range A - B) in `RLIKE` to a custom kernel|

### Bugs Fixed
|||
|:---|:---|
|[#10928](https://github.com/NVIDIA/spark-rapids/issues/10928)|[BUG] 24.06 test_conditional_with_side_effects_case_when test failed on Scala 2.13 with DATAGEN_SEED=1716656294|
|[#10941](https://github.com/NVIDIA/spark-rapids/issues/10941)|[BUG] Failed to build on databricks due to GpuOverrides.scala:4264: not found: type GpuSubqueryBroadcastMeta|
|[#10902](https://github.com/NVIDIA/spark-rapids/issues/10902)|Spark UT failed: SPARK-37360: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ|
|[#10899](https://github.com/NVIDIA/spark-rapids/issues/10899)|[BUG] format_number Spark UT failed because Type conversion is not allowed|
|[#10913](https://github.com/NVIDIA/spark-rapids/issues/10913)|[BUG] rlike with empty pattern failed with 'NoSuchElementException' when enabling regex rewrite|
|[#10774](https://github.com/NVIDIA/spark-rapids/issues/10774)|[BUG] Issues found by Spark UT Framework on RapidsRegexpExpressionsSuite|
|[#10606](https://github.com/NVIDIA/spark-rapids/issues/10606)|[BUG] Update Plugin to use the new `getPartitionedFile` method|
|[#10806](https://github.com/NVIDIA/spark-rapids/issues/10806)|[BUG] orc_write_test.py::test_write_round_trip_corner failed with DATAGEN_SEED=1715517863|
|[#10831](https://github.com/NVIDIA/spark-rapids/issues/10831)|[BUG] Failed to read data from iceberg|
|[#10810](https://github.com/NVIDIA/spark-rapids/issues/10810)|[BUG] NPE when running `ParseUrl` tests in `RapidsStringExpressionsSuite`|
|[#10797](https://github.com/NVIDIA/spark-rapids/issues/10797)|[BUG]udf_test test_single_aggregate_udf, test_group_aggregate_udf and test_group_apply_udf_more_types failed on DB 13.3|
|[#10719](https://github.com/NVIDIA/spark-rapids/issues/10719)|[BUG]   test_exact_percentile_groupby FAILED: hash_aggregate_test.py::test_exact_percentile_groupby with DATAGEN seed 1713362217|
|[#10738](https://github.com/NVIDIA/spark-rapids/issues/10738)|[BUG] test_exact_percentile_groupby_partial_fallback_to_cpu failed with DATAGEN_SEED=1713928179|
|[#10768](https://github.com/NVIDIA/spark-rapids/issues/10768)|[DOC] Dead links with tools pages|
|[#10751](https://github.com/NVIDIA/spark-rapids/issues/10751)|[BUG] Cascaded Pandas UDFs not working as expected on Databricks when plugin is enabled|
|[#10318](https://github.com/NVIDIA/spark-rapids/issues/10318)|[BUG] `fs.azure.account.keyInvalid` configuration issue while reading from Unity Catalog Tables on Azure DB|
|[#10722](https://github.com/NVIDIA/spark-rapids/issues/10722)|[BUG] "Could not find any rapids-4-spark jars in classpath" error when debugging UT in IDEA|
|[#10724](https://github.com/NVIDIA/spark-rapids/issues/10724)|[BUG] Failed to convert string with invisible characters to float|
|[#10633](https://github.com/NVIDIA/spark-rapids/issues/10633)|[BUG] ScanJson and JsonToStructs can give almost random errors|
|[#10659](https://github.com/NVIDIA/spark-rapids/issues/10659)|[BUG] from_json ArrayIndexOutOfBoundsException in 24.02|
|[#10656](https://github.com/NVIDIA/spark-rapids/issues/10656)|[BUG] Databricks cache tests failing with host memory OOM|

### PRs
|||
|:---|:---|
|[#10947](https://github.com/NVIDIA/spark-rapids/pull/10947)|Prevent contains-PrefixRange optimization if not preceded by wildcards|
|[#10934](https://github.com/NVIDIA/spark-rapids/pull/10934)|Revert "Add Support for Multiple Filtering Keys for Subquery Broadcast "|
|[#10903](https://github.com/NVIDIA/spark-rapids/pull/10903)|Use upper case for LEGACY_TIME_PARSER_POLICY to fix a spark UT|
|[#10900](https://github.com/NVIDIA/spark-rapids/pull/10900)|Fix type convert error in format_number scalar input|
|[#10868](https://github.com/NVIDIA/spark-rapids/pull/10868)|Disable default cuDF pinned pool|
|[#10914](https://github.com/NVIDIA/spark-rapids/pull/10914)|Fix NoSuchElementException when rlike with empty pattern|
|[#10861](https://github.com/NVIDIA/spark-rapids/pull/10861)|refine ut framework including Part 1 and Part 2|
|[#10872](https://github.com/NVIDIA/spark-rapids/pull/10872)|[DOC] ignore released plugin links to reduce the bother info [skip ci]|
|[#10873](https://github.com/NVIDIA/spark-rapids/pull/10873)|Auto merge PRs to branch-24.08 from branch-24.06 [skip ci]|
|[#10822](https://github.com/NVIDIA/spark-rapids/pull/10822)|Rewrite regex pattern `literal[a-b]{x}` to custom kernel in rlike|
|[#10833](https://github.com/NVIDIA/spark-rapids/pull/10833)|Filter out unused json_path tokens|
|[#10855](https://github.com/NVIDIA/spark-rapids/pull/10855)|Fix auto merge conflict 10845 [[skip ci]]|
|[#10826](https://github.com/NVIDIA/spark-rapids/pull/10826)|Add NVTX ranges to identify Spark stages and tasks|
|[#10846](https://github.com/NVIDIA/spark-rapids/pull/10846)|Update latest changelog [skip ci]|
|[#10836](https://github.com/NVIDIA/spark-rapids/pull/10836)|Catch exceptions when trying to examine Iceberg scan for metadata queries|
|[#10828](https://github.com/NVIDIA/spark-rapids/pull/10828)|Added DateTimeUtilsShims [Databricks]|
|[#10829](https://github.com/NVIDIA/spark-rapids/pull/10829)|Fix `Inheritance Shadowing` to add support for Spark 4.0.0|
|[#10715](https://github.com/NVIDIA/spark-rapids/pull/10715)|Rewrite some rlike expression to StartsWith/Contains|
|[#10812](https://github.com/NVIDIA/spark-rapids/pull/10812)|Replace ThreadPoolExecutor creation with ThreadUtils API|
|[#10816](https://github.com/NVIDIA/spark-rapids/pull/10816)|Fix a test error for DB13.3|
|[#10813](https://github.com/NVIDIA/spark-rapids/pull/10813)|Fix the errors for Pandas UDF tests on DB13.3|
|[#10795](https://github.com/NVIDIA/spark-rapids/pull/10795)|Remove fixed seed for exact `percentile` integration tests|
|[#10805](https://github.com/NVIDIA/spark-rapids/pull/10805)|Drop Support for CentOS 7|
|[#10796](https://github.com/NVIDIA/spark-rapids/pull/10796)|fixing build break on DBR|
|[#10791](https://github.com/NVIDIA/spark-rapids/pull/10791)|Fix auto merge conflict 10779 [skip ci]|
|[#10636](https://github.com/NVIDIA/spark-rapids/pull/10636)|Update actions version [skip ci]|
|[#10743](https://github.com/NVIDIA/spark-rapids/pull/10743)|initial PR for the framework reusing Vanilla Spark's unit tests|
|[#10763](https://github.com/NVIDIA/spark-rapids/pull/10763)|Add in the GpuArrayFilter command|
|[#10766](https://github.com/NVIDIA/spark-rapids/pull/10766)|Fix dead links related to tools documentation [skip ci]|
|[#10644](https://github.com/NVIDIA/spark-rapids/pull/10644)|Add logging to Integration test runs in local and local-cluster mode|
|[#10756](https://github.com/NVIDIA/spark-rapids/pull/10756)|Fix Authorization Failure While Reading Tables From Unity Catalog|
|[#10752](https://github.com/NVIDIA/spark-rapids/pull/10752)|Add SparkRapidsBuildInfoEvent to the event log|
|[#10755](https://github.com/NVIDIA/spark-rapids/pull/10755)|[DOC] Update README for prioritize-commits script [skip ci]|
|[#10728](https://github.com/NVIDIA/spark-rapids/pull/10728)|Let big data gen set nullability recursively|
|[#10740](https://github.com/NVIDIA/spark-rapids/pull/10740)|Use parse_url kernel for PATH parsing|
|[#10734](https://github.com/NVIDIA/spark-rapids/pull/10734)|Add short circuit path for get-json-object when there is separate wildcard path|
|[#10725](https://github.com/NVIDIA/spark-rapids/pull/10725)|Initial definition for Spark 4.0.0 shim|
|[#10739](https://github.com/NVIDIA/spark-rapids/pull/10739)|Use fixed seed for some random failed tests|
|[#10720](https://github.com/NVIDIA/spark-rapids/pull/10720)|Add Shims for Spark 3.4.3|
|[#10716](https://github.com/NVIDIA/spark-rapids/pull/10716)|Remove the mixedType config for JSON as it has no downsides any longer|
|[#10733](https://github.com/NVIDIA/spark-rapids/pull/10733)|Fix "Could not find any rapids-4-spark jars in classpath" error when debugging UT in IDEA|
|[#10718](https://github.com/NVIDIA/spark-rapids/pull/10718)|Change parameters for memory limit in Parquet chunked reader|
|[#10709](https://github.com/NVIDIA/spark-rapids/pull/10709)|Removing some authorizations for departed users [skip ci]|
|[#10726](https://github.com/NVIDIA/spark-rapids/pull/10726)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#10708](https://github.com/NVIDIA/spark-rapids/pull/10708)|Updated dump tool to verify get_json_object|
|[#10706](https://github.com/NVIDIA/spark-rapids/pull/10706)|Fix auto merge conflict 10704 [skip ci]|
|[#10675](https://github.com/NVIDIA/spark-rapids/pull/10675)|Fix merge conflict with branch-24.04 [skip ci]|
|[#10662](https://github.com/NVIDIA/spark-rapids/pull/10662)|Audit script - Check commits from shuffle and storage directories [skip ci]|
|[#10655](https://github.com/NVIDIA/spark-rapids/pull/10655)|Update rapids jni/private dependency to 24.06|

## Release 24.04

### Features
|||
|:---|:---|
|[#10263](https://github.com/NVIDIA/spark-rapids/issues/10263)|[FEA] Add support for reading JSON containing structs where rows are not consistent|
|[#10436](https://github.com/NVIDIA/spark-rapids/issues/10436)|[FEA] Move Spark 3.5.1 out of snapshot once released|
|[#10430](https://github.com/NVIDIA/spark-rapids/issues/10430)|[FEA] Error out when running on an unsupported GPU architecture|
|[#9750](https://github.com/NVIDIA/spark-rapids/issues/9750)|[FEA] Review `JsonToStruct` and `JsonScan` and consolidate some testing and implementation|
|[#8680](https://github.com/NVIDIA/spark-rapids/issues/8680)|[AUDIT][SPARK-42779][SQL] Allow V2 writes to indicate advisory shuffle partition size|
|[#10429](https://github.com/NVIDIA/spark-rapids/issues/10429)|[FEA] Drop support for Databricks 10.4 ML LTS|
|[#10334](https://github.com/NVIDIA/spark-rapids/issues/10334)|[FEA] Turn on memory limits for parquet reader|
|[#10344](https://github.com/NVIDIA/spark-rapids/issues/10344)|[FEA] support barrier mode for mapInPandas/mapInArrow|

### Performance
|||
|:---|:---|
|[#10578](https://github.com/NVIDIA/spark-rapids/issues/10578)|[FEA] Support project expression rewrite for the case ```stringinstr(str_col, substr) > 0``` to ```contains(str_col, substr)```|
|[#10570](https://github.com/NVIDIA/spark-rapids/issues/10570)|[FEA] See if we can optimize sort for a single batch|
|[#10531](https://github.com/NVIDIA/spark-rapids/issues/10531)|[FEA] Support "WindowGroupLimit" optimization on GPU for Databricks 13.3 ML LTS+|
|[#5553](https://github.com/NVIDIA/spark-rapids/issues/5553)|[FEA][Audit] - Push down StringEndsWith/Contains to Parquet |
|[#8208](https://github.com/NVIDIA/spark-rapids/issues/8208)|[FEA][AUDIT][SPARK-37099][SQL] Introduce the group limit of Window for rank-based filter to optimize top-k computation|
|[#10249](https://github.com/NVIDIA/spark-rapids/issues/10249)|[FEA] Support common subexpression elimination for expand operator|
|[#10301](https://github.com/NVIDIA/spark-rapids/issues/10301)|[FEA] Improve performance of from_json|

### Bugs Fixed
|||
|:---|:---|
|[#10700](https://github.com/NVIDIA/spark-rapids/issues/10700)|[BUG] get_json_object cannot handle ints or boolean values|
|[#10645](https://github.com/NVIDIA/spark-rapids/issues/10645)|[BUG] java.lang.IllegalStateException: Expected to only receive a single batch|
|[#10665](https://github.com/NVIDIA/spark-rapids/issues/10665)|[BUG] Need to update private jar's version to v24.04.1 for spark-rapids v24.04.0 release|
|[#10589](https://github.com/NVIDIA/spark-rapids/issues/10589)|[BUG] ZSTD version mismatch in integration tests|
|[#10255](https://github.com/NVIDIA/spark-rapids/issues/10255)|[BUG] parquet_tests are skipped on Dataproc CI|
|[#10624](https://github.com/NVIDIA/spark-rapids/issues/10624)|[BUG] Deploy script "gpg:sign-and-deploy-file failed: 401 Unauthorized|
|[#10631](https://github.com/NVIDIA/spark-rapids/issues/10631)|[BUG] pending `BlockState` leaks blocks if the shuffle read doesn't finish successfully|
|[#10349](https://github.com/NVIDIA/spark-rapids/issues/10349)|[BUG]Test in json_test.py failed: test_from_json_struct_decimal|
|[#9033](https://github.com/NVIDIA/spark-rapids/issues/9033)|[BUG] GpuGetJsonObject does not expand escaped characters|
|[#10216](https://github.com/NVIDIA/spark-rapids/issues/10216)|[BUG] GetJsonObject fails at spark unit test $.store.book[*].reader|
|[#10217](https://github.com/NVIDIA/spark-rapids/issues/10217)|[BUG] GetJsonObject fails at spark unit test $.store.basket[0][*].b|
|[#10537](https://github.com/NVIDIA/spark-rapids/issues/10537)|[BUG] GetJsonObject throws exception when json path contains a name starting with `'`|
|[#10194](https://github.com/NVIDIA/spark-rapids/issues/10194)|[BUG] GetJsonObject does not validate the input is JSON in the same way as Spark|
|[#10196](https://github.com/NVIDIA/spark-rapids/issues/10196)|[BUG] GetJsonObject does not process escape sequences in returned strings or queries|
|[#10212](https://github.com/NVIDIA/spark-rapids/issues/10212)|[BUG] GetJsonObject should return null for invalid query instead of throwing an exception|
|[#10218](https://github.com/NVIDIA/spark-rapids/issues/10218)|[BUG] GetJsonObject does not normalize non-string output|
|[#10591](https://github.com/NVIDIA/spark-rapids/issues/10591)|[BUG] `test_column_add_after_partition` failed on EGX Standalone cluster|
|[#10277](https://github.com/NVIDIA/spark-rapids/issues/10277)|Add monitoring for GH action deprecations|
|[#10627](https://github.com/NVIDIA/spark-rapids/issues/10627)|[BUG] Integration tests FAILED on: "nvCOMP 2.3/2.4 or newer is required for Zstandard compression"|
|[#10585](https://github.com/NVIDIA/spark-rapids/issues/10585)|[BUG]Test simple pinned blocking alloc Failed nightly tests|
|[#10586](https://github.com/NVIDIA/spark-rapids/issues/10586)|[BUG]  YARN EGX IT build failing parquet_testing_test can't find file|
|[#10133](https://github.com/NVIDIA/spark-rapids/issues/10133)|[BUG] test_hash_reduction_collect_set_on_nested_array_type failed in a distributed environment|
|[#10378](https://github.com/NVIDIA/spark-rapids/issues/10378)|[BUG] `test_range_running_window_float_decimal_sum_runs_batched` fails intermittently|
|[#10486](https://github.com/NVIDIA/spark-rapids/issues/10486)|[BUG] StructsToJson does not fall back to the CPU for unsupported timeZone options|
|[#10484](https://github.com/NVIDIA/spark-rapids/issues/10484)|[BUG] JsonToStructs does not fallback when columnNameOfCorruptRecord is set|
|[#10460](https://github.com/NVIDIA/spark-rapids/issues/10460)|[BUG] JsonToStructs should reject float numbers for integer types|
|[#10468](https://github.com/NVIDIA/spark-rapids/issues/10468)|[BUG] JsonToStructs and ScanJson should not treat quoted strings as valid integers|
|[#10470](https://github.com/NVIDIA/spark-rapids/issues/10470)|[BUG] ScanJson and JsonToStructs should support parsing quoted decimal strings that are formatted by local (at least for en-US)|
|[#10494](https://github.com/NVIDIA/spark-rapids/issues/10494)|[BUG] JsonToStructs parses INF wrong when nonNumericNumbers is enabled|
|[#10456](https://github.com/NVIDIA/spark-rapids/issues/10456)|[BUG] allowNonNumericNumbers OFF supported for JSON Scan, but not JsonToStructs|
|[#10467](https://github.com/NVIDIA/spark-rapids/issues/10467)|[BUG] JsonToStructs should reject 1. as a valid number|
|[#10469](https://github.com/NVIDIA/spark-rapids/issues/10469)|[BUG] ScanJson should accept "1." as a valid Decimal|
|[#10559](https://github.com/NVIDIA/spark-rapids/issues/10559)|[BUG] test_spark_from_json_date_with_format FAILED on : Part of the plan is not columnar class org.apache.spark.sql.execution.ProjectExec|
|[#10209](https://github.com/NVIDIA/spark-rapids/issues/10209)|[BUG] Test failure hash_aggregate_test.py::test_hash_reduction_collect_set_on_nested_array_type DATAGEN_SEED=1705515231|
|[#10319](https://github.com/NVIDIA/spark-rapids/issues/10319)|[BUG] Shuffled join OOM with 4GB of GPU memory|
|[#10507](https://github.com/NVIDIA/spark-rapids/issues/10507)|[BUG] regexp_test.py FAILED test_regexp_extract_all_idx_positive[DATAGEN_SEED=1709054829, INJECT_OOM]|
|[#10527](https://github.com/NVIDIA/spark-rapids/issues/10527)|[BUG] Build on Databricks failed with GpuGetJsonObject.scala:19: object parsing is not a member of package util|
|[#10509](https://github.com/NVIDIA/spark-rapids/issues/10509)|[BUG] scalar leaks when running nds query51|
|[#10214](https://github.com/NVIDIA/spark-rapids/issues/10214)|[BUG] GetJsonObject does not support unquoted array like notation|
|[#10215](https://github.com/NVIDIA/spark-rapids/issues/10215)|[BUG] GetJsonObject removes leading space characters|
|[#10213](https://github.com/NVIDIA/spark-rapids/issues/10213)|[BUG] GetJsonObject supports array index notation without a root|
|[#10452](https://github.com/NVIDIA/spark-rapids/issues/10452)|[BUG] JsonScan and from_json share fallback checks, but have hard coded names in the results|
|[#10455](https://github.com/NVIDIA/spark-rapids/issues/10455)|[BUG] JsonToStructs and ScanJson do not fall back/support it properly if single quotes are disabled|
|[#10219](https://github.com/NVIDIA/spark-rapids/issues/10219)|[BUG] GetJsonObject sees a double quote in a single quoted string as invalid|
|[#10431](https://github.com/NVIDIA/spark-rapids/issues/10431)|[BUG] test_casting_from_overflow_double_to_timestamp `DID NOT RAISE <class 'Exception'>`|
|[#10499](https://github.com/NVIDIA/spark-rapids/issues/10499)|[BUG] Unit tests core dump as below|
|[#9325](https://github.com/NVIDIA/spark-rapids/issues/9325)|[BUG] test_csv_infer_schema_timestamp_ntz fails|
|[#10422](https://github.com/NVIDIA/spark-rapids/issues/10422)|[BUG] test_get_json_object_single_quotes failure|
|[#10411](https://github.com/NVIDIA/spark-rapids/issues/10411)|[BUG] Some fast parquet tests fail if the time zone is not UTC|
|[#10410](https://github.com/NVIDIA/spark-rapids/issues/10410)|[BUG]delta_lake_update_test.py::test_delta_update_partitions[['a', 'b']-False] failed by DATAGEN_SEED=1707683137|
|[#10404](https://github.com/NVIDIA/spark-rapids/issues/10404)|[BUG] GpuJsonTuple memory leak|
|[#10382](https://github.com/NVIDIA/spark-rapids/issues/10382)|[BUG] Complile failed on branch-24.04 :  literals.scala:32: object codec is not a member of package org.apache.commons|

### PRs
|||
|:---|:---|
|[#10782](https://github.com/NVIDIA/spark-rapids/pull/10782)|Update latest changelog [skip ci]|
|[#10780](https://github.com/NVIDIA/spark-rapids/pull/10780)|[DOC]Update download page for v24.04.1 [skip ci]|
|[#10777](https://github.com/NVIDIA/spark-rapids/pull/10777)|Update rapids JNI dependency: private to 24.04.2|
|[#10683](https://github.com/NVIDIA/spark-rapids/pull/10683)|Update latest changelog [skip ci]|
|[#10681](https://github.com/NVIDIA/spark-rapids/pull/10681)|Update rapids JNI dependency to 24.04.0, private to 24.04.1|
|[#10660](https://github.com/NVIDIA/spark-rapids/pull/10660)|Ensure an executor broadcast is in a single batch|
|[#10676](https://github.com/NVIDIA/spark-rapids/pull/10676)|[DOC] Update docs for 24.04.0 release [skip ci]|
|[#10654](https://github.com/NVIDIA/spark-rapids/pull/10654)|Add a config to switch back to old impl for getJsonObject|
|[#10667](https://github.com/NVIDIA/spark-rapids/pull/10667)|Update rapids private dependency to 24.04.1|
|[#10664](https://github.com/NVIDIA/spark-rapids/pull/10664)|Remove build link from the premerge-CI workflow|
|[#10657](https://github.com/NVIDIA/spark-rapids/pull/10657)|Revert "Host Memory OOM handling for RowToColumnarIterator (#10617)"|
|[#10625](https://github.com/NVIDIA/spark-rapids/pull/10625)|Pin to 3.1.0 maven-gpg-plugin in deploy script [skip ci]|
|[#10637](https://github.com/NVIDIA/spark-rapids/pull/10637)|Cleanup async state when multi-threaded shuffle readers fail|
|[#10617](https://github.com/NVIDIA/spark-rapids/pull/10617)|Host Memory OOM handling for RowToColumnarIterator|
|[#10614](https://github.com/NVIDIA/spark-rapids/pull/10614)|Use random seed for `test_from_json_struct_decimal`|
|[#10581](https://github.com/NVIDIA/spark-rapids/pull/10581)|Use new jni kernel for getJsonObject|
|[#10630](https://github.com/NVIDIA/spark-rapids/pull/10630)|Fix removal of internal metadata information in 350 shim|
|[#10623](https://github.com/NVIDIA/spark-rapids/pull/10623)|Auto merge PRs to branch-24.06 from branch-24.04 [skip ci]|
|[#10616](https://github.com/NVIDIA/spark-rapids/pull/10616)|Pass metadata extractors to FileScanRDD|
|[#10620](https://github.com/NVIDIA/spark-rapids/pull/10620)|Remove unused shared lib in Jenkins files|
|[#10615](https://github.com/NVIDIA/spark-rapids/pull/10615)|Turn off state logging in HostAllocSuite|
|[#10610](https://github.com/NVIDIA/spark-rapids/pull/10610)|Do not replace TableCacheQueryStageExec|
|[#10599](https://github.com/NVIDIA/spark-rapids/pull/10599)|Call globStatus directly via PY4J in hdfs_glob to avoid calling hadoop command|
|[#10602](https://github.com/NVIDIA/spark-rapids/pull/10602)|Remove InMemoryTableScanExec support for Spark 3.5+|
|[#10608](https://github.com/NVIDIA/spark-rapids/pull/10608)|Update perfio.s3.enabled doc to fix build failure [skip ci]|
|[#10598](https://github.com/NVIDIA/spark-rapids/pull/10598)|Update CI script to build and deploy using the same CUDA classifier[skip ci]|
|[#10575](https://github.com/NVIDIA/spark-rapids/pull/10575)|Update JsonToStructs and ScanJson to have white space normalization|
|[#10597](https://github.com/NVIDIA/spark-rapids/pull/10597)|add guardword to hide cloud info|
|[#10540](https://github.com/NVIDIA/spark-rapids/pull/10540)|Handle minimum GPU architecture supported|
|[#10584](https://github.com/NVIDIA/spark-rapids/pull/10584)|Add in small optimization for instr comparison|
|[#10590](https://github.com/NVIDIA/spark-rapids/pull/10590)|Turn on transition logging in HostAllocSuite|
|[#10572](https://github.com/NVIDIA/spark-rapids/pull/10572)|Improve performance of Sort for the common single batch use case|
|[#10568](https://github.com/NVIDIA/spark-rapids/pull/10568)|Add configuration to share JNI pinned pool with cuIO|
|[#10550](https://github.com/NVIDIA/spark-rapids/pull/10550)|Enable window-group-limit optimization on|
|[#10542](https://github.com/NVIDIA/spark-rapids/pull/10542)|Make JSON parsing common between JsonToStructs and ScanJson|
|[#10562](https://github.com/NVIDIA/spark-rapids/pull/10562)|Fix test_spark_from_json_date_with_format when run in a non-UTC TZ|
|[#10564](https://github.com/NVIDIA/spark-rapids/pull/10564)|Enable specifying specific integration test methods via TESTS environment|
|[#10563](https://github.com/NVIDIA/spark-rapids/pull/10563)|Append new authorized user to blossom-ci safelist [skip ci]|
|[#10520](https://github.com/NVIDIA/spark-rapids/pull/10520)|Distinct left join|
|[#10538](https://github.com/NVIDIA/spark-rapids/pull/10538)|Move K8s cloud name into common lib for Jenkins CI|
|[#10552](https://github.com/NVIDIA/spark-rapids/pull/10552)|Fix issues when no value can be extracted from a regular expression|
|[#10522](https://github.com/NVIDIA/spark-rapids/pull/10522)|Fix missing scala-parser-combinators dependency on Databricks|
|[#10549](https://github.com/NVIDIA/spark-rapids/pull/10549)|Update to latest branch-24.02 [skip ci]|
|[#10544](https://github.com/NVIDIA/spark-rapids/pull/10544)|Fix merge conflict from branch-24.02|
|[#10503](https://github.com/NVIDIA/spark-rapids/pull/10503)|Distinct inner join|
|[#10512](https://github.com/NVIDIA/spark-rapids/pull/10512)|Move to parsing from_json input preserving quoted strings.|
|[#10528](https://github.com/NVIDIA/spark-rapids/pull/10528)|Fix auto merge conflict 10523|
|[#10519](https://github.com/NVIDIA/spark-rapids/pull/10519)|Replicate HostColumnVector.ColumnBuilder in plugin to enable host memory oom work|
|[#10521](https://github.com/NVIDIA/spark-rapids/pull/10521)|Fix Spark 3.5.1 build|
|[#10516](https://github.com/NVIDIA/spark-rapids/pull/10516)|One more metric for expand|
|[#10500](https://github.com/NVIDIA/spark-rapids/pull/10500)|Support "WindowGroupLimit" optimization on GPU|
|[#10508](https://github.com/NVIDIA/spark-rapids/pull/10508)|Move 351 shims into noSnapshot buildvers|
|[#10510](https://github.com/NVIDIA/spark-rapids/pull/10510)|Fix scalar leak in SumBinaryFixer|
|[#10466](https://github.com/NVIDIA/spark-rapids/pull/10466)|Use parser from spark to normalize json path in GetJsonObject|
|[#10490](https://github.com/NVIDIA/spark-rapids/pull/10490)|Start working on a more complete json test matrix json|
|[#10497](https://github.com/NVIDIA/spark-rapids/pull/10497)|Add minValue overflow check in ORC double-to-timestamp cast|
|[#10501](https://github.com/NVIDIA/spark-rapids/pull/10501)|Fix scalar leak in WindowRetrySuite|
|[#10474](https://github.com/NVIDIA/spark-rapids/pull/10474)|Remove Support for Databricks 10.4|
|[#10418](https://github.com/NVIDIA/spark-rapids/pull/10418)|Enable GpuShuffledSymmetricHashJoin by default|
|[#10450](https://github.com/NVIDIA/spark-rapids/pull/10450)|Improve internal row to columnar host memory by using a combined spillable buffer|
|[#10440](https://github.com/NVIDIA/spark-rapids/pull/10440)|Generate CSV data per Spark version for tools|
|[#10449](https://github.com/NVIDIA/spark-rapids/pull/10449)|[DOC] Fix table rendering issue in github.io download UI page [skip ci]|
|[#10438](https://github.com/NVIDIA/spark-rapids/pull/10438)|Integrate perfio.s3 reader|
|[#10423](https://github.com/NVIDIA/spark-rapids/pull/10423)|Disable Integration Test:`test_get_json_object_single_quotes`  on DB 10.4|
|[#10419](https://github.com/NVIDIA/spark-rapids/pull/10419)|Export TZ in tests when default TZ is used|
|[#10426](https://github.com/NVIDIA/spark-rapids/pull/10426)|Fix auto merge conflict 10425 [skip ci]|
|[#10427](https://github.com/NVIDIA/spark-rapids/pull/10427)|Update test doc for 24.04 [skip ci]|
|[#10396](https://github.com/NVIDIA/spark-rapids/pull/10396)|Remove inactive user from github workflow [skip ci]|
|[#10421](https://github.com/NVIDIA/spark-rapids/pull/10421)|Use withRetry when manifesting spillable batch in GpuShuffledHashJoinExec|
|[#10420](https://github.com/NVIDIA/spark-rapids/pull/10420)|Disable JsonTuple by default|
|[#10407](https://github.com/NVIDIA/spark-rapids/pull/10407)|Enable Single Quote Support in getJSONObject API with GetJsonObjectOptions|
|[#10415](https://github.com/NVIDIA/spark-rapids/pull/10415)|Avoid comparing Delta logs when writing partitioned tables|
|[#10247](https://github.com/NVIDIA/spark-rapids/pull/10247)|Improve `GpuExpand` by pre-projecting some columns|
|[#10248](https://github.com/NVIDIA/spark-rapids/pull/10248)|Group-by aggregation based optimization for UNBOUNDED `collect_set` window function|
|[#10406](https://github.com/NVIDIA/spark-rapids/pull/10406)|Enabled subPage chunking by default|
|[#10361](https://github.com/NVIDIA/spark-rapids/pull/10361)|Add in basic support for JSON generation in BigDataGen and improve performance of from_json|
|[#10158](https://github.com/NVIDIA/spark-rapids/pull/10158)|Add in framework for unbounded to unbounded window agg optimization|
|[#10394](https://github.com/NVIDIA/spark-rapids/pull/10394)|Fix auto merge conflict 10393 [skip ci]|
|[#10375](https://github.com/NVIDIA/spark-rapids/pull/10375)|Support barrier mode for mapInPandas/mapInArrow|
|[#10356](https://github.com/NVIDIA/spark-rapids/pull/10356)|Update locate_parquet_testing_files function to support hdfs input path for dataproc CI|
|[#10369](https://github.com/NVIDIA/spark-rapids/pull/10369)|Revert "Support barrier mode for mapInPandas/mapInArrow (#10364)"|
|[#10358](https://github.com/NVIDIA/spark-rapids/pull/10358)|Disable Spark UI by default for integration tests|
|[#10360](https://github.com/NVIDIA/spark-rapids/pull/10360)|Fix a memory leak in json tuple|
|[#10364](https://github.com/NVIDIA/spark-rapids/pull/10364)|Support barrier mode for mapInPandas/mapInArrow|
|[#10348](https://github.com/NVIDIA/spark-rapids/pull/10348)|Remove redundant joinOutputRows metric|
|[#10321](https://github.com/NVIDIA/spark-rapids/pull/10321)|Bump up dependency version to 24.04.0-SNAPSHOT|
|[#10330](https://github.com/NVIDIA/spark-rapids/pull/10330)|Add tryAcquire to GpuSemaphore|
|[#10258](https://github.com/NVIDIA/spark-rapids/pull/10258)|Init project version 24.04.0-SNAPSHOT|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
