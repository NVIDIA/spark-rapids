# Change log
Generated on 2024-05-20

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
|[#10331](https://github.com/NVIDIA/spark-rapids/pull/10331)|Revert "Update to libcudf unsigned sum aggregation types change (#10267)"|
|[#10258](https://github.com/NVIDIA/spark-rapids/pull/10258)|Init project version 24.04.0-SNAPSHOT|

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
|[#10555](https://github.com/NVIDIA/spark-rapids/pull/10555)|Update change log [skip ci]|
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
|[#10384](https://github.com/NVIDIA/spark-rapids/pull/10384)|[DOC] Update docs for 23.12.2 release [skip ci] |
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
|[#10069](https://github.com/NVIDIA/spark-rapids/pull/10069)|Revert "Support split broadcast join condition into ast and non-ast […|
|[#10070](https://github.com/NVIDIA/spark-rapids/pull/10070)|Fix 332db build failure|
|[#10060](https://github.com/NVIDIA/spark-rapids/pull/10060)|Fix failed cases for non-utc time zone|
|[#10038](https://github.com/NVIDIA/spark-rapids/pull/10038)|Remove spark.rapids.sql.nonUTC.enabled configuration option|
|[#10059](https://github.com/NVIDIA/spark-rapids/pull/10059)|Fixed Failing ToPrettyStringSuite Test for 3.5.0|
|[#10013](https://github.com/NVIDIA/spark-rapids/pull/10013)|Extended configuration of OOM injection mode|
|[#10052](https://github.com/NVIDIA/spark-rapids/pull/10052)|Set seed=0 for some integration test cases|
|[#10053](https://github.com/NVIDIA/spark-rapids/pull/10053)|Remove invalid user from CODEOWNER file [skip ci]|
|[#10049](https://github.com/NVIDIA/spark-rapids/pull/10049)|Fix out of range error from pySpark in test_timestamp_millis and other two integration test cases|
|[#9721](https://github.com/NVIDIA/spark-rapids/pull/9721)|Support date_format via Gpu for non-UTC time zone|
|[#9470](https://github.com/NVIDIA/spark-rapids/pull/9470)|Use float to string kernel|
|[#9845](https://github.com/NVIDIA/spark-rapids/pull/9845)|Use parse_url kernel for HOST parsing|
|[#10024](https://github.com/NVIDIA/spark-rapids/pull/10024)|Support hour minute second for non-UTC time zone|
|[#9973](https://github.com/NVIDIA/spark-rapids/pull/9973)|Batching support for row-based bounded window functions |
|[#10042](https://github.com/NVIDIA/spark-rapids/pull/10042)|Update tests to not have hard coded fallback when not needed|
|[#9816](https://github.com/NVIDIA/spark-rapids/pull/9816)|Support unix_timestamp and to_unix_timestamp with non-UTC timezones (non-DST)|
|[#9902](https://github.com/NVIDIA/spark-rapids/pull/9902)|Some refactor for the Python UDF code|
|[#10023](https://github.com/NVIDIA/spark-rapids/pull/10023)|GPU supports `yyyyMMdd` format by post process for the `from_unixtime` function|
|[#10033](https://github.com/NVIDIA/spark-rapids/pull/10033)|Remove GpuToTimestampImproved and spark.rapids.sql.improvedTimeOps.enabled|
|[#10016](https://github.com/NVIDIA/spark-rapids/pull/10016)|Fix infinite loop in test_str_to_map_expr_random_delimiters|
|[#9481](https://github.com/NVIDIA/spark-rapids/pull/9481)|Use parse_url kernel for PROTOCOL parsing|
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
|[#9889](https://github.com/NVIDIA/spark-rapids/pull/9889)|Fix test_cast_string_ts_valid_format test|
|[#9888](https://github.com/NVIDIA/spark-rapids/pull/9888)|Update nightly build and deploy script for arm artifacts [skip ci]|
|[#9833](https://github.com/NVIDIA/spark-rapids/pull/9833)|Fix a hang for Pandas UDFs on DB 13.3|
|[#9656](https://github.com/NVIDIA/spark-rapids/pull/9656)|Update for new retry state machine JNI APIs|
|[#9654](https://github.com/NVIDIA/spark-rapids/pull/9654)|Detect multiple jars on the classpath when init plugin|
|[#9857](https://github.com/NVIDIA/spark-rapids/pull/9857)|Skip redundant steps in nightly build [skip ci]|
|[#9812](https://github.com/NVIDIA/spark-rapids/pull/9812)|Update JNI and private dep version to 24.02.0-SNAPSHOT|
|[#9716](https://github.com/NVIDIA/spark-rapids/pull/9716)|Initiate project version 24.02.0-SNAPSHOT|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
