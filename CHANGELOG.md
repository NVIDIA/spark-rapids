# Change log
Generated on 2024-10-18

## Release 24.10

### Features
|||
|:---|:---|
|[#11525](https://github.com/NVIDIA/spark-rapids/issues/11525)|[FEA] If dump always is enabled dump before decoding the file|
|[#11461](https://github.com/NVIDIA/spark-rapids/issues/11461)|[FEA] Support non-UTC timezone for casting from date to timestamp|
|[#11445](https://github.com/NVIDIA/spark-rapids/issues/11445)|[FEA] Support format 'yyyyMMdd' in GetTimestamp operator|
|[#11442](https://github.com/NVIDIA/spark-rapids/issues/11442)|[FEA] Add in support for setting row group sizes for parquet|
|[#11330](https://github.com/NVIDIA/spark-rapids/issues/11330)|[FEA] Add companion metrics for all nsTiming metrics to measure time elapsed excluding semaphore wait|
|[#5223](https://github.com/NVIDIA/spark-rapids/issues/5223)|[FEA] Support array_join|
|[#10968](https://github.com/NVIDIA/spark-rapids/issues/10968)|[FEA] support min_by function|
|[#10437](https://github.com/NVIDIA/spark-rapids/issues/10437)|[FEA] Add Spark 3.5.2 snapshot support|

### Performance
|||
|:---|:---|
|[#10799](https://github.com/NVIDIA/spark-rapids/issues/10799)|[FEA] Optimize count distinct performance optimization with null columns reuse and post expand coalesce|
|[#8301](https://github.com/NVIDIA/spark-rapids/issues/8301)|[FEA] semaphore prioritization|
|[#11234](https://github.com/NVIDIA/spark-rapids/issues/11234)|Explore swapping build table for left outer joins|
|[#11263](https://github.com/NVIDIA/spark-rapids/issues/11263)|[FEA] Cluster/pack multi_get_json_object paths by common prefixes|

### Bugs Fixed
|||
|:---|:---|
|[#11573](https://github.com/NVIDIA/spark-rapids/issues/11573)|[BUG] very long tail task is observed when many tasks are contending for PrioritySemaphore|
|[#11367](https://github.com/NVIDIA/spark-rapids/issues/11367)|[BUG] Error "table_view.cpp:36: Column size mismatch" when using approx_percentile on a string column|
|[#11543](https://github.com/NVIDIA/spark-rapids/issues/11543)|[BUG] test_yyyyMMdd_format_for_legacy_mode[DATAGEN_SEED=1727619674, TZ=UTC] failed GPU and CPU are not both null|
|[#11500](https://github.com/NVIDIA/spark-rapids/issues/11500)|[BUG] dataproc serverless Integration tests failing in json_matrix_test.py|
|[#11384](https://github.com/NVIDIA/spark-rapids/issues/11384)|[BUG] "rs. shuffle write time" negative values seen in app history log|
|[#11509](https://github.com/NVIDIA/spark-rapids/issues/11509)|[BUG] buildall no longer works|
|[#11501](https://github.com/NVIDIA/spark-rapids/issues/11501)|[BUG] test_yyyyMMdd_format_for_legacy_mode failed in Dataproc Serverless integration tests|
|[#11502](https://github.com/NVIDIA/spark-rapids/issues/11502)|[BUG] IT script failed get jars as we stop deploying intermediate jars since 24.10|
|[#11479](https://github.com/NVIDIA/spark-rapids/issues/11479)|[BUG] spark400 build failed do not conform to class UnaryExprMeta's type parameter|
|[#8558](https://github.com/NVIDIA/spark-rapids/issues/8558)|[BUG] `from_json` generated inconsistent result comparing with CPU for input column with nested json strings|
|[#11485](https://github.com/NVIDIA/spark-rapids/issues/11485)|[BUG] Integration tests failing in join_test.py|
|[#11481](https://github.com/NVIDIA/spark-rapids/issues/11481)|[BUG] non-utc integration tests failing in json_test.py|
|[#10911](https://github.com/NVIDIA/spark-rapids/issues/10911)|from_json: when input is a bad json string, rapids would throw an exception.|
|[#10457](https://github.com/NVIDIA/spark-rapids/issues/10457)|[BUG] ScanJson and JsonToStructs allow unquoted control chars by default|
|[#10479](https://github.com/NVIDIA/spark-rapids/issues/10479)|[BUG] JsonToStructs and ScanJson should return null for non-numeric, non-boolean non-quoted strings|
|[#10534](https://github.com/NVIDIA/spark-rapids/issues/10534)|[BUG] Need Improved JSON Validation |
|[#11436](https://github.com/NVIDIA/spark-rapids/issues/11436)|[BUG] Mortgage unit tests fail with RAPIDS shuffle manager|
|[#11437](https://github.com/NVIDIA/spark-rapids/issues/11437)|[BUG] array and map casts to string tests failed|
|[#11463](https://github.com/NVIDIA/spark-rapids/issues/11463)|[BUG] hash_groupby_approx_percentile failed assert is None|
|[#11465](https://github.com/NVIDIA/spark-rapids/issues/11465)|[BUG] java.lang.NoClassDefFoundError: org/apache/spark/BuildInfo$ in non-databricks environment|
|[#11359](https://github.com/NVIDIA/spark-rapids/issues/11359)|[BUG] a couple of arithmetic_ops_test.py cases failed mismatching cpu and gpu values with [DATAGEN_SEED=1723985531, TZ=UTC, INJECT_OOM]|
|[#11392](https://github.com/NVIDIA/spark-rapids/issues/11392)|[AUDIT] Handle IgnoreNulls Expressions for Window Expressions|
|[#10770](https://github.com/NVIDIA/spark-rapids/issues/10770)|[BUG] Slow/no progress with cascaded pandas udfs/mapInPandas in Databricks|
|[#11397](https://github.com/NVIDIA/spark-rapids/issues/11397)|[BUG] We should not be using copyWithBooleanColumnAsValidity unless we can prove it is 100% safe|
|[#11372](https://github.com/NVIDIA/spark-rapids/issues/11372)|[BUG] spark400 failed compiling datagen_2.13|
|[#11364](https://github.com/NVIDIA/spark-rapids/issues/11364)|[BUG] Missing numRows in the ColumnarBatch created in GpuBringBackToHost|
|[#11350](https://github.com/NVIDIA/spark-rapids/issues/11350)|[BUG] spark400 compile failed in scala213|
|[#11346](https://github.com/NVIDIA/spark-rapids/issues/11346)|[BUG] databrick nightly failing with not able to get spark-version-info.properties|
|[#9604](https://github.com/NVIDIA/spark-rapids/issues/9604)|[BUG] Delta Lake metadata query detection can trigger extra file listing jobs|
|[#11318](https://github.com/NVIDIA/spark-rapids/issues/11318)|[BUG] GPU query is case sensitive on Hive text table's column name|
|[#10596](https://github.com/NVIDIA/spark-rapids/issues/10596)|[BUG] ScanJson and JsonToStructs does not deal with escaped single quotes properly|
|[#10351](https://github.com/NVIDIA/spark-rapids/issues/10351)|[BUG] test_from_json_mixed_types_list_struct failed|
|[#11294](https://github.com/NVIDIA/spark-rapids/issues/11294)|[BUG] binary-dedupe leaves  around a copy of "unshimmed" class files in spark-shared|
|[#11183](https://github.com/NVIDIA/spark-rapids/issues/11183)|[BUG] Failed to split an empty string with error "ai.rapids.cudf.CudfException: parallel_for failed: cudaErrorInvalidDevice: invalid device ordinal"|
|[#11008](https://github.com/NVIDIA/spark-rapids/issues/11008)|Fix tests failures in ast_test.py|
|[#11265](https://github.com/NVIDIA/spark-rapids/issues/11265)|[BUG] segfaults seen in cuDF after prefetch calls intermittently|
|[#11025](https://github.com/NVIDIA/spark-rapids/issues/11025)|Fix tests failures in date_time_test.py|
|[#11065](https://github.com/NVIDIA/spark-rapids/issues/11065)|[BUG]  Spark Connect Server (3.5.1) Can Not Running Correctly|

### PRs
|||
|:---|:---|
|[#11624](https://github.com/NVIDIA/spark-rapids/pull/11624)|Update the download link [skip ci]|
|[#11577](https://github.com/NVIDIA/spark-rapids/pull/11577)|Update latest changelog [skip ci]|
|[#11576](https://github.com/NVIDIA/spark-rapids/pull/11576)|Update rapids JNI and private dependency to 24.10.0|
|[#11582](https://github.com/NVIDIA/spark-rapids/pull/11582)|[DOC] update doc for 24.10 release [skip ci]|
|[#11588](https://github.com/NVIDIA/spark-rapids/pull/11588)|backport fixes of #11573 to branch 24.10|
|[#11569](https://github.com/NVIDIA/spark-rapids/pull/11569)|Have "dump always" dump input files before trying to decode them|
|[#11567](https://github.com/NVIDIA/spark-rapids/pull/11567)|Fix test case unix_timestamp(col, 'yyyyMMdd') failed for Africa/Casablanca timezone and LEGACY mode|
|[#11496](https://github.com/NVIDIA/spark-rapids/pull/11496)|Update test now that code is fixed|
|[#11548](https://github.com/NVIDIA/spark-rapids/pull/11548)|Fix negative rs. shuffle write time|
|[#11545](https://github.com/NVIDIA/spark-rapids/pull/11545)|Update test case related to LEACY datetime format to unblock nightly CI|
|[#11515](https://github.com/NVIDIA/spark-rapids/pull/11515)|Propagate default DIST_PROFILE_OPT profile to Maven in buildall|
|[#11497](https://github.com/NVIDIA/spark-rapids/pull/11497)|Update from_json to use new cudf features|
|[#11516](https://github.com/NVIDIA/spark-rapids/pull/11516)|Deploy all submodules for default sparkver in nightly [skip ci]|
|[#11484](https://github.com/NVIDIA/spark-rapids/pull/11484)|Fix FileAlreadyExistsException in LORE dump process|
|[#11457](https://github.com/NVIDIA/spark-rapids/pull/11457)|GPU device watermark metrics|
|[#11507](https://github.com/NVIDIA/spark-rapids/pull/11507)|Replace libmamba-solver with mamba command [skip ci]|
|[#11503](https://github.com/NVIDIA/spark-rapids/pull/11503)|Download artifacts via wget [skip ci]|
|[#11490](https://github.com/NVIDIA/spark-rapids/pull/11490)|Use UnaryLike instead of UnaryExpression|
|[#10798](https://github.com/NVIDIA/spark-rapids/pull/10798)|Optimizing Expand+Aggregate in sqls with many count distinct|
|[#11366](https://github.com/NVIDIA/spark-rapids/pull/11366)|Enable parquet suites from Spark UT|
|[#11477](https://github.com/NVIDIA/spark-rapids/pull/11477)|Install cuDF-py against python 3.10 on Databricks|
|[#11462](https://github.com/NVIDIA/spark-rapids/pull/11462)|Support non-UTC timezone for casting from date type to timestamp type|
|[#11449](https://github.com/NVIDIA/spark-rapids/pull/11449)|Support yyyyMMdd in GetTimestamp operator for LEGACY mode|
|[#11456](https://github.com/NVIDIA/spark-rapids/pull/11456)|Enable tests for all JSON white space normalization|
|[#11483](https://github.com/NVIDIA/spark-rapids/pull/11483)|Use reusable auto-merge workflow [skip ci]|
|[#11482](https://github.com/NVIDIA/spark-rapids/pull/11482)|Fix a json test for non utc time zone|
|[#11464](https://github.com/NVIDIA/spark-rapids/pull/11464)|Use improved CUDF JSON validation|
|[#11474](https://github.com/NVIDIA/spark-rapids/pull/11474)|Enable tests after string_split was fixed|
|[#11473](https://github.com/NVIDIA/spark-rapids/pull/11473)|Revert "Skip test_hash_groupby_approx_percentile byte and double test…|
|[#11466](https://github.com/NVIDIA/spark-rapids/pull/11466)|Replace scala.util.Try with a try statement in the DBR buildinfo|
|[#11469](https://github.com/NVIDIA/spark-rapids/pull/11469)|Skip test_hash_groupby_approx_percentile byte and double tests tempor…|
|[#11429](https://github.com/NVIDIA/spark-rapids/pull/11429)|Fixed some of the failing parquet_tests|
|[#11455](https://github.com/NVIDIA/spark-rapids/pull/11455)|Log DBR BuildInfo|
|[#11451](https://github.com/NVIDIA/spark-rapids/pull/11451)|xfail array and map cast to string tests|
|[#11331](https://github.com/NVIDIA/spark-rapids/pull/11331)|Add companion metrics for all nsTiming metrics without semaphore|
|[#11421](https://github.com/NVIDIA/spark-rapids/pull/11421)|[DOC] remove the redundant archive link [skip ci]|
|[#11308](https://github.com/NVIDIA/spark-rapids/pull/11308)|Dynamic Shim Detection for `build` Process|
|[#11427](https://github.com/NVIDIA/spark-rapids/pull/11427)|Update CI scripts to work with the "Dynamic Shim Detection" change [skip ci]|
|[#11425](https://github.com/NVIDIA/spark-rapids/pull/11425)|Update signoff usage [skip ci]|
|[#11420](https://github.com/NVIDIA/spark-rapids/pull/11420)|Add in array_join support|
|[#11418](https://github.com/NVIDIA/spark-rapids/pull/11418)|stop using copyWithBooleanColumnAsValidity|
|[#11411](https://github.com/NVIDIA/spark-rapids/pull/11411)|Fix asymmetric join crash when stream side is empty|
|[#11395](https://github.com/NVIDIA/spark-rapids/pull/11395)|Fix a Pandas UDF slowness issue|
|[#11371](https://github.com/NVIDIA/spark-rapids/pull/11371)|Support MinBy and MaxBy for non-float ordering|
|[#11399](https://github.com/NVIDIA/spark-rapids/pull/11399)|stop using copyWithBooleanColumnAsValidity|
|[#11389](https://github.com/NVIDIA/spark-rapids/pull/11389)|prevent duplicate queueing in the prio semaphore|
|[#11291](https://github.com/NVIDIA/spark-rapids/pull/11291)|Add distinct join support for right outer joins|
|[#11396](https://github.com/NVIDIA/spark-rapids/pull/11396)|Drop cudf-py python 3.9 support [skip ci]|
|[#11393](https://github.com/NVIDIA/spark-rapids/pull/11393)|Revert work-around for empty split-string|
|[#11334](https://github.com/NVIDIA/spark-rapids/pull/11334)|Add support for Spark 3.5.2|
|[#11388](https://github.com/NVIDIA/spark-rapids/pull/11388)|JSON tests for corrected date, timestamp, and mixed types|
|[#11375](https://github.com/NVIDIA/spark-rapids/pull/11375)|Fix spark400 build in datagen and tests|
|[#11376](https://github.com/NVIDIA/spark-rapids/pull/11376)|Create a PrioritySemaphore to back the GpuSemaphore|
|[#11383](https://github.com/NVIDIA/spark-rapids/pull/11383)|Fix nightly snapshots being downloaded in premerge build|
|[#11368](https://github.com/NVIDIA/spark-rapids/pull/11368)|Move SparkRapidsBuildInfoEvent to its own file|
|[#11329](https://github.com/NVIDIA/spark-rapids/pull/11329)|Change reference to `MapUtils` into `JSONUtils`|
|[#11365](https://github.com/NVIDIA/spark-rapids/pull/11365)|Set numRows for the ColumnBatch created in GpuBringBackToHost|
|[#11363](https://github.com/NVIDIA/spark-rapids/pull/11363)|Fix failing test compile for Spark 4.0.0|
|[#11362](https://github.com/NVIDIA/spark-rapids/pull/11362)|Add tests for repeated JSON columns/keys|
|[#11321](https://github.com/NVIDIA/spark-rapids/pull/11321)|conform dependency list in 341db to previous versions style|
|[#10604](https://github.com/NVIDIA/spark-rapids/pull/10604)|Add string escaping JSON tests to the test_json_matrix|
|[#11328](https://github.com/NVIDIA/spark-rapids/pull/11328)|Swap build side for outer joins when natural build side is explosive|
|[#11358](https://github.com/NVIDIA/spark-rapids/pull/11358)|Fix download doc [skip ci]|
|[#11357](https://github.com/NVIDIA/spark-rapids/pull/11357)|Fix auto merge conflict 11354 [skip ci]|
|[#11347](https://github.com/NVIDIA/spark-rapids/pull/11347)|Revert "Fix the mismatching default configs in integration tests  (#11283)"|
|[#11323](https://github.com/NVIDIA/spark-rapids/pull/11323)|replace inputFiles with location.rootPaths.toString|
|[#11340](https://github.com/NVIDIA/spark-rapids/pull/11340)|Audit script - Check commits from sql-hive directory [skip ci]|
|[#11283](https://github.com/NVIDIA/spark-rapids/pull/11283)|Fix the mismatching default configs in integration tests |
|[#11327](https://github.com/NVIDIA/spark-rapids/pull/11327)|Make hive column matches not case-sensitive|
|[#11324](https://github.com/NVIDIA/spark-rapids/pull/11324)|Append ustcfy to blossom-ci whitelist [skip ci]|
|[#11325](https://github.com/NVIDIA/spark-rapids/pull/11325)|Fix auto merge conflict 11317 [skip ci]|
|[#11319](https://github.com/NVIDIA/spark-rapids/pull/11319)|Update passing JSON tests after list support added in CUDF|
|[#11307](https://github.com/NVIDIA/spark-rapids/pull/11307)|Safely close multiple resources in RapidsBufferCatalog|
|[#11313](https://github.com/NVIDIA/spark-rapids/pull/11313)|Fix auto merge conflict 10845 11310 [skip ci]|
|[#11312](https://github.com/NVIDIA/spark-rapids/pull/11312)|Add jihoonson as an authorized user for blossom-ci [skip ci]|
|[#11302](https://github.com/NVIDIA/spark-rapids/pull/11302)|Fix display issue of lore.md|
|[#11301](https://github.com/NVIDIA/spark-rapids/pull/11301)|Skip deploying non-critical intermediate artifacts [skip ci]|
|[#11299](https://github.com/NVIDIA/spark-rapids/pull/11299)|Enable get_json_object by default and remove legacy version|
|[#11289](https://github.com/NVIDIA/spark-rapids/pull/11289)|Use the new chunked API from multi-get_json_object|
|[#11295](https://github.com/NVIDIA/spark-rapids/pull/11295)|Remove redundant classes from the dist jar and unshimmed list|
|[#11284](https://github.com/NVIDIA/spark-rapids/pull/11284)|Use distinct count to estimate join magnification factor|
|[#11288](https://github.com/NVIDIA/spark-rapids/pull/11288)|Move easy unshimmed classes to sql-plugin-api|
|[#11285](https://github.com/NVIDIA/spark-rapids/pull/11285)|Remove files under tools/generated_files/spark31* [skip ci]|
|[#11280](https://github.com/NVIDIA/spark-rapids/pull/11280)|Asynchronously copy table data to the host during shuffle|
|[#11258](https://github.com/NVIDIA/spark-rapids/pull/11258)|Explicitly disable ANSI mode for ast_test.py|
|[#11267](https://github.com/NVIDIA/spark-rapids/pull/11267)|Update the rapids JNI and private dependency version to 24.10.0-SNAPSHOT|
|[#11241](https://github.com/NVIDIA/spark-rapids/pull/11241)|Auto merge PRs to branch-24.10 from branch-24.08 [skip ci]|
|[#11231](https://github.com/NVIDIA/spark-rapids/pull/11231)|Cache dependencies for scala 2.13 [skip ci]|

## Release 24.08

### Features
|||
|:---|:---|
|[#9259](https://github.com/NVIDIA/spark-rapids/issues/9259)|[FEA] Create Spark 4.0.0 shim and build env|
|[#10366](https://github.com/NVIDIA/spark-rapids/issues/10366)|[FEA] It would be nice if we could support Hive-style write bucketing table|
|[#10987](https://github.com/NVIDIA/spark-rapids/issues/10987)|[FEA] Implement lore framework to support all operators.|
|[#11087](https://github.com/NVIDIA/spark-rapids/issues/11087)|[FEA] Support regex pattern with brackets when rewrite to PrefixRange patten in rlike|
|[#22](https://github.com/NVIDIA/spark-rapids/issues/22)|[FEA] Add support for bucketed writes|
|[#9939](https://github.com/NVIDIA/spark-rapids/issues/9939)|[FEA] `GpuInsertIntoHiveTable` supports parquet format|

### Performance
|||
|:---|:---|
|[#8750](https://github.com/NVIDIA/spark-rapids/issues/8750)|[FEA] Rework GpuSubstringIndex to use cudf::slice_strings|
|[#7404](https://github.com/NVIDIA/spark-rapids/issues/7404)|[FEA] explore a hash agg passthrough on partial aggregates|
|[#10976](https://github.com/NVIDIA/spark-rapids/issues/10976)|Rewrite `pattern1|pattern2|pattern3` to multiple contains in `rlike`|

### Bugs Fixed
|||
|:---|:---|
|[#11287](https://github.com/NVIDIA/spark-rapids/issues/11287)|[BUG] String split APIs on empty string produce incorrect result|
|[#11270](https://github.com/NVIDIA/spark-rapids/issues/11270)|[BUG] test_regexp_replace[DATAGEN_SEED=1722297411, TZ=UTC] hanging there forever in pre-merge CI intermittently|
|[#9682](https://github.com/NVIDIA/spark-rapids/issues/9682)|[BUG] Casting FLOAT64 to DECIMAL(12,7) produces different rows from Apache Spark CPU|
|[#10809](https://github.com/NVIDIA/spark-rapids/issues/10809)|[BUG] cast(9.95 as decimal(3,1)), actual: 9.9, expected: 10.0|
|[#11266](https://github.com/NVIDIA/spark-rapids/issues/11266)|[BUG] test_broadcast_hash_join_constant_keys failed in databricks runtimes|
|[#11243](https://github.com/NVIDIA/spark-rapids/issues/11243)|[BUG] ArrayIndexOutOfBoundsException on a left outer join|
|[#11030](https://github.com/NVIDIA/spark-rapids/issues/11030)|Fix tests failures in string_test.py|
|[#11245](https://github.com/NVIDIA/spark-rapids/issues/11245)|[BUG] mvn verify for the source-javadoc fails and no pre-merge check catches it|
|[#11223](https://github.com/NVIDIA/spark-rapids/issues/11223)|[BUG] Remove unreferenced `CUDF_VER=xxx` in the CI script|
|[#11114](https://github.com/NVIDIA/spark-rapids/issues/11114)|[BUG] Update nightly tests for Scala 2.13 to use JDK 17 only|
|[#11229](https://github.com/NVIDIA/spark-rapids/issues/11229)|[BUG] test_delta_name_column_mapping_no_field_ids fails on Spark |
|[#11031](https://github.com/NVIDIA/spark-rapids/issues/11031)|Fix tests failures in multiple files |
|[#10948](https://github.com/NVIDIA/spark-rapids/issues/10948)|Figure out why `MapFromArrays ` appears in the tests for hive parquet write|
|[#11018](https://github.com/NVIDIA/spark-rapids/issues/11018)|Fix tests failures in hash_aggregate_test.py|
|[#11173](https://github.com/NVIDIA/spark-rapids/issues/11173)|[BUG] The `rs. serialization time` metric is misleading|
|[#11017](https://github.com/NVIDIA/spark-rapids/issues/11017)|Fix tests failures in url_test.py|
|[#11201](https://github.com/NVIDIA/spark-rapids/issues/11201)|[BUG] Delta Lake tables with name mapping can throw exceptions on read|
|[#11175](https://github.com/NVIDIA/spark-rapids/issues/11175)|[BUG] Clean up unused and duplicated 'org/roaringbitmap' folder in the spark3xx shims|
|[#11196](https://github.com/NVIDIA/spark-rapids/issues/11196)|[BUG] pipeline failed due to class not found exception: NoClassDefFoundError: com/nvidia/spark/rapids/GpuScalar|
|[#11189](https://github.com/NVIDIA/spark-rapids/issues/11189)|[BUG] regression in NDS after PR #11170|
|[#11167](https://github.com/NVIDIA/spark-rapids/issues/11167)|[BUG] UnsupportedOperationException during delta write with `optimize()`|
|[#11172](https://github.com/NVIDIA/spark-rapids/issues/11172)|[BUG] `get_json_object` returns wrong output with wildcard path|
|[#11148](https://github.com/NVIDIA/spark-rapids/issues/11148)|[BUG] Integration test `test_write_hive_bucketed_table` fails|
|[#11155](https://github.com/NVIDIA/spark-rapids/issues/11155)|[BUG] ArrayIndexOutOfBoundsException in BatchWithPartitionData.splitColumnarBatch|
|[#11152](https://github.com/NVIDIA/spark-rapids/issues/11152)|[BUG] LORE dumping consumes too much memory.|
|[#11029](https://github.com/NVIDIA/spark-rapids/issues/11029)|Fix tests failures in subquery_test.py|
|[#11150](https://github.com/NVIDIA/spark-rapids/issues/11150)|[BUG] hive_parquet_write_test.py::test_insert_hive_bucketed_table failure|
|[#11070](https://github.com/NVIDIA/spark-rapids/issues/11070)|[BUG] numpy2 fail fastparquet cases: numpy.dtype size changed|
|[#11136](https://github.com/NVIDIA/spark-rapids/issues/11136)|UnaryPositive expression doesn't extend UnaryExpression|
|[#11122](https://github.com/NVIDIA/spark-rapids/issues/11122)|[BUG] UT MetricRange failed 651070526 was not less than 1.5E8 in spark313|
|[#11119](https://github.com/NVIDIA/spark-rapids/issues/11119)|[BUG] window_function_test.py::test_window_group_limits_fallback_for_row_number fails in a distributed environment|
|[#11023](https://github.com/NVIDIA/spark-rapids/issues/11023)|Fix tests failures in dpp_test.py|
|[#11026](https://github.com/NVIDIA/spark-rapids/issues/11026)|Fix tests failures in map_test.py|
|[#11020](https://github.com/NVIDIA/spark-rapids/issues/11020)|Fix tests failures in grouping_sets_test.py|
|[#11113](https://github.com/NVIDIA/spark-rapids/issues/11113)|[BUG] Update premerge tests for Scala 2.13 to use JDK 17 only|
|[#11027](https://github.com/NVIDIA/spark-rapids/issues/11027)|Fix tests failures in sort_test.py|
|[#10775](https://github.com/NVIDIA/spark-rapids/issues/10775)|[BUG] Issues found by Spark UT Framework on RapidsStringExpressionsSuite|
|[#11033](https://github.com/NVIDIA/spark-rapids/issues/11033)|[BUG] CICD failed a case: cmp_test.py::test_empty_filter[>]|
|[#11103](https://github.com/NVIDIA/spark-rapids/issues/11103)|[BUG] UCX Shuffle With scala.MatchError |
|[#11007](https://github.com/NVIDIA/spark-rapids/issues/11007)|Fix tests failures in array_test.py|
|[#10801](https://github.com/NVIDIA/spark-rapids/issues/10801)|[BUG] JDK17 nightly build after Spark UT Framework is merged|
|[#11019](https://github.com/NVIDIA/spark-rapids/issues/11019)|Fix tests failures in window_function_test.py|
|[#11063](https://github.com/NVIDIA/spark-rapids/issues/11063)|[BUG] op time for GpuCoalesceBatches is more than actual|
|[#11006](https://github.com/NVIDIA/spark-rapids/issues/11006)|Fix test failures in arithmetic_ops_test.py|
|[#10995](https://github.com/NVIDIA/spark-rapids/issues/10995)|Fallback TimeZoneAwareExpression that only support UTC with zoneId instead of timeZone config|
|[#8652](https://github.com/NVIDIA/spark-rapids/issues/8652)|[BUG] array_item test failures on Spark 3.3.x|
|[#11053](https://github.com/NVIDIA/spark-rapids/issues/11053)|[BUG] Build on Databricks 330 fails|
|[#10925](https://github.com/NVIDIA/spark-rapids/issues/10925)| Concat cannot accept no parameter|
|[#10975](https://github.com/NVIDIA/spark-rapids/issues/10975)|[BUG] regex `^.*literal` cannot be rewritten as `contains(literal)` for multiline strings|
|[#10956](https://github.com/NVIDIA/spark-rapids/issues/10956)|[BUG] hive_parquet_write_test.py: test_write_compressed_parquet_into_hive_table integration test failures|
|[#10772](https://github.com/NVIDIA/spark-rapids/issues/10772)|[BUG] Issues found by Spark UT Framework on RapidsDataFrameAggregateSuite|
|[#10986](https://github.com/NVIDIA/spark-rapids/issues/10986)|[BUG]Cast from string to float using hand-picked values failed in CastOpSuite|
|[#10972](https://github.com/NVIDIA/spark-rapids/issues/10972)|Spark 4.0 compile errors |
|[#10794](https://github.com/NVIDIA/spark-rapids/issues/10794)|[BUG] Incorrect cast of string columns containing various infinity notations with trailing spaces |
|[#10964](https://github.com/NVIDIA/spark-rapids/issues/10964)|[BUG] Improve stability of pre-merge jenkinsfile|
|[#10714](https://github.com/NVIDIA/spark-rapids/issues/10714)|Signature changed for `PythonUDFRunner.writeUDFs` |
|[#10712](https://github.com/NVIDIA/spark-rapids/issues/10712)|[AUDIT] BatchScanExec/DataSourceV2Relation to group splits by join keys if they differ from partition keys|
|[#10673](https://github.com/NVIDIA/spark-rapids/issues/10673)|[AUDIT] Rename plan nodes for PythonMapInArrowExec|
|[#10710](https://github.com/NVIDIA/spark-rapids/issues/10710)|[AUDIT] `uncacheTableOrView` changed in CommandUtils |
|[#10711](https://github.com/NVIDIA/spark-rapids/issues/10711)|[AUDIT] Match DataSourceV2ScanExecBase changes to groupPartitions method |
|[#10669](https://github.com/NVIDIA/spark-rapids/issues/10669)|Supporting broadcast of multiple filtering keys in DynamicPruning |

### PRs
|||
|:---|:---|
|[#11400](https://github.com/NVIDIA/spark-rapids/pull/11400)|[DOC] update notes in download page for the decompressing gzip issue [skip ci]|
|[#11355](https://github.com/NVIDIA/spark-rapids/pull/11355)|Update changelog for the v24.08 release [skip ci]|
|[#11353](https://github.com/NVIDIA/spark-rapids/pull/11353)|Update download doc for v24.08.1 [skip ci]|
|[#11352](https://github.com/NVIDIA/spark-rapids/pull/11352)|Update version to 24.08.1-SNAPSHOT [skip ci]|
|[#11337](https://github.com/NVIDIA/spark-rapids/pull/11337)|Update changelog for the v24.08 release [skip ci]|
|[#11335](https://github.com/NVIDIA/spark-rapids/pull/11335)|Fix Delta Lake truncation of min/max string values|
|[#11304](https://github.com/NVIDIA/spark-rapids/pull/11304)|Update changelog for v24.08.0 release [skip ci]|
|[#11303](https://github.com/NVIDIA/spark-rapids/pull/11303)|Update rapids JNI and private dependency to 24.08.0|
|[#11296](https://github.com/NVIDIA/spark-rapids/pull/11296)|[DOC] update doc for 2408 release [skip CI]|
|[#11309](https://github.com/NVIDIA/spark-rapids/pull/11309)|[Doc ]Update lore doc about the range [skip ci]|
|[#11292](https://github.com/NVIDIA/spark-rapids/pull/11292)|Add work around for string split with empty input.|
|[#11278](https://github.com/NVIDIA/spark-rapids/pull/11278)|Fix formatting of advanced configs doc|
|[#10917](https://github.com/NVIDIA/spark-rapids/pull/10917)|Adopt changes from JNI for casting from float to decimal|
|[#11269](https://github.com/NVIDIA/spark-rapids/pull/11269)|Revert "upgrade ucx to 1.17.0"|
|[#11260](https://github.com/NVIDIA/spark-rapids/pull/11260)|Mitigate intermittent test_buckets and shuffle_smoke_test OOM issue|
|[#11268](https://github.com/NVIDIA/spark-rapids/pull/11268)|Fix degenerate conditional nested loop join detection|
|[#11244](https://github.com/NVIDIA/spark-rapids/pull/11244)|Fix ArrayIndexOutOfBoundsException on join counts with constant join keys|
|[#11259](https://github.com/NVIDIA/spark-rapids/pull/11259)|CI Docker to support integration tests with Rocky OS + jdk17 [skip ci]|
|[#11247](https://github.com/NVIDIA/spark-rapids/pull/11247)|Fix `string_test.py` errors on Spark 4.0|
|[#11246](https://github.com/NVIDIA/spark-rapids/pull/11246)|Rework Maven Source Plugin Skip|
|[#11149](https://github.com/NVIDIA/spark-rapids/pull/11149)|Rework on substring index|
|[#11236](https://github.com/NVIDIA/spark-rapids/pull/11236)|Remove the unused vars from the version-def CI script|
|[#11237](https://github.com/NVIDIA/spark-rapids/pull/11237)|Fork jvm for maven-source-plugin|
|[#11200](https://github.com/NVIDIA/spark-rapids/pull/11200)|Multi-get_json_object|
|[#11230](https://github.com/NVIDIA/spark-rapids/pull/11230)|Skip test where Delta Lake may not be fully compatible with Spark|
|[#11220](https://github.com/NVIDIA/spark-rapids/pull/11220)|Avoid failing spark bug SPARK-44242 while generate run_dir|
|[#11226](https://github.com/NVIDIA/spark-rapids/pull/11226)|Fix auto merge conflict 11212|
|[#11129](https://github.com/NVIDIA/spark-rapids/pull/11129)|Spark 4: Fix miscellaneous tests including logic, repart, hive_delimited.|
|[#11163](https://github.com/NVIDIA/spark-rapids/pull/11163)|Support `MapFromArrays` on GPU|
|[#11219](https://github.com/NVIDIA/spark-rapids/pull/11219)|Fix hash_aggregate_test.py to run with ANSI enabled|
|[#11186](https://github.com/NVIDIA/spark-rapids/pull/11186)|from_json Json to Struct Exception Logging|
|[#11180](https://github.com/NVIDIA/spark-rapids/pull/11180)|More accurate estimation for the result serialization time in RapidsShuffleThreadedWriterBase|
|[#11194](https://github.com/NVIDIA/spark-rapids/pull/11194)|Fix ANSI mode test failures in url_test.py|
|[#11202](https://github.com/NVIDIA/spark-rapids/pull/11202)|Fix read from Delta Lake table with name column mapping and missing Parquet IDs|
|[#11185](https://github.com/NVIDIA/spark-rapids/pull/11185)|Fix multi-release jar problem|
|[#11144](https://github.com/NVIDIA/spark-rapids/pull/11144)|Build the Scala2.13 dist jar with JDK17|
|[#11197](https://github.com/NVIDIA/spark-rapids/pull/11197)|Fix class not found error: com/nvidia/spark/rapids/GpuScalar|
|[#11191](https://github.com/NVIDIA/spark-rapids/pull/11191)|Fix dynamic pruning regression in GpuFileSourceScanExec|
|[#10994](https://github.com/NVIDIA/spark-rapids/pull/10994)|Add Spark 4.0.0 Build Profile and Other Supporting Changes|
|[#11192](https://github.com/NVIDIA/spark-rapids/pull/11192)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#11179](https://github.com/NVIDIA/spark-rapids/pull/11179)|Allow more expressions to be tiered|
|[#11141](https://github.com/NVIDIA/spark-rapids/pull/11141)|Enable some Rapids config in RapidsSQLTestsBaseTrait for Spark UT|
|[#11170](https://github.com/NVIDIA/spark-rapids/pull/11170)|Avoid listFiles or inputFiles on relations with static partitioning|
|[#11159](https://github.com/NVIDIA/spark-rapids/pull/11159)|Drop spark31x shims|
|[#10951](https://github.com/NVIDIA/spark-rapids/pull/10951)|Case when performance improvement: reduce the `copy_if_else`|
|[#11165](https://github.com/NVIDIA/spark-rapids/pull/11165)|Fix some GpuBroadcastToRowExec by not dropping columns|
|[#11126](https://github.com/NVIDIA/spark-rapids/pull/11126)|Coalesce batches after a logical coalesce operation|
|[#11164](https://github.com/NVIDIA/spark-rapids/pull/11164)|fix the bucketed write error for non-utc cases|
|[#11132](https://github.com/NVIDIA/spark-rapids/pull/11132)|Add deletion vector metrics for low shuffle merge.|
|[#11156](https://github.com/NVIDIA/spark-rapids/pull/11156)|Fix batch splitting for partition column size on row-count-only batches|
|[#11153](https://github.com/NVIDIA/spark-rapids/pull/11153)|Fix LORE dump oom.|
|[#11102](https://github.com/NVIDIA/spark-rapids/pull/11102)|Fix ANSI mode failures in subquery_test.py|
|[#11151](https://github.com/NVIDIA/spark-rapids/pull/11151)|Fix the test error of the bucketed write for the non-utc case|
|[#11147](https://github.com/NVIDIA/spark-rapids/pull/11147)|upgrade ucx to 1.17.0|
|[#11138](https://github.com/NVIDIA/spark-rapids/pull/11138)|Update fastparquet to 2024.5.0 for numpy2 compatibility|
|[#11137](https://github.com/NVIDIA/spark-rapids/pull/11137)|Handle the change for UnaryPositive now extending RuntimeReplaceable|
|[#11094](https://github.com/NVIDIA/spark-rapids/pull/11094)|Add `HiveHash` support on GPU|
|[#11139](https://github.com/NVIDIA/spark-rapids/pull/11139)|Improve MetricsSuite to allow more gc jitter|
|[#11133](https://github.com/NVIDIA/spark-rapids/pull/11133)|Fix `test_window_group_limits_fallback`|
|[#11097](https://github.com/NVIDIA/spark-rapids/pull/11097)|Fix miscellaneous integ tests for Spark 4|
|[#11118](https://github.com/NVIDIA/spark-rapids/pull/11118)|Fix issue with DPP and AQE on reused broadcast exchanges|
|[#11043](https://github.com/NVIDIA/spark-rapids/pull/11043)|Dataproc serverless test fixes|
|[#10965](https://github.com/NVIDIA/spark-rapids/pull/10965)|Profiler: Disable collecting async allocation events by default|
|[#11117](https://github.com/NVIDIA/spark-rapids/pull/11117)|Update Scala2.13 premerge CI against JDK17|
|[#11084](https://github.com/NVIDIA/spark-rapids/pull/11084)|Introduce LORE framework.|
|[#11099](https://github.com/NVIDIA/spark-rapids/pull/11099)|Spark 4: Handle ANSI mode in sort_test.py|
|[#11115](https://github.com/NVIDIA/spark-rapids/pull/11115)|Fix match error in RapidsShuffleIterator.scala [scala2.13]|
|[#11088](https://github.com/NVIDIA/spark-rapids/pull/11088)|Support regex patterns with brackets when rewriting to PrefixRange pattern in rlike.|
|[#10950](https://github.com/NVIDIA/spark-rapids/pull/10950)|Add a heuristic to skip second or third agg pass|
|[#11048](https://github.com/NVIDIA/spark-rapids/pull/11048)|Fixed array_tests for Spark 4.0.0|
|[#11049](https://github.com/NVIDIA/spark-rapids/pull/11049)|Fix some cast_tests for Spark 4.0.0|
|[#11066](https://github.com/NVIDIA/spark-rapids/pull/11066)|Replaced spark3xx-common references to spark-shared|
|[#11083](https://github.com/NVIDIA/spark-rapids/pull/11083)|Exclude a case based on JDK version in Spark UT|
|[#10997](https://github.com/NVIDIA/spark-rapids/pull/10997)|Fix some test issues in Spark UT and keep RapidsTestSettings update-to-date|
|[#11073](https://github.com/NVIDIA/spark-rapids/pull/11073)|Disable ANSI mode for window function tests|
|[#11076](https://github.com/NVIDIA/spark-rapids/pull/11076)|Improve the diagnostics for 'conv' fallback explain|
|[#11092](https://github.com/NVIDIA/spark-rapids/pull/11092)|Add GpuBucketingUtils shim to Spark 4.0.0|
|[#11062](https://github.com/NVIDIA/spark-rapids/pull/11062)|fix duplicate counted metrics like op time for GpuCoalesceBatches|
|[#11044](https://github.com/NVIDIA/spark-rapids/pull/11044)|Fixed Failing tests in arithmetic_ops_tests for Spark 4.0.0|
|[#11086](https://github.com/NVIDIA/spark-rapids/pull/11086)|upgrade blossom-ci actions version [skip ci]|
|[#10957](https://github.com/NVIDIA/spark-rapids/pull/10957)|Support bucketing write for GPU|
|[#10979](https://github.com/NVIDIA/spark-rapids/pull/10979)|[FEA] Introduce low shuffle merge.|
|[#10996](https://github.com/NVIDIA/spark-rapids/pull/10996)|Fallback non-UTC TimeZoneAwareExpression with zoneId|
|[#11072](https://github.com/NVIDIA/spark-rapids/pull/11072)|Workaround numpy2 failed fastparquet compatibility tests|
|[#11046](https://github.com/NVIDIA/spark-rapids/pull/11046)|Calculate parallelism to speed up pre-merge CI|
|[#11054](https://github.com/NVIDIA/spark-rapids/pull/11054)|fix flaky array_item test failures|
|[#11051](https://github.com/NVIDIA/spark-rapids/pull/11051)|[FEA] Increase parallelism of deltalake test on databricks|
|[#10993](https://github.com/NVIDIA/spark-rapids/pull/10993)|`binary-dedupe` changes for Spark 4.0.0|
|[#11060](https://github.com/NVIDIA/spark-rapids/pull/11060)|Add in the ability to fingerprint JSON columns|
|[#11059](https://github.com/NVIDIA/spark-rapids/pull/11059)|Revert "Add in the ability to fingerprint JSON columns (#11002)" [skip ci]|
|[#11039](https://github.com/NVIDIA/spark-rapids/pull/11039)|Concat() Exception bug fix|
|[#11002](https://github.com/NVIDIA/spark-rapids/pull/11002)|Add in the ability to fingerprint JSON columns|
|[#10977](https://github.com/NVIDIA/spark-rapids/pull/10977)|Rewrite multiple literal choice regex to multiple contains in rlike|
|[#11035](https://github.com/NVIDIA/spark-rapids/pull/11035)|Fix auto merge conflict 11034 [skip ci]|
|[#11040](https://github.com/NVIDIA/spark-rapids/pull/11040)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#11036](https://github.com/NVIDIA/spark-rapids/pull/11036)|Update blossom-ci ACL to secure format [skip ci]|
|[#11032](https://github.com/NVIDIA/spark-rapids/pull/11032)|Fix a hive write test failure for Spark 350|
|[#10998](https://github.com/NVIDIA/spark-rapids/pull/10998)|Improve log to print more lines in build [skip ci]|
|[#10992](https://github.com/NVIDIA/spark-rapids/pull/10992)|Addressing the Named Parameter change in Spark 4.0.0|
|[#10943](https://github.com/NVIDIA/spark-rapids/pull/10943)|Fix Spark UT issues in RapidsDataFrameAggregateSuite|
|[#10963](https://github.com/NVIDIA/spark-rapids/pull/10963)|Add rapids configs to enable GPU running in Spark UT|
|[#10978](https://github.com/NVIDIA/spark-rapids/pull/10978)|More compilation fixes for Spark 4.0.0|
|[#10953](https://github.com/NVIDIA/spark-rapids/pull/10953)|Speed up the integration tests by running them in parallel on the Databricks cluster|
|[#10958](https://github.com/NVIDIA/spark-rapids/pull/10958)|Fix a hive write test failure|
|[#10970](https://github.com/NVIDIA/spark-rapids/pull/10970)|Move Support for `RaiseError` to a Shim Excluding Spark 4.0.0|
|[#10966](https://github.com/NVIDIA/spark-rapids/pull/10966)|Add default value for REF of premerge jenkinsfile to avoid bad overwritten [skip ci]|
|[#10959](https://github.com/NVIDIA/spark-rapids/pull/10959)|Add new ID to blossom-ci allow list [skip ci]|
|[#10952](https://github.com/NVIDIA/spark-rapids/pull/10952)|Add shims to take care of the signature change for writeUDFs in PythonUDFRunner|
|[#10931](https://github.com/NVIDIA/spark-rapids/pull/10931)|Add Support for Renaming of PythonMapInArrow|
|[#10949](https://github.com/NVIDIA/spark-rapids/pull/10949)|Change dependency version to 24.08.0-SNAPSHOT|
|[#10857](https://github.com/NVIDIA/spark-rapids/pull/10857)|[Spark 4.0] Account for `PartitionedFileUtil.splitFiles` signature change.|
|[#10912](https://github.com/NVIDIA/spark-rapids/pull/10912)|GpuInsertIntoHiveTable supports parquet format|
|[#10863](https://github.com/NVIDIA/spark-rapids/pull/10863)|[Spark 4.0] Account for `CommandUtils.uncacheTableOrView` signature change.|
|[#10944](https://github.com/NVIDIA/spark-rapids/pull/10944)|Added Shim for BatchScanExec to Support Spark 4.0|
|[#10946](https://github.com/NVIDIA/spark-rapids/pull/10946)|Unarchive Spark test jar for spark.read(ability)|
|[#10945](https://github.com/NVIDIA/spark-rapids/pull/10945)|Add Support for Multiple Filtering Keys for Subquery Broadcast|
|[#10871](https://github.com/NVIDIA/spark-rapids/pull/10871)|Add classloader diagnostics to initShuffleManager error message|
|[#10933](https://github.com/NVIDIA/spark-rapids/pull/10933)|Fixed Databricks build|
|[#10929](https://github.com/NVIDIA/spark-rapids/pull/10929)|Append new authorized user to blossom-ci whitelist [skip ci]|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
