# Change log
Generated on 2024-10-31

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
|[#11558](https://github.com/NVIDIA/spark-rapids/issues/11558)|[BUG] test_sortmerge_join_ridealong fails on DB 13.3|
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
|[#11676](https://github.com/NVIDIA/spark-rapids/pull/11676)| Fix race condition with Parquet filter pushdown modifying shared hadoop Configuration|
|[#11626](https://github.com/NVIDIA/spark-rapids/pull/11626)|Update latest changelog [skip ci]|
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

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
