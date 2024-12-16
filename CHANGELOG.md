# Change log
Generated on 2024-12-16

## Release 24.12

### Features
|||
|:---|:---|
|[#11630](https://github.com/NVIDIA/spark-rapids/issues/11630)|[FEA] enable from_json and json scan by default|
|[#11709](https://github.com/NVIDIA/spark-rapids/issues/11709)|[FEA] Add support for `MonthsBetween`|
|[#11666](https://github.com/NVIDIA/spark-rapids/issues/11666)|[FEA] support task limit profiling for specified stages|
|[#11662](https://github.com/NVIDIA/spark-rapids/issues/11662)|[FEA] Support Apache Spark 3.4.4|
|[#11657](https://github.com/NVIDIA/spark-rapids/issues/11657)|[FEA] Support format 'yyyyMMdd HH:mm:ss' for legacy mode|
|[#11419](https://github.com/NVIDIA/spark-rapids/issues/11419)|[FEA] Support Spark 3.5.3 release|
|[#11505](https://github.com/NVIDIA/spark-rapids/issues/11505)|[FEA] Support yyyymmdd format for GetTimestamp for LEGACY mode.|

### Performance
|||
|:---|:---|
|[#8391](https://github.com/NVIDIA/spark-rapids/issues/8391)|[FEA] Do a hash based re-partition instead of a sort based fallback for hash aggregate|
|[#11560](https://github.com/NVIDIA/spark-rapids/issues/11560)|[FEA] Improve `GpuJsonToStructs` performance|
|[#11458](https://github.com/NVIDIA/spark-rapids/issues/11458)|[FEA] enable prune_columns for from_json|

### Bugs Fixed
|||
|:---|:---|
|[#10907](https://github.com/NVIDIA/spark-rapids/issues/10907)|from_json function parses a column containing an empty array, throws an exception.|
|[#11793](https://github.com/NVIDIA/spark-rapids/issues/11793)|[BUG] "Time in Heuristic" should not include previous operator's compute time|
|[#11798](https://github.com/NVIDIA/spark-rapids/issues/11798)|[BUG] mismatch CPU and GPU result in test_months_between_first_day[DATAGEN_SEED=1733006411, TZ=Africa/Casablanca]|
|[#11790](https://github.com/NVIDIA/spark-rapids/issues/11790)|[BUG] test_hash_* failed "java.util.NoSuchElementException: head of empty list" or "Too many times of repartition, may hit a bug?"|
|[#11643](https://github.com/NVIDIA/spark-rapids/issues/11643)|[BUG] Support AQE with Broadcast Hash Join and DPP on Databricks 14.3|
|[#10910](https://github.com/NVIDIA/spark-rapids/issues/10910)|from_json, when input = empty object, rapids throws an exception.|
|[#10891](https://github.com/NVIDIA/spark-rapids/issues/10891)|Parsing a column containing invalid json into StructureType with schema throws an Exception.|
|[#11741](https://github.com/NVIDIA/spark-rapids/issues/11741)|[BUG] Fix spark400 build due to writeWithV1 return value change|
|[#11533](https://github.com/NVIDIA/spark-rapids/issues/11533)|Fix JSON Matrix tests on Databricks 14.3|
|[#11722](https://github.com/NVIDIA/spark-rapids/issues/11722)|[BUG] Spark 4.0.0 has moved `NullIntolerant` and builds are breaking because they are unable to find it.|
|[#11726](https://github.com/NVIDIA/spark-rapids/issues/11726)|[BUG] Databricks 14.3 nightly deploy fails due to incorrect DB_SHIM_NAME|
|[#11293](https://github.com/NVIDIA/spark-rapids/issues/11293)|[BUG] A user query with from_json failed with "JSON Parser encountered an invalid format at location"|
|[#9592](https://github.com/NVIDIA/spark-rapids/issues/9592)|[BUG][JSON] `from_json` to Map type should produce null for invalid entries|
|[#11715](https://github.com/NVIDIA/spark-rapids/issues/11715)|[BUG] parquet_testing_test.py failed on "AssertionError: GPU and CPU boolean values are different"|
|[#11716](https://github.com/NVIDIA/spark-rapids/issues/11716)|[BUG] delta_lake_write_test.py failed on "AssertionError: GPU and CPU boolean values are different"|
|[#11684](https://github.com/NVIDIA/spark-rapids/issues/11684)|[BUG] 24.12 Precommit fails with wrong number of arguments in `GpuDataSource`|
|[#11168](https://github.com/NVIDIA/spark-rapids/issues/11168)|[BUG] reserve allocation should be displayed when erroring due to lack of memory on startup|
|[#7585](https://github.com/NVIDIA/spark-rapids/issues/7585)|[BUG] [Regexp] Line anchor '$' incorrect matching of unicode line terminators|
|[#11622](https://github.com/NVIDIA/spark-rapids/issues/11622)|[BUG] GPU Parquet scan filter pushdown fails with timestamp/INT96 column|
|[#11646](https://github.com/NVIDIA/spark-rapids/issues/11646)|[BUG] NullPointerException in GpuRand|
|[#10498](https://github.com/NVIDIA/spark-rapids/issues/10498)|[BUG] Unit tests failed: [INTERVAL_ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and return NULL instead|
|[#11659](https://github.com/NVIDIA/spark-rapids/issues/11659)|[BUG] parse_url throws exception if partToExtract is invalid while Spark returns null|
|[#10894](https://github.com/NVIDIA/spark-rapids/issues/10894)|Parsing a column containing a nested structure to json thows an exception|
|[#10895](https://github.com/NVIDIA/spark-rapids/issues/10895)|Converting a column containing a map into json throws an exception|
|[#10896](https://github.com/NVIDIA/spark-rapids/issues/10896)|Converting an column containing an array into json throws an exception|
|[#10915](https://github.com/NVIDIA/spark-rapids/issues/10915)|to_json when converts an array will throw an exception:|
|[#10916](https://github.com/NVIDIA/spark-rapids/issues/10916)|to_json function doesn't support map[string, struct] to json conversion.|
|[#10919](https://github.com/NVIDIA/spark-rapids/issues/10919)|to_json converting map[string, integer] to json, throws an exception|
|[#10920](https://github.com/NVIDIA/spark-rapids/issues/10920)|to_json converting an array with maps throws an exception.|
|[#10921](https://github.com/NVIDIA/spark-rapids/issues/10921)|to_json - array with single map|
|[#10923](https://github.com/NVIDIA/spark-rapids/issues/10923)|[BUG] Spark UT framework: to_json function to convert the array with a single empty row to a JSON string throws an exception.|
|[#10924](https://github.com/NVIDIA/spark-rapids/issues/10924)|[BUG] Spark UT framework: to_json when converts an empty array into json throws an exception. |
|[#11024](https://github.com/NVIDIA/spark-rapids/issues/11024)|Fix tests failures in parquet_write_test.py|
|[#11174](https://github.com/NVIDIA/spark-rapids/issues/11174)|Opcode Suite fails for Scala 2.13.8+ |
|[#10483](https://github.com/NVIDIA/spark-rapids/issues/10483)|[BUG] JsonToStructs fails to parse all empty dicts and invalid lines|
|[#10489](https://github.com/NVIDIA/spark-rapids/issues/10489)|[BUG] from_json does not support input with \n in it.|
|[#10347](https://github.com/NVIDIA/spark-rapids/issues/10347)|[BUG] Failures in Integration Tests on Dataproc Serverless|
|[#11021](https://github.com/NVIDIA/spark-rapids/issues/11021)|Fix tests failures in orc_cast_test.py|
|[#11609](https://github.com/NVIDIA/spark-rapids/issues/11609)|[BUG] test_hash_repartition_long_overflow_ansi_exception failed on 341DB|
|[#11600](https://github.com/NVIDIA/spark-rapids/issues/11600)|[BUG] regex_test failed mismatched cpu and gpu values in UT and IT|
|[#11611](https://github.com/NVIDIA/spark-rapids/issues/11611)|[BUG] Spark 4.0 build failure - value cannotSaveIntervalIntoExternalStorageError is not a member of object org.apache.spark.sql.errors.QueryCompilationErrors|
|[#10922](https://github.com/NVIDIA/spark-rapids/issues/10922)|from_json cannot support line separator in the input string.|
|[#11009](https://github.com/NVIDIA/spark-rapids/issues/11009)|Fix tests failures in cast_test.py|
|[#11572](https://github.com/NVIDIA/spark-rapids/issues/11572)|[BUG] MultiFileReaderThreadPool may flood the console with log messages|

### PRs
|||
|:---|:---|
|[#11849](https://github.com/NVIDIA/spark-rapids/pull/11849)|Update rapids JNI and private dependency to 24.12.0|
|[#11857](https://github.com/NVIDIA/spark-rapids/pull/11857)|Increase the pre-merge CI timeout to 6 hours|
|[#11845](https://github.com/NVIDIA/spark-rapids/pull/11845)|Fix leak in isTimeStamp|
|[#11823](https://github.com/NVIDIA/spark-rapids/pull/11823)|Fix for `LEAD/LAG` window function test failures.|
|[#11832](https://github.com/NVIDIA/spark-rapids/pull/11832)|Fix leak in GpuBroadcastNestedLoopJoinExecBase|
|[#11763](https://github.com/NVIDIA/spark-rapids/pull/11763)|Orc writes don't fully support Booleans with nulls |
|[#11794](https://github.com/NVIDIA/spark-rapids/pull/11794)|exclude previous operator's time out of firstBatchHeuristic|
|[#11802](https://github.com/NVIDIA/spark-rapids/pull/11802)|Fall back to CPU for non-UTC months_between|
|[#11792](https://github.com/NVIDIA/spark-rapids/pull/11792)|[BUG] Fix issue 11790|
|[#11768](https://github.com/NVIDIA/spark-rapids/pull/11768)|Fix `dpp_test.py` failures on  14.3|
|[#11752](https://github.com/NVIDIA/spark-rapids/pull/11752)|Ability to decompress snappy and zstd Parquet files via CPU|
|[#11777](https://github.com/NVIDIA/spark-rapids/pull/11777)|Append knoguchi22 to blossom-ci whitelist [skip ci]|
|[#11712](https://github.com/NVIDIA/spark-rapids/pull/11712)|repartition-based fallback for hash aggregate v3|
|[#11771](https://github.com/NVIDIA/spark-rapids/pull/11771)|Fix query hang when using rapids multithread shuffle manager with kudo|
|[#11759](https://github.com/NVIDIA/spark-rapids/pull/11759)|Avoid using StringBuffer in single-threaded methods.|
|[#11766](https://github.com/NVIDIA/spark-rapids/pull/11766)|Fix Kudo batch serializer to only read header in hasNext|
|[#11730](https://github.com/NVIDIA/spark-rapids/pull/11730)|Add support for asynchronous writing for parquet|
|[#11750](https://github.com/NVIDIA/spark-rapids/pull/11750)|Fix aqe_test failures on  14.3.|
|[#11753](https://github.com/NVIDIA/spark-rapids/pull/11753)|Enable JSON Scan and from_json by default|
|[#11733](https://github.com/NVIDIA/spark-rapids/pull/11733)|Print out the current attempt object when OOM inside a retry block|
|[#11618](https://github.com/NVIDIA/spark-rapids/pull/11618)|Execute `from_json` with struct schema using `JSONUtils.fromJSONToStructs`|
|[#11725](https://github.com/NVIDIA/spark-rapids/pull/11725)|host watermark metric|
|[#11746](https://github.com/NVIDIA/spark-rapids/pull/11746)|Remove batch size bytes limits|
|[#11723](https://github.com/NVIDIA/spark-rapids/pull/11723)|Add NVIDIA Copyright|
|[#11721](https://github.com/NVIDIA/spark-rapids/pull/11721)|Add a few more JSON tests for MAP<STRING,STRING>|
|[#11744](https://github.com/NVIDIA/spark-rapids/pull/11744)|Do not package the Databricks 14.3 shim into the dist jar [skip ci]|
|[#11724](https://github.com/NVIDIA/spark-rapids/pull/11724)|Integrate with kudo|
|[#11739](https://github.com/NVIDIA/spark-rapids/pull/11739)|Update to Spark 4.0 changing signature of SupportsV1Write.writeWithV1|
|[#11737](https://github.com/NVIDIA/spark-rapids/pull/11737)|Add in support for months_between|
|[#11700](https://github.com/NVIDIA/spark-rapids/pull/11700)|Fix leak with RapidsHostColumnBuilder in GpuUserDefinedFunction|
|[#11727](https://github.com/NVIDIA/spark-rapids/pull/11727)|Widen type promotion for decimals with larger scale in Parquet Read|
|[#11719](https://github.com/NVIDIA/spark-rapids/pull/11719)|Skip `from_json` overflow tests for  14.3|
|[#11708](https://github.com/NVIDIA/spark-rapids/pull/11708)|Support profiling for specific stages on a limited number of tasks|
|[#11731](https://github.com/NVIDIA/spark-rapids/pull/11731)|Add NullIntolerantShim to adapt to Spark 4.0 removing NullIntolerant|
|[#11413](https://github.com/NVIDIA/spark-rapids/pull/11413)|Support multi string contains|
|[#11728](https://github.com/NVIDIA/spark-rapids/pull/11728)|Change Databricks 14.3 shim name to spark350db143 [skip ci]|
|[#11702](https://github.com/NVIDIA/spark-rapids/pull/11702)|Improve JSON scan and `from_json`|
|[#11635](https://github.com/NVIDIA/spark-rapids/pull/11635)|Added Shims for adding Databricks 14.3 Support|
|[#11714](https://github.com/NVIDIA/spark-rapids/pull/11714)|Let AWS Databricks automatically choose an Availability Zone|
|[#11703](https://github.com/NVIDIA/spark-rapids/pull/11703)|Simplify $ transpiling and fix newline character bug|
|[#11707](https://github.com/NVIDIA/spark-rapids/pull/11707)|impalaFile cannot be found by UT framework. |
|[#11697](https://github.com/NVIDIA/spark-rapids/pull/11697)|Make delta-lake shim dependencies parametrizable|
|[#11710](https://github.com/NVIDIA/spark-rapids/pull/11710)|Add shim version 344 to LogicalPlanShims.scala|
|[#11706](https://github.com/NVIDIA/spark-rapids/pull/11706)|Add retry support in sub hash join|
|[#11673](https://github.com/NVIDIA/spark-rapids/pull/11673)|Fix Parquet Writer tests on  14.3|
|[#11669](https://github.com/NVIDIA/spark-rapids/pull/11669)|Fix `string_test` for  14.3|
|[#11692](https://github.com/NVIDIA/spark-rapids/pull/11692)|Add Spark 3.4.4 Shim |
|[#11695](https://github.com/NVIDIA/spark-rapids/pull/11695)|Fix spark400 build due to LogicalRelation signature changes|
|[#11689](https://github.com/NVIDIA/spark-rapids/pull/11689)|Update the Maven repository to download Spark JAR files [skip ci]|
|[#11670](https://github.com/NVIDIA/spark-rapids/pull/11670)|Fix `misc_expr_test` for  14.3|
|[#11652](https://github.com/NVIDIA/spark-rapids/pull/11652)|Fix skipping fixed_length_char ORC tests on  > 13.3|
|[#11644](https://github.com/NVIDIA/spark-rapids/pull/11644)|Skip AQE-join-DPP tests for  14.3|
|[#11667](https://github.com/NVIDIA/spark-rapids/pull/11667)|Preparation for the coming Kudo support|
|[#11685](https://github.com/NVIDIA/spark-rapids/pull/11685)|Exclude shimplify-generated files from scalastyle|
|[#11282](https://github.com/NVIDIA/spark-rapids/pull/11282)|Reserve allocation should be displayed when erroring due to lack of memory on startup|
|[#11671](https://github.com/NVIDIA/spark-rapids/pull/11671)|Use the new host memory allocation API|
|[#11682](https://github.com/NVIDIA/spark-rapids/pull/11682)|Fix auto merge conflict 11679 [skip ci]|
|[#11663](https://github.com/NVIDIA/spark-rapids/pull/11663)|Simplify Transpilation of $ with Extended Line Separator Support in cuDF Regex|
|[#11672](https://github.com/NVIDIA/spark-rapids/pull/11672)|Fix race condition with Parquet filter pushdown modifying shared hadoop Configuration|
|[#11596](https://github.com/NVIDIA/spark-rapids/pull/11596)|Add a new NVTX range for task GPU ownership|
|[#11664](https://github.com/NVIDIA/spark-rapids/pull/11664)|Fix `orc_write_test.py` for  14.3|
|[#11656](https://github.com/NVIDIA/spark-rapids/pull/11656)|[DOC] update the supported OS in download page [skip ci]|
|[#11665](https://github.com/NVIDIA/spark-rapids/pull/11665)|Generate classes identical up to the shim package name|
|[#11647](https://github.com/NVIDIA/spark-rapids/pull/11647)|Fix a NPE issue in GpuRand|
|[#11658](https://github.com/NVIDIA/spark-rapids/pull/11658)|Support format 'yyyyMMdd HH:mm:ss' for legacy mode|
|[#11661](https://github.com/NVIDIA/spark-rapids/pull/11661)|Support invalid partToExtract for parse_url|
|[#11520](https://github.com/NVIDIA/spark-rapids/pull/11520)|UT adjust override checkScanSchemata & enabling ut of exclude_by_suffix fea.|
|[#11634](https://github.com/NVIDIA/spark-rapids/pull/11634)|Put DF_UDF plugin code into the main uber jar.|
|[#11522](https://github.com/NVIDIA/spark-rapids/pull/11522)|UT adjust test SPARK-26677: negated null-safe equality comparison|
|[#11521](https://github.com/NVIDIA/spark-rapids/pull/11521)|Datetime rebasing issue fixed|
|[#11642](https://github.com/NVIDIA/spark-rapids/pull/11642)|Update to_json to be more generic and fix some bugs|
|[#11615](https://github.com/NVIDIA/spark-rapids/pull/11615)|Spark 4 parquet_writer_test.py fixes|
|[#11623](https://github.com/NVIDIA/spark-rapids/pull/11623)|Fix `collection_ops_test` for  14.3|
|[#11553](https://github.com/NVIDIA/spark-rapids/pull/11553)|Fix udf-compiler scala2.13 internal return statements|
|[#11640](https://github.com/NVIDIA/spark-rapids/pull/11640)|Disable date/timestamp types by default when parsing JSON|
|[#11570](https://github.com/NVIDIA/spark-rapids/pull/11570)|Add support for Spark 3.5.3|
|[#11591](https://github.com/NVIDIA/spark-rapids/pull/11591)|Spark UT framework: Read Parquet file generated by parquet-thrift Rapids, UT case adjust.|
|[#11631](https://github.com/NVIDIA/spark-rapids/pull/11631)|Update JSON tests based on a closed/fixed issues|
|[#11617](https://github.com/NVIDIA/spark-rapids/pull/11617)|Quick fix for the build script failure of Scala 2.13 jars [skip ci]|
|[#11614](https://github.com/NVIDIA/spark-rapids/pull/11614)|Ensure repartition overflow test always overflows|
|[#11612](https://github.com/NVIDIA/spark-rapids/pull/11612)|Revert "Disable regex tests to unblock CI (#11606)"|
|[#11597](https://github.com/NVIDIA/spark-rapids/pull/11597)|`install_deps` changes for Databricks 14.3|
|[#11608](https://github.com/NVIDIA/spark-rapids/pull/11608)|Use mvn -f scala2.13/ in the build scripts to build the 2.13 jars|
|[#11610](https://github.com/NVIDIA/spark-rapids/pull/11610)|Change DataSource calendar interval error to fix spark400 build|
|[#11549](https://github.com/NVIDIA/spark-rapids/pull/11549)|Adopt `JSONUtils.concatenateJsonStrings` for concatenating JSON strings|
|[#11595](https://github.com/NVIDIA/spark-rapids/pull/11595)|Remove an unused config shuffle.spillThreads|
|[#11606](https://github.com/NVIDIA/spark-rapids/pull/11606)|Disable regex tests to unblock CI|
|[#11605](https://github.com/NVIDIA/spark-rapids/pull/11605)|Fix auto merge conflict 11604 [skip ci]|
|[#11587](https://github.com/NVIDIA/spark-rapids/pull/11587)|avoid long tail tasks due to PrioritySemaphore, remaing part|
|[#11574](https://github.com/NVIDIA/spark-rapids/pull/11574)|avoid long tail tasks due to PrioritySemaphore|
|[#11559](https://github.com/NVIDIA/spark-rapids/pull/11559)|[Spark 4.0] Address test failures in cast_test.py|
|[#11579](https://github.com/NVIDIA/spark-rapids/pull/11579)|Fix merge conflict with branch-24.10|
|[#11571](https://github.com/NVIDIA/spark-rapids/pull/11571)|Log reconfigure multi-file thread pool only once|
|[#11564](https://github.com/NVIDIA/spark-rapids/pull/11564)|Disk spill metric|
|[#11561](https://github.com/NVIDIA/spark-rapids/pull/11561)|Add in a basic plugin for dataframe UDF support in Apache Spark|
|[#11563](https://github.com/NVIDIA/spark-rapids/pull/11563)|Fix the latest merge conflict in integration tests|
|[#11542](https://github.com/NVIDIA/spark-rapids/pull/11542)|Update rapids JNI and private dependency to 24.12.0-SNAPSHOT [skip ci]|
|[#11493](https://github.com/NVIDIA/spark-rapids/pull/11493)|Support legacy mode for yyyymmdd format|

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
|[#11683](https://github.com/NVIDIA/spark-rapids/pull/11683)|[DOC] update download page for 2410 hot fix release [skip ci]|
|[#11680](https://github.com/NVIDIA/spark-rapids/pull/11680)|Update latest changelog [skip ci]|
|[#11678](https://github.com/NVIDIA/spark-rapids/pull/11678)|Update version to 24.10.1-SNAPSHOT [skip ci]|
|[#11676](https://github.com/NVIDIA/spark-rapids/pull/11676)| Fix race condition with Parquet filter pushdown modifying shared hadoop Configuration|
|[#11626](https://github.com/NVIDIA/spark-rapids/pull/11626)|Update latest changelog [skip ci]|
|[#11624](https://github.com/NVIDIA/spark-rapids/pull/11624)|Update the download link [skip ci]|
|[#11577](https://github.com/NVIDIA/spark-rapids/pull/11577)|Update latest changelog [skip ci]|
|[#11576](https://github.com/NVIDIA/spark-rapids/pull/11576)|Update rapids JNI and private dependency to 24.10.0|
|[#11582](https://github.com/NVIDIA/spark-rapids/pull/11582)|[DOC] update doc for 24.10 release [skip ci]|
|[#11414](https://github.com/NVIDIA/spark-rapids/pull/11414)|Fix `collection_ops_tests` for Spark 4.0|
|[#11588](https://github.com/NVIDIA/spark-rapids/pull/11588)|backport fixes of #11573 to branch 24.10|
|[#11569](https://github.com/NVIDIA/spark-rapids/pull/11569)|Have "dump always" dump input files before trying to decode them|
|[#11544](https://github.com/NVIDIA/spark-rapids/pull/11544)|Update test case related to LEACY datetime format to unblock nightly CI|
|[#11567](https://github.com/NVIDIA/spark-rapids/pull/11567)|Fix test case unix_timestamp(col, 'yyyyMMdd') failed for Africa/Casablanca timezone and LEGACY mode|
|[#11519](https://github.com/NVIDIA/spark-rapids/pull/11519)|Spark 4:  Fix parquet_test.py|
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

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
