# Change log
Generated on 2025-08-05
## Release 25.04

### Features
|||
|:---|:---|
|[#5221](https://github.com/NVIDIA/spark-rapids/issues/5221)|[FEA] Support function array_distinct|
|[#5199](https://github.com/NVIDIA/spark-rapids/issues/5199)|[FEA]Support function approx_count_distinct|
|[#12367](https://github.com/NVIDIA/spark-rapids/issues/12367)|[FEA] Allow BigSizedJoinIterator#buildPartitioner to produce more subparittions to avoid CudfColumnSizeOverflowException|
|[#5224](https://github.com/NVIDIA/spark-rapids/issues/5224)|[FEA]Support array_position|
|[#12261](https://github.com/NVIDIA/spark-rapids/issues/12261)|[FEA] Enable Hive text writer by default|
|[#12297](https://github.com/NVIDIA/spark-rapids/issues/12297)|[FEA] Enable bucketed scan in Hybrid Scan|
|[#9161](https://github.com/NVIDIA/spark-rapids/issues/9161)|[AUDIT][SPARK-36612][SQL] Support left outer join build left or right outer join build right in shuffled hash join|
|[#4411](https://github.com/NVIDIA/spark-rapids/issues/4411)|[FEA] Add AST expression support for IsNull and IsNotNull|
|[#12187](https://github.com/NVIDIA/spark-rapids/issues/12187)|[FEA] Support AST with isNull function|
|[#12234](https://github.com/NVIDIA/spark-rapids/issues/12234)|[FEA] Support legacy mode for yyyy-mm-dd format|
|[#11607](https://github.com/NVIDIA/spark-rapids/issues/11607)|[FEA] it would be nice if we could support org.apache.spark.sql.catalyst.expressions.Slice|

### Performance
|||
|:---|:---|
|[#12256](https://github.com/NVIDIA/spark-rapids/issues/12256)|[FEA] BroadcastNestedLoopJoin does not support stream-side first adjustment|
|[#11386](https://github.com/NVIDIA/spark-rapids/issues/11386)|[FEA] move to multi-get_json_object for json_tuple|
|[#11999](https://github.com/NVIDIA/spark-rapids/issues/11999)|[FEA] HybridParquetScan: support asynchronous prefetching|
|[#11985](https://github.com/NVIDIA/spark-rapids/issues/11985)|[FEA] upgrade to ucx 1.18|
|[#11888](https://github.com/NVIDIA/spark-rapids/issues/11888)|[FEA] spill bounce buffer on disk->device and device->disk needs to be a pool|

### Bugs Fixed
|||
|:---|:---|
|[#12530](https://github.com/NVIDIA/spark-rapids/issues/12530)|[BUG] Outer join result is incorrect when Spark is 3.5.x, join side is outer side,  join on struct column and there is null|
|[#12410](https://github.com/NVIDIA/spark-rapids/issues/12410)|[BUG] ThrottlingExecutorSuite: test task metrics failed intermittently|
|[#12435](https://github.com/NVIDIA/spark-rapids/issues/12435)|[BUG] Running integration tests with `PERFILE` results in failed tests|
|[#12438](https://github.com/NVIDIA/spark-rapids/issues/12438)|[BUG] Incorrect supported ops CSV file generation|
|[#12360](https://github.com/NVIDIA/spark-rapids/issues/12360)|[BUG] delta_lake_test test_delta_deletion_vector cases failed in databricks 14.3 runtime|
|[#12123](https://github.com/NVIDIA/spark-rapids/issues/12123)|[BUG] delta_lake_update_test.test_delta_update_fallback_with_deletion_vectors failed assertion failed: Could not find RapidsDeltaWriteExec in the GPU plans with spark34Xshims|
|[#12405](https://github.com/NVIDIA/spark-rapids/issues/12405)|[BUG] test_delta_deletion_vector_fallback fails on [databricks] 14.3 CI|
|[#12460](https://github.com/NVIDIA/spark-rapids/issues/12460)|[BUG] Fallback to the CPU when FileSourceScan is reading Deletion Vectors on Databricks 14.3|
|[#12027](https://github.com/NVIDIA/spark-rapids/issues/12027)|[BUG] [DB 14.3] `tightBounds` stat in Delta Lake tables is set incorrectly|
|[#12379](https://github.com/NVIDIA/spark-rapids/issues/12379)|[BUG] test_parse_url_supported fails on [databricks] 14.3|
|[#12428](https://github.com/NVIDIA/spark-rapids/issues/12428)|[BUG] Multiple python udf integration test cases failed in DB 14.3|
|[#12408](https://github.com/NVIDIA/spark-rapids/issues/12408)|[BUG] Job timeout registration pathologically fails in some [databricks] CI_PART1 pipelines|
|[#12413](https://github.com/NVIDIA/spark-rapids/issues/12413)|[BUG] nightly shuffle multi-thread/UCX CI failed possibly out of memory or process/resource limits reached|
|[#12376](https://github.com/NVIDIA/spark-rapids/issues/12376)|[BUG] test_col_size_exceeding_cudf_limit fails on [databricks]|
|[#12378](https://github.com/NVIDIA/spark-rapids/issues/12378)|[BUG] test_parquet_partition_batch_row_count_only_splitting fails on [databricks] 14.3|
|[#12398](https://github.com/NVIDIA/spark-rapids/issues/12398)|[BUG] test_window_aggregate_udf_array_from_python fails on Spark 3.2.0|
|[#12158](https://github.com/NVIDIA/spark-rapids/issues/12158)|[BUG] split should not happen immediately after fallback on memory contention (OOM state machine bug) |
|[#12333](https://github.com/NVIDIA/spark-rapids/issues/12333)|[BUG]  Pandas UDF tests hang on Databricks 14.3|
|[#12331](https://github.com/NVIDIA/spark-rapids/issues/12331)|[BUG] Add timeouts to prevent hanging Pytests|
|[#12351](https://github.com/NVIDIA/spark-rapids/issues/12351)|[BUG] CudfColumnSizeOverflowException could cause misleading logs|
|[#12345](https://github.com/NVIDIA/spark-rapids/issues/12345)|[BUG] CoalescedHostResult should be spillable to avoid CPU OOM|
|[#12361](https://github.com/NVIDIA/spark-rapids/issues/12361)|[BUG] HybridExecutionUtils.useHybridScan fail to do type check correctly|
|[#12339](https://github.com/NVIDIA/spark-rapids/issues/12339)|[BUG] test_array_slice_with_negative_length and  test_array_slice_with_zero_start fail on Databricks 14.3|
|[#12332](https://github.com/NVIDIA/spark-rapids/issues/12332)|[BUG] Scala2.13 binary-dedupe fails with buildver 400 included|
|[#12228](https://github.com/NVIDIA/spark-rapids/issues/12228)|[BUG] spark.rapids.memory.gpu.pool=NONE is not respected with spark.rapids.sql.python.gpu.enabled=true|
|[#12314](https://github.com/NVIDIA/spark-rapids/issues/12314)|[BUG] Deprecated `spark.rapids.memory.gpu.pooling.enabled` overrules `spark.rapids.memory.gpu.pool`|
|[#11215](https://github.com/NVIDIA/spark-rapids/issues/11215)|[BUG] ai.rapids.cudf.CudfException: parallel_for failed: cudaErrorInvalidDevice: invalid device ordinal in ParquetChunkedReader|
|[#12326](https://github.com/NVIDIA/spark-rapids/issues/12326)|[BUG] orc_write_test.py::test_write_with_stripe_size_rows failed in HDFS env|
|[#12335](https://github.com/NVIDIA/spark-rapids/issues/12335)|[BUG] Iceberg loading should be disabled temporarily.|
|[#12062](https://github.com/NVIDIA/spark-rapids/issues/12062)|[BUG] Spark-4.0 build failure due to update in package name to org.apache.spark.sql.classic |
|[#12309](https://github.com/NVIDIA/spark-rapids/issues/12309)|[BUG] spark400 nightly failed multiple compilation issues|
|[#8268](https://github.com/NVIDIA/spark-rapids/issues/8268)|[BUG] NDS query 16 fails on EMR 6.10 with java.lang.ClassCastException|
|[#12319](https://github.com/NVIDIA/spark-rapids/issues/12319)|[BUG] leak in GpuSubPartitionHashJoin|
|[#12316](https://github.com/NVIDIA/spark-rapids/issues/12316)|[BUG] build fail TrampolineUtil is not a member of package ...sql.rapids.execution of integration_tests module|
|[#12245](https://github.com/NVIDIA/spark-rapids/issues/12245)|[BUG] Bloop project import for VSCode/Metals fails for Databricks Maven profiles|
|[#11735](https://github.com/NVIDIA/spark-rapids/issues/11735)|[BUG] GPU file writes only test writing a single row group or stripe|
|[#12277](https://github.com/NVIDIA/spark-rapids/issues/12277)|[BUG] QueryExecutionErrors.unexpectedValueForLengthInFunctionError being changed after Spark400+|
|[#12274](https://github.com/NVIDIA/spark-rapids/issues/12274)|[BUG] Databricks350+ has different interface on unexpectedValue-like SparkException|
|[#12267](https://github.com/NVIDIA/spark-rapids/issues/12267)|[BUG] HybridScan: Filter elimination may fail when AQE is enabled|
|[#12260](https://github.com/NVIDIA/spark-rapids/issues/12260)|[BUG] Databricks nightly build fails with "object roaringbitmap is not a member"|
|[#11385](https://github.com/NVIDIA/spark-rapids/issues/11385)|[BUG] get_json_object and json_tuple do not match on escaped characters in name|
|[#12164](https://github.com/NVIDIA/spark-rapids/issues/12164)|[BUG] Method mergeOnHost is deprecated in KudoSerializer|
|[#12047](https://github.com/NVIDIA/spark-rapids/issues/12047)|[BUG]  [DB 14.3] `numRecords` stat in Delta Lake tables is not set on GPU|
|[#12211](https://github.com/NVIDIA/spark-rapids/issues/12211)|[BUG] Deadlock during spill|
|[#12207](https://github.com/NVIDIA/spark-rapids/issues/12207)|[BUG] Build error in premerge on Spark 400 due to `Strategy` being not found|
|[#12147](https://github.com/NVIDIA/spark-rapids/issues/12147)|[BUG] test_min_max_in_groupby_and_reduction failed End address is too high for getInt when kudo_enabled:true intermittently|
|[#12056](https://github.com/NVIDIA/spark-rapids/issues/12056)|[TEST BUG] CollectLimit falling off of GPU on Databricks 14.3|
|[#12137](https://github.com/NVIDIA/spark-rapids/issues/12137)|[BUG] rapids_databricks_nightly-dev-github fails on DB 14.3 with RapidsShuffleManager class not found|
|[#12100](https://github.com/NVIDIA/spark-rapids/issues/12100)|[BUG] Potential duplicate call to the `close` method of a SpillableHostConcatResult in sized hash join.|
|[#12087](https://github.com/NVIDIA/spark-rapids/issues/12087)|[BUG] Nightly tests fail for Databricks 14.3 on branch-25.04|
|[#12020](https://github.com/NVIDIA/spark-rapids/issues/12020)|[BUG] mortgage_test.py failing on Databricks 14.3|
|[#11990](https://github.com/NVIDIA/spark-rapids/issues/11990)|[BUG] row-based_udf_test.py::test_hive_empty_* fail for Databricks 14.3|
|[#11988](https://github.com/NVIDIA/spark-rapids/issues/11988)|[BUG] datasourcev2_read_test failure on Databricks 14.3|
|[#12074](https://github.com/NVIDIA/spark-rapids/issues/12074)|[BUG] Build broken on Databricks 14.3 after #11996|

### PRs
|||
|:---|:---|
|[#12543](https://github.com/NVIDIA/spark-rapids/pull/12543)|Update changelog for the v25.04 release [skip ci]|
|[#12535](https://github.com/NVIDIA/spark-rapids/pull/12535)|Fix bug when join side is outer side|
|[#12494](https://github.com/NVIDIA/spark-rapids/pull/12494)|Update changelog for v25.04.0 release [skip ci]|
|[#12497](https://github.com/NVIDIA/spark-rapids/pull/12497)|[DOC] update the download page for 2504 release [skip ci]|
|[#12473](https://github.com/NVIDIA/spark-rapids/pull/12473)|Update dependency version JNI, private, hybrid to 25.04.0|
|[#12485](https://github.com/NVIDIA/spark-rapids/pull/12485)|Enable the  14.3 Shim|
|[#12490](https://github.com/NVIDIA/spark-rapids/pull/12490)|Fix bug where rows dropped if partitioned column size is too large|
|[#12481](https://github.com/NVIDIA/spark-rapids/pull/12481)|Fix incorrect supported DataSources in generated tools generated files|
|[#12445](https://github.com/NVIDIA/spark-rapids/pull/12445)|Tag UpdateCommand to fallback to the CPU when deletion vectors are enabled on Databricks 14.3|
|[#12488](https://github.com/NVIDIA/spark-rapids/pull/12488)|xfail json_matrix test on Databricks 14.3|
|[#12444](https://github.com/NVIDIA/spark-rapids/pull/12444)|Tag DeleteCommand to fallback to the CPU when deletion vectors are enabled on Databricks 14.3|
|[#12462](https://github.com/NVIDIA/spark-rapids/pull/12462)|Fallback GpuDeltaParquetFileFormat to CPU in presence of deletion vectors|
|[#12451](https://github.com/NVIDIA/spark-rapids/pull/12451)|Match logic of DV enabling in Gpu Stats Collection|
|[#12456](https://github.com/NVIDIA/spark-rapids/pull/12456)|Fix the Pandas UDF test failures on DB14.3|
|[#12464](https://github.com/NVIDIA/spark-rapids/pull/12464)|Disable HLLPP precision 4 due to cuCollection bug|
|[#12448](https://github.com/NVIDIA/spark-rapids/pull/12448)|Ignore flaky "test task metrics" from ThrottlingExecutorSuite|
|[#12426](https://github.com/NVIDIA/spark-rapids/pull/12426)|Use a single listener for all timeout fixture invocations|
|[#12397](https://github.com/NVIDIA/spark-rapids/pull/12397)|Set the `spark.rapids.sql.format.parquet.reader.type` to `AUTO` even on Databricks 14.3|
|[#12415](https://github.com/NVIDIA/spark-rapids/pull/12415)|Use 1hr as the default Spark action timeout|
|[#12419](https://github.com/NVIDIA/spark-rapids/pull/12419)|WAR: hardcode fsspec==2025.3.0 [skip ci]|
|[#12312](https://github.com/NVIDIA/spark-rapids/pull/12312)|fix premature concensus on "all blocked" in OOM state machine|
|[#12404](https://github.com/NVIDIA/spark-rapids/pull/12404)|Remove special timeout for udf_test|
|[#12372](https://github.com/NVIDIA/spark-rapids/pull/12372)|Allow BigSizedJoinIterator#buildPartitioner to produce more subparittions|
|[#12224](https://github.com/NVIDIA/spark-rapids/pull/12224)|add test case for issue 12158|
|[#12383](https://github.com/NVIDIA/spark-rapids/pull/12383)|Fix a hanging issue for Python UDF|
|[#11638](https://github.com/NVIDIA/spark-rapids/pull/11638)|Add support for Hyper Log Log PLus Plus(HLL++)|
|[#12346](https://github.com/NVIDIA/spark-rapids/pull/12346)|Implement Job-level timeout for pytests to avoid forever hangs|
|[#12380](https://github.com/NVIDIA/spark-rapids/pull/12380)|add doc for ai.rapids.memory.bookkeep|
|[#12352](https://github.com/NVIDIA/spark-rapids/pull/12352)|Refine split-retry logs to expose the real reason|
|[#12338](https://github.com/NVIDIA/spark-rapids/pull/12338)|print spillable summary on bookkeep|
|[#12375](https://github.com/NVIDIA/spark-rapids/pull/12375)|Updating link to RapidsUDF.java|
|[#12349](https://github.com/NVIDIA/spark-rapids/pull/12349)|Make KudoHostMergeResultWrapper spillable|
|[#12362](https://github.com/NVIDIA/spark-rapids/pull/12362)|HybridScan: a hotfix over previous changes on HybridExecutionUtils.useHybridScan|
|[#12363](https://github.com/NVIDIA/spark-rapids/pull/12363)|Added Databricks 14.3 Specific Error Messages|
|[#12357](https://github.com/NVIDIA/spark-rapids/pull/12357)|Add lore replay doc|
|[#12356](https://github.com/NVIDIA/spark-rapids/pull/12356)|Add 350db143 as supported by spark320 SparkSessionUtils|
|[#12325](https://github.com/NVIDIA/spark-rapids/pull/12325)|Add option to dump kudo tables for debugging|
|[#12342](https://github.com/NVIDIA/spark-rapids/pull/12342)|Shim functions to fix binary-dedupe failures on Spark-4.0|
|[#12238](https://github.com/NVIDIA/spark-rapids/pull/12238)|Add Limited Read Support for Deletion Vectors on Databricks 14.3|
|[#12344](https://github.com/NVIDIA/spark-rapids/pull/12344)|HybridScan: Log fallback reason when fallbacks to GpuScan|
|[#12308](https://github.com/NVIDIA/spark-rapids/pull/12308)|Add support for `org.apache.spark.sql.catalyst.expressions.ArrayPosition`|
|[#12317](https://github.com/NVIDIA/spark-rapids/pull/12317)|Always respect the new conf for RMM allocation mode|
|[#12334](https://github.com/NVIDIA/spark-rapids/pull/12334)|Run `test_write_with_stripe_size_rows` only on apache runtime and databricks runtime|
|[#12336](https://github.com/NVIDIA/spark-rapids/pull/12336)|Disable iceberg load temporarily|
|[#12313](https://github.com/NVIDIA/spark-rapids/pull/12313)|Fix Spark-4.0 build errors for tests module.|
|[#12248](https://github.com/NVIDIA/spark-rapids/pull/12248)|Reenable JsonTuple|
|[#12328](https://github.com/NVIDIA/spark-rapids/pull/12328)|Fix empty broadcast conversion|
|[#12324](https://github.com/NVIDIA/spark-rapids/pull/12324)|print the count of threads being blocked when OOM is thrown from retr…|
|[#12330](https://github.com/NVIDIA/spark-rapids/pull/12330)|[Doc] Update doc: Hybrid execution limitations [skip ci]|
|[#12303](https://github.com/NVIDIA/spark-rapids/pull/12303)|Support group by binary|
|[#12320](https://github.com/NVIDIA/spark-rapids/pull/12320)|Fix leaks when we re-partition in GpuSubPartitionHashJoin|
|[#12318](https://github.com/NVIDIA/spark-rapids/pull/12318)|Fix integration tests compilation due to missing function in TrampolineUtil|
|[#12249](https://github.com/NVIDIA/spark-rapids/pull/12249)|Fix bloop project generation on|
|[#12286](https://github.com/NVIDIA/spark-rapids/pull/12286)|short term solution for PinnedMemoryPool issue|
|[#12310](https://github.com/NVIDIA/spark-rapids/pull/12310)|Support more types for a Seq to dump the total size when an OOM happens|
|[#12300](https://github.com/NVIDIA/spark-rapids/pull/12300)|Fix Spark-4.0 build errors for sql-plugin, datagen and integration_tests module|
|[#11743](https://github.com/NVIDIA/spark-rapids/pull/11743)|Add test for ORC write with two stripes|
|[#12262](https://github.com/NVIDIA/spark-rapids/pull/12262)|Enable GPU acceleration for Hive delimited text write|
|[#12288](https://github.com/NVIDIA/spark-rapids/pull/12288)|Update spark400 version to 4.0.1-SNAPSHOT|
|[#12292](https://github.com/NVIDIA/spark-rapids/pull/12292)|HybridScan: Enable bucketed read|
|[#12285](https://github.com/NVIDIA/spark-rapids/pull/12285)|Support build side is join side for outer join|
|[#12296](https://github.com/NVIDIA/spark-rapids/pull/12296)|Fix auto merge conflict 12293 [skip ci]|
|[#12290](https://github.com/NVIDIA/spark-rapids/pull/12290)|Update supported exprs and docs to fix build error [skip ci]|
|[#12287](https://github.com/NVIDIA/spark-rapids/pull/12287)|Disable kudo by default|
|[#12178](https://github.com/NVIDIA/spark-rapids/pull/12178)|Update ExprChecks for RowNumber.|
|[#12221](https://github.com/NVIDIA/spark-rapids/pull/12221)|Add support for `ORC` stripe cudf size configs|
|[#12257](https://github.com/NVIDIA/spark-rapids/pull/12257)|Materialize the stream side first for BroadcastNestedLoopJoins|
|[#12283](https://github.com/NVIDIA/spark-rapids/pull/12283)|Fix an API change issue for DB350+|
|[#12192](https://github.com/NVIDIA/spark-rapids/pull/12192)|Support HiveHash in GPU partitioning|
|[#12268](https://github.com/NVIDIA/spark-rapids/pull/12268)|HybridScan: fix Filter Elimination under AQE|
|[#12270](https://github.com/NVIDIA/spark-rapids/pull/12270)|Add AST expression support for `IsNull` and `IsNotNull`|
|[#12269](https://github.com/NVIDIA/spark-rapids/pull/12269)|Fix auto merge conflict 12265 [skip ci]|
|[#12264](https://github.com/NVIDIA/spark-rapids/pull/12264)|Fix missing roaring bitmap dependency|
|[#12241](https://github.com/NVIDIA/spark-rapids/pull/12241)|Add validation for `start` and `length` parameters for `slice`|
|[#12251](https://github.com/NVIDIA/spark-rapids/pull/12251)|Support to retry the host allocation for hybrid converters|
|[#12263](https://github.com/NVIDIA/spark-rapids/pull/12263)|Fix auto merge conflict 12258 [skip ci]|
|[#12247](https://github.com/NVIDIA/spark-rapids/pull/12247)|Remove Multiget Testing Config|
|[#12208](https://github.com/NVIDIA/spark-rapids/pull/12208)|Upgrade iceberg support to 1.6.1|
|[#12244](https://github.com/NVIDIA/spark-rapids/pull/12244)|Add retry to cardinality estimation|
|[#12214](https://github.com/NVIDIA/spark-rapids/pull/12214)|Change JsonTuple Implemenation to MultiGetJSONObject|
|[#12233](https://github.com/NVIDIA/spark-rapids/pull/12233)|Add retry with SpillableHostBuffer to GPU scans|
|[#12236](https://github.com/NVIDIA/spark-rapids/pull/12236)|make kudo shuffle read retryable and spillable|
|[#12237](https://github.com/NVIDIA/spark-rapids/pull/12237)|Simplify mvn verify cache key [skip ci]|
|[#12227](https://github.com/NVIDIA/spark-rapids/pull/12227)|Refactor SparkSession to fix Spark-4.0 build|
|[#12181](https://github.com/NVIDIA/spark-rapids/pull/12181)|add memory bookkeeping for CPU Memory|
|[#12235](https://github.com/NVIDIA/spark-rapids/pull/12235)|Support legacy mode for yyyy-mm-dd format|
|[#12222](https://github.com/NVIDIA/spark-rapids/pull/12222)|Enable kudo serializer by default|
|[#12198](https://github.com/NVIDIA/spark-rapids/pull/12198)|Refactor SparkStrategy to fix Spark-4.0 build|
|[#12223](https://github.com/NVIDIA/spark-rapids/pull/12223)|Fix auto merge conflict 12220 [skip ci]|
|[#12148](https://github.com/NVIDIA/spark-rapids/pull/12148)|Add the pre-split support to GPU project|
|[#12183](https://github.com/NVIDIA/spark-rapids/pull/12183)|Add GPU support for `spark.sql.catalyst.expressions.Slice`|
|[#12196](https://github.com/NVIDIA/spark-rapids/pull/12196)|Write `numRecords` in CDC files regardless of whether we are using  deletion vectors|
|[#11998](https://github.com/NVIDIA/spark-rapids/pull/11998)|HybridParquetScan: support asynchronous prefetching|
|[#12201](https://github.com/NVIDIA/spark-rapids/pull/12201)|Try Revert #12068 [skip ci]|
|[#12191](https://github.com/NVIDIA/spark-rapids/pull/12191)|Fix auto merge conflict 12190 [skip ci]|
|[#12180](https://github.com/NVIDIA/spark-rapids/pull/12180)|Add Tests to JSON Tuples|
|[#12175](https://github.com/NVIDIA/spark-rapids/pull/12175)|Remove unnecessary output stream wrapper in kudo serializer.|
|[#12156](https://github.com/NVIDIA/spark-rapids/pull/12156)|Remove usage of deprecated kudo api|
|[#12161](https://github.com/NVIDIA/spark-rapids/pull/12161)|Make Partitioning shim-able like getExprs, getExecs and getScans|
|[#12146](https://github.com/NVIDIA/spark-rapids/pull/12146)|Remove unnecessary kudo debug metrics|
|[#12155](https://github.com/NVIDIA/spark-rapids/pull/12155)|Fix auto merge conflict 12153 [skip ci]|
|[#12141](https://github.com/NVIDIA/spark-rapids/pull/12141)|Xfail test_delta_update_fallback_with_deletion_vectors on|
|[#12138](https://github.com/NVIDIA/spark-rapids/pull/12138)|Use SPARK_SHIM_VER in RapidsShuffleManager package|
|[#12110](https://github.com/NVIDIA/spark-rapids/pull/12110)|Upgrade ucx to 1.18|
|[#12101](https://github.com/NVIDIA/spark-rapids/pull/12101)|Fix a potential NPE error in the sized hash join|
|[#12117](https://github.com/NVIDIA/spark-rapids/pull/12117)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#12105](https://github.com/NVIDIA/spark-rapids/pull/12105)|revert ucx 1.18 upgrade|
|[#12094](https://github.com/NVIDIA/spark-rapids/pull/12094)|Fix the flaky test in ThrottlingExecutorSuite|
|[#12048](https://github.com/NVIDIA/spark-rapids/pull/12048)|Fix failing nightly-build on branch-25.04 for Databricks 14.3|
|[#12081](https://github.com/NVIDIA/spark-rapids/pull/12081)|Actually enable/disable measuring kudo buffer copy according to config|
|[#12082](https://github.com/NVIDIA/spark-rapids/pull/12082)|Use 350db143 jars in integration tests on  14.3|
|[#12075](https://github.com/NVIDIA/spark-rapids/pull/12075)|Use SHUFFLE_KUDO_SERIALIZER_MEASURE_BUFFER_COPY_ENABLED for|
|[#11996](https://github.com/NVIDIA/spark-rapids/pull/11996)|Add option to disable measuring buffer copy|
|[#12030](https://github.com/NVIDIA/spark-rapids/pull/12030)|Update dependency version JNI, private, hybrid to 25.04.0-SNAPSHOT|

## Release 25.02

### Features
|||
|:---|:---|
|[#12225](https://github.com/NVIDIA/spark-rapids/issues/12225)|[FEA] Support Spark 3.5.5|
|[#11648](https://github.com/NVIDIA/spark-rapids/issues/11648)|[FEA] it would be nice if we could support org.apache.spark.sql.catalyst.expressions.Bin|
|[#11891](https://github.com/NVIDIA/spark-rapids/issues/11891)|[FEA] Support Spark 3.5.4 release|
|[#11928](https://github.com/NVIDIA/spark-rapids/issues/11928)|[FEA] make maxCpuBatchSize in GpuPartitioning configurable|
|[#10505](https://github.com/NVIDIA/spark-rapids/issues/10505)|[FOLLOW UP] Support `row_number()` filters for `GpuWindowGroupLimitExec`|
|[#11853](https://github.com/NVIDIA/spark-rapids/issues/11853)|[FEA] Ability to dump tables on a write|
|[#11804](https://github.com/NVIDIA/spark-rapids/issues/11804)|[FEA] Support TruncDate expression|
|[#11674](https://github.com/NVIDIA/spark-rapids/issues/11674)|[FEA] HiveHash supports nested types|

### Performance
|||
|:---|:---|
|[#11342](https://github.com/NVIDIA/spark-rapids/issues/11342)|[FEA] Put file writes in a background thread|
|[#11729](https://github.com/NVIDIA/spark-rapids/issues/11729)|[FEA] optimize the multi-contains generated by rlike|
|[#11860](https://github.com/NVIDIA/spark-rapids/issues/11860)|[FEA] kernel for date_trunc and trunc that has a scalar format|
|[#11812](https://github.com/NVIDIA/spark-rapids/issues/11812)|[FEA] Support escape characters in search list when rewrite `regexp_replace` to string replace|

### Bugs Fixed
|||
|:---|:---|
|[#12230](https://github.com/NVIDIA/spark-rapids/issues/12230)|[BUG] parquet_test.py::test_many_column_project test fails on GB100 and cuda12.8|
|[#12231](https://github.com/NVIDIA/spark-rapids/issues/12231)|[BUG] hash_aggregate_test.py::test_hash_multiple_grpby_pivot and row_conversion_test.py::test_row_conversions_fixed_width_wide fails on GB100 and cuda12.8|
|[#12152](https://github.com/NVIDIA/spark-rapids/issues/12152)|[BUG] mvn-verify-check failed cache non-snapshot deps|
|[#11835](https://github.com/NVIDIA/spark-rapids/issues/11835)|[BUG] Intermittent result discrepancy for NDS SF3K query86 on L40S|
|[#12111](https://github.com/NVIDIA/spark-rapids/issues/12111)|[BUG] Hit velox runtime error when filtering df with timestamp column inside when enabling hybrid|
|[#12091](https://github.com/NVIDIA/spark-rapids/issues/12091)|[BUG]An assertion error in the sized hash join|
|[#12113](https://github.com/NVIDIA/spark-rapids/issues/12113)|[BUG] HybridParquetScan fails on the `select count(1)` case|
|[#12096](https://github.com/NVIDIA/spark-rapids/issues/12096)|[BUG] CI_PART1 for DBR 14.3 hangs in the nightly pre-release pipeline|
|[#12076](https://github.com/NVIDIA/spark-rapids/issues/12076)|[BUG] ExtraPlugins might be loaded duplicated|
|[#11433](https://github.com/NVIDIA/spark-rapids/issues/11433)|[BUG] Spark UT framework: SPARK-34212 Parquet should read decimals correctly|
|[#12038](https://github.com/NVIDIA/spark-rapids/issues/12038)|[BUG] spark321 failed core dump in nightly|
|[#12046](https://github.com/NVIDIA/spark-rapids/issues/12046)|[BUG] orc_test fail non-UTC cases with Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec|
|[#12036](https://github.com/NVIDIA/spark-rapids/issues/12036)|[BUG] The check in assertIsOnTheGpu method to test if a plan is on the GPU is not accurate|
|[#11989](https://github.com/NVIDIA/spark-rapids/issues/11989)|[BUG] ParquetCachedBatchSerializer does not grab the GPU semaphore and does not have retry blocks|
|[#11651](https://github.com/NVIDIA/spark-rapids/issues/11651)|[BUG] Parse regular expressions using JDK to make error behavior more consistent between CPU and GPU|
|[#11628](https://github.com/NVIDIA/spark-rapids/issues/11628)|[BUG] Spark UT framework: select one deep nested complex field after join, IOException parsing parquet|
|[#11629](https://github.com/NVIDIA/spark-rapids/issues/11629)|[BUG] Spark UT framework: select one deep nested complex field after outer join, IOException parsing parquet|
|[#11620](https://github.com/NVIDIA/spark-rapids/issues/11620)|[BUG] Spark UT framework: "select a single complex field and partition column" causes java.lang.IndexOutOfBoundsException|
|[#11621](https://github.com/NVIDIA/spark-rapids/issues/11621)|[BUG] Spark UT framework: partial schema intersection - select missing subfield causes java.lang.IndexOutOfBoundsException|
|[#11619](https://github.com/NVIDIA/spark-rapids/issues/11619)|[BUG] Spark UT framework: "select a single complex field" causes java.lang.IndexOutOfBoundsException|
|[#11975](https://github.com/NVIDIA/spark-rapids/issues/11975)|[FOLLOWUP] We should have a separate version definition for the  rapids-4-spark-hybrid dependency|
|[#11976](https://github.com/NVIDIA/spark-rapids/issues/11976)|[BUG] scala 2.13 rapids_integration test failed|
|[#11971](https://github.com/NVIDIA/spark-rapids/issues/11971)|[BUG] scala213 nightly build failed rapids-4-spark-tests_2.13 of spark400|
|[#11903](https://github.com/NVIDIA/spark-rapids/issues/11903)|[BUG] Unexpected large output batches due to implementation defects|
|[#11914](https://github.com/NVIDIA/spark-rapids/issues/11914)|[BUG] Nightly CI does not upload sources and Javadoc JARs as the release script does|
|[#11896](https://github.com/NVIDIA/spark-rapids/issues/11896)|[BUG] [BUILD] CI passes without checking for `operatorsScore.csv`, `supportedExprs.csv` update|
|[#11895](https://github.com/NVIDIA/spark-rapids/issues/11895)|[BUG] BasePythonRunner has a new parameter metrics in Spark 4.0|
|[#11107](https://github.com/NVIDIA/spark-rapids/issues/11107)|[BUG] Rework RapidsShuffleManager initialization for Apache Spark 4.0.0|
|[#11897](https://github.com/NVIDIA/spark-rapids/issues/11897)|[BUG] JsonScanRetrySuite is failing in the CI.|
|[#11885](https://github.com/NVIDIA/spark-rapids/issues/11885)|[BUG] data corruption with spill framework changes|
|[#11762](https://github.com/NVIDIA/spark-rapids/issues/11762)|[BUG] Non-nullable bools in a nullable struct fails|
|[#11526](https://github.com/NVIDIA/spark-rapids/issues/11526)|Fix Arithmetic tests on Databricks 14.3|
|[#11866](https://github.com/NVIDIA/spark-rapids/issues/11866)|[BUG]The CHANGELOG is generated based on the project's roadmap rather than the target branch.|
|[#11749](https://github.com/NVIDIA/spark-rapids/issues/11749)|[BUG] Include Databricks 14.3 shim into the dist jar|
|[#11822](https://github.com/NVIDIA/spark-rapids/issues/11822)|[BUG] [Spark 4] Type mismatch Exceptions from DFUDFShims.scala with Spark-4.0.0 expressions.Expression|
|[#11760](https://github.com/NVIDIA/spark-rapids/issues/11760)|[BUG] isTimestamp leaks a Scalar|
|[#11796](https://github.com/NVIDIA/spark-rapids/issues/11796)|[BUG] populate-daily-cache action masks errors|
|[#10901](https://github.com/NVIDIA/spark-rapids/issues/10901)|from_json throws exception when the json's structure only partially matches the provided schema|
|[#11736](https://github.com/NVIDIA/spark-rapids/issues/11736)|[BUG] Orc writes don't fully support Booleans with nulls|

### PRs
|||
|:---|:---|
|[#12294](https://github.com/NVIDIA/spark-rapids/pull/12294)|Update changelog for the v25.02 release [skip ci]|
|[#12289](https://github.com/NVIDIA/spark-rapids/pull/12289)|Update dependency version JNI, private to 25.02.1|
|[#12282](https://github.com/NVIDIA/spark-rapids/pull/12282)|[DOC] update the download page for 2502.1 hot release [skip ci]|
|[#12280](https://github.com/NVIDIA/spark-rapids/pull/12280)|Disable hybrid cases for spark35X [skip ci]|
|[#12259](https://github.com/NVIDIA/spark-rapids/pull/12259)|Add Spark 3.5.5 shim|
|[#12252](https://github.com/NVIDIA/spark-rapids/pull/12252)|Update dependency version private to 25.02.1-SNAPSHOT|
|[#12253](https://github.com/NVIDIA/spark-rapids/pull/12253)|Accelerate nightly build [skip ci]|
|[#12239](https://github.com/NVIDIA/spark-rapids/pull/12239)|Mirror the Cloudera Maven repository|
|[#12189](https://github.com/NVIDIA/spark-rapids/pull/12189)|Update version to 25.02.1-SNAPSHOT|
|[#12204](https://github.com/NVIDIA/spark-rapids/pull/12204)|[DOC] address ghpage pr comments [skip ci]|
|[#12212](https://github.com/NVIDIA/spark-rapids/pull/12212)|Fix deadlock in BounceBufferPool|
|[#12205](https://github.com/NVIDIA/spark-rapids/pull/12205)|Workaround: tmply disable the HostAllocSuite to unblock CI [skip ci]|
|[#12188](https://github.com/NVIDIA/spark-rapids/pull/12188)|Update dependency version JNI to 25.02.1-SNAPSHOT|
|[#12157](https://github.com/NVIDIA/spark-rapids/pull/12157)|Add hybrid sha1 to mvn verify cache key [skip ci]|
|[#12154](https://github.com/NVIDIA/spark-rapids/pull/12154)|Update latest changelog [skip ci]|
|[#12129](https://github.com/NVIDIA/spark-rapids/pull/12129)|Update dependency version JNI, private, hybrid to 25.02.0 [skip ci]|
|[#12102](https://github.com/NVIDIA/spark-rapids/pull/12102)|[DOC] update the download page for 2502 release [skip ci]|
|[#12112](https://github.com/NVIDIA/spark-rapids/pull/12112)|HybridParquetScan: Fix velox runtime error in hybrid scan when filter timestamp|
|[#12092](https://github.com/NVIDIA/spark-rapids/pull/12092)|Fix an assertion error in the sized hash join|
|[#12114](https://github.com/NVIDIA/spark-rapids/pull/12114)|Fix HybridParquetScan over select(1)|
|[#12109](https://github.com/NVIDIA/spark-rapids/pull/12109)|revert ucx 1.18 upgrade|
|[#12103](https://github.com/NVIDIA/spark-rapids/pull/12103)|Revert "Enable event log for qualification & profiling tools testing …|
|[#12058](https://github.com/NVIDIA/spark-rapids/pull/12058)|upgrade jucx to 1.18|
|[#12077](https://github.com/NVIDIA/spark-rapids/pull/12077)|Fix the issue of ExtraPlugins loading multiple times|
|[#12080](https://github.com/NVIDIA/spark-rapids/pull/12080)|Quick fix for hybrid tests without git information.|
|[#12068](https://github.com/NVIDIA/spark-rapids/pull/12068)|Do not build Spark-4.0.0-SNAPSHOT [skip ci]|
|[#12064](https://github.com/NVIDIA/spark-rapids/pull/12064)|Run mvn with the project's pom.xml in hybrid_execution.sh|
|[#12060](https://github.com/NVIDIA/spark-rapids/pull/12060)|Relax decimal metadata checks for mismatched precision/scale|
|[#12054](https://github.com/NVIDIA/spark-rapids/pull/12054)|Update the version of the rapids-hybrid-execution dependency.|
|[#11970](https://github.com/NVIDIA/spark-rapids/pull/11970)|Explicitly set Delta table props to accommodate for different defaults|
|[#12044](https://github.com/NVIDIA/spark-rapids/pull/12044)|Set CI=true for complete failure reason in summary|
|[#12050](https://github.com/NVIDIA/spark-rapids/pull/12050)|Fixed `FileSourceScanExec` and `BatchScanExec` inadvertently falling to the CPU in non-utc orc tests |
|[#12000](https://github.com/NVIDIA/spark-rapids/pull/12000)|HybridParquetScan: Refine filter push down to avoid double evaluation|
|[#12037](https://github.com/NVIDIA/spark-rapids/pull/12037)|Removed the assumption if a plan is Columnar it probably is on the GPU|
|[#11991](https://github.com/NVIDIA/spark-rapids/pull/11991)|Grab the GPU Semaphore when reading cached batch data with the GPU|
|[#11880](https://github.com/NVIDIA/spark-rapids/pull/11880)|Perform handle spill IO outside of locked section in SpillFramework|
|[#11997](https://github.com/NVIDIA/spark-rapids/pull/11997)|Configure 14.3 support at runtime|
|[#11977](https://github.com/NVIDIA/spark-rapids/pull/11977)|Use bounce buffer pools in the Spill Framework|
|[#11912](https://github.com/NVIDIA/spark-rapids/pull/11912)|Ensure Java Compatibility Check for Regex Patterns|
|[#11984](https://github.com/NVIDIA/spark-rapids/pull/11984)|Include the size information when printing a SCB|
|[#11889](https://github.com/NVIDIA/spark-rapids/pull/11889)|Change order of initialization so pinned pool is available for spill framework buffers|
|[#11956](https://github.com/NVIDIA/spark-rapids/pull/11956)|Enable tests in RapidsParquetSchemaPruningSuite|
|[#11981](https://github.com/NVIDIA/spark-rapids/pull/11981)|Protect the batch read by a retry block in agg|
|[#11967](https://github.com/NVIDIA/spark-rapids/pull/11967)|Add support for `org.apache.spark.sql.catalyst.expressions.Bin`|
|[#11982](https://github.com/NVIDIA/spark-rapids/pull/11982)|Use common add-to-project action [skip ci]|
|[#11978](https://github.com/NVIDIA/spark-rapids/pull/11978)|Try to fix Scala 2.13 nightly failure: can not find version-def.sh|
|[#11973](https://github.com/NVIDIA/spark-rapids/pull/11973)|Minor change: Make Hybrid version a separate config like priviate repo|
|[#11969](https://github.com/NVIDIA/spark-rapids/pull/11969)|Support `raise_error()` on  14.3, Spark 4.|
|[#11972](https://github.com/NVIDIA/spark-rapids/pull/11972)|Update MockTaskContext to support new functions added in Spark-4.0|
|[#11906](https://github.com/NVIDIA/spark-rapids/pull/11906)|Enable Hybrid test cases in premerge/nightly CIs|
|[#11720](https://github.com/NVIDIA/spark-rapids/pull/11720)|Introduce hybrid (CPU) scan for Parquet read|
|[#11911](https://github.com/NVIDIA/spark-rapids/pull/11911)|Avoid concatentating multiple host buffers when reading Parquet|
|[#11960](https://github.com/NVIDIA/spark-rapids/pull/11960)|Remove jlowe as committer since he retired|
|[#11958](https://github.com/NVIDIA/spark-rapids/pull/11958)|Update to use vulnerability-scan runner [skip ci]|
|[#11955](https://github.com/NVIDIA/spark-rapids/pull/11955)|Add Spark 3.5.4 shim|
|[#11959](https://github.com/NVIDIA/spark-rapids/pull/11959)|Remove inactive user from github workflow[skip ci]|
|[#11952](https://github.com/NVIDIA/spark-rapids/pull/11952)|Fix auto merge conflict 11948 [skip ci]|
|[#11908](https://github.com/NVIDIA/spark-rapids/pull/11908)|Fix two potential OOM issues in GPU aggregate.|
|[#11936](https://github.com/NVIDIA/spark-rapids/pull/11936)|Add throttle time metrics for async write|
|[#11929](https://github.com/NVIDIA/spark-rapids/pull/11929)|make maxCpuBatchSize in GpuPartitioning configurable|
|[#11939](https://github.com/NVIDIA/spark-rapids/pull/11939)|[DOC] update release note to add spark 353 support [skip ci]|
|[#11920](https://github.com/NVIDIA/spark-rapids/pull/11920)|Remove Alluxio support|
|[#11938](https://github.com/NVIDIA/spark-rapids/pull/11938)|Update codeowners file to use team [skip ci]|
|[#11915](https://github.com/NVIDIA/spark-rapids/pull/11915)|Deploy the sources and Javadoc JARs in the nightly CICD [skip ci]|
|[#11917](https://github.com/NVIDIA/spark-rapids/pull/11917)|Fix issue with CustomerShuffleReaderExec metadata copy|
|[#11910](https://github.com/NVIDIA/spark-rapids/pull/11910)|fix bug: enable if_modified_files check for all shims in github actions [skip ci]|
|[#11909](https://github.com/NVIDIA/spark-rapids/pull/11909)|Update copyright year in NOTICE [skip ci]|
|[#11907](https://github.com/NVIDIA/spark-rapids/pull/11907)|Fix generated doc for xxhash64 for Spark 400|
|[#11905](https://github.com/NVIDIA/spark-rapids/pull/11905)|Fix the build error for Spark 400|
|[#11904](https://github.com/NVIDIA/spark-rapids/pull/11904)|Eagerly initialize RapidsShuffleManager for SPARK-45762|
|[#11865](https://github.com/NVIDIA/spark-rapids/pull/11865)|Async write support for ORC|
|[#11816](https://github.com/NVIDIA/spark-rapids/pull/11816)|address some comments for 11792|
|[#11789](https://github.com/NVIDIA/spark-rapids/pull/11789)|Improve the retry support for nondeterministic expressions|
|[#11898](https://github.com/NVIDIA/spark-rapids/pull/11898)|Add missing json reader options for JsonScanRetrySuite|
|[#11859](https://github.com/NVIDIA/spark-rapids/pull/11859)|Xxhash64 supports nested types|
|[#11890](https://github.com/NVIDIA/spark-rapids/pull/11890)|Update operatorsScore,supportedExprs for TruncDate, TruncTimestamp|
|[#11886](https://github.com/NVIDIA/spark-rapids/pull/11886)|Support group-limit optimization for `ROW_NUMBER`|
|[#11887](https://github.com/NVIDIA/spark-rapids/pull/11887)|Make sure that the chunked packer bounce buffer is realease after the synchronize|
|[#11894](https://github.com/NVIDIA/spark-rapids/pull/11894)|Fix bug: add timeout for cache deps steps [skip ci]|
|[#11810](https://github.com/NVIDIA/spark-rapids/pull/11810)|Use faster multi-contains in `rlike` regex rewrite|
|[#11882](https://github.com/NVIDIA/spark-rapids/pull/11882)|Add metrics GpuPartitioning.CopyToHostTime|
|[#11864](https://github.com/NVIDIA/spark-rapids/pull/11864)|Add support for dumping write data to try and reproduce error cases|
|[#11781](https://github.com/NVIDIA/spark-rapids/pull/11781)|Fix non-nullable under nullable struct write|
|[#11877](https://github.com/NVIDIA/spark-rapids/pull/11877)|Fix auto merge conflict 11873 [skip ci]|
|[#11833](https://github.com/NVIDIA/spark-rapids/pull/11833)|Support `trunc` and `date_trunc` SQL function|
|[#11660](https://github.com/NVIDIA/spark-rapids/pull/11660)|Add `HiveHash` support for nested types|
|[#11855](https://github.com/NVIDIA/spark-rapids/pull/11855)|Add integration test for parquet async writer|
|[#11747](https://github.com/NVIDIA/spark-rapids/pull/11747)|Spill framework refactor for better performance and extensibility|
|[#11870](https://github.com/NVIDIA/spark-rapids/pull/11870)|Workaround: Exclude cudf_log.txt in RAT check|
|[#11867](https://github.com/NVIDIA/spark-rapids/pull/11867)|Generate the CHANGELOG based on the PR's target branch [skip ci]|
|[#11821](https://github.com/NVIDIA/spark-rapids/pull/11821)|add a few more stage level metrics|
|[#11856](https://github.com/NVIDIA/spark-rapids/pull/11856)|Document Hive text write serialization format checks|
|[#11805](https://github.com/NVIDIA/spark-rapids/pull/11805)|Enable some integration tests for `from_json`|
|[#11840](https://github.com/NVIDIA/spark-rapids/pull/11840)|Support running Databricks CI_PART2 integration tests with JARs built by CI_PART1|
|[#11847](https://github.com/NVIDIA/spark-rapids/pull/11847)|Some small improvements|
|[#11811](https://github.com/NVIDIA/spark-rapids/pull/11811)|Fix bug: populate cache deps [skip ci]|
|[#11817](https://github.com/NVIDIA/spark-rapids/pull/11817)|Optimize Databricks Jenkins scripts [skip ci]|
|[#11829](https://github.com/NVIDIA/spark-rapids/pull/11829)|Some minor improvements identified during benchmark|
|[#11827](https://github.com/NVIDIA/spark-rapids/pull/11827)|Deal with Spark changes for column<->expression conversions|
|[#11826](https://github.com/NVIDIA/spark-rapids/pull/11826)|Balance the pre-merge CI job's time for the ci_1 and ci_2 tests|
|[#11784](https://github.com/NVIDIA/spark-rapids/pull/11784)|Add support for kudo write metrics|
|[#11783](https://github.com/NVIDIA/spark-rapids/pull/11783)|Fix the task count check in TrafficController|
|[#11813](https://github.com/NVIDIA/spark-rapids/pull/11813)|Support some escape chars when rewriting regexp_replace to stringReplace|
|[#11819](https://github.com/NVIDIA/spark-rapids/pull/11819)|Add the 'test_type' parameter for Databricks script|
|[#11786](https://github.com/NVIDIA/spark-rapids/pull/11786)|Enable license header check|
|[#11791](https://github.com/NVIDIA/spark-rapids/pull/11791)|Incorporate checksum of internal dependencies in the GH cache key [skip ci]|
|[#11788](https://github.com/NVIDIA/spark-rapids/pull/11788)|Support running Databricks CI_PART2 integration tests with JARs built by CI_PART1|
|[#11778](https://github.com/NVIDIA/spark-rapids/pull/11778)|Remove unnecessary toBeReturned field from serialized batch iterators|
|[#11785](https://github.com/NVIDIA/spark-rapids/pull/11785)|Update advanced configs introduced by private repo [skip ci]|
|[#11772](https://github.com/NVIDIA/spark-rapids/pull/11772)|Update rapids JNI and private dependency to 25.02.0-SNAPSHOT|
|[#11756](https://github.com/NVIDIA/spark-rapids/pull/11756)|remove excluded release shim and TODO|

