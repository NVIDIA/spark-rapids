# Change log
Generated on 2025-10-13

## Release 25.10

### Features
|||
|:---|:---|
|[#12821](https://github.com/NVIDIA/spark-rapids/issues/12821)|[FEA] Zero Conf - set spark.rapids.memory.gpu.allocSize based on memory type|
|[#13503](https://github.com/NVIDIA/spark-rapids/issues/13503)|[FEA] Add integration test case to verify that falling back to cpu when insert into partitioned iceberg table with unsupported partition.|
|[#13400](https://github.com/NVIDIA/spark-rapids/issues/13400)|[FEA] Add gpu iceberg rolling file writer|
|[#13380](https://github.com/NVIDIA/spark-rapids/issues/13380)|[FEA] Add iceberg append data integration tests.|
|[#12575](https://github.com/NVIDIA/spark-rapids/issues/12575)|[FEA][DV]  Support Parquet+DV Scan using PERFILE GpuParquetReader|
|[#13418](https://github.com/NVIDIA/spark-rapids/issues/13418)|[FEA] Support Spark 4.0.1|
|[#13378](https://github.com/NVIDIA/spark-rapids/issues/13378)|[FEA] Add support for iceberg data writer factory.|
|[#13440](https://github.com/NVIDIA/spark-rapids/issues/13440)|[FEA] Use `RapidsFileIO` interface from `spark-rapids-jni`|
|[#13376](https://github.com/NVIDIA/spark-rapids/issues/13376)|[FEA] Add support for iceberg spark write|
|[#13394](https://github.com/NVIDIA/spark-rapids/issues/13394)|[FEA] Accelerate optimizing clustered tables|
|[#13377](https://github.com/NVIDIA/spark-rapids/issues/13377)|[FEA] Add support for iceberg partitioner|
|[#12553](https://github.com/NVIDIA/spark-rapids/issues/12553)|[FEA][Liquid] Accelerate writes to Clustered Tables|
|[#12882](https://github.com/NVIDIA/spark-rapids/issues/12882)|[FEA] It would be nice if we support Asia/Shanghai timezone for orc file scan|
|[#13379](https://github.com/NVIDIA/spark-rapids/issues/13379)|[FEA] Add support for iceberg parquet file writer.|
|[#13403](https://github.com/NVIDIA/spark-rapids/issues/13403)|[FEA] Add DV read support without support for combining files|
|[#12982](https://github.com/NVIDIA/spark-rapids/issues/12982)|[FEA] Support uuid|
|[#12552](https://github.com/NVIDIA/spark-rapids/issues/12552)|[FEA] [Liquid] Accelerate reads from Clustered Tables|
|[#13301](https://github.com/NVIDIA/spark-rapids/issues/13301)|[FEA] Setup integration tests against rest catalog for iceberg.|
|[#11193](https://github.com/NVIDIA/spark-rapids/issues/11193)|[FEA] Support "Fail on error" for `parse_url`|
|[#13012](https://github.com/NVIDIA/spark-rapids/issues/13012)|[FEA] Optimize command support for Delta IO|
|[#7008](https://github.com/NVIDIA/spark-rapids/issues/7008)|[FEA] Validate ORC deflate (zlib) compression|

### Performance
|||
|:---|:---|
|[#13311](https://github.com/NVIDIA/spark-rapids/issues/13311)|[FEA] Explore alternative way to calculate M2 for stddev* and variance*|
|[#13321](https://github.com/NVIDIA/spark-rapids/issues/13321)|Run NDS-H (TPC-H) SF100 locally on Delta 3.3.x|
|[#12471](https://github.com/NVIDIA/spark-rapids/issues/12471)|[FEA] Zero Config - Figure out a good default for CPU memory limits|

### Bugs Fixed
|||
|:---|:---|
|[#13578](https://github.com/NVIDIA/spark-rapids/issues/13578)|[BUG] Query failure in InMemoryTableScan with AQE enabled for aggregation|
|[#13554](https://github.com/NVIDIA/spark-rapids/issues/13554)|[BUG] iceberg-rest-catalog CI failed: Missing "org.apache.iceberg.rest.RESTCatalog"|
|[#13312](https://github.com/NVIDIA/spark-rapids/issues/13312)|[BUG] zip_with test failures|
|[#13513](https://github.com/NVIDIA/spark-rapids/issues/13513)|[BUG] Data mismatch when running a query with a filter against Delta table with DV|
|[#13562](https://github.com/NVIDIA/spark-rapids/issues/13562)|[BUG] cuda13 spark-rapids fails executors when profiler is enabled|
|[#12796](https://github.com/NVIDIA/spark-rapids/issues/12796)|[BUG] Delta CDF tests potentially use an incorrect read option|
|[#13519](https://github.com/NVIDIA/spark-rapids/issues/13519)|[BUG] NDS run failed java.lang.NoClassDefFoundError: org/apache/spark/sql/delta/DeltaParquetFileFormat|
|[#13553](https://github.com/NVIDIA/spark-rapids/issues/13553)|[BUG] iceberg_append_test failures due to IllegalArgumentException in pre_release|
|[#13542](https://github.com/NVIDIA/spark-rapids/issues/13542)|[BUG] new op time collection has huge overhead when op fallback happens|
|[#13535](https://github.com/NVIDIA/spark-rapids/issues/13535)|[BUG] iceberg_append_test failed Part of the plan is not columnar class org.apache.spark.sql.execution.SortExec|
|[#13531](https://github.com/NVIDIA/spark-rapids/issues/13531)|[BUG] IncrementMetric expression causes ColumnarToRow fallback|
|[#13514](https://github.com/NVIDIA/spark-rapids/issues/13514)|[BUG] MetricsEventLogValidationSuite failed: operator time metrics are less|
|[#13505](https://github.com/NVIDIA/spark-rapids/issues/13505)|[BUG] iceberg_append_test case failed: BasicColumnarWriteJobStatsTracker has no safety check for Active SparkContext|
|[#13445](https://github.com/NVIDIA/spark-rapids/issues/13445)|[BUG] com.nvidia.spark.rapids.jni.CpuRetryOOM: CPU OutOfMemory in performance test|
|[#13449](https://github.com/NVIDIA/spark-rapids/issues/13449)|[BUG] Device memory allocation failure when running the delete command for Delta tables with deletion vector enabled|
|[#13358](https://github.com/NVIDIA/spark-rapids/issues/13358)|[BUG] Failed to run IT on dataproc serverless with spark.executor.memoryOverhead|
|[#13477](https://github.com/NVIDIA/spark-rapids/issues/13477)|[BUG] MetricsEventLogValidationSuite failed Total operator time exceed total executor run time intermittently|
|[#13465](https://github.com/NVIDIA/spark-rapids/issues/13465)|[BUG] MetricsEventLogValidationSuite failed in spark400|
|[#13458](https://github.com/NVIDIA/spark-rapids/issues/13458)|[BUG] [TEST] Optimization for unit test: Reduce TimeZonePerfSuite test time|
|[#13457](https://github.com/NVIDIA/spark-rapids/issues/13457)|[BUG] delta_lake_liquid_clustering_test.test_delta_rtas_sql_liquid_clustering failed Part of the plan is not columnar class|
|[#12883](https://github.com/NVIDIA/spark-rapids/issues/12883)|[BUG] HostAllocSuite failed split should not happen immediately after fallback on memory contention failed|
|[#13455](https://github.com/NVIDIA/spark-rapids/issues/13455)|[BUG] CI integration test images fails build: Secondary flag is not valid for non-boolean flag|
|[#13443](https://github.com/NVIDIA/spark-rapids/issues/13443)|[BUG] CI failed: could not initialize class com.databricks.sql.transaction.tahoe.DeltaValidation$|
|[#13360](https://github.com/NVIDIA/spark-rapids/issues/13360)|[BUG] Column Support mismatch error with collect_list + JOIN when AQE is enabled|
|[#13396](https://github.com/NVIDIA/spark-rapids/issues/13396)|[BUG] markdown link check failed dead link loan-performance-data.html|
|[#13416](https://github.com/NVIDIA/spark-rapids/issues/13416)|[BUG] All IT join test and NDS test failed due to cudaErrorIllegalAddress an illegal memory access was encountered|
|[#13198](https://github.com/NVIDIA/spark-rapids/issues/13198)|[BUG] all tasks are bigger than the total memory limit FAILED|
|[#13370](https://github.com/NVIDIA/spark-rapids/issues/13370)|[BUG] liquid clustering should be triggered after enabled for testing|
|[#13361](https://github.com/NVIDIA/spark-rapids/issues/13361)|[BUG] Array index out of bounds when running iceberg query|
|[#10988](https://github.com/NVIDIA/spark-rapids/issues/10988)|[AUDIT][SPARK-48019] Fix incorrect behavior in ColumnVector/ColumnarArray with dictionary and nulls|
|[#13224](https://github.com/NVIDIA/spark-rapids/issues/13224)|[BUG] slightly deep expression planning/processing is slow|
|[#11921](https://github.com/NVIDIA/spark-rapids/issues/11921)|[BUG][AUDIT][SPARK-50258][SQL] Fix output column order changed issue after AQE optimization|
|[#13348](https://github.com/NVIDIA/spark-rapids/issues/13348)|[BUG] init_cudf_udf.sh failed: Script exit status is non-zero for databricks runtime since 25.10|
|[#13353](https://github.com/NVIDIA/spark-rapids/issues/13353)|[BUG] Regression with RapidsFileIO change|
|[#13326](https://github.com/NVIDIA/spark-rapids/issues/13326)|[BUG] hive_write_test.py::test_optimized_hive_ctas_configs_orc failure|
|[#13342](https://github.com/NVIDIA/spark-rapids/issues/13342)|[BUG] Build failure due to deprecation of some methods in ParseURI class from spark-rapids-jni|
|[#13337](https://github.com/NVIDIA/spark-rapids/issues/13337)|[BUG] `ParseURI` method deprecations causing pre-merge CI jobs failures|
|[#11106](https://github.com/NVIDIA/spark-rapids/issues/11106)|[BUG] Revisit integration tests with @disable_ansi_mode label|
|[#13319](https://github.com/NVIDIA/spark-rapids/issues/13319)|[BUG] some nvtx ranges pop out of order or have the wrong lifecycle|
|[#13138](https://github.com/NVIDIA/spark-rapids/issues/13138)|[FEA] Use iceberg FileIO to access parquet files.|
|[#13331](https://github.com/NVIDIA/spark-rapids/issues/13331)|[BUG] test_delta_optimize_fallback_on_clustered_table failed does not support truncate in batch mode|
|[#13270](https://github.com/NVIDIA/spark-rapids/issues/13270)|[BUG] Iceberg integration failed under spark connect|
|[#13145](https://github.com/NVIDIA/spark-rapids/issues/13145)|[BUG] NDS queries fail with sufficiently low offHeap limits in kudo deserializer|

### PRs
|||
|:---|:---|
|[#13593](https://github.com/NVIDIA/spark-rapids/pull/13593)|[DOC] update the download doc for 2510 release [skip ci]|
|[#13587](https://github.com/NVIDIA/spark-rapids/pull/13587)|Update dependency version JNI, private, hybrid to 25.10.0|
|[#13581](https://github.com/NVIDIA/spark-rapids/pull/13581)|Avoid host-vector access under TableCache by replacing CPU ColumnarToRow with GPU variant|
|[#13580](https://github.com/NVIDIA/spark-rapids/pull/13580)|Remove nested quotes and double quotes [skip ci]|
|[#13569](https://github.com/NVIDIA/spark-rapids/pull/13569)|Disable predicate pushdown filters in MULTITHREADED Delta Scan when reading Deletion Vectors|
|[#13563](https://github.com/NVIDIA/spark-rapids/pull/13563)|Fallback gracefully if libprofiler cannot be loaded|
|[#13532](https://github.com/NVIDIA/spark-rapids/pull/13532)|Fix read option name for change data feed for Delta|
|[#13545](https://github.com/NVIDIA/spark-rapids/pull/13545)|Add iceberg tests of insert into with values|
|[#13558](https://github.com/NVIDIA/spark-rapids/pull/13558)|Avoid throwing ClassNotFoundExceptions when checking isSupportedFormat with Delta Lake versions < 2.0.1|
|[#13541](https://github.com/NVIDIA/spark-rapids/pull/13541)|fix overhead of new op time collection upon c2r|
|[#13539](https://github.com/NVIDIA/spark-rapids/pull/13539)|Add env header for iceberg test variable [skip ci]|
|[#13537](https://github.com/NVIDIA/spark-rapids/pull/13537)|Fix iceberg integration test test_insert_into_partitioned_table_unsupported_partition_fallback [skip ci]|
|[#13335](https://github.com/NVIDIA/spark-rapids/pull/13335)|Add a GpuOverride for IncrementMetric |
|[#12967](https://github.com/NVIDIA/spark-rapids/pull/12967)|limit gpu alloc on integrated cards|
|[#13497](https://github.com/NVIDIA/spark-rapids/pull/13497)|Switch rounding calls to using new API|
|[#13515](https://github.com/NVIDIA/spark-rapids/pull/13515)|Fallback to cpu when inserting into partitioned iceberg table with unsupported partition.|
|[#13517](https://github.com/NVIDIA/spark-rapids/pull/13517)|more relaxed margin for MetricsEventLogValidationSuite|
|[#13516](https://github.com/NVIDIA/spark-rapids/pull/13516)|Fix wrong usage of GpuWriteJobStatsTracker in iceberg write|
|[#13499](https://github.com/NVIDIA/spark-rapids/pull/13499)|Refine column size estimation based on previous batch for Host2Gpu|
|[#13491](https://github.com/NVIDIA/spark-rapids/pull/13491)|Add Deletion Vector Read Support to Multithreaded Parquet Reader|
|[#13476](https://github.com/NVIDIA/spark-rapids/pull/13476)|Add GpuRollingFileWriter for iceberg.|
|[#13490](https://github.com/NVIDIA/spark-rapids/pull/13490)|Fix a Memory leak introduced as part of adding Deletion Vector support |
|[#13460](https://github.com/NVIDIA/spark-rapids/pull/13460)|Add support for Iceberg insert integration tests.|
|[#13492](https://github.com/NVIDIA/spark-rapids/pull/13492)|Update archive.apache usage in dockerfile [skip ci]|
|[#13439](https://github.com/NVIDIA/spark-rapids/pull/13439)|Add Partial support for Deletion Vector for PERFILE Reader Type|
|[#13481](https://github.com/NVIDIA/spark-rapids/pull/13481)|allow some margin for maxExpectedOperatorTime|
|[#13474](https://github.com/NVIDIA/spark-rapids/pull/13474)|Revert "insert coalesce after join" [skip ci]|
|[#13480](https://github.com/NVIDIA/spark-rapids/pull/13480)|avoid using Noop as default excludeMetric|
|[#13466](https://github.com/NVIDIA/spark-rapids/pull/13466)|fix evenlog by default zstd compression in spark 400|
|[#13459](https://github.com/NVIDIA/spark-rapids/pull/13459)|Optimization for unit test: Reduce TimeZonePerfSuite test time|
|[#13429](https://github.com/NVIDIA/spark-rapids/pull/13429)|refactor optime collection for correctness|
|[#13461](https://github.com/NVIDIA/spark-rapids/pull/13461)|Add a missing allow-non-gpu tag for CreateTableExec for test_delta_rtas_sql_liquid_clustering|
|[#13447](https://github.com/NVIDIA/spark-rapids/pull/13447)|Add Spark 4.0.1 shim|
|[#13444](https://github.com/NVIDIA/spark-rapids/pull/13444)|insert coalesce after join because join can cause row reductions|
|[#13456](https://github.com/NVIDIA/spark-rapids/pull/13456)|Remove legacy spacy pkg [skip ci]|
|[#13434](https://github.com/NVIDIA/spark-rapids/pull/13434)|Support InMemoryTableScan with AQE for Spark 3.5.2+|
|[#13369](https://github.com/NVIDIA/spark-rapids/pull/13369)|[DOC] Update memory debugging docs with scalar support [skip ci]|
|[#13441](https://github.com/NVIDIA/spark-rapids/pull/13441)|Substitue RapidsFileIO interface from spark-rapids-jni for RapidsFileIO in spark-rapids|
|[#13446](https://github.com/NVIDIA/spark-rapids/pull/13446)|Add support for iceberg GpuSparkWrite|
|[#13414](https://github.com/NVIDIA/spark-rapids/pull/13414)|Add support for the Delta optimize table command for clustered tables|
|[#13415](https://github.com/NVIDIA/spark-rapids/pull/13415)|Add gpu iceberg partitioner|
|[#13438](https://github.com/NVIDIA/spark-rapids/pull/13438)|Add rule to override StaticInvoke for iceberg|
|[#13417](https://github.com/NVIDIA/spark-rapids/pull/13417)|Add write support for Delta clustered tables|
|[#13367](https://github.com/NVIDIA/spark-rapids/pull/13367)|Allows AQE to fall back to the CPU for non-reused broadcast|
|[#13246](https://github.com/NVIDIA/spark-rapids/pull/13246)|Fix docstring typo in collect_data_or_exception [skip ci]|
|[#13052](https://github.com/NVIDIA/spark-rapids/pull/13052)|Orc supports non-UTC timezone when reader/writer timezones are the same|
|[#13409](https://github.com/NVIDIA/spark-rapids/pull/13409)|[DOC] update the download doc to add download plugin from maven [skip ci]|
|[#13407](https://github.com/NVIDIA/spark-rapids/pull/13407)|Add iceberg gpu parquet file appender and file writer factory|
|[#13366](https://github.com/NVIDIA/spark-rapids/pull/13366)|Fix the flaky test in `TrafficControllerSuite`|
|[#13359](https://github.com/NVIDIA/spark-rapids/pull/13359)|limit concurrent gpu task by a config|
|[#13392](https://github.com/NVIDIA/spark-rapids/pull/13392)|Add GpuAppendDataExec as prerequisite to support iceberg append|
|[#13364](https://github.com/NVIDIA/spark-rapids/pull/13364)|fix wrong timing to sleep(yield) in HostAllocSuite|
|[#13397](https://github.com/NVIDIA/spark-rapids/pull/13397)|Add support for iceberg bucket expression of int and long types.|
|[#12960](https://github.com/NVIDIA/spark-rapids/pull/12960)|Use async profiler to create per stage flame graph|
|[#13373](https://github.com/NVIDIA/spark-rapids/pull/13373)|Add an executor cache class|
|[#13371](https://github.com/NVIDIA/spark-rapids/pull/13371)|Optimize table to trigger liquid clustering before test|
|[#13372](https://github.com/NVIDIA/spark-rapids/pull/13372)|Fix index out of bound bug in iceberg parquet reader.|
|[#13289](https://github.com/NVIDIA/spark-rapids/pull/13289)|Filter out timezones not supported by Python|
|[#13308](https://github.com/NVIDIA/spark-rapids/pull/13308)|Add UUID function support|
|[#13225](https://github.com/NVIDIA/spark-rapids/pull/13225)|Fix performance issue with nested add|
|[#13318](https://github.com/NVIDIA/spark-rapids/pull/13318)|Add a missing numbers import in PySpark RAPIDS daemons|
|[#13329](https://github.com/NVIDIA/spark-rapids/pull/13329)|Add rest catalog integration tests for iceberg|
|[#13327](https://github.com/NVIDIA/spark-rapids/pull/13327)|Add read tests for the delta clustered table|
|[#13349](https://github.com/NVIDIA/spark-rapids/pull/13349)|Make the iceberg bug easier to produce.|
|[#13356](https://github.com/NVIDIA/spark-rapids/pull/13356)|CUDF Conda package has dropped cuda11 python packages from 25.10 [skip ci]|
|[#13354](https://github.com/NVIDIA/spark-rapids/pull/13354)|This fixes a big regression due to serialization of the hadoop configuration|
|[#13352](https://github.com/NVIDIA/spark-rapids/pull/13352)|Add ANSI mode support for parse_url function|
|[#13328](https://github.com/NVIDIA/spark-rapids/pull/13328)|Fix test_optimized_hive_ctas_configs_orc for non_utc timezones|
|[#13346](https://github.com/NVIDIA/spark-rapids/pull/13346)|Merge branch-25.08 to branch-25.10 [skip ci]|
|[#13338](https://github.com/NVIDIA/spark-rapids/pull/13338)|Suppress deprecation warnings for `ParseURI`|
|[#13317](https://github.com/NVIDIA/spark-rapids/pull/13317)|nvtx fixes in shuffle manager|
|[#13177](https://github.com/NVIDIA/spark-rapids/pull/13177)|enable more logging for BUFN_PLUS related issues|
|[#13252](https://github.com/NVIDIA/spark-rapids/pull/13252)|Add Rapids file io layer.|
|[#13333](https://github.com/NVIDIA/spark-rapids/pull/13333)|Fix test_delta_optimize_fallback_on_clustered_table test failure with Spark 3.5.6|
|[#13250](https://github.com/NVIDIA/spark-rapids/pull/13250)|add nvtx, fix row count, add data size metric|
|[#13299](https://github.com/NVIDIA/spark-rapids/pull/13299)|LORE: Add support to dump parquet with exact column names|
|[#13330](https://github.com/NVIDIA/spark-rapids/pull/13330)|Enable shellcheck: Fix nits [skip ci]|
|[#13323](https://github.com/NVIDIA/spark-rapids/pull/13323)|Update stringrepr for collation for StringGen|
|[#13309](https://github.com/NVIDIA/spark-rapids/pull/13309)|Enable shellcheck|
|[#13313](https://github.com/NVIDIA/spark-rapids/pull/13313)|Add support for optimize table command for Detla IO 3.3|
|[#13320](https://github.com/NVIDIA/spark-rapids/pull/13320)|Fix the checklists in the PR template to reduce confusion [skip test]|
|[#13316](https://github.com/NVIDIA/spark-rapids/pull/13316)|Fixed bug with arithmetic overflow on some map_zip_with tests|
|[#13322](https://github.com/NVIDIA/spark-rapids/pull/13322)|Fix testing.md to use fixed relative paths [skip ci]|
|[#13053](https://github.com/NVIDIA/spark-rapids/pull/13053)|Allowlist zlib for Orc writes|
|[#13220](https://github.com/NVIDIA/spark-rapids/pull/13220)|Add test cases for Collate, CharType and VarcharType|
|[#13295](https://github.com/NVIDIA/spark-rapids/pull/13295)|Avoid thread safety issues on input iterator in AsymmetricJoinSizer|
|[#13087](https://github.com/NVIDIA/spark-rapids/pull/13087)|Add Support for map_zip_with|
|[#13303](https://github.com/NVIDIA/spark-rapids/pull/13303)|wrap shuffle read's underlying input stream with timing logic|
|[#13288](https://github.com/NVIDIA/spark-rapids/pull/13288)|Replace 'get' with `getOrElse` to avoid None.get exception in GpuBatchScanExec|
|[#13287](https://github.com/NVIDIA/spark-rapids/pull/13287)|Revert "enable shellcheck" [skip ci]|
|[#13273](https://github.com/NVIDIA/spark-rapids/pull/13273)|enable offheap limits by default|
|[#13277](https://github.com/NVIDIA/spark-rapids/pull/13277)|Fix auto merge conflict 13276 [skip ci]|
|[#13122](https://github.com/NVIDIA/spark-rapids/pull/13122)|enable shellcheck|
|[#13262](https://github.com/NVIDIA/spark-rapids/pull/13262)|Run test_decimal_precision_over_max for all Spark versions|
|[#13255](https://github.com/NVIDIA/spark-rapids/pull/13255)|concat metrics should not contain iter.hasNext|
|[#13241](https://github.com/NVIDIA/spark-rapids/pull/13241)|Fix ThrottlingExecutor not closed bug for PR 12674|
|[#12674](https://github.com/NVIDIA/spark-rapids/pull/12674)|async threads for shuffle read|
|[#13229](https://github.com/NVIDIA/spark-rapids/pull/13229)|Update dependency version JNI, private, hybrid to 25.10.0-SNAPSHOT|

## Release 25.08

### Features
|||
|:---|:---|
|[#12720](https://github.com/NVIDIA/spark-rapids/issues/12720)|[FEA] Enable qa iceberg daily tests.|
|[#13054](https://github.com/NVIDIA/spark-rapids/issues/13054)|[FEA] Support GpuMultiply with ANSI mode|
|[#11101](https://github.com/NVIDIA/spark-rapids/issues/11101)|[audit] [SPARK-48649][SQL] Add "ignoreInvalidPartitionPaths" and "spark.sql.files.ignoreInvalidPartitionPaths|
|[#5114](https://github.com/NVIDIA/spark-rapids/issues/5114)|[FEA] Support aggregates when ANSI mode is enabled|
|[#12574](https://github.com/NVIDIA/spark-rapids/issues/12574)|[FEA] Add delta-33x module with base implementation|
|[#12714](https://github.com/NVIDIA/spark-rapids/issues/12714)|[FEA] Throw ExtendedAnalysisException while casting when ansi is enabled|
|[#12991](https://github.com/NVIDIA/spark-rapids/issues/12991)|[FEA] Implement GpuCreateDeltaTableCommand for Delta 3.3.x|
|[#12580](https://github.com/NVIDIA/spark-rapids/issues/12580)|[FEA] Accelerate autoOptimize.autoCompact|
|[#12988](https://github.com/NVIDIA/spark-rapids/issues/12988)|[FEA] Implement GpuUpdateCommand for Delta 3.3.x|
|[#12989](https://github.com/NVIDIA/spark-rapids/issues/12989)|[FEA] Implement GpuDeleteCommand for Delta 3.3.x|
|[#12785](https://github.com/NVIDIA/spark-rapids/issues/12785)|[FEA] Update iceberg doc.|
|[#12232](https://github.com/NVIDIA/spark-rapids/issues/12232)|[FEA] Support s3 table for iceberg.|
|[#12679](https://github.com/NVIDIA/spark-rapids/issues/12679)|[FEA] Update to CUDA 12.9|
|[#12578](https://github.com/NVIDIA/spark-rapids/issues/12578)|[FEA] Accelerate autoOptimize.optimizeWrite adaptive shuffled writes|
|[#12793](https://github.com/NVIDIA/spark-rapids/issues/12793)|[FEA] Add GpuOptimisticTransaction to Delta 3.3.0 Shim|
|[#12966](https://github.com/NVIDIA/spark-rapids/issues/12966)|[FEA] Diff between CPU and GPU for casting from Timestamp to other type|
|[#6846](https://github.com/NVIDIA/spark-rapids/issues/6846)|[FEA] Support parsing dates and timestamps with timezone IDs in the string|
|[#10035](https://github.com/NVIDIA/spark-rapids/issues/10035)|[FEA] Support Cast for String to Timestamps for non-UTC timezones|
|[#12738](https://github.com/NVIDIA/spark-rapids/issues/12738)|[FEA] better task.resource.gpu.amount experience|

### Performance
|||
|:---|:---|
|[#12903](https://github.com/NVIDIA/spark-rapids/issues/12903)|[FEA] Run NDS benchmark for Spark-4.0|
|[#12626](https://github.com/NVIDIA/spark-rapids/issues/12626)|[TASK] Run NDS benchmarks once basic Delta 3.3.0 Read is supported|
|[#10571](https://github.com/NVIDIA/spark-rapids/issues/10571)|[AUDIT] [SPARK-46366] Implement `Between` expression GpuOverrides|
|[#13007](https://github.com/NVIDIA/spark-rapids/issues/13007)|[FEA] Look into better performance for ANSI mode SUM reduction/group by aggregation|
|[#13035](https://github.com/NVIDIA/spark-rapids/issues/13035)|[FEA] Add Debug Metrics measuring the actual parallelism of multithreaded reader|
|[#12927](https://github.com/NVIDIA/spark-rapids/issues/12927)|[FEA] optimize casting string to date|

### Bugs Fixed
|||
|:---|:---|
|[#13112](https://github.com/NVIDIA/spark-rapids/issues/13112)|[BUG] GpuRegExpextract exception num_blocks must be > 0|
|[#13242](https://github.com/NVIDIA/spark-rapids/issues/13242)|[BUG] Integration test Intermittently failed Method GpuShuffleExchangeExec.ensReqDPMetricTag is abstract in DB 14.3 runtime|
|[#13131](https://github.com/NVIDIA/spark-rapids/issues/13131)|[BUG] Find a way to disable false ERROR messages for pinned memory|
|[#13279](https://github.com/NVIDIA/spark-rapids/issues/13279)|[BUG] Integration test failures with Delta|
|[#13265](https://github.com/NVIDIA/spark-rapids/issues/13265)|[BUG] test failed Expected error IllegalArgumentException/ArrayIndexOutOfBoundsException|
|[#13254](https://github.com/NVIDIA/spark-rapids/issues/13254)|[BUG] delta_lake_time_travel_test.py::test_time_travel_on_non_existing_table failed Expected error 'AnalysisException' did not appear|
|[#13049](https://github.com/NVIDIA/spark-rapids/issues/13049)|[BUG] hash_aggregate_test tests FAILED on [DATABRICKS]|
|[#13164](https://github.com/NVIDIA/spark-rapids/issues/13164)|[BUG][DATAGEN_SEED=1753338612] test_array_slice_with_negative_length failed in scala 2.13 integration|
|[#13204](https://github.com/NVIDIA/spark-rapids/issues/13204)|[BUG] multi-* expressions does not handle partial replacement within a single expression tree|
|[#13179](https://github.com/NVIDIA/spark-rapids/issues/13179)|[BUG] arithmetic_ops_test.py::test_try_sum_groupby_fallback_to_cpu[Long][DATAGEN_SEED=1753421319] failed in rapids_it-MT-shuffle-standalone|
|[#12949](https://github.com/NVIDIA/spark-rapids/issues/12949)|[BUG] cudaErrorIllegalAddress and cudaErrorIllegalInstruction error on NDS-H parquet3k snappy on Spark 3.3.3|
|[#13217](https://github.com/NVIDIA/spark-rapids/issues/13217)|[BUG] integration test src.main.python.ast_test.test_refer_to_non_fixed_width_column failed on scala 2.13|
|[#13101](https://github.com/NVIDIA/spark-rapids/issues/13101)|[BUG] conditionals_test.py::test_case_when failed due to com.nvidia.spark.rapids.jni.CpuRetryOOM: CPU OutOfMemory in dataproc serverless IT job|
|[#13183](https://github.com/NVIDIA/spark-rapids/issues/13183)|[BUG] json_test.py failure test_arrays_to_json with time stamp failure|
|[#12505](https://github.com/NVIDIA/spark-rapids/issues/12505)|[BUG] AST report ai.rapids.cudf.CudfException:  Invalid, non-fixed-width type|
|[#13188](https://github.com/NVIDIA/spark-rapids/issues/13188)|[BUG] [Spark-4.0] NDS query-18 is failing due to Divide_By_Zero error|
|[#13197](https://github.com/NVIDIA/spark-rapids/issues/13197)|[BUG] databricks 12.2 test failed to install required packages for fastparquet|
|[#13154](https://github.com/NVIDIA/spark-rapids/issues/13154)|[BUG] nds run illegal access seen last night|
|[#11512](https://github.com/NVIDIA/spark-rapids/issues/11512)|[BUG] Support for wider types in read schemas for Parquet Reads|
|[#13040](https://github.com/NVIDIA/spark-rapids/issues/13040)|[BUG] tests fail on systems with more than one GPU|
|[#11931](https://github.com/NVIDIA/spark-rapids/issues/11931)|[BUG][AUDIT][SPARK-50480][SQL] Extend CharType and VarcharType from StringType|
|[#13146](https://github.com/NVIDIA/spark-rapids/issues/13146)|[BUG] CPU off heap limit default subtracts spark.offHeap size from memory overhead|
|[#13152](https://github.com/NVIDIA/spark-rapids/issues/13152)|[BUG] json_test.py::test_structs_to_json_timestamp/arrays_to_json/maps_to_json fails|
|[#13137](https://github.com/NVIDIA/spark-rapids/issues/13137)|[BUG] SQL CASE condition is bypassed leading to error in ELSE branch|
|[#13153](https://github.com/NVIDIA/spark-rapids/issues/13153)|[BUG] arithmetic_ops_test.py::test_try_avg_fallback_to_cpu - GPU and CPU float values are different|
|[#11987](https://github.com/NVIDIA/spark-rapids/issues/11987)|[AUDIT 4.0] [SPARK-50446][PYTHON] Concurrent level in Arrow-optimized Python UDF|
|[#13133](https://github.com/NVIDIA/spark-rapids/issues/13133)|[BUG] test_delta_rtas_sql_liquid_clustering_fallback fails with Spark 3.5.6 pipeline|
|[#13129](https://github.com/NVIDIA/spark-rapids/issues/13129)|[BUG] delta_lake_write_test.py::test_delta_write_optimized_supported_types_partitioned FAILED ON "Part of the plan is not columnar class org.apache.spark.sql.execution.command.ExecutedCommandExec"|
|[#12992](https://github.com/NVIDIA/spark-rapids/issues/12992)|[TASK] Run all integration tests that were xfailed for Spark 3.5.3+|
|[#13062](https://github.com/NVIDIA/spark-rapids/issues/13062)|[BUG] Fallback writes to cpu when liquid clustering is enabled for delta 3.3.x|
|[#13118](https://github.com/NVIDIA/spark-rapids/issues/13118)|[TASK] Avoid skipping tests for optimize writes|
|[#13065](https://github.com/NVIDIA/spark-rapids/issues/13065)|[BUG] cast_test.py::test_ansi_cast_failures_decimal_to_decimal test failed|
|[#13102](https://github.com/NVIDIA/spark-rapids/issues/13102)|[BUG] delta_lake_write_test.py::test_delta_write_round_trip_managed failed due to IllegalArgumentException in databricks nightly test with 350db|
|[#11130](https://github.com/NVIDIA/spark-rapids/issues/11130)|[BUG] `get_json_test.py::test_get_json_object_quoted_question` fails on Spark 4 with mismatched output|
|[#12806](https://github.com/NVIDIA/spark-rapids/issues/12806)|[BUG] Shimplify.py skips shims that are excluded in pom.xml |
|[#13045](https://github.com/NVIDIA/spark-rapids/issues/13045)|[BUG] Add check in `OptimizeExecutor` to ensure that no deletion vector is enabled in delta 3.3.x|
|[#10908](https://github.com/NVIDIA/spark-rapids/issues/10908)|[BUG] Cast string to decimal won't return null for out of range floats|
|[#12969](https://github.com/NVIDIA/spark-rapids/issues/12969)|[BUG] Multiple test failures in `src.main.python.hash_aggregate_test` across various CI jobs|
|[#11047](https://github.com/NVIDIA/spark-rapids/issues/11047)|Revisit the ANSI tests which is enabled by default in Spark 4.0.0|
|[#13096](https://github.com/NVIDIA/spark-rapids/issues/13096)|[BUG] [Spark-4.0] NDS query_94 and query_95 are failing with IllegalArgumentException|
|[#13018](https://github.com/NVIDIA/spark-rapids/issues/13018)|[BUG] AppendDataExecV1 falls back when running CTAS/RTAS on Delta 3.3.x|
|[#13022](https://github.com/NVIDIA/spark-rapids/issues/13022)|[BUG] GpuRowToColumnarExec with RequireSingleBatch allocates a large amount of memory|
|[#12857](https://github.com/NVIDIA/spark-rapids/issues/12857)|[BUG] GPU generate different output as CPU|
|[#13063](https://github.com/NVIDIA/spark-rapids/issues/13063)|[BUG] rapids_it-matrix-dev-github delta_lake_time_travel_test.py test failures DATAGEN_SEED=1751797993|
|[#13070](https://github.com/NVIDIA/spark-rapids/issues/13070)|[BUG] Integration tests failure in json_test.py|
|[#13064](https://github.com/NVIDIA/spark-rapids/issues/13064)|[BUG] rapids_it-matrix-dev-github test_delta_update_fallback_with_deletion_vectors failing DATAGEN_SEED=1751798742|
|[#13037](https://github.com/NVIDIA/spark-rapids/issues/13037)|[BUG] NDS query39_part1  and and query39_part2 are failing due to SparkArithmeticException thrown from the plugin.|
|[#12879](https://github.com/NVIDIA/spark-rapids/issues/12879)|[BUG] Implement MergeIntoCommand for Delta Lake 3.3.0|
|[#13051](https://github.com/NVIDIA/spark-rapids/issues/13051)|[BUG] jason_test.py::test_structs_to_json FAILED on _assert_gpu_and_cpu_are_equal|
|[#13019](https://github.com/NVIDIA/spark-rapids/issues/13019)|[BUG] `GpuOptimisticTransaction` should add the identity column stats tracker as a statsTracker for Delta IO 3.3|
|[#11552](https://github.com/NVIDIA/spark-rapids/issues/11552)|[BUG] [Spark 4] Raw Cudf/JNI exception encountered during casting with ANSI enabled|
|[#12950](https://github.com/NVIDIA/spark-rapids/issues/12950)|[BUG] CREATE VIEW does not function|
|[#13027](https://github.com/NVIDIA/spark-rapids/issues/13027)|[BUG] get-deps-sha1 to support new central.sonatype.com|
|[#11550](https://github.com/NVIDIA/spark-rapids/issues/11550)|[BUG] [Spark 4] Decimal casting errors raised from the plugin do not match those from Spark 4.0 in ANSI mode|
|[#13002](https://github.com/NVIDIA/spark-rapids/issues/13002)|[BUG] Multiple GpuLoreSuite tests failed in nightly UT|
|[#12999](https://github.com/NVIDIA/spark-rapids/issues/12999)|[BUG] delta lake case failed NoClassDefFoundError: org/apache/spark/sql/catalyst/plans/logical/SupportsNonDeterministicExpression in spark350|
|[#12924](https://github.com/NVIDIA/spark-rapids/issues/12924)|[BUG] Gpu statistics on Delta Lake 3.3.0 don't match the expected value in the `delta_lake_write_test.py::test_delta_write_stat_column_limits`|
|[#12930](https://github.com/NVIDIA/spark-rapids/issues/12930)|[BUG] Implement `GpuAppendDataExecV1` for Delta Lake 3.3.x|
|[#12931](https://github.com/NVIDIA/spark-rapids/issues/12931)|[BUG] Implement `GpuAtomicCreateTableAsSelectExec` and `GpuAtomicReplaceTableAsSelectExec` for Delta Lake 3.3.x|
|[#12932](https://github.com/NVIDIA/spark-rapids/issues/12932)|[BUG] Implement `GpuOverwriteByExpressionExecV1` for Delta Lake 3.3.x|
|[#11004](https://github.com/NVIDIA/spark-rapids/issues/11004)|Fix integration test failures for Spark 4.0.0 |
|[#12929](https://github.com/NVIDIA/spark-rapids/issues/12929)|[BUG] Add support for CreatableRelationProvider for Delta Lake 3.3.x|
|[#11555](https://github.com/NVIDIA/spark-rapids/issues/11555)|[BUG] [Spark 4] Invalid results from Casting timestamps to integral types|
|[#12836](https://github.com/NVIDIA/spark-rapids/issues/12836)|[BUG] XFAIL failing tests for Delta 3.3.0|
|[#12001](https://github.com/NVIDIA/spark-rapids/issues/12001)|[BUG] Fix unit test failures in Spark-4.0|
|[#12841](https://github.com/NVIDIA/spark-rapids/issues/12841)|XFAIL the failing tests and assert fallback in delta_zorder_test.py|
|[#12019](https://github.com/NVIDIA/spark-rapids/issues/12019)|[BUG] Spark-4.0: Tests failures in CastOpSuite and conditional_test.py|
|[#11018](https://github.com/NVIDIA/spark-rapids/issues/11018)|Fix tests failures in hash_aggregate_test.py|
|[#11007](https://github.com/NVIDIA/spark-rapids/issues/11007)|Fix tests failures in array_test.py|
|[#12840](https://github.com/NVIDIA/spark-rapids/issues/12840)|XFAIL the failing tests and assert fallback in delta_lake_write_test.py|
|[#12933](https://github.com/NVIDIA/spark-rapids/issues/12933)|Fix integration test failures in dpp_test.py|
|[#11028](https://github.com/NVIDIA/spark-rapids/issues/11028)|Fix tests failures in qa_nightly_select_test.py|
|[#11022](https://github.com/NVIDIA/spark-rapids/issues/11022)|Fix tests failures in join_test.py|
|[#12954](https://github.com/NVIDIA/spark-rapids/issues/12954)|[BUG] GpuDeleteFilterSuite failed in nightly UT|
|[#11017](https://github.com/NVIDIA/spark-rapids/issues/11017)|Fix tests failures in url_test.py|
|[#11030](https://github.com/NVIDIA/spark-rapids/issues/11030)|Fix tests failures in string_test.py|
|[#11016](https://github.com/NVIDIA/spark-rapids/issues/11016)|Fix tests failures in csv_test.py|
|[#11009](https://github.com/NVIDIA/spark-rapids/issues/11009)|Fix tests failures in cast_test.py|
|[#12772](https://github.com/NVIDIA/spark-rapids/issues/12772)|[BUG] Drop the `materializeSourceTimeMs` from some delta_lake_merge_tests |
|[#11014](https://github.com/NVIDIA/spark-rapids/issues/11014)|Fix tests failures in json_test.py|
|[#12926](https://github.com/NVIDIA/spark-rapids/issues/12926)|[BUG] A diff issue which could only be reproduce on Ampere GPU|
|[#11015](https://github.com/NVIDIA/spark-rapids/issues/11015)|Fix tests failures in parquet_test.py|
|[#11012](https://github.com/NVIDIA/spark-rapids/issues/11012)|Fix tests failures in conditionals_test.py|
|[#11010](https://github.com/NVIDIA/spark-rapids/issues/11010)|Fix tests failures in cache_test.py|
|[#11005](https://github.com/NVIDIA/spark-rapids/issues/11005)|Fix test failures in aqe_test.py|
|[#11013](https://github.com/NVIDIA/spark-rapids/issues/11013)|Fix tests failures in orc_test.py|
|[#12915](https://github.com/NVIDIA/spark-rapids/issues/12915)|[BUG] Delta Lake nightly tests failing due to a regression|
|[#12838](https://github.com/NVIDIA/spark-rapids/issues/12838)|XFAIL the failing tests and assert fallback in delta_lake_merge_test.py|
|[#12884](https://github.com/NVIDIA/spark-rapids/issues/12884)|[BUG] [Spark-4.0] Scala tests in integration_tests module are failing|
|[#12870](https://github.com/NVIDIA/spark-rapids/issues/12870)|[BUG] 25.08 ansi_cast string to bool (invalid values) failed: java.lang.AssertionError: Column has non-empty nulls|

### PRs
|||
|:---|:---|
|[#13344](https://github.com/NVIDIA/spark-rapids/pull/13344)|[DOC] update the wrong links in download doc [skip ci]|
|[#13292](https://github.com/NVIDIA/spark-rapids/pull/13292)|Update changelog for the v25.08 release [skip ci]|
|[#13286](https://github.com/NVIDIA/spark-rapids/pull/13286)|Temporarily disable timezone America/Coyhaique to unblock branch-25.08 release|
|[#13282](https://github.com/NVIDIA/spark-rapids/pull/13282)|Update changelog for the v25.08 release [skip ci]|
|[#13280](https://github.com/NVIDIA/spark-rapids/pull/13280)|Fix fallback test params for Delta MergeCommand, UpdateCommand, and DeleteCommand|
|[#13258](https://github.com/NVIDIA/spark-rapids/pull/13258)|Update changelog for the v25.08 release [skip ci]|
|[#13257](https://github.com/NVIDIA/spark-rapids/pull/13257)|Update dependency version JNI, private, hybrid to 25.08.0|
|[#13123](https://github.com/NVIDIA/spark-rapids/pull/13123)|Enable MERGE, UPDATE, DELETE by default for Delta 3.3.0|
|[#13267](https://github.com/NVIDIA/spark-rapids/pull/13267)|Use pytest.ExceptionInfo to create the same exception text as pytest raises|
|[#13244](https://github.com/NVIDIA/spark-rapids/pull/13244)|[DOC] update the download doc for 2508 release [skip ci]|
|[#13261](https://github.com/NVIDIA/spark-rapids/pull/13261)|Print DB runtime version details for debugging purpose [skip ci]|
|[#13253](https://github.com/NVIDIA/spark-rapids/pull/13253)|Add override tag to the 'ensReqDPMetricTag' and change it to a 'val'|
|[#13248](https://github.com/NVIDIA/spark-rapids/pull/13248)|Close shared buffers at batch read completion in kudo deser stream iter|
|[#13243](https://github.com/NVIDIA/spark-rapids/pull/13243)|Define the DB required method named 'ensReqDPMetricTag' in GpuShuffleExechangeExec.|
|[#13236](https://github.com/NVIDIA/spark-rapids/pull/13236)|Fix case where null start index or length would throw invalid parameter, instead of returning null rows|
|[#13211](https://github.com/NVIDIA/spark-rapids/pull/13211)|Improve the pull request template [skip ci]|
|[#13231](https://github.com/NVIDIA/spark-rapids/pull/13231)|Implements scalar * scalar overload function|
|[#13213](https://github.com/NVIDIA/spark-rapids/pull/13213)|Fix expression combining when only some can combine|
|[#13196](https://github.com/NVIDIA/spark-rapids/pull/13196)|Run try_ functions aggregation arithmetic tests on single partition|
|[#13199](https://github.com/NVIDIA/spark-rapids/pull/13199)|Fallback to CPU for unsupported binary formats|
|[#13218](https://github.com/NVIDIA/spark-rapids/pull/13218)|Use non-ANSI mode to fix overflow error on Spark 400|
|[#13207](https://github.com/NVIDIA/spark-rapids/pull/13207)|Specify timestamp format for to_json for Spark 400|
|[#12506](https://github.com/NVIDIA/spark-rapids/pull/12506)|Fix AST report can not create non-fixed-width column error|
|[#13192](https://github.com/NVIDIA/spark-rapids/pull/13192)|Fix bug with divide by zero in ANSI avg on decimal128 output|
|[#13108](https://github.com/NVIDIA/spark-rapids/pull/13108)|Support ANSI multiply|
|[#13206](https://github.com/NVIDIA/spark-rapids/pull/13206)|Add the project name for jdk-profiles [skip ci]|
|[#13202](https://github.com/NVIDIA/spark-rapids/pull/13202)|Disable fastparquet tests for  versions < 13.3|
|[#13180](https://github.com/NVIDIA/spark-rapids/pull/13180)|Add tests for the cases of decimal precision going beyond 38|
|[#13195](https://github.com/NVIDIA/spark-rapids/pull/13195)|disable offheap limit by default|
|[#13194](https://github.com/NVIDIA/spark-rapids/pull/13194)|Fix a small memory leak that can show up in some error cases|
|[#13165](https://github.com/NVIDIA/spark-rapids/pull/13165)|Allow for better parquet type conversion|
|[#13163](https://github.com/NVIDIA/spark-rapids/pull/13163)|xfail mixed read for 25.08 release [skip ci]|
|[#13143](https://github.com/NVIDIA/spark-rapids/pull/13143)|Add in ANSI AVG support|
|[#13178](https://github.com/NVIDIA/spark-rapids/pull/13178)|Add config to skip dumping lore plan.|
|[#13078](https://github.com/NVIDIA/spark-rapids/pull/13078)|kudo gpu kernel usage|
|[#13158](https://github.com/NVIDIA/spark-rapids/pull/13158)|Host limits fix device count|
|[#13151](https://github.com/NVIDIA/spark-rapids/pull/13151)|Use the "StringType" object for the string type comparison in `case-match` statements|
|[#13157](https://github.com/NVIDIA/spark-rapids/pull/13157)|dont subtract spark offheap from spark memoryOverhead|
|[#13150](https://github.com/NVIDIA/spark-rapids/pull/13150)|Adds ctas/rtas sql test with managed tables for deltalake 3.3.x|
|[#13127](https://github.com/NVIDIA/spark-rapids/pull/13127)|Add tests for between sql and between expr|
|[#13162](https://github.com/NVIDIA/spark-rapids/pull/13162)|Fix failed to_json test cases|
|[#13149](https://github.com/NVIDIA/spark-rapids/pull/13149)|Support side-effect check for the "else" path in GpuCaseWhen|
|[#13155](https://github.com/NVIDIA/spark-rapids/pull/13155)|Add approximate_float decorator to try_avg_fallback_to_cpu  integration test|
|[#13095](https://github.com/NVIDIA/spark-rapids/pull/13095)|Fallback to CPU for try_ functions.|
|[#13126](https://github.com/NVIDIA/spark-rapids/pull/13126)|Supports `Invoke` generated by `RuntimeReplaceable` expressions.|
|[#13117](https://github.com/NVIDIA/spark-rapids/pull/13117)|Improve performance of ANSI SUM integer aggregations|
|[#13139](https://github.com/NVIDIA/spark-rapids/pull/13139)|Allow override mvn command in version-def [skip ci]|
|[#13031](https://github.com/NVIDIA/spark-rapids/pull/13031)|Add in code to halt on shutdown timeouts|
|[#13120](https://github.com/NVIDIA/spark-rapids/pull/13120)|[Spark-4.0] Throw RuntimeException for invalid string to boolean cast in ansi mode|
|[#13136](https://github.com/NVIDIA/spark-rapids/pull/13136)|Set CONDA_PLUGINS_AUTO_ACCEPT_TOS=yes for CI docker build and support python 3.12+ IT test [skip ci]|
|[#13135](https://github.com/NVIDIA/spark-rapids/pull/13135)|Skip test_delta_rtas_sql_liquid_clustering_fallback on Spark 3.5.6|
|[#13132](https://github.com/NVIDIA/spark-rapids/pull/13132)|Fix skip condition for test_delta_write_optimized_supported_types_partitioned|
|[#13111](https://github.com/NVIDIA/spark-rapids/pull/13111)|Ensure deltalake 3.3.x writes fallback when liquid clustering is enabled|
|[#13119](https://github.com/NVIDIA/spark-rapids/pull/13119)|Avoid skipping optimizewrite tests on Spark 3.5.3+|
|[#13113](https://github.com/NVIDIA/spark-rapids/pull/13113)|Add test to verify materialization while merging|
|[#13109](https://github.com/NVIDIA/spark-rapids/pull/13109)|Use the original input column to get the overflow decimal values|
|[#13107](https://github.com/NVIDIA/spark-rapids/pull/13107)|Xfail `test_delta_write_round_trip_managed` on Databricks 14.3|
|[#13114](https://github.com/NVIDIA/spark-rapids/pull/13114)|Fix conda build CondaToSNonInteractiveError issue [skip ci]|
|[#13104](https://github.com/NVIDIA/spark-rapids/pull/13104)|Add Spark 400 shim for get_json_object to handle named string with '?'|
|[#12807](https://github.com/NVIDIA/spark-rapids/pull/12807)|Ignore `dyn.shim.excluded.releases` when shimplifying |
|[#13094](https://github.com/NVIDIA/spark-rapids/pull/13094)|Remove `AppendDataExecV1` from the allow_non_gpu list in `delta_lake_write_test.py` |
|[#13068](https://github.com/NVIDIA/spark-rapids/pull/13068)|Add some debug info for iceberg test.|
|[#13103](https://github.com/NVIDIA/spark-rapids/pull/13103)|Add sanity check in `GpuOptimizeExecutor` to ensure deletion vector is disabled.|
|[#13089](https://github.com/NVIDIA/spark-rapids/pull/13089)|Fix up the exception type for casting strings to decimals.|
|[#13056](https://github.com/NVIDIA/spark-rapids/pull/13056)|Revert "declare shuffle thread for threaded writer and reader"|
|[#13069](https://github.com/NVIDIA/spark-rapids/pull/13069)|avoid using Thread.sleep in the flaky test case|
|[#13082](https://github.com/NVIDIA/spark-rapids/pull/13082)|Add a write round trip test for managed delta tables|
|[#13083](https://github.com/NVIDIA/spark-rapids/pull/13083)|Handle StagedTableCatalog when creating AppendDataExecV1|
|[#12948](https://github.com/NVIDIA/spark-rapids/pull/12948)|enable off heap limit by default|
|[#13085](https://github.com/NVIDIA/spark-rapids/pull/13085)|[DOC] fix broken link in download page [skip ci]|
|[#13032](https://github.com/NVIDIA/spark-rapids/pull/13032)|Update tests/aggregations for ANSI mode, when they don't care|
|[#13084](https://github.com/NVIDIA/spark-rapids/pull/13084)|Skip `test_time_travel_on_non_existing_table` with 'AS OF' before Spark 3.3.0|
|[#13017](https://github.com/NVIDIA/spark-rapids/pull/13017)|Add Create Delta Table command support on GPU for Delta 3.3.x |
|[#13079](https://github.com/NVIDIA/spark-rapids/pull/13079)|Add $ in front of bash variable IS_SPARK_35X in spark-test.sh|
|[#13075](https://github.com/NVIDIA/spark-rapids/pull/13075)|Revert "Support RuntimeReplaceable version of StructsToJson for Spark 400  (#13029)"|
|[#13043](https://github.com/NVIDIA/spark-rapids/pull/13043)|Spark-4.0: Enable ansi mode for some of the integration tests|
|[#13066](https://github.com/NVIDIA/spark-rapids/pull/13066)|Fix delta time travel test|
|[#13067](https://github.com/NVIDIA/spark-rapids/pull/13067)|Fix incorrect deltalake update fallback test|
|[#13046](https://github.com/NVIDIA/spark-rapids/pull/13046)|Replace the `copyToHost` with `getScalarElement` when dealing with some exception cases.|
|[#13050](https://github.com/NVIDIA/spark-rapids/pull/13050)|Merge nulls for left and right operands before performing a div|
|[#13038](https://github.com/NVIDIA/spark-rapids/pull/13038)|Add iceberg s3tables test in integration test [skip ci]|
|[#13034](https://github.com/NVIDIA/spark-rapids/pull/13034)|Add GpuMergeIntoCommand for Delta Lake 3.3.x |
|[#13048](https://github.com/NVIDIA/spark-rapids/pull/13048)|Add tests for time travel in deltalake|
|[#13060](https://github.com/NVIDIA/spark-rapids/pull/13060)|Fix failed to_json cases on non-UTC timezones|
|[#13039](https://github.com/NVIDIA/spark-rapids/pull/13039)|Fix failed cases in ParseDateTimeSuite for Spark 400|
|[#13058](https://github.com/NVIDIA/spark-rapids/pull/13058)|Disable approx_percentile tests temporarily to unblock CI|
|[#13033](https://github.com/NVIDIA/spark-rapids/pull/13033)|Add support for identity column for Delta IO|
|[#13042](https://github.com/NVIDIA/spark-rapids/pull/13042)|Re-xfail test_delta_update_fallback_with_deletion_vectors for databricks|
|[#13036](https://github.com/NVIDIA/spark-rapids/pull/13036)|Add Debug Metrics tracking the parallelism of multithreaded reader|
|[#13023](https://github.com/NVIDIA/spark-rapids/pull/13023)|Auto compact|
|[#13029](https://github.com/NVIDIA/spark-rapids/pull/13029)|Support RuntimeReplaceable version of StructsToJson for Spark 400|
|[#13014](https://github.com/NVIDIA/spark-rapids/pull/13014)|Implement update command for deltalake 3.3.x|
|[#13024](https://github.com/NVIDIA/spark-rapids/pull/13024)|Align the exception type with Spark for casting to numbers|
|[#13010](https://github.com/NVIDIA/spark-rapids/pull/13010)|[BUG] Auto compact tests should use the confs with auto compact enabled|
|[#12986](https://github.com/NVIDIA/spark-rapids/pull/12986)|Add in ANSI support for sum aggregation|
|[#13030](https://github.com/NVIDIA/spark-rapids/pull/13030)|mvn verify cache: replace xmllint with ubuntu built-in commands [skip ci]|
|[#13026](https://github.com/NVIDIA/spark-rapids/pull/13026)|Update snapshot repo to central.sonatype.com|
|[#13003](https://github.com/NVIDIA/spark-rapids/pull/13003)|Introduce delete command for deltalake 3.3.0|
|[#13013](https://github.com/NVIDIA/spark-rapids/pull/13013)|Remove redundant tests to accelerate premerge CI|
|[#12940](https://github.com/NVIDIA/spark-rapids/pull/12940)|Add iceberg aws s3table support.|
|[#13000](https://github.com/NVIDIA/spark-rapids/pull/13000)|Align the exception message for casting floats to decimals|
|[#12938](https://github.com/NVIDIA/spark-rapids/pull/12938)|Add support for optimizeWrite for Delta IO|
|[#12998](https://github.com/NVIDIA/spark-rapids/pull/12998)|Some missing changes for CTAS and RTAS for Delta Lake 3.3.x|
|[#13004](https://github.com/NVIDIA/spark-rapids/pull/13004)|Fix lore GpuInsertIntoHiveTableunit test failures in spark332,330cdh,334|
|[#12526](https://github.com/NVIDIA/spark-rapids/pull/12526)|Add sort time metrics for GPU write exec|
|[#13001](https://github.com/NVIDIA/spark-rapids/pull/13001)|Fix spark-test for delta 3.3.0 [skip ci]|
|[#12985](https://github.com/NVIDIA/spark-rapids/pull/12985)|Align the exception message with that of Spark for casting to Decimals|
|[#12901](https://github.com/NVIDIA/spark-rapids/pull/12901)|fix GpuDeviceManager off heap memory log line|
|[#12987](https://github.com/NVIDIA/spark-rapids/pull/12987)|Remove user|
|[#12990](https://github.com/NVIDIA/spark-rapids/pull/12990)|Add ctas, rtas, append, overwrite for deltalake 3.3.x.|
|[#12923](https://github.com/NVIDIA/spark-rapids/pull/12923)|LoRE: support GpuInsertIntoHiveTable command|
|[#12983](https://github.com/NVIDIA/spark-rapids/pull/12983)|Revert signature changes of executeTasks in GpuFileFormatWriter.|
|[#12981](https://github.com/NVIDIA/spark-rapids/pull/12981)|Enable delta lake tests when TEST_MODE is DEFAULT |
|[#12984](https://github.com/NVIDIA/spark-rapids/pull/12984)|enable rmm state log for troubleshooting 12883|
|[#12977](https://github.com/NVIDIA/spark-rapids/pull/12977)|Support writing to table in deltalake 33x|
|[#12970](https://github.com/NVIDIA/spark-rapids/pull/12970)|Update exception message for spark400 integration tests|
|[#12978](https://github.com/NVIDIA/spark-rapids/pull/12978)|Set overflow rows to nulls when casting timestamps to integrals for Spark 400|
|[#12973](https://github.com/NVIDIA/spark-rapids/pull/12973)|declare shuffle thread for threaded writer and reader|
|[#12971](https://github.com/NVIDIA/spark-rapids/pull/12971)|Ignore .mvn [skip ci]|
|[#12965](https://github.com/NVIDIA/spark-rapids/pull/12965)|Fix Spark400 failed cases in CastOpSuite|
|[#12962](https://github.com/NVIDIA/spark-rapids/pull/12962)|optimize shuffle read perf after getting a cpu wall time flame graph from customer|
|[#12964](https://github.com/NVIDIA/spark-rapids/pull/12964)|Fix failures in hash aggregate tests|
|[#12893](https://github.com/NVIDIA/spark-rapids/pull/12893)|change a critical log's log level|
|[#12955](https://github.com/NVIDIA/spark-rapids/pull/12955)|Fix the failures in array_test.py for Spark 400|
|[#12935](https://github.com/NVIDIA/spark-rapids/pull/12935)|XFAIL failing tests in `delta_lake_write_test.py` and assert fallback for Delta Lake 3.3.x|
|[#12958](https://github.com/NVIDIA/spark-rapids/pull/12958)|Fix Spark 400 IT failures in date_time_test.py|
|[#12956](https://github.com/NVIDIA/spark-rapids/pull/12956)|Disable ansi mode for the kudo dump tests.|
|[#12957](https://github.com/NVIDIA/spark-rapids/pull/12957)|Xfail some tests in json_matrix_test.py for Spark 400+|
|[#12959](https://github.com/NVIDIA/spark-rapids/pull/12959)|Allow the `EmptyRelationExec` on CPU for dpp empty relation tests|
|[#12953](https://github.com/NVIDIA/spark-rapids/pull/12953)|Disable ansi mode for some tests in qa nightly|
|[#12939](https://github.com/NVIDIA/spark-rapids/pull/12939)|[Spark-4.0] Resolve integration test failures in join_test.py|
|[#12961](https://github.com/NVIDIA/spark-rapids/pull/12961)|Fix get string in GpuDeleteFilter test|
|[#12952](https://github.com/NVIDIA/spark-rapids/pull/12952)|xfail the url tests for Spark 400+|
|[#12944](https://github.com/NVIDIA/spark-rapids/pull/12944)|[follow-up] Refactor: rename castStringToDateAnsi to castStringToDate|
|[#12947](https://github.com/NVIDIA/spark-rapids/pull/12947)|Fix Spark 4.0 IT failures: test_conv|
|[#12946](https://github.com/NVIDIA/spark-rapids/pull/12946)|Fix Spark 4.0 IT failures: csv_test.py|
|[#12942](https://github.com/NVIDIA/spark-rapids/pull/12942)|Fix IT: cast_test.py|
|[#12945](https://github.com/NVIDIA/spark-rapids/pull/12945)|Fix JSON test failures in for 400|
|[#12784](https://github.com/NVIDIA/spark-rapids/pull/12784)|Add GpuDeleteFilterSuite|
|[#12824](https://github.com/NVIDIA/spark-rapids/pull/12824)|Fix bug in casting string to timestamp: Spark400+ and DB35 do not support pattern: spaces + Thh:mm:ss|
|[#12669](https://github.com/NVIDIA/spark-rapids/pull/12669)|Fully support casting string to date via new kernel|
|[#12843](https://github.com/NVIDIA/spark-rapids/pull/12843)|Add Spark 3.5.3 and Delta 3.3.0 to `spark-tests.sh`|
|[#12925](https://github.com/NVIDIA/spark-rapids/pull/12925)|[Spark-4.0]: Resolve integration tests in parquet_test|
|[#12904](https://github.com/NVIDIA/spark-rapids/pull/12904)|Spark-4.0 - Resolve/skip integration test failures|
|[#12914](https://github.com/NVIDIA/spark-rapids/pull/12914)|Skip Ansi check for Count agg.|
|[#12869](https://github.com/NVIDIA/spark-rapids/pull/12869)|Revert "Remove Delta Lake 3.3.0 support from the plugin for Spark 3.5.3, 3.5.4 and 3.5.5 (#12853)"|
|[#12908](https://github.com/NVIDIA/spark-rapids/pull/12908)|Fix test_orc_fallback for Spark400|
|[#12916](https://github.com/NVIDIA/spark-rapids/pull/12916)|Make `assert_func` an optional parameter with a default value|
|[#12910](https://github.com/NVIDIA/spark-rapids/pull/12910)|Revert iceberg changes for 25.06 release|
|[#12912](https://github.com/NVIDIA/spark-rapids/pull/12912)|Fix auto merge conflict 12911 [skip ci]|
|[#12898](https://github.com/NVIDIA/spark-rapids/pull/12898)|Add more sanity check for iceberg multi thread reader|
|[#12892](https://github.com/NVIDIA/spark-rapids/pull/12892)|Fix Column to Expression conversion for Spark 4.0|
|[#12880](https://github.com/NVIDIA/spark-rapids/pull/12880)|XFAIL'd Delta Lake 3.3.0 merge tests |
|[#12680](https://github.com/NVIDIA/spark-rapids/pull/12680)|cpu mem defaults|
|[#12890](https://github.com/NVIDIA/spark-rapids/pull/12890)|Add a new metric to indicate the IO time in GPU write.|
|[#12875](https://github.com/NVIDIA/spark-rapids/pull/12875)|Support spark400 shim's integration tests|
|[#12831](https://github.com/NVIDIA/spark-rapids/pull/12831)|Support eager I/O prefetch for scan as two-sides of bucket ShuffleSizedHashJoin|
|[#12854](https://github.com/NVIDIA/spark-rapids/pull/12854)|Drop the new time based metric in commitInfo while comparing Delta Logs|
|[#12847](https://github.com/NVIDIA/spark-rapids/pull/12847)|Update spark400 to released version|
|[#12777](https://github.com/NVIDIA/spark-rapids/pull/12777)|Fix auto merge conflict 12774 [skip ci]|
|[#12761](https://github.com/NVIDIA/spark-rapids/pull/12761)|Use cuda12 as default, drop cuda11 support|
|[#12721](https://github.com/NVIDIA/spark-rapids/pull/12721)|Fix auto merge conflict 12710 [skip ci]|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
