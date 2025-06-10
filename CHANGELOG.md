# Change log
Generated on 2025-06-10

## Release 25.06

### Features
|||
|:---|:---|
|[#12210](https://github.com/NVIDIA/spark-rapids/issues/12210)|[FEA] Support merge on read of iceberg table.|
|[#12779](https://github.com/NVIDIA/spark-rapids/issues/12779)|[FEA] Support Spark 3.5.6|
|[#12757](https://github.com/NVIDIA/spark-rapids/issues/12757)|[FEA] Accelerated Delta Scan without deletionvector support|
|[#12740](https://github.com/NVIDIA/spark-rapids/issues/12740)|[FEA] Introduce gpu delete filter for iceberg merge on read scan.|
|[#12755](https://github.com/NVIDIA/spark-rapids/issues/12755)|[FEA] Add iceberg merge on read integration tests.|
|[#12790](https://github.com/NVIDIA/spark-rapids/issues/12790)|[FEA] Support `unix_timestamp` for `America/Los_Angeles` timezone|
|[#12209](https://github.com/NVIDIA/spark-rapids/issues/12209)|[FEA] Supports reading iceberg table without deletions|
|[#9077](https://github.com/NVIDIA/spark-rapids/issues/9077)|[FEA] Support bit_or|
|[#12571](https://github.com/NVIDIA/spark-rapids/issues/12571)|[FEA] bit_count|
|[#12623](https://github.com/NVIDIA/spark-rapids/issues/12623)|[FEA] Add a minimum delta-33x module that compiles common code|
|[#10221](https://github.com/NVIDIA/spark-rapids/issues/10221)|[FEA] [follow-up] Parse just time to timestamp|
|[#12024](https://github.com/NVIDIA/spark-rapids/issues/12024)|[FEA] Support from_utc_timestamp for PST/EST/CST|
|[#11639](https://github.com/NVIDIA/spark-rapids/issues/11639)|[FEA] Support to_utc_timestamp for US/Central timezone|
|[#12537](https://github.com/NVIDIA/spark-rapids/issues/12537)|[FEA] Do we need a timezone for Date to String and String to Date|
|[#12593](https://github.com/NVIDIA/spark-rapids/issues/12593)|[FEA] Move `SparkSchemaConverter` to a top class.|
|[#12519](https://github.com/NVIDIA/spark-rapids/issues/12519)|[FEA] CICD Ubuntu dockerfiles to support 24.04 LTS|
|[#6840](https://github.com/NVIDIA/spark-rapids/issues/6840)|[FEA] Support timestamp transitions to and from UTC for repeating rules (daylight savings time zones)|
|[#12591](https://github.com/NVIDIA/spark-rapids/issues/12591)|[FEA] Add some utility class from iceberg-spark-runtime.|
|[#7035](https://github.com/NVIDIA/spark-rapids/issues/7035)|[FEA]Support function sha1|
|[#8511](https://github.com/NVIDIA/spark-rapids/issues/8511)|[FEA] Support conv function|
|[#12368](https://github.com/NVIDIA/spark-rapids/issues/12368)|[FEA] HybridScan: Add a specific SQL Hint to enable HYBRID_SCAN on certain tables|
|[#12358](https://github.com/NVIDIA/spark-rapids/issues/12358)|[FEA] Update rapids JNI, private and hybrid dependency version to 25.06.0-SNAPSHOT|

### Performance
|||
|:---|:---|
|[#12472](https://github.com/NVIDIA/spark-rapids/issues/12472)|[FEA] Zero Config - Centralized Priority Assignment|
|[#12557](https://github.com/NVIDIA/spark-rapids/issues/12557)|[FEA] by default enable skip agg when the inputs of agg cannot reduce|
|[#12457](https://github.com/NVIDIA/spark-rapids/issues/12457)|[FEA] Add a specific physical rule for LocalAggregate|

### Bugs Fixed
|||
|:---|:---|
|[#12855](https://github.com/NVIDIA/spark-rapids/issues/12855)|[BUG] delta_lake_test.py::test_delta_read_column_mapping fails with delta 3.3.0|
|[#12845](https://github.com/NVIDIA/spark-rapids/issues/12845)|[BUG] [test] Specify timezones in pytest according to JVM version|
|[#12856](https://github.com/NVIDIA/spark-rapids/issues/12856)|[BUG] 400+ iceberg test cases failed DATAGEN_SEED=1748978166, INJECT_OOM with spark356|
|[#12816](https://github.com/NVIDIA/spark-rapids/issues/12816)|[BUG] Iceberg nightly test failed code5 with spark355 "No tests collected"|
|[#11235](https://github.com/NVIDIA/spark-rapids/issues/11235)|[BUG] 229 Unit tests failed in Spark 400 against cuda 11 and scala 2.13|
|[#11556](https://github.com/NVIDIA/spark-rapids/issues/11556)|[BUG] [Spark 4] Exceptions from casting to `DATE`/`TIMESTAMP` do not match the Spark's exceptions, with ANSI enabled|
|[#12808](https://github.com/NVIDIA/spark-rapids/issues/12808)|[BUG] Integration test fails with undefined use_cdf|
|[#12015](https://github.com/NVIDIA/spark-rapids/issues/12015)|[BUG] Spark-4.0: Tests failures in WindowFunctionSuite|
|[#12022](https://github.com/NVIDIA/spark-rapids/issues/12022)|[BUG] Spark-4.0: Tests failures in HashAggregatesSuite|
|[#12013](https://github.com/NVIDIA/spark-rapids/issues/12013)|[BUG] Spark-4.0: Test failure in JoinsSuite|
|[#12009](https://github.com/NVIDIA/spark-rapids/issues/12009)|[BUG] Spark-4.0: Tests failures in CostBasedOptimizerSuite|
|[#12006](https://github.com/NVIDIA/spark-rapids/issues/12006)|[BUG] Spark-4.0: Tests failures in AdaptiveQueryExecSuite|
|[#12794](https://github.com/NVIDIA/spark-rapids/issues/12794)|[BUG] Iceberg 3.5.2 failed|
|[#12802](https://github.com/NVIDIA/spark-rapids/issues/12802)|[BUG] iceberg integration tests failed on python 3.8.|
|[#12012](https://github.com/NVIDIA/spark-rapids/issues/12012)|[BUG] Spark-4.0: Tests failures in ParquetWriterSuite|
|[#12758](https://github.com/NVIDIA/spark-rapids/issues/12758)|[BUG] dataframe write option for optimizeWrite for delta is ignored on databricks|
|[#12770](https://github.com/NVIDIA/spark-rapids/issues/12770)|[BUG] scala2.13 iceberg nightly test failed ClassNotFoundException: com.nvidia.spark.rapids.iceberg.IcebergProviderImpl|
|[#12004](https://github.com/NVIDIA/spark-rapids/issues/12004)|[BUG] Spark-4.0: Tests failures in StringOperatorsSuite|
|[#12014](https://github.com/NVIDIA/spark-rapids/issues/12014)|[BUG] Spark-4.0: Tests failures in TimeOperatorsSuite|
|[#12723](https://github.com/NVIDIA/spark-rapids/issues/12723)|[BUG] spark.rapids.sql.mode=explainOnly fails without GPU present|
|[#12608](https://github.com/NVIDIA/spark-rapids/issues/12608)|[BUG] Unhandled exceptions in Python worker|
|[#12703](https://github.com/NVIDIA/spark-rapids/issues/12703)|[BUG] Spilling state not cleaned properly when a task is killed by spark speculation|
|[#12682](https://github.com/NVIDIA/spark-rapids/issues/12682)|[BUG] Multiple Delta Lake tests failing|
|[#12677](https://github.com/NVIDIA/spark-rapids/issues/12677)|[BUG] `iceberg-common` produced scala classes can't be interpreted by scala compiler.|
|[#12702](https://github.com/NVIDIA/spark-rapids/issues/12702)|[BUG]  key not found: maxWritersNumber in databricks runtimes|
|[#12659](https://github.com/NVIDIA/spark-rapids/issues/12659)|[BUG] spark350 build failed GpuMultiFileBatchReader.java: error: not found: type ParquetFileInfo*|
|[#12003](https://github.com/NVIDIA/spark-rapids/issues/12003)|[BUG] Spark-4.0: Tests failures in ParseDateTimeSuite|
|[#12637](https://github.com/NVIDIA/spark-rapids/issues/12637)|[BUG] date_time_test.py failed to import zoneinfo for python 3.8 (databricks 11 and 12 runtimes)|
|[#12529](https://github.com/NVIDIA/spark-rapids/issues/12529)|[BUG][metric] build time total should exclude parent operator time.|
|[#12633](https://github.com/NVIDIA/spark-rapids/issues/12633)|[BUG] Integration Tests Fail For Months_Between|
|[#12625](https://github.com/NVIDIA/spark-rapids/issues/12625)|[BUG] UnsupportedOperationException When Reading Boolean Fields From Bigquery|
|[#12016](https://github.com/NVIDIA/spark-rapids/issues/12016)|[BUG] Spark-4.0: Tests failures in GpuLoreSuite|
|[#12017](https://github.com/NVIDIA/spark-rapids/issues/12017)|[BUG]Spark-4.0: Tests failures in ExpandExecSuite|
|[#12018](https://github.com/NVIDIA/spark-rapids/issues/12018)|[BUG] Spark-4.0: Tests failures in OrcScanSuite|
|[#12008](https://github.com/NVIDIA/spark-rapids/issues/12008)|[BUG] Spark-4.0: Test failure in  HashSortOptimizeSuite|
|[#12010](https://github.com/NVIDIA/spark-rapids/issues/12010)|[BUG] Spark-4.0: Test failure in GpuBringBackToHostSuite|
|[#12011](https://github.com/NVIDIA/spark-rapids/issues/12011)|[BUG] Spark-4.0: Test failure in ParquetFilterSuite|
|[#12002](https://github.com/NVIDIA/spark-rapids/issues/12002)|[BUG] Spark-4.0: Tests failures in OrcFilterSuite|
|[#12005](https://github.com/NVIDIA/spark-rapids/issues/12005)|[BUG] Spark-4.0: Tests failures in UnaryOperatorsSuite|
|[#12007](https://github.com/NVIDIA/spark-rapids/issues/12007)|[BUG] Spark-4.0: Test failure in DecimalBinaryOpSuite|
|[#12586](https://github.com/NVIDIA/spark-rapids/issues/12586)|[BUG] Integration tests  AssertionError: CPU and GPU list have different lengths at [] CPU: xxx GPU: xxx-1|
|[#12539](https://github.com/NVIDIA/spark-rapids/issues/12539)|[BUG] Unable to display rows from a Delta Table on Databricks 13.3 with Deletion Vectors enabled|
|[#12611](https://github.com/NVIDIA/spark-rapids/issues/12611)|[BUG] spark400 build failed: GpuAtomicCreateTableAsSelectExec.scala:77: not enough arguments for method writeToTable|
|[#12587](https://github.com/NVIDIA/spark-rapids/issues/12587)|[BUG] Installing pip on Databricks 11.3/12.2 failed due to `get-pip.py` is archived for python 3.8 an below|
|[#12583](https://github.com/NVIDIA/spark-rapids/issues/12583)|[BUG] test_std_variance and test_std_variance_nulls failed Input to `group_merge_m2` has invalid children type.|
|[#12468](https://github.com/NVIDIA/spark-rapids/issues/12468)|[BUG]GpuStddevPop produces different values than Spark when a group number is larger than Int.MaxValue|
|[#12566](https://github.com/NVIDIA/spark-rapids/issues/12566)|[BUG] Python UDF test failure|
|[#9784](https://github.com/NVIDIA/spark-rapids/issues/9784)|[BUG] test_conv_dec_to_from_hex for different datagen_seeds|
|[#12528](https://github.com/NVIDIA/spark-rapids/issues/12528)|[BUG][metric] time in heuristic (excl. SemWait)|
|[#12538](https://github.com/NVIDIA/spark-rapids/issues/12538)|[BUG] rapids_scala213_nightly-dev-github build failure spark400 |
|[#12532](https://github.com/NVIDIA/spark-rapids/issues/12532)|[BUG] missing companion metrics for Gpu Scan scan time|
|[#11326](https://github.com/NVIDIA/spark-rapids/issues/11326)|[BUG] Dynamic partitions metric for insert into hive appears to be off|
|[#12523](https://github.com/NVIDIA/spark-rapids/issues/12523)|[BUG] shuffle example dockerfile build fails rdma-core compilation with ubuntu22|
|[#12454](https://github.com/NVIDIA/spark-rapids/issues/12454)|[BUG] Crashing pyspark pytests lead to misreported errors|
|[#12453](https://github.com/NVIDIA/spark-rapids/issues/12453)|[BUG] loading LZ4 Parquet files SIGSEGs|
|[#12412](https://github.com/NVIDIA/spark-rapids/issues/12412)|[BUG] fastparquet_compatibility_test failed TypeError: 'type' object is not subscriptable with python3.8|
|[#12369](https://github.com/NVIDIA/spark-rapids/issues/12369)|[BUG] Kudo raised IllegalArgumentException when using with celeborn|

### PRs
|||
|:---|:---|
|[#12909](https://github.com/NVIDIA/spark-rapids/pull/12909)|Update dependency version JNI, hybrid to 25.06.0; private to 25.06.1|
|[#12905](https://github.com/NVIDIA/spark-rapids/pull/12905)|xfail iceberg test_iceberg_v2_mixed_deletes [skip ci]|
|[#12894](https://github.com/NVIDIA/spark-rapids/pull/12894)|Disable multi thread reader with deletions in iceberg|
|[#12896](https://github.com/NVIDIA/spark-rapids/pull/12896)|Fix mem_debug rmm deadlink [skip ci]|
|[#12842](https://github.com/NVIDIA/spark-rapids/pull/12842)|Add the NV license to relevant py files[skip ci]|
|[#12868](https://github.com/NVIDIA/spark-rapids/pull/12868)|Fix double column mapping in delta 3.3 parquet reader|
|[#12862](https://github.com/NVIDIA/spark-rapids/pull/12862)|Fix a potential issue that could cause a NPE when dumping kudo tables|
|[#12851](https://github.com/NVIDIA/spark-rapids/pull/12851)|fix shebang in prioritize-commits.sh|
|[#12853](https://github.com/NVIDIA/spark-rapids/pull/12853)|Remove Delta Lake 3.3.0 support from the plugin for Spark 3.5.3, 3.5.4 and 3.5.5|
|[#12864](https://github.com/NVIDIA/spark-rapids/pull/12864)|[HybridScan] hotfix: mark the async thread preloading the HybridScan as pool thread|
|[#12859](https://github.com/NVIDIA/spark-rapids/pull/12859)|Fix iceberg npe during read.|
|[#12849](https://github.com/NVIDIA/spark-rapids/pull/12849)|Fix iceberg test missing problem|
|[#12825](https://github.com/NVIDIA/spark-rapids/pull/12825)|[Doc] Update doc description for castStringToTimestamp config|
|[#12813](https://github.com/NVIDIA/spark-rapids/pull/12813)|Spark-4.0: Skip scala tests when ansi mode is enabled|
|[#12792](https://github.com/NVIDIA/spark-rapids/pull/12792)|Added support for Spark 3.5.6|
|[#12598](https://github.com/NVIDIA/spark-rapids/pull/12598)|Fully support casting string with timezone to timestamp|
|[#12812](https://github.com/NVIDIA/spark-rapids/pull/12812)|Replace stale use_cdf parameter reference in test_delta_scan_read pytest|
|[#12817](https://github.com/NVIDIA/spark-rapids/pull/12817)|Fix the dead links issue for Pandas UDFs doc[skip ci]|
|[#12809](https://github.com/NVIDIA/spark-rapids/pull/12809)|Temporarily disable test case test_delta_scan_read|
|[#12810](https://github.com/NVIDIA/spark-rapids/pull/12810)|Fix ca cerificate shuffle [skip ci]|
|[#12783](https://github.com/NVIDIA/spark-rapids/pull/12783)|Spark-4.0: Skip ScalaTests when ANSI mode is enabled until implemented|
|[#12767](https://github.com/NVIDIA/spark-rapids/pull/12767)|Add support for DeltaScan without DV for Delta Lake 3.3.0|
|[#12795](https://github.com/NVIDIA/spark-rapids/pull/12795)|Fix spark 3.5.2 for iceberg|
|[#12744](https://github.com/NVIDIA/spark-rapids/pull/12744)|Add gpu delete filter for merge on read scan.|
|[#12798](https://github.com/NVIDIA/spark-rapids/pull/12798)|Update dockerfile to use HTTPS service for ubuntu repos [skip ci]|
|[#12759](https://github.com/NVIDIA/spark-rapids/pull/12759)|Fix dataframe writer option for delta optimizeWrite on Databricks|
|[#12766](https://github.com/NVIDIA/spark-rapids/pull/12766)|Use task priority for spill priority|
|[#12756](https://github.com/NVIDIA/spark-rapids/pull/12756)|GPUSemaphore now uses unified priority|
|[#12765](https://github.com/NVIDIA/spark-rapids/pull/12765)|Fix spill metrics for disk|
|[#12776](https://github.com/NVIDIA/spark-rapids/pull/12776)|Enable pr desc check [skip ci]|
|[#12760](https://github.com/NVIDIA/spark-rapids/pull/12760)|Accelerate premerge CI with non-PVC workspace|
|[#12771](https://github.com/NVIDIA/spark-rapids/pull/12771)|Fix iceberg 2.13 build|
|[#12684](https://github.com/NVIDIA/spark-rapids/pull/12684)|Spark-4.0: Fix/Skip non-ansi unit test failures|
|[#12732](https://github.com/NVIDIA/spark-rapids/pull/12732)|Enable iceberg reader without deletions.|
|[#12701](https://github.com/NVIDIA/spark-rapids/pull/12701)|migrate RapidsShuffleInternalManagerBase NvtxRanges to NvtxRangeWithDoc|
|[#12746](https://github.com/NVIDIA/spark-rapids/pull/12746)|Enable the companion metric in the static "ns" method|
|[#12724](https://github.com/NVIDIA/spark-rapids/pull/12724)|Fix explainMode to work when GPU isn't present|
|[#12688](https://github.com/NVIDIA/spark-rapids/pull/12688)|Propagate errors to JVM while initializing GPU memory in python worker|
|[#12606](https://github.com/NVIDIA/spark-rapids/pull/12606)|Support `bit_count` SQL function|
|[#12745](https://github.com/NVIDIA/spark-rapids/pull/12745)|Disable ArrayIntersect in hybrid scan filter pushdown|
|[#12725](https://github.com/NVIDIA/spark-rapids/pull/12725)|Unified priority task life-cycle|
|[#12739](https://github.com/NVIDIA/spark-rapids/pull/12739)|Output more information of the current GPU before an executor exits due to fatal cuda exceptions|
|[#12716](https://github.com/NVIDIA/spark-rapids/pull/12716)|Add Delta 3.3.0 module to Maven|
|[#12726](https://github.com/NVIDIA/spark-rapids/pull/12726)|Gpu iceberg multithread and coeleacing reader.|
|[#12731](https://github.com/NVIDIA/spark-rapids/pull/12731)|Update SHIM_M2DIR for all shims build if DEV_MODE is enabled [skip ci]|
|[#12722](https://github.com/NVIDIA/spark-rapids/pull/12722)|Add a new debug-level metric for MultiFileCloudPartitionReader: bufferTime with semaphore|
|[#12672](https://github.com/NVIDIA/spark-rapids/pull/12672)|Introduce iceberg parquet reader interface and perfile reader|
|[#12607](https://github.com/NVIDIA/spark-rapids/pull/12607)|Support bitwise aggregate functions (`bit_and`, `bit_or` and `bit_xor`) in groupby and reduction|
|[#12693](https://github.com/NVIDIA/spark-rapids/pull/12693)|Disable Databricks 11.3 shim build|
|[#12712](https://github.com/NVIDIA/spark-rapids/pull/12712)|Fix test_formats_for_legacy_mode_other_formats_tz_rules Test|
|[#12705](https://github.com/NVIDIA/spark-rapids/pull/12705)|fix spill framework bug causing gpu oom|
|[#12622](https://github.com/NVIDIA/spark-rapids/pull/12622)|Fix the failing delta log query test test_delta_read_with_deletion_vectors_enabled|
|[#12695](https://github.com/NVIDIA/spark-rapids/pull/12695)|Add parquet converter layer for shading.|
|[#12699](https://github.com/NVIDIA/spark-rapids/pull/12699)|Disable accelerated columnar to row to avoid data corruption|
|[#12704](https://github.com/NVIDIA/spark-rapids/pull/12704)|Update 'maxNumWriters' metric only when it exists|
|[#12681](https://github.com/NVIDIA/spark-rapids/pull/12681)|AsyncOutputStream should copy the buffer before scheduling write task|
|[#12374](https://github.com/NVIDIA/spark-rapids/pull/12374)|Use metrics about memory usage to adjust the number of concurrent tasks|
|[#12656](https://github.com/NVIDIA/spark-rapids/pull/12656)|Add a metric to represent the max number of open writers|
|[#12658](https://github.com/NVIDIA/spark-rapids/pull/12658)|Add gpu spark scan related classes for iceberg.|
|[#12687](https://github.com/NVIDIA/spark-rapids/pull/12687)|Revert "Add Delta 3.3.0 module to Maven  (#12624)"|
|[#12590](https://github.com/NVIDIA/spark-rapids/pull/12590)|Introduce GpuParquetReaderPostProcessor|
|[#12647](https://github.com/NVIDIA/spark-rapids/pull/12647)|refine bookkeeping logs to ease OOM related troubleshooting|
|[#12667](https://github.com/NVIDIA/spark-rapids/pull/12667)|Allow 0 for some footer fields [Databricks]|
|[#12613](https://github.com/NVIDIA/spark-rapids/pull/12613)|Supports ToPrettyString for date type in non-UTC timezone|
|[#12670](https://github.com/NVIDIA/spark-rapids/pull/12670)|Add deepwiki badge [skip ci]|
|[#12661](https://github.com/NVIDIA/spark-rapids/pull/12661)|Skip generating scaladoc for iceberg module [skip ci]|
|[#12624](https://github.com/NVIDIA/spark-rapids/pull/12624)|Add Delta 3.3.0 module to Maven|
|[#12636](https://github.com/NVIDIA/spark-rapids/pull/12636)|[Spark-4.0] ParseDateDateTimeSuite- Skip some of the unit tests when ansi is enabled|
|[#12594](https://github.com/NVIDIA/spark-rapids/pull/12594)|Move `SparkSchemaConverter` to top class|
|[#12602](https://github.com/NVIDIA/spark-rapids/pull/12602)|Update UCX default ver to 1.18.1 to support ubuntu24.04|
|[#12643](https://github.com/NVIDIA/spark-rapids/pull/12643)|Remove copied classes from iceberg library|
|[#12651](https://github.com/NVIDIA/spark-rapids/pull/12651)|xfail delta_lake_test.py::test_delta_read |
|[#12641](https://github.com/NVIDIA/spark-rapids/pull/12641)|Remove Usage of ZoneInfo|
|[#12558](https://github.com/NVIDIA/spark-rapids/pull/12558)|by default enable skip agg|
|[#12634](https://github.com/NVIDIA/spark-rapids/pull/12634)|Allow NonGPU for MonthsBetween Tests|
|[#12618](https://github.com/NVIDIA/spark-rapids/pull/12618)|Restrict the pyarrow version to 19.0.1 in requirements.txt|
|[#12627](https://github.com/NVIDIA/spark-rapids/pull/12627)|Always use boolean columnar to columnar conversion|
|[#12502](https://github.com/NVIDIA/spark-rapids/pull/12502)|Add Support For Timezone Rules|
|[#12592](https://github.com/NVIDIA/spark-rapids/pull/12592)|Add trampoline access to utility classes from iceberg-spark-runtime|
|[#12582](https://github.com/NVIDIA/spark-rapids/pull/12582)|Add iceberg-common module|
|[#12433](https://github.com/NVIDIA/spark-rapids/pull/12433)|Run unit tests with both ANSI modes enabled and disabled containing HashAggregates for Spark 4.0|
|[#12572](https://github.com/NVIDIA/spark-rapids/pull/12572)|Add sha1|
|[#12554](https://github.com/NVIDIA/spark-rapids/pull/12554)|Fallback Delta scan if deletion vectors are being read in Databricks 13.3|
|[#12616](https://github.com/NVIDIA/spark-rapids/pull/12616)|Fix Spark-4.0 build error - Add overwrite argument to writeToTable [skip ci]|
|[#12601](https://github.com/NVIDIA/spark-rapids/pull/12601)|Add DEV_MODE in nightly build script for dev build job [skip ci]|
|[#12588](https://github.com/NVIDIA/spark-rapids/pull/12588)|Fix the Python pip installation failure using 'get-pip.py'|
|[#12569](https://github.com/NVIDIA/spark-rapids/pull/12569)|Fix overflow for `stddev` and `variance` SQL functions|
|[#12394](https://github.com/NVIDIA/spark-rapids/pull/12394)|Add a way to override the PLUGIN_JARS|
|[#12414](https://github.com/NVIDIA/spark-rapids/pull/12414)|HybridScan: Fix supported expressions list in filter pushdown|
|[#12568](https://github.com/NVIDIA/spark-rapids/pull/12568)|A quick fix for the rmm import issue|
|[#12565](https://github.com/NVIDIA/spark-rapids/pull/12565)|fix broken nvtx_ranges link [skip ci]|
|[#12548](https://github.com/NVIDIA/spark-rapids/pull/12548)|Move parquet classes to parquet namespace|
|[#12559](https://github.com/NVIDIA/spark-rapids/pull/12559)|Update Databricks CICD default Ubuntu Docker image to 22.04|
|[#12437](https://github.com/NVIDIA/spark-rapids/pull/12437)|Fully support Conv expression|
|[#12541](https://github.com/NVIDIA/spark-rapids/pull/12541)|Exclude parent op time in Hash Aggregate heuristic time|
|[#12540](https://github.com/NVIDIA/spark-rapids/pull/12540)|HybridScan SQL HINT: adapt the spec change of LogicalRelation in spark400 [dataBricks]|
|[#12534](https://github.com/NVIDIA/spark-rapids/pull/12534)|companion metrics for all timing metrics|
|[#12496](https://github.com/NVIDIA/spark-rapids/pull/12496)|Align the semantics of the `dynamic partitions` metric for GPU write with Spark|
|[#12458](https://github.com/NVIDIA/spark-rapids/pull/12458)|Add a physical rule to fold two-stages LocalAggregate|
|[#12035](https://github.com/NVIDIA/spark-rapids/pull/12035)|Add class for documented Nvtx Ranges|
|[#12531](https://github.com/NVIDIA/spark-rapids/pull/12531)|Remove inactive user from github workflow[skip ci]|
|[#12525](https://github.com/NVIDIA/spark-rapids/pull/12525)|Explicitly disable LTO in shuffle example dockerfiles [skip ci]|
|[#12370](https://github.com/NVIDIA/spark-rapids/pull/12370)|HybridScan: Add a specific SQL Hint|
|[#12455](https://github.com/NVIDIA/spark-rapids/pull/12455)|Crash pytest worker if the Pyspark child JVM is unusable|
|[#12517](https://github.com/NVIDIA/spark-rapids/pull/12517)|Add TaskMetrics about the watermark of PinnedMemory and PageableMemory |
|[#12513](https://github.com/NVIDIA/spark-rapids/pull/12513)|Update CICD defaults to ubuntu 22.04|
|[#12512](https://github.com/NVIDIA/spark-rapids/pull/12512)|Fix auto merge conflict 12511 [skip ci]|
|[#12467](https://github.com/NVIDIA/spark-rapids/pull/12467)|LoRE: support dump data from shuffle related nodes|
|[#12306](https://github.com/NVIDIA/spark-rapids/pull/12306)|Accelerate ArrayDistinct|
|[#12463](https://github.com/NVIDIA/spark-rapids/pull/12463)|Fix the flaky ThrottlingExecutor task metrics test|
|[#12416](https://github.com/NVIDIA/spark-rapids/pull/12416)|Enable kudo by default.|
|[#12482](https://github.com/NVIDIA/spark-rapids/pull/12482)|Remove Lz4 xfail tests|
|[#12483](https://github.com/NVIDIA/spark-rapids/pull/12483)|Xfail json_matrix_test for Databricks 14.3|
|[#12459](https://github.com/NVIDIA/spark-rapids/pull/12459)|Unblock CI due to #12452 and #12453|
|[#12373](https://github.com/NVIDIA/spark-rapids/pull/12373)|Update dependency version JNI, private, hybrid to 25.06.0-SNAPSHOT|

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

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
