# Change log
Generated on 2026-02-09

## Release 26.02

### Features
|||
|:---|:---|
|[#13381](https://github.com/NVIDIA/spark-rapids/issues/13381)|[FEA] Add support for iceberg partition calculation.|
|[#14137](https://github.com/NVIDIA/spark-rapids/issues/14137)|[FEA] Switch from Java 17 release to Java 8 target for Spark 411 shim.|
|[#14083](https://github.com/NVIDIA/spark-rapids/issues/14083)|[FEA][AUDIT][SPARK-52921][SQL] Specify outputPartitioning for UnionExec for same output partitoning as children operators|
|[#14056](https://github.com/NVIDIA/spark-rapids/issues/14056)|[FEA] Add support for Spark 4.1.1|
|[#13388](https://github.com/NVIDIA/spark-rapids/issues/13388)|[FEA] Add support for iceberg void transform.|
|[#13389](https://github.com/NVIDIA/spark-rapids/issues/13389)|[FEA] Add support for iceberg identity transform.|
|[#13382](https://github.com/NVIDIA/spark-rapids/issues/13382)|[FEA] Add support for iceberg bucket transform.|
|[#9080](https://github.com/NVIDIA/spark-rapids/issues/9080)|[FEA] Support sha2|
|[#13935](https://github.com/NVIDIA/spark-rapids/issues/13935)|[FEA] Add support for iceberg view.|
|[#14066](https://github.com/NVIDIA/spark-rapids/issues/14066)|[FEA] Set correct iceberg table property for spark rapids.|
|[#13750](https://github.com/NVIDIA/spark-rapids/issues/13750)|[FEA]Drop Spark 3.2.x support|
|[#13882](https://github.com/NVIDIA/spark-rapids/issues/13882)|[FEA] Support spark 3.5 + iceberg 1.9.2|
|[#13794](https://github.com/NVIDIA/spark-rapids/issues/13794)|[FEA] Update rapids JNI, private and hybrid dependency version to 26.02|

### Performance
|||
|:---|:---|
|[#14131](https://github.com/NVIDIA/spark-rapids/issues/14131)|[FEA] Enabled Kudo GPU reads by default|
|[#13715](https://github.com/NVIDIA/spark-rapids/issues/13715)|[FEA] Expand gpuSpillTime and gpuReadSpillTime metrics to include full spill operations|
|[#13812](https://github.com/NVIDIA/spark-rapids/issues/13812)|[PERF] Unnecessary data transformation steps in GPU aggregations|

### Bugs Fixed
|||
|:---|:---|
|[#14267](https://github.com/NVIDIA/spark-rapids/issues/14267)|[BUG] Spark 3.5.3 delta reads slower then 3.4.1|
|[#14262](https://github.com/NVIDIA/spark-rapids/issues/14262)|[BUG] NDS query66 unmatched results error in Spark 4.1.1 + ANSI run|
|[#14233](https://github.com/NVIDIA/spark-rapids/issues/14233)|[BUG] test_parquet_testing_valid_files for parquet-testing/data/null_list.parquet falls back|
|[#14197](https://github.com/NVIDIA/spark-rapids/issues/14197)|[BUG] LHA test fails with cudaErrorInvalidResourceHandle during CSV reading on L40S cluster|
|[#14215](https://github.com/NVIDIA/spark-rapids/issues/14215)|[BUG] Test Failure: SparkNumberFormatException - [CAST_INVALID_INPUT] The value '+1.2' of the type "STRING" cannot be cast to "INT" because it is malformed.|
|[#14218](https://github.com/NVIDIA/spark-rapids/issues/14218)|[BUG] Build errors on databricks with shuffle v2 phase 2|
|[#14134](https://github.com/NVIDIA/spark-rapids/issues/14134)|Change calls to cuDF partitioning apis to account for the numRows offset|
|[#14174](https://github.com/NVIDIA/spark-rapids/issues/14174)|OOM when a single shuffle partition exceeds 2GB in RapidsShuffleThreadedWriter|
|[#14201](https://github.com/NVIDIA/spark-rapids/issues/14201)|AB-BA deadlock between SpillableHostStore and SpillableHandle during shutdown|
|[#14196](https://github.com/NVIDIA/spark-rapids/issues/14196)|[BUG] Iceberg identity partition fallback tests failing after GPU support implementation|
|[#14099](https://github.com/NVIDIA/spark-rapids/issues/14099)|[BUG] IllegalStateException during Shuffle Write when Join result exceeds 2GB string limit.|
|[#14037](https://github.com/NVIDIA/spark-rapids/issues/14037)|[AUDIT] [SPARK-52962][SQL] BroadcastExchangeExec should not reset metrics|
|[#14188](https://github.com/NVIDIA/spark-rapids/issues/14188)|[BUG] JDK11 Nightly Build: DatasetSuite test failures for RelationalGroupedDataset toString format|
|[#7520](https://github.com/NVIDIA/spark-rapids/issues/7520)|[AUDIT][SPARK-42045][SQL] ANSI SQL mode: Round/Bround should return an error on tiny/small/big integer overflow|
|[#14179](https://github.com/NVIDIA/spark-rapids/issues/14179)|Merger thread can become zombie when task is killed, blocking subsequent tasks|
|[#14096](https://github.com/NVIDIA/spark-rapids/issues/14096)|[BUG] Window function tests fail with GPU/CPU string value mismatches in min aggregations in cuda13|
|[#13954](https://github.com/NVIDIA/spark-rapids/issues/13954)|[BUG] gpu kudo does not make its inputs spillable|
|[#14030](https://github.com/NVIDIA/spark-rapids/issues/14030)|[BUG] Iceberg GPU result is diff from CPU when doing merge|
|[#13365](https://github.com/NVIDIA/spark-rapids/issues/13365)|[BUG] NDS query57 completed with "IllegalArgumentException : Invalid kudo offset buffer content" error on dataproc|
|[#14092](https://github.com/NVIDIA/spark-rapids/issues/14092)|[BUG] Fix incorrect Iceberg-write row in supportedDataSource.csv‎|
|[#14075](https://github.com/NVIDIA/spark-rapids/issues/14075)|[BUG] Iceberg integration tests fail with pyspark.errors.exceptions.captured.UnsupportedOperationException: Creating a view is not supported by catalog: spark_catalog|
|[#14078](https://github.com/NVIDIA/spark-rapids/issues/14078)|[BUG] SizeInBytes metric display incorrect for data sizes exceeding TB|
|[#14025](https://github.com/NVIDIA/spark-rapids/issues/14025)|[BUG] Integration tests timeout on Dataproc 2.1-debian11: Job 0 exceeded 3600 second limit|
|[#13629](https://github.com/NVIDIA/spark-rapids/issues/13629)|[BUG] error using UCX shuffle with Spark 4.0|
|[#14057](https://github.com/NVIDIA/spark-rapids/issues/14057)|[BUG] Spark 4.0.0 master/worker startup failure: ClassNotFoundException and spark-class script error|
|[#14043](https://github.com/NVIDIA/spark-rapids/issues/14043)|[BUG] `test_avg_divide_by_zero` failed for OSS Spark 3.3.0  and across all Databricks runtime versions|
|[#13739](https://github.com/NVIDIA/spark-rapids/issues/13739)|[AutoSparkUT] Read row group containing both dictionary and plain encoded pages - Missing GPU verification test|
|[#13760](https://github.com/NVIDIA/spark-rapids/issues/13760)|[AutoSparkUT]DATE_FROM_UNIX_DATE test case in RapidsDateExpressionsSuite threw java.lang.ArithmeticException: integer overflow|
|[#13907](https://github.com/NVIDIA/spark-rapids/issues/13907)|[BUG] `join on` the condition `cast <row> to <boolean>` brings errors on the GPU engines|
|[#13953](https://github.com/NVIDIA/spark-rapids/issues/13953)|[BUG] Test Failure: SPARK-24788 RelationalGroupedDataset.toString missing 'type: GroupBy' in output spark330 JDK11|
|[#13914](https://github.com/NVIDIA/spark-rapids/issues/13914)|[BUG] An empty table  `crossJoin`  another table brings errors on the GPU engines|
|[#13765](https://github.com/NVIDIA/spark-rapids/issues/13765)|[AutoSparkUT] SPARK-38237 shuffle distribution test needs GPU-specific implementation|
|[#13892](https://github.com/NVIDIA/spark-rapids/issues/13892)|[BUG] CUDF UDF tests fail with kvikio symbol undefined error: _ZN6kvikio12RemoteHandle5preadEPvmmmPN2BS11thread_poolE 26.02 cudf-nightly|
|[#13899](https://github.com/NVIDIA/spark-rapids/issues/13899)|[BUG] Race condition in host-to-disk spill due to premature disk handle exposure|
|[#13820](https://github.com/NVIDIA/spark-rapids/issues/13820)|[BUG] NullPointerException - aggregate.TypedImperativeAggregate.merge when final agg on CPU|

### PRs
|||
|:---|:---|
|[#14254](https://github.com/NVIDIA/spark-rapids/pull/14254)|Update dependency version JNI, private, hybrid to 26.02.0|
|[#14271](https://github.com/NVIDIA/spark-rapids/pull/14271)|Fix combining small files when reading Delta tables using multi-threaded reader|
|[#14241](https://github.com/NVIDIA/spark-rapids/pull/14241)|[DOC] update for download page 2602 release [skip ci]|
|[#14264](https://github.com/NVIDIA/spark-rapids/pull/14264)|Fix GpuHashAggregateExec outputPartitioning for aliased grouping keys|
|[#14243](https://github.com/NVIDIA/spark-rapids/pull/14243)|[SPARK-54220] Xfail null_list.parquet for Spark 4.1.0+ due to array<void> inference|
|[#14226](https://github.com/NVIDIA/spark-rapids/pull/14226)|Disable RAPIDS Shuffle Manager when spark.shuffle.checksum.enabled is true|
|[#14230](https://github.com/NVIDIA/spark-rapids/pull/14230)|Fallback to CPU for hash joins with struct keys having different field names|
|[#14206](https://github.com/NVIDIA/spark-rapids/pull/14206)|Spark-4.1.1: Resolve integration tests in map_test.py and bloom_filter tests|
|[#14164](https://github.com/NVIDIA/spark-rapids/pull/14164)|Align GpuUnionExec with Spark 4.1's partitioner-aware union behavior|
|[#14120](https://github.com/NVIDIA/spark-rapids/pull/14120)|[FEA] Add support for Spark 4.1.1|
|[#14217](https://github.com/NVIDIA/spark-rapids/pull/14217)|Add tests for dml operations after schema evolution.|
|[#14175](https://github.com/NVIDIA/spark-rapids/pull/14175)|Fix OOM when shuffle partition exceeds 2GB in threaded writer|
|[#14202](https://github.com/NVIDIA/spark-rapids/pull/14202)|Fix AB-BA deadlock between SpillableHostStore and SpillableHandle during shutdown|
|[#14204](https://github.com/NVIDIA/spark-rapids/pull/14204)|Fix iceberg identity test failure.|
|[#14189](https://github.com/NVIDIA/spark-rapids/pull/14189)|BroadcastExchangeExec should not reset metrics|
|[#14192](https://github.com/NVIDIA/spark-rapids/pull/14192)|Fix JDK version diff in RelationalGroupedDataset|
|[#14183](https://github.com/NVIDIA/spark-rapids/pull/14183)|Support for Iceberg identity partitioning|
|[#14182](https://github.com/NVIDIA/spark-rapids/pull/14182)|Artifactory credentials for wget used in spark-premerge-build.sh|
|[#14180](https://github.com/NVIDIA/spark-rapids/pull/14180)|Fix merger thread deadlock when task is killed|
|[#14139](https://github.com/NVIDIA/spark-rapids/pull/14139)|Add withRetry to GpuBatchedBoundedWindowIterator|
|[#14166](https://github.com/NVIDIA/spark-rapids/pull/14166)|Fix failed cases due to Spark 41x changed the default mode from unsafe to safe|
|[#14170](https://github.com/NVIDIA/spark-rapids/pull/14170)|Update authorized users|
|[#14161](https://github.com/NVIDIA/spark-rapids/pull/14161)|Add in the proper output ordering and partitioning to GpuWindowLimitExec|
|[#13724](https://github.com/NVIDIA/spark-rapids/pull/13724)|rapids shuffle manager V2 phase 1: writer use as much memory as allowed and pipelined write|
|[#14157](https://github.com/NVIDIA/spark-rapids/pull/14157)|[AutoSparkUT]Add Dataset, DataFrameFunctions and ColumnExpression suites|
|[#14151](https://github.com/NVIDIA/spark-rapids/pull/14151)|[AutoSparkUT]Enable several Spark UT suites|
|[#14141](https://github.com/NVIDIA/spark-rapids/pull/14141)|[AutoSparkUT]Enable several UT Suites|
|[#14125](https://github.com/NVIDIA/spark-rapids/pull/14125)|enable GPU kudo reads by default|
|[#14003](https://github.com/NVIDIA/spark-rapids/pull/14003)|Add in basic GPU/CPU bridge operation|
|[#14130](https://github.com/NVIDIA/spark-rapids/pull/14130)|[AutoSparkUT]Add RapidsCollectionExpressionsSuite|
|[#13995](https://github.com/NVIDIA/spark-rapids/pull/13995)|Add a debug option to check if memory allocation is covered by retry framework|
|[#14031](https://github.com/NVIDIA/spark-rapids/pull/14031)|Fix Iceberg data corruption when meets null in the condition in merge process|
|[#14095](https://github.com/NVIDIA/spark-rapids/pull/14095)|Fix Scala 2.13 compilation warnings and enforce stricter checks|
|[#14035](https://github.com/NVIDIA/spark-rapids/pull/14035)|Allow for AST join build side selection and add some heuristics|
|[#14089](https://github.com/NVIDIA/spark-rapids/pull/14089)|Add length check for materializing to host memory buffer from DiskHandle.|
|[#14001](https://github.com/NVIDIA/spark-rapids/pull/14001)|Supports all types for Iceberg bucket transform|
|[#14093](https://github.com/NVIDIA/spark-rapids/pull/14093)|Fix incorrect Iceberg-write row in supportedDataSource.csv|
|[#14101](https://github.com/NVIDIA/spark-rapids/pull/14101)|Migrate pre-merge CI image usage to new artifactory instance|
|[#14100](https://github.com/NVIDIA/spark-rapids/pull/14100)|Add Spark version notice to generated documentation|
|[#14091](https://github.com/NVIDIA/spark-rapids/pull/14091)|[SparkUT]Fix a from-json case to expect a different exception|
|[#14094](https://github.com/NVIDIA/spark-rapids/pull/14094)|Fix compile error in `HashFunctions.scala`|
|[#14038](https://github.com/NVIDIA/spark-rapids/pull/14038)|[FEA] SHA-2 hash support.|
|[#14087](https://github.com/NVIDIA/spark-rapids/pull/14087)|Delete useless UT suites which run on CPU only|
|[#14086](https://github.com/NVIDIA/spark-rapids/pull/14086)|Remove the 3rd `DataType` parameter from all the calls to GpuColumnVector.from(GpuScalar, int, DataType)|
|[#14082](https://github.com/NVIDIA/spark-rapids/pull/14082)|Fix Iceberg view test failures in CICD|
|[#14070](https://github.com/NVIDIA/spark-rapids/pull/14070)|Kudo supports schema check when serializing batches|
|[#14080](https://github.com/NVIDIA/spark-rapids/pull/14080)|Set JDK 17 as the default for nightly builds across both scala2.12 and scala2.13|
|[#14079](https://github.com/NVIDIA/spark-rapids/pull/14079)|Fix metric display for data sizes exceeding TB (#14078)|
|[#14010](https://github.com/NVIDIA/spark-rapids/pull/14010)|support withRetry with split for GPU shuffle coalesce|
|[#14071](https://github.com/NVIDIA/spark-rapids/pull/14071)|[DOC] update supported spark versions [skip ci]|
|[#14033](https://github.com/NVIDIA/spark-rapids/pull/14033)|[BUG] Fix initialization order NPE for RapidsShuffleManager in UCX mode for Spark 4+|
|[#14042](https://github.com/NVIDIA/spark-rapids/pull/14042)|Add Iceberg view test|
|[#14058](https://github.com/NVIDIA/spark-rapids/pull/14058)|Update documentation to reflect the shims updates [skip ci]|
|[#14039](https://github.com/NVIDIA/spark-rapids/pull/14039)|Add support for docs generation in `buildall`|
|[#13975](https://github.com/NVIDIA/spark-rapids/pull/13975)|support withRetry with split for shuffle exchange exec base|
|[#14044](https://github.com/NVIDIA/spark-rapids/pull/14044)|Adds ignore_order for groupBy agg test that returns multiple rows|
|[#14026](https://github.com/NVIDIA/spark-rapids/pull/14026)|Add in the missing RmmSpark calls for the coalescing reader|
|[#14029](https://github.com/NVIDIA/spark-rapids/pull/14029)|Increase executor memory for iceberg tests to avoid OOM error [skip ci]|
|[#14022](https://github.com/NVIDIA/spark-rapids/pull/14022)|Add RapidsDateFunctionsSuite|
|[#13993](https://github.com/NVIDIA/spark-rapids/pull/13993)|Update plugin scala212 to build 330+ only|
|[#14024](https://github.com/NVIDIA/spark-rapids/pull/14024)|Update actions/setup-java@v5 [skip ci]|
|[#13986](https://github.com/NVIDIA/spark-rapids/pull/13986)|Add iceberg 1.9.2 support.|
|[#14008](https://github.com/NVIDIA/spark-rapids/pull/14008)|Fix auto merge conflict 14007 [skip ci]|
|[#13982](https://github.com/NVIDIA/spark-rapids/pull/13982)|Add parquet mixed-encodings test|
|[#13994](https://github.com/NVIDIA/spark-rapids/pull/13994)|[SparkUT]Add try-catch on dataframe.collect in UT framework|
|[#13997](https://github.com/NVIDIA/spark-rapids/pull/13997)|Add pmattione to blossom: Attempt 2  [skip ci]|
|[#13992](https://github.com/NVIDIA/spark-rapids/pull/13992)|Building scala213 plugin for spark350+|
|[#13987](https://github.com/NVIDIA/spark-rapids/pull/13987)|Add layer of indirection when converting expressions to the GPU|
|[#13981](https://github.com/NVIDIA/spark-rapids/pull/13981)|Add in support for getting SQL metrics from expressions|
|[#13983](https://github.com/NVIDIA/spark-rapids/pull/13983)|Support custom parallelism in non-default test modes [skip ci]|
|[#13964](https://github.com/NVIDIA/spark-rapids/pull/13964)|Fix a join where only a single column is used as a condition|
|[#13970](https://github.com/NVIDIA/spark-rapids/pull/13970)|Make the GPU UDF has different name than the CPU one|
|[#13933](https://github.com/NVIDIA/spark-rapids/pull/13933)|[DOC] fix dead link in testing page [skip ci]|
|[#13845](https://github.com/NVIDIA/spark-rapids/pull/13845)|Add a non strict mode for lore dump|
|[#13959](https://github.com/NVIDIA/spark-rapids/pull/13959)|[SparkUT]Check Java version to decide the expected string in one case of DataFrameAggregateSuite|
|[#13955](https://github.com/NVIDIA/spark-rapids/pull/13955)|[DOC] Update RapidsUDF output types with decimal 128 [skip ci]|
|[#13938](https://github.com/NVIDIA/spark-rapids/pull/13938)|Fix a special case in limit where it could return an empty batch with the wrong number of columns|
|[#13945](https://github.com/NVIDIA/spark-rapids/pull/13945)|Set WONT_FIX_ISSUE cases in RadpisDataFrameWindowFunctionsSuite|
|[#13947](https://github.com/NVIDIA/spark-rapids/pull/13947)|Fix auto merge conflict 13946 [skip ci]|
|[#13936](https://github.com/NVIDIA/spark-rapids/pull/13936)|Add testRapids case to match GPU execution in RapidsDataFrameWindowFunctionsSuite|
|[#13905](https://github.com/NVIDIA/spark-rapids/pull/13905)|Refine GpuTaskMetrics over SpillFrameWork|
|[#13931](https://github.com/NVIDIA/spark-rapids/pull/13931)|Use strict priority in conda process [skip ci]|
|[#13911](https://github.com/NVIDIA/spark-rapids/pull/13911)|[AutoSparkUT]Enable RapidsCsvExpressionsSuite & RapidsCSVInferSchemaSuite|
|[#13900](https://github.com/NVIDIA/spark-rapids/pull/13900)|fix race condition due to premature disk handle exposure|
|[#13902](https://github.com/NVIDIA/spark-rapids/pull/13902)|[AutoSparkUT]Add RapidsCsvSuite|
|[#13875](https://github.com/NVIDIA/spark-rapids/pull/13875)|Persist the bufConverter of TypedImperativeAggregate into LogicalPlan instead of PhysicalPlan|
|[#13883](https://github.com/NVIDIA/spark-rapids/pull/13883)|[AutoSparkUT]Enable several Spark UT suites|
|[#13887](https://github.com/NVIDIA/spark-rapids/pull/13887)|Fix auto merge conflict 13877 [skip ci]|
|[#13857](https://github.com/NVIDIA/spark-rapids/pull/13857)|Use shim to identify whether DataWriting is supported for LoRe|
|[#13819](https://github.com/NVIDIA/spark-rapids/pull/13819)|Add the missing spark 357 version to GpuWriteFilesUnsupportedVersions|
|[#13777](https://github.com/NVIDIA/spark-rapids/pull/13777)|[AutoSparkUT] Migrate DataFrameNaFunctionsSuite tests to RAPIDS|
|[#13796](https://github.com/NVIDIA/spark-rapids/pull/13796)|Update dependency version JNI, private, hybrid to 26.02.0-SNAPSHOT [skip ci]|
|[#13767](https://github.com/NVIDIA/spark-rapids/pull/13767)|[AutoSparkUT] Migrate DataFrameComplexTypeSuite tests to RAPIDS|
|[#13762](https://github.com/NVIDIA/spark-rapids/pull/13762)|[AutoSparkUT]Enable Spark UT DateExpressionsSuite|
|[#13795](https://github.com/NVIDIA/spark-rapids/pull/13795)|Bump up version to 26.02 [skip ci]|
|[#13790](https://github.com/NVIDIA/spark-rapids/pull/13790)|Use wildcard mark to import all Rapids test suites migrated from Apache Spark Suites|

## Release 25.12

### Features
|||
|:---|:---|
|[#13520](https://github.com/NVIDIA/spark-rapids/issues/13520)|[FEA] DML Support for iceberg|
|[#12229](https://github.com/NVIDIA/spark-rapids/issues/12229)|[FEA] Support insert statement for iceberg table.|
|[#13419](https://github.com/NVIDIA/spark-rapids/issues/13419)|[FEA] Enable iceberg insert support by default.|
|[#13383](https://github.com/NVIDIA/spark-rapids/issues/13383)|[FEA] Add support for iceberg truncate transform.|
|[#12719](https://github.com/NVIDIA/spark-rapids/issues/12719)|[FEA] Run iceberg nds power run tests.|
|[#13548](https://github.com/NVIDIA/spark-rapids/issues/13548)|[FEA] Accelerate deletes on clustered tables|
|[#13547](https://github.com/NVIDIA/spark-rapids/issues/13547)|[FEA] Accelerate updating clustered tables|
|[#13386](https://github.com/NVIDIA/spark-rapids/issues/13386)|[FEA] Add support for iceberg day transform.|
|[#13387](https://github.com/NVIDIA/spark-rapids/issues/13387)|[FEA] Add support for iceberg hour transform.|
|[#13384](https://github.com/NVIDIA/spark-rapids/issues/13384)|[FEA] Add support for iceberg year transform.|
|[#13385](https://github.com/NVIDIA/spark-rapids/issues/13385)|[FEA] Add support for iceberg month transform.|
|[#13546](https://github.com/NVIDIA/spark-rapids/issues/13546)|[FEA] Accelerate merging clustered tables|
|[#9079](https://github.com/NVIDIA/spark-rapids/issues/9079)|[FEA] Support map_from_entries|
|[#13636](https://github.com/NVIDIA/spark-rapids/issues/13636)|[FEA] Support merge command for iceberg's merge on read mode.|
|[#13635](https://github.com/NVIDIA/spark-rapids/issues/13635)|[FEA] Support update command for iceberg's merge on read mode.|
|[#13634](https://github.com/NVIDIA/spark-rapids/issues/13634)|[FEA] Support delete command for iceberg's merge on read mode.|
|[#13705](https://github.com/NVIDIA/spark-rapids/issues/13705)|[FEA] Support GBK encoded data|
|[#13470](https://github.com/NVIDIA/spark-rapids/issues/13470)|[FEA] Support Spark 3.5.7|
|[#13468](https://github.com/NVIDIA/spark-rapids/issues/13468)|[FEA] Update rapids JNI, private and hybrid dependency version to 25.12.0-SNAPSHOT|
|[#13339](https://github.com/NVIDIA/spark-rapids/issues/13339)|[FEA] Support Delta Lake 4.0.0|
|[#13525](https://github.com/NVIDIA/spark-rapids/issues/13525)|[FEA] Support merge statement for iceberg's copy on write mode.|
|[#13524](https://github.com/NVIDIA/spark-rapids/issues/13524)|[FEA] Support Update statement for iceberg's copy on write mode.|
|[#13523](https://github.com/NVIDIA/spark-rapids/issues/13523)|[FEA] Support Delete statement for iceberg's copy on write mode.|
|[#13522](https://github.com/NVIDIA/spark-rapids/issues/13522)|[FEA] Support RTAS for iceberg.|
|[#13605](https://github.com/NVIDIA/spark-rapids/issues/13605)|[FEA] Support insert into overwrite static for iceberg.|
|[#13604](https://github.com/NVIDIA/spark-rapids/issues/13604)|[FEA] Support insert into dynamic overwrite for iceberg.|
|[#13521](https://github.com/NVIDIA/spark-rapids/issues/13521)|[FEA] Support CTAS in for iceberg.|
|[#13110](https://github.com/NVIDIA/spark-rapids/issues/13110)|[FEA] Support DeltaDynamicPartitionOverwriteCommand for deltalake 3.3.x|

### Performance
|||
|:---|:---|
|[#13266](https://github.com/NVIDIA/spark-rapids/issues/13266)|[FEA] Upgrade to UCX 1.19|
|[#13462](https://github.com/NVIDIA/spark-rapids/issues/13462)|[TASK] Add microbenchmarks for Delta|
|[#12968](https://github.com/NVIDIA/spark-rapids/issues/12968)|[FEA] Use kudo kernels in the plugin|
|[#12919](https://github.com/NVIDIA/spark-rapids/issues/12919)|[FEA] Triple buffering: traffic control by a customized thread pool|
|[#13568](https://github.com/NVIDIA/spark-rapids/issues/13568)|[TASK] Analyze Deletion Vector read performance against CPU FileSourceScanExec with Predicate Pushdowns|

### Bugs Fixed
|||
|:---|:---|
|[#14009](https://github.com/NVIDIA/spark-rapids/issues/14009)|[BUG] Delta Lake deletion vector tests failing on Spark != 3.5 due to version incompatibilities|
|[#14004](https://github.com/NVIDIA/spark-rapids/issues/14004)|[BUG] test_delta_deletion_vector_read_drop_row_group fails with https://github.com/NVIDIA/spark-rapids/pull/13980|
|[#13950](https://github.com/NVIDIA/spark-rapids/issues/13950)|[BUG] Delta parquet files are not split when they can be|
|[#13873](https://github.com/NVIDIA/spark-rapids/issues/13873)|[BUG]  `join` an empty table brings errors on the GPU engines|
|[#13917](https://github.com/NVIDIA/spark-rapids/issues/13917)|[BUG] NDS2 Delta Microbenchmark Consistent Test Failures on SPARK2A Cluster|
|[#13881](https://github.com/NVIDIA/spark-rapids/issues/13881)|[BUG] Iceberg REST catalog integration tests timeout after 5 hours|
|[#13956](https://github.com/NVIDIA/spark-rapids/issues/13956)|[BUG] iceberg AWS S3 Tables test timeout 24 hours|
|[#13940](https://github.com/NVIDIA/spark-rapids/issues/13940)|[BUG] [Iceberg] After update MoR run select collect raise column not match|
|[#13854](https://github.com/NVIDIA/spark-rapids/issues/13854)|[BUG] run_other_join_modes_tests Integration tests: 1121 broadcast join failures and timeout the CI|
|[#13885](https://github.com/NVIDIA/spark-rapids/issues/13885)|[BUG] New added iceberg cases causes nightly integration tests timeout after 12 hours|
|[#13788](https://github.com/NVIDIA/spark-rapids/issues/13788)|[BUG] Scala test 'AsyncCpuTask task priority' failed UT|
|[#13798](https://github.com/NVIDIA/spark-rapids/issues/13798)|[BUG] GpuLoreSuite test failure: GpuInsertIntoHiveTable LoRE replay produces no data (Spark 3.5.7)|
|[#13848](https://github.com/NVIDIA/spark-rapids/issues/13848)|[BUG] test_delta_merge_sql_liquid_clustering fails with Delta 4|
|[#13860](https://github.com/NVIDIA/spark-rapids/issues/13860)|[Test] ERROR: Could not find a version that satisfies the requirement pyspark-client==3.5.6  and 3.5.7|
|[#13852](https://github.com/NVIDIA/spark-rapids/issues/13852)|[BUG] Iceberg merge tests failing after Delta clustered table update support|
|[#13855](https://github.com/NVIDIA/spark-rapids/issues/13855)|[BUG] Scala test GpuInsertIntoHiveTable with LoRE dump and replay failed in CI|
|[#13718](https://github.com/NVIDIA/spark-rapids/issues/13718)|[BUG] Spark Connect smoke test uses a  pip package `pyspark[connect]` with jars|
|[#13731](https://github.com/NVIDIA/spark-rapids/issues/13731)|[BUG]  `.select()` query with `left_outer join()` brings errors when using the GPU engines|
|[#13824](https://github.com/NVIDIA/spark-rapids/issues/13824)|[BUG] ArrayIndexOutOfBounds when reading iceberg table.|
|[#13826](https://github.com/NVIDIA/spark-rapids/issues/13826)|[BUG] markdown link check fails 404 of logging_resource_adaptor|
|[#13660](https://github.com/NVIDIA/spark-rapids/issues/13660)|[BUG] dataproc serverless failed: AssertionError: Assertion should have executed on driver|
|[#13737](https://github.com/NVIDIA/spark-rapids/issues/13737)|[AutoSparkUT] conv - Base conversion function fails on GPU|
|[#13738](https://github.com/NVIDIA/spark-rapids/issues/13738)|[AutoSparkUT] log10 - Function has different output on GPU|
|[#13740](https://github.com/NVIDIA/spark-rapids/issues/13740)|[AutoSparkUT] log2 - Function has different output on GPU|
|[#13741](https://github.com/NVIDIA/spark-rapids/issues/13741)|[AutoSparkUT] binary log - GPU returns NaN instead of null for negative base|
|[#13689](https://github.com/NVIDIA/spark-rapids/issues/13689)|[BUG] Failed to authenticate in iceberg rest catalog tests|
|[#13800](https://github.com/NVIDIA/spark-rapids/issues/13800)|[BUG] test_csv_read_gbk_encoded_data fail Part of the plan is not columnar class of DB 14.3 runtime|
|[#13779](https://github.com/NVIDIA/spark-rapids/issues/13779)|[BUG] Divide-by-0 exception in Delta optimized write|
|[#13771](https://github.com/NVIDIA/spark-rapids/issues/13771)|[BUG] RapidsMathExpressionsSuite fails on Spark 330: atanh FAILED|
|[#13774](https://github.com/NVIDIA/spark-rapids/issues/13774)|[BUG] pool tasks can mistakenly be registered as dedicated|
|[#13732](https://github.com/NVIDIA/spark-rapids/issues/13732)|[BUG] Small per task leak in RmmSpark task metrics|
|[#13716](https://github.com/NVIDIA/spark-rapids/issues/13716)|[BUG] A special `select` query with the `where` expression brings inconsistency when using the CPU and GPU engines|
|[#13708](https://github.com/NVIDIA/spark-rapids/issues/13708)|[BUG] A special `select` query with the `join on` expression brings inconsistency when using the CPU and GPU engines|
|[#13690](https://github.com/NVIDIA/spark-rapids/issues/13690)|[BUG] java.io.NotSerializableException: com.nvidia.spark.rapids.RapidsConf occurred in iceberg rest catalog tests|
|[#13664](https://github.com/NVIDIA/spark-rapids/issues/13664)|[BUG] some hash_aggregate integration tests failed with kudo.serializer.mode=GPU|
|[#13651](https://github.com/NVIDIA/spark-rapids/issues/13651)|[BUG] plugin generate different output from CPU run|
|[#13694](https://github.com/NVIDIA/spark-rapids/issues/13694)|[BUG] databricks shims failed compile: not found: value StatsExprShim|
|[#13662](https://github.com/NVIDIA/spark-rapids/issues/13662)|[BUG] GpuRetryOOM in KudoGpuSerializer splitAndSerializeToDevice|
|[#13661](https://github.com/NVIDIA/spark-rapids/issues/13661)|[BUG] Multiple NDS queries failed due to unmatched results on Dataproc|
|[#13612](https://github.com/NVIDIA/spark-rapids/issues/13612)|[BUG] Kudo CPU serializer overflows with columns in a customer join query|
|[#13641](https://github.com/NVIDIA/spark-rapids/issues/13641)|[BUG]  test_delta_overwrite_dynamic_missing_clauses fails with "RapidsDeltaWrite is not found in any captured plan"|
|[#13648](https://github.com/NVIDIA/spark-rapids/issues/13648)|[BUG] delta_lake_merge_test:test_delta_merge_match_delete_only failed mismatch cpu and gpu outputs|
|[#13652](https://github.com/NVIDIA/spark-rapids/issues/13652)|[BUG] mortgate_test and dpp_test failure in CI|
|[#12246](https://github.com/NVIDIA/spark-rapids/issues/12246)|[BUG] The plugin can use a different GPU to validate the GPU architecture when multiple GPUs are equipped|
|[#13659](https://github.com/NVIDIA/spark-rapids/issues/13659)|[BUG] validateGpuArchitecture sometimes looks at the wrong GPU|
|[#13647](https://github.com/NVIDIA/spark-rapids/issues/13647)|[BUG] AssertUtils is not a member of package com.nvidia.spark.rapids|
|[#13623](https://github.com/NVIDIA/spark-rapids/issues/13623)|[BUG] Iceberg s3table cases failed multiple different errors|
|[#13059](https://github.com/NVIDIA/spark-rapids/issues/13059)|[BUG] Delta Lake writes are not being checked for CPU fallback in integration tests|
|[#13607](https://github.com/NVIDIA/spark-rapids/issues/13607)|[BUG] DB builds are failing in premerge.|
|[#13583](https://github.com/NVIDIA/spark-rapids/issues/13583)|[BUG] shuffle dockerfile build failed: UCX has no cuda13 release for rocky/centos|
|[#12891](https://github.com/NVIDIA/spark-rapids/issues/12891)|[BUG] NullPointerException at ai.rapids.cudf.HostColumnVectorCore.getByte|

### PRs
|||
|:---|:---|
|[#14077](https://github.com/NVIDIA/spark-rapids/pull/14077)|[DOC] Update document issue [skip ci]|
|[#14028](https://github.com/NVIDIA/spark-rapids/pull/14028)|[DOC] update cuda13 related jars in download doc [skip ci]|
|[#14045](https://github.com/NVIDIA/spark-rapids/pull/14045)|Update version to 25.12.1-SNAPSHOT|
|[#13968](https://github.com/NVIDIA/spark-rapids/pull/13968)|Update changelog for the v25.12 release [skip ci]|
|[#14011](https://github.com/NVIDIA/spark-rapids/pull/14011)|[DOC] update for download page 2512 release [skip ci]|
|[#14013](https://github.com/NVIDIA/spark-rapids/pull/14013)|Fixes various issues in 25.12 release branch|
|[#13988](https://github.com/NVIDIA/spark-rapids/pull/13988)|Update dependency version JNI, private, hybrid to 25.12.0|
|[#13980](https://github.com/NVIDIA/spark-rapids/pull/13980)|Delta table scan should be optimized when deletion vectors don't exist|
|[#14002](https://github.com/NVIDIA/spark-rapids/pull/14002)|Skip test_delta_filter_out_metadata_col on|
|[#13991](https://github.com/NVIDIA/spark-rapids/pull/13991)|Drop _tmp_metadata_row_index column from the output of Delta Scan on GPU|
|[#13903](https://github.com/NVIDIA/spark-rapids/pull/13903)|Fix join bug on csv datasources|
|[#13971](https://github.com/NVIDIA/spark-rapids/pull/13971)|Fix a special case in limit where it could return an empty batch with the wrong number of columns (#13938)|
|[#13962](https://github.com/NVIDIA/spark-rapids/pull/13962)|Cut iceberg test cases for remote catalogs.|
|[#13941](https://github.com/NVIDIA/spark-rapids/pull/13941)|Fix Iceberg column not match error when select count after cor update|
|[#13942](https://github.com/NVIDIA/spark-rapids/pull/13942)|Add null safety guards to the conversion methods in FromIcebergShaded|
|[#13926](https://github.com/NVIDIA/spark-rapids/pull/13926)|Fix columnar mismatch bug in iceberg dml when aqe enabled.|
|[#13929](https://github.com/NVIDIA/spark-rapids/pull/13929)|Fix the ordering of join gather maps, and make it harder to break|
|[#13922](https://github.com/NVIDIA/spark-rapids/pull/13922)|Remove low priority iceberg it test cases to save test time [skip ci]|
|[#13919](https://github.com/NVIDIA/spark-rapids/pull/13919)|fix race condition due to premature disk handle exposure|
|[#13891](https://github.com/NVIDIA/spark-rapids/pull/13891)|Enable iceberg write by default, and disable dml iceberg operation on mor table by default.|
|[#13910](https://github.com/NVIDIA/spark-rapids/pull/13910)|Fix the unstable unit test case: ResourceBoundedExecutorSuite|
|[#13797](https://github.com/NVIDIA/spark-rapids/pull/13797)|Supports Iceberg truncate transform|
|[#13886](https://github.com/NVIDIA/spark-rapids/pull/13886)|[WAR] Remove ICEBERG_ONLY tests from default mode [skip ci]|
|[#13872](https://github.com/NVIDIA/spark-rapids/pull/13872)|[Backport to 25.12] add missing spark 357 to GpuWriteFilesUnsupportedVersions|
|[#13849](https://github.com/NVIDIA/spark-rapids/pull/13849)|Fix test_delta_merge_sql_liquid_clustering for Delta 4|
|[#13867](https://github.com/NVIDIA/spark-rapids/pull/13867)|Use pyspark-client only for Spark 4.x [skip ci]|
|[#13864](https://github.com/NVIDIA/spark-rapids/pull/13864)|[WAR] Move run_other_join_modes_tests out of DEFAULT mode [skip ci]|
|[#13859](https://github.com/NVIDIA/spark-rapids/pull/13859)|Delete support for clustered tables|
|[#13856](https://github.com/NVIDIA/spark-rapids/pull/13856)|Fix test cases: Iceberg datetime transforms|
|[#13778](https://github.com/NVIDIA/spark-rapids/pull/13778)|Use pyspark-client for the Spark Connect smoke test|
|[#13822](https://github.com/NVIDIA/spark-rapids/pull/13822)|Support update for Delta clustered tables|
|[#13726](https://github.com/NVIDIA/spark-rapids/pull/13726)|Supports year/month/day/hour partition transforms for Iceberg|
|[#13830](https://github.com/NVIDIA/spark-rapids/pull/13830)|Fix ArrayIndexOutOfBounds in iceberg reader.|
|[#13719](https://github.com/NVIDIA/spark-rapids/pull/13719)|Upgrade ucx to 1.19.1-rc2|
|[#13814](https://github.com/NVIDIA/spark-rapids/pull/13814)|Support merging clustered tables for Delta IO|
|[#13717](https://github.com/NVIDIA/spark-rapids/pull/13717)|Add different join strategies, join logging, and heuristic confs|
|[#13806](https://github.com/NVIDIA/spark-rapids/pull/13806)|Support GpuMapFromEntries to prevent fallback|
|[#13755](https://github.com/NVIDIA/spark-rapids/pull/13755)|Add update/merge command support for iceberg's merge on read mode.|
|[#13827](https://github.com/NVIDIA/spark-rapids/pull/13827)|Fix broken link to rmm logging_resource_adaptor source code [skip ci]|
|[#13818](https://github.com/NVIDIA/spark-rapids/pull/13818)|Fixes an issue were we were not properly checking log params and nullable output|
|[#13803](https://github.com/NVIDIA/spark-rapids/pull/13803)|Fix a GBK test failure on DB 143+|
|[#13725](https://github.com/NVIDIA/spark-rapids/pull/13725)|Add delete support for iceberg's merge on read mode.|
|[#13792](https://github.com/NVIDIA/spark-rapids/pull/13792)|Add doc for RANDOM_SELECT and oom_injection_mode in integration tests readme [skip ci]|
|[#13780](https://github.com/NVIDIA/spark-rapids/pull/13780)|Add missing handling for the 0 input partition count in computing partition count in GpuOptimizeWriteExchangeExec|
|[#13742](https://github.com/NVIDIA/spark-rapids/pull/13742)|Add Spark 3.5.7 support|
|[#13773](https://github.com/NVIDIA/spark-rapids/pull/13773)|Add an option to run a random subset of integration tests|
|[#13786](https://github.com/NVIDIA/spark-rapids/pull/13786)|Return true when comparing 2 variables if both  isNaN in Unit Test|
|[#13761](https://github.com/NVIDIA/spark-rapids/pull/13761)|[AutoSparkUT] Migrate DataFramePivotSuite tests to RAPIDS|
|[#13775](https://github.com/NVIDIA/spark-rapids/pull/13775)|Ensure that task threads are the only ones registered as dedicated|
|[#13733](https://github.com/NVIDIA/spark-rapids/pull/13733)|Calls removeTaskMetrics to remove task metrics in RmmSpark after task completion|
|[#13688](https://github.com/NVIDIA/spark-rapids/pull/13688)|Use new API to do Iceberg partition.|
|[#13768](https://github.com/NVIDIA/spark-rapids/pull/13768)|Refactor RapidsTestSettings imports to one suite per line|
|[#13753](https://github.com/NVIDIA/spark-rapids/pull/13753)|[AutoSparkUT] Add RapidsParquetEncodingSuite migration|
|[#13748](https://github.com/NVIDIA/spark-rapids/pull/13748)|[AutoSparkUT]Enable Spark UT RapidsMathExpressionsSuite, RapidsMiscFunctionsSuite on 330|
|[#13735](https://github.com/NVIDIA/spark-rapids/pull/13735)|Align with CPU behavior for the null handling in GpuInSet|
|[#13656](https://github.com/NVIDIA/spark-rapids/pull/13656)|Add op time metric to Hybrid Scan|
|[#13721](https://github.com/NVIDIA/spark-rapids/pull/13721)|Enable Delta Lake 4.0.x integration tests in nightly CI/CD jobs|
|[#13710](https://github.com/NVIDIA/spark-rapids/pull/13710)|Enable Delta Lake 4.0.x support for Spark 4.0.1|
|[#13489](https://github.com/NVIDIA/spark-rapids/pull/13489)|Support kudo GPU shuffle reads in the plugin|
|[#13711](https://github.com/NVIDIA/spark-rapids/pull/13711)|define usesKudoGPUSlicing override got GpuSinglePartitioning|
|[#13691](https://github.com/NVIDIA/spark-rapids/pull/13691)|Support the null-aware anti join in GPU broadcast hash join|
|[#13413](https://github.com/NVIDIA/spark-rapids/pull/13413)|Fix core dump in MemoryCleaner|
|[#13707](https://github.com/NVIDIA/spark-rapids/pull/13707)|Refactor DeltaDynamicPartitionOverwriteCommand and enable Delta 4.0 integration tests|
|[#13701](https://github.com/NVIDIA/spark-rapids/pull/13701)|Add StatsExprShim in databricks shim|
|[#13700](https://github.com/NVIDIA/spark-rapids/pull/13700)|Support ChangeLog for the new branch model [skip ci]|
|[#13695](https://github.com/NVIDIA/spark-rapids/pull/13695)|[DOC] update the download doc [skip ci]|
|[#13692](https://github.com/NVIDIA/spark-rapids/pull/13692)|Add missing javadocs in delta-33x-40x refactored code [skip ci]|
|[#13666](https://github.com/NVIDIA/spark-rapids/pull/13666)|Add common infrastructure and Spark 4.0 compatibility for Delta Lake 4.0.0 support|
|[#13663](https://github.com/NVIDIA/spark-rapids/pull/13663)|Add withRetry for gpuSplitAndSerialize in GPU kudo|
|[#13643](https://github.com/NVIDIA/spark-rapids/pull/13643)|Add allow_non_gpu_delta_write_if to conditionally allow CPU fallback for delta writes|
|[#13130](https://github.com/NVIDIA/spark-rapids/pull/13130)|Resource-bounded MultiFileCloudPartitionReader|
|[#13657](https://github.com/NVIDIA/spark-rapids/pull/13657)|Add support for update iceberg table with copy on write mode.|
|[#13670](https://github.com/NVIDIA/spark-rapids/pull/13670)|Revert "Temporarily disable test_dpp_reuse_broadcast_exchange and mor…|
|[#13639](https://github.com/NVIDIA/spark-rapids/pull/13639)|Support delete command for iceberg's copy on write mode|
|[#13658](https://github.com/NVIDIA/spark-rapids/pull/13658)|move validateGpuArch check to after initializeGpuAndMemory|
|[#13644](https://github.com/NVIDIA/spark-rapids/pull/13644)|Add FileFormat, Catalog, Provider, and RowIndexFilters base classes and refactor delta-33x|
|[#13654](https://github.com/NVIDIA/spark-rapids/pull/13654)|Move AssertUtils to the api submodule|
|[#13655](https://github.com/NVIDIA/spark-rapids/pull/13655)|Temporarily disable test_dpp_reuse_broadcast_exchange and mortgage_test given cuDF issue|
|[#13650](https://github.com/NVIDIA/spark-rapids/pull/13650)|Mitigate mamba hang on Rocky linux [skip ci]|
|[#13642](https://github.com/NVIDIA/spark-rapids/pull/13642)| Add Create/Optimize table command base classes and refactor delta-33x|
|[#13584](https://github.com/NVIDIA/spark-rapids/pull/13584)|Use assertInTests for cases with potential side effects, or expensive calls|
|[#13637](https://github.com/NVIDIA/spark-rapids/pull/13637)|Update automerge pattern to release/* [skip ci]|
|[#13619](https://github.com/NVIDIA/spark-rapids/pull/13619)|Add replace table as select for iceberg.|
|[#13614](https://github.com/NVIDIA/spark-rapids/pull/13614)|Migrate most remaining NvtxRanges to use NvtxId/NvtxRangeWithDoc|
|[#13626](https://github.com/NVIDIA/spark-rapids/pull/13626)|Add Update/Merge command base classes and  refactor delta-33x|
|[#13620](https://github.com/NVIDIA/spark-rapids/pull/13620)|Add shims layer, Delete command base classes and refactor delta-33x Delete|
|[#13586](https://github.com/NVIDIA/spark-rapids/pull/13586)|Assert RapidsDeltaWrite in integration tests|
|[#13613](https://github.com/NVIDIA/spark-rapids/pull/13613)|Add Delta Lake 4.0.x module skeleton and build infrastructure|
|[#13611](https://github.com/NVIDIA/spark-rapids/pull/13611)|Add insert static overwrite support for iceberg.|
|[#13606](https://github.com/NVIDIA/spark-rapids/pull/13606)|Add insert overwrite dynamic support for iceberg.|
|[#13595](https://github.com/NVIDIA/spark-rapids/pull/13595)|Add create table as select support for iceberg.|
|[#13573](https://github.com/NVIDIA/spark-rapids/pull/13573)|Accelerate DeltaDynamicPartitionOverwriteCommand|
|[#13608](https://github.com/NVIDIA/spark-rapids/pull/13608)|fix db GpuBroadcastHashJoinExec CollectTimeIterator args|
|[#13594](https://github.com/NVIDIA/spark-rapids/pull/13594)|Use Cuda 11 ucx build for rocky dockerfiles|
|[#12900](https://github.com/NVIDIA/spark-rapids/pull/12900)|Migrate CollectTimeIterator to use new NvtxIdWithMetrics class|
|[#13599](https://github.com/NVIDIA/spark-rapids/pull/13599)|Fix auto merge conflict 13598 [skip ci]|
|[#13501](https://github.com/NVIDIA/spark-rapids/pull/13501)|Use `RapidsFileIO` for writing data.|
|[#13572](https://github.com/NVIDIA/spark-rapids/pull/13572)|Upgrade UCX to 1.19-rc1|
|[#13544](https://github.com/NVIDIA/spark-rapids/pull/13544)|GpuBubbleTime: A new metric recording GPU underutilization wall time|
|[#13500](https://github.com/NVIDIA/spark-rapids/pull/13500)|Add Spark Connect smoke test in nightly integration tests|
|[#13399](https://github.com/NVIDIA/spark-rapids/pull/13399)|Add two metrics for the sized join|
|[#13454](https://github.com/NVIDIA/spark-rapids/pull/13454)|log if maxMem is called twice in a task|
|[#13475](https://github.com/NVIDIA/spark-rapids/pull/13475)|Update dependency version JNI, private, hybrid to 25.12.0-SNAPSHOT|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
