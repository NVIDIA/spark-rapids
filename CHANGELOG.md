# Change log
Generated on 2023-02-14

## Release 23.02

### Features
|||
|:---|:---|
|[#6419](https://github.com/NVIDIA/spark-rapids/issues/6419)|[FEA] Support write a Hive text table |
|[#7280](https://github.com/NVIDIA/spark-rapids/issues/7280)|[FEA] Support UpdateCommand for Delta Lake|
|[#7281](https://github.com/NVIDIA/spark-rapids/issues/7281)|[FEA] Support DeleteCommand for Delta Lake|
|[#5272](https://github.com/NVIDIA/spark-rapids/issues/5272)|[FEA] Support from_json to get a MapType|
|[#7007](https://github.com/NVIDIA/spark-rapids/issues/7007)|[FEA] Support Delta table MERGE INTO on Databricks.|
|[#7521](https://github.com/NVIDIA/spark-rapids/issues/7521)|[FEA] Allow user to set concurrentGpuTasks after startup|
|[#3300](https://github.com/NVIDIA/spark-rapids/issues/3300)|[FEA] Support batched full join|
|[#6698](https://github.com/NVIDIA/spark-rapids/issues/6698)|[FEA] Support json_tuple|
|[#6885](https://github.com/NVIDIA/spark-rapids/issues/6885)|[FEA] Support reverse|
|[#6879](https://github.com/NVIDIA/spark-rapids/issues/6879)|[FEA] Support Databricks 11.3 ML LTS|

### Performance
|||
|:---|:---|
|[#7436](https://github.com/NVIDIA/spark-rapids/issues/7436)|[FEA] Pruning partition columns supports cases of GPU file scan with CPU project and filter|
|[#7219](https://github.com/NVIDIA/spark-rapids/issues/7219)|Improve performance of small file parquet reads from blobstores|
|[#6807](https://github.com/NVIDIA/spark-rapids/issues/6807)|Improve the current documentation on RapidsShuffleManager|
|[#5039](https://github.com/NVIDIA/spark-rapids/issues/5039)|[FEA] Parallelize shuffle compress/decompress with opportunistic idle task threads|
|[#7196](https://github.com/NVIDIA/spark-rapids/issues/7196)|RegExpExtract does both extract and contains_re which is inefficient|
|[#6862](https://github.com/NVIDIA/spark-rapids/issues/6862)|[FEA] GpuRowToColumnarExec for Binary is really slow compared to string|

### Bugs Fixed
|||
|:---|:---|
|[#7448](https://github.com/NVIDIA/spark-rapids/issues/7448)|[BUG] GDS cufile test failed in elder cuda runtime|
|[#7686](https://github.com/NVIDIA/spark-rapids/issues/7686)|[BUG] Large floating point values written as `Inf` not `Infinity` in Hive text writer|
|[#7703](https://github.com/NVIDIA/spark-rapids/issues/7703)|[BUG] test_basic_hive_text_write fails|
|[#7693](https://github.com/NVIDIA/spark-rapids/issues/7693)|[BUG] `test_partitioned_sql_parquet_write` fails on CDH|
|[#7382](https://github.com/NVIDIA/spark-rapids/issues/7382)|[BUG] add dynamic partition overwrite tests for all formats|
|[#7597](https://github.com/NVIDIA/spark-rapids/issues/7597)|[BUG] Incompatible timestamps in Hive delimited text writes|
|[#7675](https://github.com/NVIDIA/spark-rapids/issues/7675)|[BUG] Multi-threaded shuffle bails with division-by-zero ArithmeticException|
|[#7530](https://github.com/NVIDIA/spark-rapids/issues/7530)|[BUG] Add tests for RapidsShuffleManager|
|[#7679](https://github.com/NVIDIA/spark-rapids/issues/7679)|[BUG] test_partitioned_parquet_write[PartitionWriteMode.Dynamic] failed in databricks runtimes|
|[#7637](https://github.com/NVIDIA/spark-rapids/issues/7637)|[BUG] GpuBroadcastNestedLoopJoinExec:  Close called too many times|
|[#7595](https://github.com/NVIDIA/spark-rapids/issues/7595)|[BUG] test_mod_mixed decimal test fails on 330db (Databricks 11.3) and TBD 340|
|[#7575](https://github.com/NVIDIA/spark-rapids/issues/7575)|[BUG] On Databricks 11.3, Executor broadcast shuffles should stay on GPU even without columnar children |
|[#7607](https://github.com/NVIDIA/spark-rapids/issues/7607)|[BUG] RegularExpressionTranspilerSuite exhibits multiple failures|
|[#7574](https://github.com/NVIDIA/spark-rapids/issues/7574)|[BUG] simple json_tuple test failing with cudf error|
|[#7446](https://github.com/NVIDIA/spark-rapids/issues/7446)|[BUG] prune_partition_column_test fails for Json in UCX CI|
|[#7603](https://github.com/NVIDIA/spark-rapids/issues/7603)|[BUG] GpuIntervalUtilsTest leaks memory|
|[#7090](https://github.com/NVIDIA/spark-rapids/issues/7090)|[BUG] Refactor line terminator handling code|
|[#7472](https://github.com/NVIDIA/spark-rapids/issues/7472)|[BUG] The parquet chunked reader can fail for certain list cases.|
|[#7560](https://github.com/NVIDIA/spark-rapids/issues/7560)|[BUG] Outstanding allocations detected at shutdown in python integration tests|
|[#7516](https://github.com/NVIDIA/spark-rapids/issues/7516)|[BUG] multiple join cases cpu and gpu outputs mismatched in yarn|
|[#7537](https://github.com/NVIDIA/spark-rapids/issues/7537)|[AUDIT] [SPARK-42039][SQL] SPJ: Remove Option in KeyGroupedPartitioning#partitionValuesOpt|
|[#7535](https://github.com/NVIDIA/spark-rapids/issues/7535)|[BUG] Timestamp test cases failed due to Python time zone is not UTC|
|[#7432](https://github.com/NVIDIA/spark-rapids/issues/7432)|[BUG][SPARK-41713][SPARK-41726] Spark 3.4 build fails.|
|[#7517](https://github.com/NVIDIA/spark-rapids/issues/7517)|[DOC] doc/source of spark330db Shuffle Manager for Databricks-11.3 shim|
|[#7505](https://github.com/NVIDIA/spark-rapids/issues/7505)|[BUG] Delta Lake writes with AQE can have unnecessary row transitions |
|[#7454](https://github.com/NVIDIA/spark-rapids/issues/7454)|[BUG] Investigate binaryop.cpp:205: Unsupported operator for these types on Databricks 11.3|
|[#7469](https://github.com/NVIDIA/spark-rapids/issues/7469)|[BUG] test_arrays_zip failures on nightly |
|[#6894](https://github.com/NVIDIA/spark-rapids/issues/6894)|[BUG] Multithreaded rapids shuffle metrics incorrect|
|[#7325](https://github.com/NVIDIA/spark-rapids/issues/7325)|[BUG] Fix integration test failures on Databricks-11.3|
|[#7348](https://github.com/NVIDIA/spark-rapids/issues/7348)|[BUG] Fix integration tests for decimal type on db330 shim|
|[#6978](https://github.com/NVIDIA/spark-rapids/issues/6978)|[BUG] NDS query 16 fails on EMR 6.8.0 with AQE enabled|
|[#7133](https://github.com/NVIDIA/spark-rapids/issues/7133)|[BUG] Followup to AQE issues with reused columnar broadcast exchanges|
|[#7208](https://github.com/NVIDIA/spark-rapids/issues/7208)|[BUG] Explicitly check if the platform is supported|
|[#7397](https://github.com/NVIDIA/spark-rapids/issues/7397)|[BUG] shuffle smoke test hash_aggregate_test.py::test_hash_grpby_sum failed OOM intermittently in premerge|
|[#7443](https://github.com/NVIDIA/spark-rapids/issues/7443)|[BUG] prune_partition_column_test fails for Avro|
|[#7415](https://github.com/NVIDIA/spark-rapids/issues/7415)|[BUG] regex integration test failures on Databricks-11.3|
|[#7226](https://github.com/NVIDIA/spark-rapids/issues/7226)|[BUG] GpuFileSourceScanExec always generates all partition columns|
|[#7402](https://github.com/NVIDIA/spark-rapids/issues/7402)|[BUG] array_test.py::test_array_intersect failing cloudera|
|[#7324](https://github.com/NVIDIA/spark-rapids/issues/7324)|[BUG] Support DayTimeIntervalType in Databricks-11.3 to fix integration tests|
|[#7426](https://github.com/NVIDIA/spark-rapids/issues/7426)|[BUG] bloop compile times out on Databricks 11.3|
|[#7328](https://github.com/NVIDIA/spark-rapids/issues/7328)|[BUG] Databricks-11.3 integration test failing due to IllegalStateException: the broadcast must be on the GPU too|
|[#7327](https://github.com/NVIDIA/spark-rapids/issues/7327)|[BUG] Update Exception to fix Assertion error in Databricks-11.3 integration tests.|
|[#7403](https://github.com/NVIDIA/spark-rapids/issues/7403)|[BUG] test_dynamic_partition_write_round_trip failed in CDH tests|
|[#7368](https://github.com/NVIDIA/spark-rapids/issues/7368)|[BUG] with_hidden_metadata_fallback test failures on Databricks 11.3|
|[#7400](https://github.com/NVIDIA/spark-rapids/issues/7400)|[BUG] parquet multithreaded combine reader can calculate size wrong|
|[#7383](https://github.com/NVIDIA/spark-rapids/issues/7383)|[BUG] hive text partitioned reads using the `GpuHiveTableScanExec` are broken|
|[#7350](https://github.com/NVIDIA/spark-rapids/issues/7350)|[BUG] test_conditional_with_side_effects_case_when fails|
|[#7373](https://github.com/NVIDIA/spark-rapids/issues/7373)|[BUG] RapidsUDF does not support UDFs with no inputs|
|[#7213](https://github.com/NVIDIA/spark-rapids/issues/7213)|[BUG] Parquet unsigned int scan test failure|
|[#7344](https://github.com/NVIDIA/spark-rapids/issues/7344)|[BUG] Add PythonMapInArrowExec in 330db shim to fix integration test.|
|[#7367](https://github.com/NVIDIA/spark-rapids/issues/7367)|[BUG] test_re_replace_repetition failed|
|[#7345](https://github.com/NVIDIA/spark-rapids/issues/7345)|[BUG] Integration tests failing on Databricks-11.3 due to mixing of aggregations in HashAggregateExec and SortAggregateExec |
|[#7378](https://github.com/NVIDIA/spark-rapids/issues/7378)|[BUG][ORC] `GpuInsertIntoHadoopFsRelationCommand` should use staging directory for dynamic partition overwrite|
|[#7374](https://github.com/NVIDIA/spark-rapids/issues/7374)|[BUG] Hive reader does not always fall back to CPU when table contains nested types|
|[#7284](https://github.com/NVIDIA/spark-rapids/issues/7284)|[BUG] Generated supported data source file is inaccurate for write data formats|
|[#7347](https://github.com/NVIDIA/spark-rapids/issues/7347)|[BUG] Fix integration tests in Databricks-11.3 runtime by removing config spark.sql.ansi.strictIndexOperator|
|[#7326](https://github.com/NVIDIA/spark-rapids/issues/7326)|[BUG] Support RoundCeil and RoundFloor on Databricks-11.3 shim to fix integration tests.|
|[#7352](https://github.com/NVIDIA/spark-rapids/issues/7352)|[BUG] Databricks-11.3 IT failures - IllegalArgumentException: requirement failed for complexTypeExtractors|
|[#6285](https://github.com/NVIDIA/spark-rapids/issues/6285)|[BUG] Add null values back to test_array_intersect for Spark 3.3+ and Databricks 10.4+|
|[#7329](https://github.com/NVIDIA/spark-rapids/issues/7329)|[BUG] intermittently could not find ExecutedCommandExec in the GPU plan in delta_lake_write test|
|[#7184](https://github.com/NVIDIA/spark-rapids/issues/7184)|[BUG] Fix integration test failures on Databricks 11.3|
|[#7225](https://github.com/NVIDIA/spark-rapids/issues/7225)|[BUG] Partition columns mishandled when Parquet read is coalesced and chunked|
|[#7303](https://github.com/NVIDIA/spark-rapids/issues/7303)|[BUG] Fix CPU fallback for custom timestamp formats in Hive delimited text|
|[#7086](https://github.com/NVIDIA/spark-rapids/issues/7086)|[BUG] GPU Hive delimited text reader is more permissive than `LazySimpleSerDe` for timestamps|
|[#7089](https://github.com/NVIDIA/spark-rapids/issues/7089)|[BUG] Reading invalid `DATE` strings yields exceptions instead of nulls|
|[#6047](https://github.com/NVIDIA/spark-rapids/issues/6047)|[BUG] Look into field IDs when reading parquet using the native footer parser|
|[#6989](https://github.com/NVIDIA/spark-rapids/issues/6989)|[BUG] Spark-3.4 - Integration test failures in array_test|
|[#7122](https://github.com/NVIDIA/spark-rapids/issues/7122)|[BUG] Some tests of `limit_test` fail on Spark 3.4|
|[#7144](https://github.com/NVIDIA/spark-rapids/issues/7144)|[BUG] Spark-3.4 integration test failures due to code update in FileFormatWriter.|
|[#6915](https://github.com/NVIDIA/spark-rapids/issues/6915)|[BUG] Parquet of a binary decimal is not supported|

### PRs
|||
|:---|:---|
|[#7761](https://github.com/NVIDIA/spark-rapids/pull/7761)|[Doc] remove xgboost demo from aws-emr doc due to nccl issue [skip ci]|
|[#7760](https://github.com/NVIDIA/spark-rapids/pull/7760)|Add notice in gds to install cuda 11.8 [skip ci]|
|[#7570](https://github.com/NVIDIA/spark-rapids/pull/7570)|[Doc] 23.02 doc updates [skip ci]|
|[#7735](https://github.com/NVIDIA/spark-rapids/pull/7735)|Update JNI version to released 23.02.0|
|[#7721](https://github.com/NVIDIA/spark-rapids/pull/7721)|Fix issue where UCX mode was trying to use catalog from the driver|
|[#7713](https://github.com/NVIDIA/spark-rapids/pull/7713)|Workaround for incompatible serialization for `Inf` floats in Hive text write|
|[#7700](https://github.com/NVIDIA/spark-rapids/pull/7700)|Init 23.02 changelog and move 22 changelog to archives [skip ci]|
|[#7708](https://github.com/NVIDIA/spark-rapids/pull/7708)|Set Hive write test to ignore order on verification.|
|[#7697](https://github.com/NVIDIA/spark-rapids/pull/7697)|Disable Dynamic partitioning tests on CDH|
|[#7556](https://github.com/NVIDIA/spark-rapids/pull/7556)|Write support for Hive delimited text tables|
|[#7681](https://github.com/NVIDIA/spark-rapids/pull/7681)|Disable Hive delimited text reader tests for CDH.|
|[#7610](https://github.com/NVIDIA/spark-rapids/pull/7610)|Add multithreaded Shuffle test|
|[#7652](https://github.com/NVIDIA/spark-rapids/pull/7652)|Support Delta Lake UpdateCommand|
|[#7680](https://github.com/NVIDIA/spark-rapids/pull/7680)|Fix Parquet dynamic partition test for|
|[#7653](https://github.com/NVIDIA/spark-rapids/pull/7653)|Add dynamic partitioning test for Parquet writer.|
|[#7576](https://github.com/NVIDIA/spark-rapids/pull/7576)|Support EXECUTOR_BROADCAST on Databricks 11.3 in BroadcastNestedLoopJoin|
|[#7620](https://github.com/NVIDIA/spark-rapids/pull/7620)|Support Delta Lake DeleteCommand|
|[#7638](https://github.com/NVIDIA/spark-rapids/pull/7638)|Update UCX docs to call out Ubuntu 22.04 is not supported yet [skip ci]|
|[#7644](https://github.com/NVIDIA/spark-rapids/pull/7644)|Create a new ColumnarBatch from the broadcast when LazySpillable takes ownership|
|[#7632](https://github.com/NVIDIA/spark-rapids/pull/7632)|Update UCX shuffle doc w/ memlock limit config [skip ci]|
|[#7628](https://github.com/NVIDIA/spark-rapids/pull/7628)|Disable HiveTextReaders for CDH due to NVIDIA#7423|
|[#7633](https://github.com/NVIDIA/spark-rapids/pull/7633)|Bump up add-to-project version to 0.4.0 [skip ci]|
|[#7609](https://github.com/NVIDIA/spark-rapids/pull/7609)|Fallback to CPU for mod only for DB11.3 [Databricks]|
|[#7591](https://github.com/NVIDIA/spark-rapids/pull/7591)|Prevent fixup of GpuShuffleExchangeExec when using EXECUTOR_BROADCAST|
|[#7626](https://github.com/NVIDIA/spark-rapids/pull/7626)|Revert "Skip/xfail some regexp tests (#7608)"|
|[#7598](https://github.com/NVIDIA/spark-rapids/pull/7598)|Disable decimal `pmod` due to #7553|
|[#7571](https://github.com/NVIDIA/spark-rapids/pull/7571)|Refact deploy script to support gpg and nvsec signature [skip ci]|
|[#7590](https://github.com/NVIDIA/spark-rapids/pull/7590)|Fix `json_tuple` cudf error and java array out-of-bound issue|
|[#7604](https://github.com/NVIDIA/spark-rapids/pull/7604)|Fix memory leak in GpuIntervalUtilsTest|
|[#7600](https://github.com/NVIDIA/spark-rapids/pull/7600)|Centralize source-related properties in parent pom for consistent usage in submodules|
|[#7608](https://github.com/NVIDIA/spark-rapids/pull/7608)|Skip/xfail some regexp tests|
|[#7211](https://github.com/NVIDIA/spark-rapids/pull/7211)|Fix regressions related to cuDF changes in handline of end-of-line/string anchors|
|[#7596](https://github.com/NVIDIA/spark-rapids/pull/7596)|Fix deprecation warnings|
|[#7578](https://github.com/NVIDIA/spark-rapids/pull/7578)|Fix double close on exception in GpuCoalesceBatches|
|[#7584](https://github.com/NVIDIA/spark-rapids/pull/7584)|Write test for multithreaded combine wrong buffer size bug |
|[#7580](https://github.com/NVIDIA/spark-rapids/pull/7580)|Support Delta Lake MergeIntoCommand|
|[#7554](https://github.com/NVIDIA/spark-rapids/pull/7554)|Add mixed decimal testing for binary arithmetic ops|
|[#7567](https://github.com/NVIDIA/spark-rapids/pull/7567)|Fix a small leak in generate|
|[#7563](https://github.com/NVIDIA/spark-rapids/pull/7563)|Add patched Hive Metastore Client jar to  deps|
|[#7533](https://github.com/NVIDIA/spark-rapids/pull/7533)|Support EXECUTOR_BROADCAST on Databricks 11.3 in BroadcastHashJoin|
|[#7532](https://github.com/NVIDIA/spark-rapids/pull/7532)|Update Databricks docs to add a limitation against DB 11.3 [skip ci]|
|[#7555](https://github.com/NVIDIA/spark-rapids/pull/7555)|Fix case where tracking batch is never updated in batched full join|
|[#7547](https://github.com/NVIDIA/spark-rapids/pull/7547)|Refactor Delta Lake code to handle multiple versions per Spark version|
|[#7513](https://github.com/NVIDIA/spark-rapids/pull/7513)|Remove usage to `ColumnView.repeatStringsSizes`|
|[#7538](https://github.com/NVIDIA/spark-rapids/pull/7538)|Fix Spark 340 build error due to change in KeyGroupedPartitioning|
|[#7549](https://github.com/NVIDIA/spark-rapids/pull/7549)|Remove unused Maven property shim.module.name|
|[#7548](https://github.com/NVIDIA/spark-rapids/pull/7548)|Make RapidsBufferHandle AutoCloseable to prevent extra attempts to remove buffers|
|[#7499](https://github.com/NVIDIA/spark-rapids/pull/7499)|README.md for auditing purposes [skip ci]|
|[#7512](https://github.com/NVIDIA/spark-rapids/pull/7512)|Adds RapidsBufferHandle as an indirection layer to RapidsBufferId|
|[#7527](https://github.com/NVIDIA/spark-rapids/pull/7527)|Allow concurrentGpuTasks to be set per job|
|[#7539](https://github.com/NVIDIA/spark-rapids/pull/7539)|Enable automerge from 23.02 to 23.04 [skip ci]|
|[#7536](https://github.com/NVIDIA/spark-rapids/pull/7536)|Fix datetime out-of-range error in pytest when timezone is not UTC|
|[#7502](https://github.com/NVIDIA/spark-rapids/pull/7502)|Fix Spark 3.4 build errors|
|[#7522](https://github.com/NVIDIA/spark-rapids/pull/7522)|Update docs and add Rapids shuffle manager for Databricks-11.3|
|[#7507](https://github.com/NVIDIA/spark-rapids/pull/7507)|Fallback to CPU for unrecognized Distributions|
|[#7515](https://github.com/NVIDIA/spark-rapids/pull/7515)|Fix leak in GpuHiveTableScanExec|
|[#7504](https://github.com/NVIDIA/spark-rapids/pull/7504)|Align CI test scripts with new init scripts for Databricks [skip ci]|
|[#7506](https://github.com/NVIDIA/spark-rapids/pull/7506)|Add RapidsDeltaWrite node to fix undesired transitions with AQE|
|[#7414](https://github.com/NVIDIA/spark-rapids/pull/7414)|batched full hash join|
|[#7509](https://github.com/NVIDIA/spark-rapids/pull/7509)|Fix json_test.py imports|
|[#7269](https://github.com/NVIDIA/spark-rapids/pull/7269)|Add hadoop-def.sh to support multiple spark release tarballs|
|[#7494](https://github.com/NVIDIA/spark-rapids/pull/7494)|Implement a simplified version of `from_json`|
|[#7434](https://github.com/NVIDIA/spark-rapids/pull/7434)|Support `json_tuple`|
|[#7489](https://github.com/NVIDIA/spark-rapids/pull/7489)|Remove the MIT license from tools jar[skip ci]|
|[#7497](https://github.com/NVIDIA/spark-rapids/pull/7497)|Inserting multiple times to HashedPriorityQueue should not corrupt the heap|
|[#7475](https://github.com/NVIDIA/spark-rapids/pull/7475)|Inject GpuCast for decimal AddSub when operands' precision/scale differ on 340, 330db|
|[#7486](https://github.com/NVIDIA/spark-rapids/pull/7486)|Allow Shims to replace Hive execs|
|[#7484](https://github.com/NVIDIA/spark-rapids/pull/7484)|Fix arrays_zip to not rely on broken segmented gather|
|[#7460](https://github.com/NVIDIA/spark-rapids/pull/7460)|More cases support partition column pruning|
|[#7462](https://github.com/NVIDIA/spark-rapids/pull/7462)|Changing metric component counters to avoid extraneous accruals|
|[#7467](https://github.com/NVIDIA/spark-rapids/pull/7467)|Remove spark2-sql-plugin|
|[#7474](https://github.com/NVIDIA/spark-rapids/pull/7474)|xfail array zip tests|
|[#7464](https://github.com/NVIDIA/spark-rapids/pull/7464)|Enable integration tests against Databricks 11.3 in premerge|
|[#7455](https://github.com/NVIDIA/spark-rapids/pull/7455)|Use GpuAlias when handling Empty2Null in GpuOptimisticTransaction|
|[#7456](https://github.com/NVIDIA/spark-rapids/pull/7456)|Sort Delta log objects when comparing and avoid caching all logs|
|[#7431](https://github.com/NVIDIA/spark-rapids/pull/7431)|Remove release script of spark-rapids/tools [skip ci]|
|[#7421](https://github.com/NVIDIA/spark-rapids/pull/7421)|Remove spark-rapids/tools|
|[#7418](https://github.com/NVIDIA/spark-rapids/pull/7418)|Fix for AQE+DPP issue on AWS EMR|
|[#7444](https://github.com/NVIDIA/spark-rapids/pull/7444)|Explicitly check if the platform is supported|
|[#7417](https://github.com/NVIDIA/spark-rapids/pull/7417)|Make cudf-udf tests runnable on Databricks 11.3|
|[#7420](https://github.com/NVIDIA/spark-rapids/pull/7420)|Ubuntu build&test images default as 20.04|
|[#7442](https://github.com/NVIDIA/spark-rapids/pull/7442)|Fix parsing of Delta Lake logs containing multi-line JSON records|
|[#7438](https://github.com/NVIDIA/spark-rapids/pull/7438)|Add documentation for runnable command enable configs|
|[#7439](https://github.com/NVIDIA/spark-rapids/pull/7439)|Suppress unknown RunnableCommand warnings by default|
|[#7346](https://github.com/NVIDIA/spark-rapids/pull/7346)|[Doc]revert the changes in FAQ for deltalake table support[skip ci]|
|[#7413](https://github.com/NVIDIA/spark-rapids/pull/7413)|[Doc]update getting started doc for EMR and databricks[skip ci]|
|[#7445](https://github.com/NVIDIA/spark-rapids/pull/7445)|Support pruning partition columns for avro file scan|
|[#7447](https://github.com/NVIDIA/spark-rapids/pull/7447)|Xfail the test of pruning partition column for json read|
|[#7440](https://github.com/NVIDIA/spark-rapids/pull/7440)|Xfail largest decimals window aggregation|
|[#7437](https://github.com/NVIDIA/spark-rapids/pull/7437)|Fix the regexp test failures on DB11.3|
|[#7428](https://github.com/NVIDIA/spark-rapids/pull/7428)|Support pruning partition columns for GpuFileSourceScan|
|[#7435](https://github.com/NVIDIA/spark-rapids/pull/7435)|Update IntelliJ IDEA doc [skip ci]|
|[#7416](https://github.com/NVIDIA/spark-rapids/pull/7416)|Reorganize and shim ScanExecMeta overrides and fix interval file IO|
|[#7427](https://github.com/NVIDIA/spark-rapids/pull/7427)|Install-file log4j-core on Databricks 11.3|
|[#7424](https://github.com/NVIDIA/spark-rapids/pull/7424)|xfail Hive text tests failing on CDH|
|[#7408](https://github.com/NVIDIA/spark-rapids/pull/7408)|Fallback to CPU for unrecognized ShuffleOrigin|
|[#7422](https://github.com/NVIDIA/spark-rapids/pull/7422)|Skip test_dynamic_partition_write_round_trip in 321cdh|
|[#7406](https://github.com/NVIDIA/spark-rapids/pull/7406)|Fix array_test.py::test_array_intersect for cloudera spark330|
|[#6761](https://github.com/NVIDIA/spark-rapids/pull/6761)|Switch string to float casting to use new kernel|
|[#7411](https://github.com/NVIDIA/spark-rapids/pull/7411)|Skip Int division test that causes scale less than precision|
|[#7410](https://github.com/NVIDIA/spark-rapids/pull/7410)|Remove 314 from dist build list|
|[#7412](https://github.com/NVIDIA/spark-rapids/pull/7412)|Fix the `with_hidden_metadata_fallback` test failures on DB11.3|
|[#7362](https://github.com/NVIDIA/spark-rapids/pull/7362)|Fix multiplication and division test failures in 330db and 340 shim|
|[#7405](https://github.com/NVIDIA/spark-rapids/pull/7405)|Fix multithreaded combine code initial size calculation|
|[#7395](https://github.com/NVIDIA/spark-rapids/pull/7395)|Enable Delta Lake write acceleration by default|
|[#7384](https://github.com/NVIDIA/spark-rapids/pull/7384)|Fix implementation of createReadRDDForDirectories to match DataSource…|
|[#7391](https://github.com/NVIDIA/spark-rapids/pull/7391)|Exclude GDS test suite as default|
|[#7390](https://github.com/NVIDIA/spark-rapids/pull/7390)|Aggregate Databricks 11.3 shim in the nightly dist jar|
|[#7381](https://github.com/NVIDIA/spark-rapids/pull/7381)|Filter out some new timestamp-related Delta Lake tags when comparing logs|
|[#7371](https://github.com/NVIDIA/spark-rapids/pull/7371)|Avoid shutting down RMM until all allocations have cleared|
|[#7377](https://github.com/NVIDIA/spark-rapids/pull/7377)|Update RapidsUDF interface to support UDFs with no input parameters|
|[#7388](https://github.com/NVIDIA/spark-rapids/pull/7388)|Fix `test_array_element_at_zero_index_fail` and `test_div_overflow_exception_when_ansi` DB 11.3 integration test failures|
|[#7380](https://github.com/NVIDIA/spark-rapids/pull/7380)|[FEA] Support `reverse` for arrays|
|[#7365](https://github.com/NVIDIA/spark-rapids/pull/7365)|Move PythonMapInArrowExec to shim for shared 330+ functionality (db11.3)|
|[#7372](https://github.com/NVIDIA/spark-rapids/pull/7372)|Fix for incorrect nested-unsigned test|
|[#7386](https://github.com/NVIDIA/spark-rapids/pull/7386)|Spark 3.1.4 snapshot fix setting of reproduceEmptyStringBug|
|[#7385](https://github.com/NVIDIA/spark-rapids/pull/7385)|Fix Databricks version comparison in pytests|
|[#7379](https://github.com/NVIDIA/spark-rapids/pull/7379)|Fixes bug where dynamic partition overwrite mode didn't work for ORC|
|[#7370](https://github.com/NVIDIA/spark-rapids/pull/7370)|Fix non file read DayTimeInterval errors|
|[#7375](https://github.com/NVIDIA/spark-rapids/pull/7375)|Fix CPU fallback for GpuHiveTableScanExec|
|[#7355](https://github.com/NVIDIA/spark-rapids/pull/7355)|Fix Add and Subtract test failures in 330db and 340 shim|
|[#7299](https://github.com/NVIDIA/spark-rapids/pull/7299)|Qualification tool: Update parsing of write data format|
|[#7357](https://github.com/NVIDIA/spark-rapids/pull/7357)|Update the integration tests  to fit the removed config ` spark.sql.ansi.strictIndexOperator` in DB11.3 and Spark 3.4|
|[#7369](https://github.com/NVIDIA/spark-rapids/pull/7369)|Re-enable some tests in ParseDateTimeSuite|
|[#7366](https://github.com/NVIDIA/spark-rapids/pull/7366)|Support Databricks 11.3|
|[#7354](https://github.com/NVIDIA/spark-rapids/pull/7354)|Fix arithmetic error messages in 330db, 340 shims|
|[#7364](https://github.com/NVIDIA/spark-rapids/pull/7364)|Move Rounding ops to shim for 330+ (db11.3)|
|[#7298](https://github.com/NVIDIA/spark-rapids/pull/7298)|Improve performance of small file parquet reads from blob stores|
|[#7363](https://github.com/NVIDIA/spark-rapids/pull/7363)|Fix ExtractValue assertion with 330db shim|
|[#7340](https://github.com/NVIDIA/spark-rapids/pull/7340)|Fix nested-unsigned test issues.|
|[#7358](https://github.com/NVIDIA/spark-rapids/pull/7358)|Update Delta version to 1.1.0|
|[#7301](https://github.com/NVIDIA/spark-rapids/pull/7301)|Add null values back to test_array_intersect for Spark 3.3.1+ and Databricks 10.4+|
|[#7152](https://github.com/NVIDIA/spark-rapids/pull/7152)|Add a shim for Databricks 11.3 spark330db|
|[#7333](https://github.com/NVIDIA/spark-rapids/pull/7333)|Avoid row number computation when the partition schema is empty|
|[#7317](https://github.com/NVIDIA/spark-rapids/pull/7317)|Make multi-threaded shuffle not experimental and update docs|
|[#7342](https://github.com/NVIDIA/spark-rapids/pull/7342)|Update plan capture listener to handle multiple plans per query.|
|[#7338](https://github.com/NVIDIA/spark-rapids/pull/7338)|Fix auto merge conflict 7336 [skip ci]|
|[#7335](https://github.com/NVIDIA/spark-rapids/pull/7335)|Fix auto merge conflict 7334[skip ci]|
|[#7312](https://github.com/NVIDIA/spark-rapids/pull/7312)|Fix documentation bug|
|[#7315](https://github.com/NVIDIA/spark-rapids/pull/7315)|Fix merge conflict with branch-22.12|
|[#7296](https://github.com/NVIDIA/spark-rapids/pull/7296)|Enable hive text reads by default|
|[#7304](https://github.com/NVIDIA/spark-rapids/pull/7304)|Correct partition columns handling for coalesced and chunked read|
|[#7305](https://github.com/NVIDIA/spark-rapids/pull/7305)|Fix CPU fallback for custom timestamp formats in Hive text tables|
|[#7293](https://github.com/NVIDIA/spark-rapids/pull/7293)|Refine '$LOCAL_JAR_PATH' as optional for integration test on databricks [skip ci]|
|[#7285](https://github.com/NVIDIA/spark-rapids/pull/7285)|[FEA] Support `reverse` for strings|
|[#7297](https://github.com/NVIDIA/spark-rapids/pull/7297)|Remove Decimal Support Section from compatibility docs [skip ci]|
|[#7291](https://github.com/NVIDIA/spark-rapids/pull/7291)|Improve IntelliJ IDEA doc and usability|
|[#7245](https://github.com/NVIDIA/spark-rapids/pull/7245)|Fix boolean, int, and float parsing. Improve decimal parsing for hive|
|[#7265](https://github.com/NVIDIA/spark-rapids/pull/7265)|Fix Hive Delimited Text timestamp parsing|
|[#7221](https://github.com/NVIDIA/spark-rapids/pull/7221)|Fix date parsing in Hive Delimited Text reader|
|[#7287](https://github.com/NVIDIA/spark-rapids/pull/7287)|Don't use native parquet footer parser if field ids for read are needed|
|[#7262](https://github.com/NVIDIA/spark-rapids/pull/7262)|Moving generated files to standalone tools dir|
|[#7268](https://github.com/NVIDIA/spark-rapids/pull/7268)|Remove deprecated compatibility support in premerge|
|[#7248](https://github.com/NVIDIA/spark-rapids/pull/7248)|Fix AlluxioUtilsSuite build on Databricks|
|[#7207](https://github.com/NVIDIA/spark-rapids/pull/7207)|Change the hive text file parser to not use CSV input format|
|[#7209](https://github.com/NVIDIA/spark-rapids/pull/7209)|Change RegExpExtract to use isNull checks instead of contains_re|
|[#7212](https://github.com/NVIDIA/spark-rapids/pull/7212)|Removing common module and adding common files to sql-plugin|
|[#7202](https://github.com/NVIDIA/spark-rapids/pull/7202)|Avoid sort in v1 write for static columns|
|[#7167](https://github.com/NVIDIA/spark-rapids/pull/7167)|Handle two changes related to `FileFormatWriter` since Spark 340|
|[#7194](https://github.com/NVIDIA/spark-rapids/pull/7194)|Skip tests that fail due to recent cuDF changes related to end of string/line anchors|
|[#7170](https://github.com/NVIDIA/spark-rapids/pull/7170)|Fix the `limit_test` failures on Spark 3.4|
|[#7126](https://github.com/NVIDIA/spark-rapids/pull/7126)|Fix support for binary encoded decimal for parquet|
|[#7113](https://github.com/NVIDIA/spark-rapids/pull/7113)|Use an improved API for appending binary to host vector|
|[#7130](https://github.com/NVIDIA/spark-rapids/pull/7130)|Enable chunked parquet reads by default|
|[#7074](https://github.com/NVIDIA/spark-rapids/pull/7074)|Update JNI and cudf-py version to 23.02|
|[#7063](https://github.com/NVIDIA/spark-rapids/pull/7063)|Init version 23.02.0|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
