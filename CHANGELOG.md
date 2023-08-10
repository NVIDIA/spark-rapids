# Change log
Generated on 2023-08-10

## Release 23.08

### Features
|||
|:---|:---|
|[#5509](https://github.com/NVIDIA/spark-rapids/issues/5509)|[FEA] Support order-by on Array|
|[#7876](https://github.com/NVIDIA/spark-rapids/issues/7876)|[FEA] Add initial support for Databricks 12.2 ML LTS|
|[#8660](https://github.com/NVIDIA/spark-rapids/issues/8660)|[FEA][Databricks 12.2] Update docs to state that Delta Lake on Databricks 12.2 is supported|
|[#8547](https://github.com/NVIDIA/spark-rapids/issues/8547)|[FEA] Add support for Delta Lake 2.4 with Spark 3.4|
|[#8691](https://github.com/NVIDIA/spark-rapids/issues/8691)|[FEA] Driver log warning on startup when GPU is limiting scheduling resource|
|[#8633](https://github.com/NVIDIA/spark-rapids/issues/8633)|[FEA] Add support for xxHash64 function|
|[#4929](https://github.com/NVIDIA/spark-rapids/issues/4929)|[FEA] Support min/max aggregation/reduction for arrays of structs and arrays of strings|
|[#8668](https://github.com/NVIDIA/spark-rapids/issues/8668)|[FEA] Support min and max for arrays|
|[#4887](https://github.com/NVIDIA/spark-rapids/issues/4887)|[FEA] Hash partitioning on ArrayType|
|[#6680](https://github.com/NVIDIA/spark-rapids/issues/6680)|Support hashaggregate for Array[Any]|
|[#8085](https://github.com/NVIDIA/spark-rapids/issues/8085)|[FEA] Add support for MillisToTimestamp|
|[#7801](https://github.com/NVIDIA/spark-rapids/issues/7801)|[FEA] Window Expression orderBy column is not supported in a window range function, found  DoubleType|
|[#8556](https://github.com/NVIDIA/spark-rapids/issues/8556)|[FEA] [Delta Lake] Add support for new metrics in MERGE|
|[#308](https://github.com/NVIDIA/spark-rapids/issues/308)|[FEA] Spark 3.1 adding support for  TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS functions|
|[#8122](https://github.com/NVIDIA/spark-rapids/issues/8122)|[FEA] Add spark 3.4.1 snapshot shim|
|[#8423](https://github.com/NVIDIA/spark-rapids/issues/8423)|[FEA] [Databricks 12.2] Get Delta Lake integration tests passing|
|[#8184](https://github.com/NVIDIA/spark-rapids/issues/8184)|Enable asserts for checking non-empty nulls|
|[#8382](https://github.com/NVIDIA/spark-rapids/issues/8382)|[FEA] Implement a heuristic to split a project's input based on output and add to hash aggregate|
|[#8453](https://github.com/NVIDIA/spark-rapids/issues/8453)|[FEA] Support SplitNRetry aggregations without concat on the first pass|
|[#8525](https://github.com/NVIDIA/spark-rapids/issues/8525)|[FEA] Add support for org.apache.spark.sql.functions.flatten|
|[#8319](https://github.com/NVIDIA/spark-rapids/issues/8319)|[FEA] Remove hard-coded versions from databricks build script|
|[#8202](https://github.com/NVIDIA/spark-rapids/issues/8202)|[FEA] List supported Spark builds when the Shim is not found|
|[#8461](https://github.com/NVIDIA/spark-rapids/issues/8461)|[FEA] Support spill+retry for GpuExpandExec|

### Performance
|||
|:---|:---|
|[#8231](https://github.com/NVIDIA/spark-rapids/issues/8231)|[FEA] Add filecache support to ORC scans|
|[#8141](https://github.com/NVIDIA/spark-rapids/issues/8141)|[FEA] Explore how to best deal with large numbers of aggregations in the short term|

### Bugs Fixed
|||
|:---|:---|
|[#8610](https://github.com/NVIDIA/spark-rapids/issues/8610)|[BUG] query 95 @ SF30K fails with OOM exception|
|[#8955](https://github.com/NVIDIA/spark-rapids/issues/8955)|[BUG] Bloom filter join tests can fail with multiple join columns|
|[#45](https://github.com/NVIDIA/spark-rapids/issues/45)|[BUG] very large shuffles can fail|
|[#8779](https://github.com/NVIDIA/spark-rapids/issues/8779)|[BUG] Put shared Databricks test script together for ease of maintenance|
|[#8930](https://github.com/NVIDIA/spark-rapids/issues/8930)|[BUG] checkoutSCM plugin is unstable for pre-merge CI, it is often unable to clone submodules|
|[#8923](https://github.com/NVIDIA/spark-rapids/issues/8923)|[BUG] Mortgage test failing with 'JavaPackage' error on AWS Databricks|
|[#8303](https://github.com/NVIDIA/spark-rapids/issues/8303)|[BUG] GpuExpression columnarEval can return scalars from subqueries that may be unhandled|
|[#8318](https://github.com/NVIDIA/spark-rapids/issues/8318)|[BUG][Databricks 12.2] GpuRowBasedHiveGenericUDF ClassCastException|
|[#8822](https://github.com/NVIDIA/spark-rapids/issues/8822)|[BUG] Early terminate CI if submodule init failed|
|[#8847](https://github.com/NVIDIA/spark-rapids/issues/8847)|[BUG] github actions CI messed up w/ JDK versions intermittently|
|[#8716](https://github.com/NVIDIA/spark-rapids/issues/8716)|[BUG] `test_hash_groupby_collect_set_on_nested_type` and `test_hash_reduction_collect_set_on_nested_type` failed|
|[#8827](https://github.com/NVIDIA/spark-rapids/issues/8827)|[BUG] databricks cudf_udf night build failing with pool size exceeded errors|
|[#8630](https://github.com/NVIDIA/spark-rapids/issues/8630)|[BUG] Parquet with RLE encoded booleans loads corrupted data|
|[#8735](https://github.com/NVIDIA/spark-rapids/issues/8735)|[BUG] test_orc_column_name_with_dots fails in nightly EGX tests|
|[#6980](https://github.com/NVIDIA/spark-rapids/issues/6980)|[BUG] Partitioned writes release GPU semaphore with unspillable GPU memory|
|[#8784](https://github.com/NVIDIA/spark-rapids/issues/8784)|[BUG] hash_aggregate_test.py::test_min_max_in_groupby_and_reduction failed on "TypeError: object of type 'NoneType' has no len()"|
|[#8756](https://github.com/NVIDIA/spark-rapids/issues/8756)|[BUG] [Databricks 12.2] RapidsDeltaWrite queries that reference internal metadata fail to run|
|[#8636](https://github.com/NVIDIA/spark-rapids/issues/8636)|[BUG] AWS Databricks 12.2 integration tests failed due to Iceberg check|
|[#8754](https://github.com/NVIDIA/spark-rapids/issues/8754)|[BUG] databricks build broke after adding bigDataGen|
|[#8726](https://github.com/NVIDIA/spark-rapids/issues/8726)|[BUG] Test "parquet_write_test.py::test_hive_timestamp_value[INJECT_OOM]" failed on Databricks |
|[#8690](https://github.com/NVIDIA/spark-rapids/issues/8690)|[BUG buildall script does not support JDK11 profile|
|[#8702](https://github.com/NVIDIA/spark-rapids/issues/8702)|[BUG] test_min_max_for_single_level_struct failed|
|[#8727](https://github.com/NVIDIA/spark-rapids/issues/8727)|[BUG] test_column_add_after_partition failed in databricks 10.4 runtime|
|[#8669](https://github.com/NVIDIA/spark-rapids/issues/8669)|[BUG] SpillableColumnarBatch doesn't always take ownership|
|[#8655](https://github.com/NVIDIA/spark-rapids/issues/8655)|[BUG] There are some potential device memory leaks in `AbstractGpuCoalesceIterator`|
|[#8685](https://github.com/NVIDIA/spark-rapids/issues/8685)|[BUG] install build fails with Maven 3.9.3|
|[#8156](https://github.com/NVIDIA/spark-rapids/issues/8156)|[BUG] Install phase for modules with Spark build classifier fails for install plugin versions 3.0.0+|
|[#1130](https://github.com/NVIDIA/spark-rapids/issues/1130)|[BUG] TIMESTAMP_MILLIS not handled in isDateTimeRebaseNeeded|
|[#7676](https://github.com/NVIDIA/spark-rapids/issues/7676)|[BUG] SparkShimsImpl  class initialization in SparkShimsSuite for 340 too eager|
|[#8278](https://github.com/NVIDIA/spark-rapids/issues/8278)|[BUG] NDS query 16 hangs at SF30K|
|[#8665](https://github.com/NVIDIA/spark-rapids/issues/8665)|[BUG] EGX nightly tests fail to detect Spark version on startup|
|[#8647](https://github.com/NVIDIA/spark-rapids/issues/8647)|[BUG] array_test.py::test_array_min_max[Float][INJECT_OOM] failed mismatched CPU and GPU output in nightly|
|[#8640](https://github.com/NVIDIA/spark-rapids/issues/8640)|[BUG] Optimize Databricks pre-merge scripts, move it out into a new CI file|
|[#8308](https://github.com/NVIDIA/spark-rapids/issues/8308)|[BUG] Device Memory leak seen in integration_tests when AssertEmptyNulls are enabled|
|[#8602](https://github.com/NVIDIA/spark-rapids/issues/8602)|[BUG] AutoCloseable Broadcast results are getting closed by Spark|
|[#8603](https://github.com/NVIDIA/spark-rapids/issues/8603)|[BUG] SerializeConcatHostBuffersDeserializeBatch.writeObject fails with ArrayIndexOutOfBoundsException on rows-only table|
|[#8615](https://github.com/NVIDIA/spark-rapids/issues/8615)|[BUG] RapidsShuffleThreadedWriterSuite temp shuffle file test failure|
|[#6872](https://github.com/NVIDIA/spark-rapids/issues/6872)|[BUG] awk: cmd. line:1: warning: regexp escape sequence `\ ' is not a known regexp operator|
|[#8588](https://github.com/NVIDIA/spark-rapids/issues/8588)|[BUG] Spark 3.3.x integration tests failed due to missing jars|
|[#7775](https://github.com/NVIDIA/spark-rapids/issues/7775)|[BUG] scala version hardcoded irrespective of Spark dependency|
|[#8548](https://github.com/NVIDIA/spark-rapids/issues/8548)|[BUG] cache_test:test_batch_no_cols test FAILED on spark-3.3.0+|
|[#8579](https://github.com/NVIDIA/spark-rapids/issues/8579)|[BUG] build failed on Databricks clusters "GpuDeleteCommand.scala:104: type mismatch" |
|[#8380](https://github.com/NVIDIA/spark-rapids/issues/8380)|Remove the legacy Spark support from CachedBatchWriterSuite|
|[#8187](https://github.com/NVIDIA/spark-rapids/issues/8187)|[BUG] Integration test test_window_running_no_part can produce non-empty nulls (cudf scan)|
|[#8493](https://github.com/NVIDIA/spark-rapids/issues/8493)|[BUG] branch-23.08 fails to build on Databricks 12.2|

### PRs
|||
|:---|:---|
|[#8920](https://github.com/NVIDIA/spark-rapids/pull/8920)|Add test case to test orc dictionary encoding with lots of rows for nested types|
|[#8940](https://github.com/NVIDIA/spark-rapids/pull/8940)|Add test case for ORC statistics test|
|[#8909](https://github.com/NVIDIA/spark-rapids/pull/8909)|Match Spark's NaN handling in collect_set|
|[#8892](https://github.com/NVIDIA/spark-rapids/pull/8892)|Experimental support for BloomFilterAggregate expression in a reduction context|
|[#8957](https://github.com/NVIDIA/spark-rapids/pull/8957)|Fix building dockerfile.cuda hanging at tzdata installation [skip ci]|
|[#8944](https://github.com/NVIDIA/spark-rapids/pull/8944)|Fix issues around bloom filter with multple columns|
|[#8744](https://github.com/NVIDIA/spark-rapids/pull/8744)|Add test for selecting a single complex field array and its parent struct array|
|[#8936](https://github.com/NVIDIA/spark-rapids/pull/8936)|Device synchronize prior to freeing a set of RapidsBuffer|
|[#8935](https://github.com/NVIDIA/spark-rapids/pull/8935)|Don't go over shuffle limits on CPU|
|[#8927](https://github.com/NVIDIA/spark-rapids/pull/8927)|Skipping test_map_scalars_supported_key_types because of distributed …|
|[#8931](https://github.com/NVIDIA/spark-rapids/pull/8931)|Clone submodule using git command instead of checkoutSCM plugin|
|[#8917](https://github.com/NVIDIA/spark-rapids/pull/8917)|Databricks shim version for integration test|
|[#8775](https://github.com/NVIDIA/spark-rapids/pull/8775)|Support BloomFilterMightContain expression|
|[#8833](https://github.com/NVIDIA/spark-rapids/pull/8833)|Binary and ternary handling of scalar audit and some fixes|
|[#7233](https://github.com/NVIDIA/spark-rapids/pull/7233)|[FEA] Support `order by` on single-level array|
|[#8893](https://github.com/NVIDIA/spark-rapids/pull/8893)|Fix regression in Hive Generic UDF support on Databricks 12.2|
|[#8828](https://github.com/NVIDIA/spark-rapids/pull/8828)|Put shared part together for Databricks test scripts|
|[#8872](https://github.com/NVIDIA/spark-rapids/pull/8872)|Terminate CI if fail to clone submodule|
|[#8787](https://github.com/NVIDIA/spark-rapids/pull/8787)|Add in support for ExponentialDistribution|
|[#8868](https://github.com/NVIDIA/spark-rapids/pull/8868)|Add a test case for testing ORC version V_0_11 and V_0_12|
|[#8795](https://github.com/NVIDIA/spark-rapids/pull/8795)|Add ORC writing test cases for not implicitly lowercase columns|
|[#8871](https://github.com/NVIDIA/spark-rapids/pull/8871)|Adjust parallelism in spark-tests script to reduce memory footprint [skip ci]|
|[#8869](https://github.com/NVIDIA/spark-rapids/pull/8869)|Specify expected JAVA_HOME and bin for mvn-verify-check [skip ci]|
|[#8785](https://github.com/NVIDIA/spark-rapids/pull/8785)|Add test cases for ORC writing according to options orc.compress and compression|
|[#8810](https://github.com/NVIDIA/spark-rapids/pull/8810)|Fall back to CPU for deletion vectors writes on Databricks|
|[#8830](https://github.com/NVIDIA/spark-rapids/pull/8830)|Update documentation to add Databricks 12.2 as a supported platform [skip ci]|
|[#8799](https://github.com/NVIDIA/spark-rapids/pull/8799)|Add tests to cover some odd corner cases with nulls and empty arrays|
|[#8783](https://github.com/NVIDIA/spark-rapids/pull/8783)|Fix collect_set_on_nested_type tests failed|
|[#8855](https://github.com/NVIDIA/spark-rapids/pull/8855)|Fix bug: Check GPU file instead of CPU file [skip ci]|
|[#8852](https://github.com/NVIDIA/spark-rapids/pull/8852)|Update test scripts and dockerfiles to match cudf conda pkg change [skip ci]|
|[#8848](https://github.com/NVIDIA/spark-rapids/pull/8848)|Try mitigate mismatched JDK versions in mvn-verify checks [skip ci]|
|[#8825](https://github.com/NVIDIA/spark-rapids/pull/8825)|Add a case to test ORC writing/reading with lots of nulls|
|[#8802](https://github.com/NVIDIA/spark-rapids/pull/8802)|Treat unbounded windows as truly non-finite.|
|[#8798](https://github.com/NVIDIA/spark-rapids/pull/8798)|Add ORC writing test cases for dictionary compression|
|[#8829](https://github.com/NVIDIA/spark-rapids/pull/8829)|Enable rle_boolean_encoding.parquet test|
|[#8667](https://github.com/NVIDIA/spark-rapids/pull/8667)|Make state spillable in partitioned writer|
|[#8801](https://github.com/NVIDIA/spark-rapids/pull/8801)|Fix shuffling an empty Struct() column with UCX|
|[#8748](https://github.com/NVIDIA/spark-rapids/pull/8748)|Add driver log warning when GPU is limiting scheduling resource|
|[#8786](https://github.com/NVIDIA/spark-rapids/pull/8786)|Add support for row-based execution in RapidsDeltaWrite|
|[#8791](https://github.com/NVIDIA/spark-rapids/pull/8791)|Auto merge to branch-23.10 from branch-23.08[skip ci]|
|[#8790](https://github.com/NVIDIA/spark-rapids/pull/8790)|Update ubuntu dockerfiles default to 20.04 and deprecating centos one [skip ci]|
|[#8777](https://github.com/NVIDIA/spark-rapids/pull/8777)|Install python packages with shared scripts on Databricks|
|[#8772](https://github.com/NVIDIA/spark-rapids/pull/8772)|Test concurrent writer update file metrics|
|[#8646](https://github.com/NVIDIA/spark-rapids/pull/8646)|Add testing of Parquet files from apache/parquet-testing|
|[#8684](https://github.com/NVIDIA/spark-rapids/pull/8684)|Add 'submodule update --init' when build spark-rapids|
|[#8769](https://github.com/NVIDIA/spark-rapids/pull/8769)|Remove iceberg scripts from Databricks test scripts|
|[#8773](https://github.com/NVIDIA/spark-rapids/pull/8773)|Add a test case for reading/write null to ORC|
|[#8749](https://github.com/NVIDIA/spark-rapids/pull/8749)|Add test cases for read/write User Defined Type (UDT) to ORC|
|[#8768](https://github.com/NVIDIA/spark-rapids/pull/8768)|Add support for xxhash64|
|[#8751](https://github.com/NVIDIA/spark-rapids/pull/8751)|Ensure columnarEval always returns a GpuColumnVector|
|[#8765](https://github.com/NVIDIA/spark-rapids/pull/8765)|Add in support for maps to big data gen|
|[#8758](https://github.com/NVIDIA/spark-rapids/pull/8758)|Normal and Multi Distributions for BigDataGen|
|[#8755](https://github.com/NVIDIA/spark-rapids/pull/8755)|Add in dependency for databricks on integration tests|
|[#8737](https://github.com/NVIDIA/spark-rapids/pull/8737)|Fix parquet_write_test.py::test_hive_timestamp_value failure for Databricks|
|[#8745](https://github.com/NVIDIA/spark-rapids/pull/8745)|Conventional jar layout is not required for JDK9+|
|[#8706](https://github.com/NVIDIA/spark-rapids/pull/8706)|Add a tool to support generating large amounts of data|
|[#8747](https://github.com/NVIDIA/spark-rapids/pull/8747)|xfail hash_groupby_collect_set and hash_reduction_collect_set on nested type cases|
|[#8689](https://github.com/NVIDIA/spark-rapids/pull/8689)|Support nested arrays for `min`/`max` aggregations in groupby and reduction|
|[#8699](https://github.com/NVIDIA/spark-rapids/pull/8699)|Regression test for array of struct with a single field name "element" in Parquet|
|[#8733](https://github.com/NVIDIA/spark-rapids/pull/8733)|Avoid generating numeric null partition values on Databricks 10.4|
|[#8728](https://github.com/NVIDIA/spark-rapids/pull/8728)|Use specific mamba version and install libarchive explictly [skip ci]|
|[#8594](https://github.com/NVIDIA/spark-rapids/pull/8594)|String generation from complex regex in integration tests|
|[#8700](https://github.com/NVIDIA/spark-rapids/pull/8700)|Add regression test to ensure Parquet doesn't interpret timestamp values differently from Hive 0.14.0+|
|[#8711](https://github.com/NVIDIA/spark-rapids/pull/8711)|Factor out modules shared among shim profiles|
|[#8697](https://github.com/NVIDIA/spark-rapids/pull/8697)|Spillable columnar batch takes ownership and improve code coverage|
|[#8705](https://github.com/NVIDIA/spark-rapids/pull/8705)|Add schema evolution integration tests for partitioned data|
|[#8673](https://github.com/NVIDIA/spark-rapids/pull/8673)|Fix some potential memory leaks|
|[#8707](https://github.com/NVIDIA/spark-rapids/pull/8707)|Update config docs for new filecache configs [skip ci]|
|[#8695](https://github.com/NVIDIA/spark-rapids/pull/8695)|Always create the main artifact along with a shim-classifier artifact|
|[#8704](https://github.com/NVIDIA/spark-rapids/pull/8704)|Add tests for column names with dots|
|[#8703](https://github.com/NVIDIA/spark-rapids/pull/8703)|Comment out min/max agg test for nested structs to unblock CI|
|[#8698](https://github.com/NVIDIA/spark-rapids/pull/8698)|Cache last ORC stripe footer to avoid redundant remote reads|
|[#8687](https://github.com/NVIDIA/spark-rapids/pull/8687)|Handle TIMESTAMP_MILLIS for rebase check|
|[#8688](https://github.com/NVIDIA/spark-rapids/pull/8688)|Enable the 340 shim test|
|[#8656](https://github.com/NVIDIA/spark-rapids/pull/8656)|Return result from filecache message instead of null|
|[#8659](https://github.com/NVIDIA/spark-rapids/pull/8659)|Filter out nulls for build batches when needed in hash joins|
|[#8682](https://github.com/NVIDIA/spark-rapids/pull/8682)|[DOC] Update CUDA requirements in documentation and Dockerfiles[skip ci]|
|[#8637](https://github.com/NVIDIA/spark-rapids/pull/8637)|Support Float order-by columns for RANGE window functions|
|[#8681](https://github.com/NVIDIA/spark-rapids/pull/8681)|changed container name to adapt to blossom-lib refactor [skip ci]|
|[#8573](https://github.com/NVIDIA/spark-rapids/pull/8573)|Add support for Delta Lake 2.4.0|
|[#8671](https://github.com/NVIDIA/spark-rapids/pull/8671)|Fix use-after-freed bug in `GpuFloatArrayMin`|
|[#8650](https://github.com/NVIDIA/spark-rapids/pull/8650)|Support TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS|
|[#8495](https://github.com/NVIDIA/spark-rapids/pull/8495)|Speed up PCBS CPU read path by not recalculating as much|
|[#8389](https://github.com/NVIDIA/spark-rapids/pull/8389)|Add filecache support for ORC|
|[#8658](https://github.com/NVIDIA/spark-rapids/pull/8658)|Check if need to run Databricks pre-merge|
|[#8649](https://github.com/NVIDIA/spark-rapids/pull/8649)|Add Spark 3.4.1 shim|
|[#8624](https://github.com/NVIDIA/spark-rapids/pull/8624)|Rename numBytesAdded/Removed metrics and add deletion vector metrics in Databricks 12.2 shims|
|[#8645](https://github.com/NVIDIA/spark-rapids/pull/8645)|Fix "PytestUnknownMarkWarning: Unknown pytest.mark.inject_oom" warning|
|[#8608](https://github.com/NVIDIA/spark-rapids/pull/8608)|Matrix stages to dynamically build Databricks shims|
|[#8517](https://github.com/NVIDIA/spark-rapids/pull/8517)|Revert "Disable asserts for non-empty nulls (#8183)"|
|[#8628](https://github.com/NVIDIA/spark-rapids/pull/8628)|Enable Delta Write fallback tests on Databricks 12.2|
|[#8632](https://github.com/NVIDIA/spark-rapids/pull/8632)|Fix GCP examples and getting started guide [skip ci]|
|[#8638](https://github.com/NVIDIA/spark-rapids/pull/8638)|Support nested structs for `min`/`max` aggregations in groupby and reduction|
|[#8639](https://github.com/NVIDIA/spark-rapids/pull/8639)|Add iceberg test for nightly DB12.2 IT pipeline[skip ci]|
|[#8618](https://github.com/NVIDIA/spark-rapids/pull/8618)|Heuristic to speed up partial aggregates that get larger|
|[#8605](https://github.com/NVIDIA/spark-rapids/pull/8605)|[Doc] Fix demo link in index.md [skip ci]|
|[#8619](https://github.com/NVIDIA/spark-rapids/pull/8619)|Enable output batches metric for GpuShuffleCoalesceExec by default|
|[#8617](https://github.com/NVIDIA/spark-rapids/pull/8617)|Fixes broadcast spill serialization/deserialization|
|[#8531](https://github.com/NVIDIA/spark-rapids/pull/8531)|filecache: Modify FileCacheLocalityManager.init to pass in Spark context|
|[#8613](https://github.com/NVIDIA/spark-rapids/pull/8613)|Try print JVM core dump files if any test failures in CI|
|[#8616](https://github.com/NVIDIA/spark-rapids/pull/8616)|Wait for futures in multi-threaded writers even on exception|
|[#8578](https://github.com/NVIDIA/spark-rapids/pull/8578)|Add in metric to see how much computation time is lost due to retry|
|[#8590](https://github.com/NVIDIA/spark-rapids/pull/8590)|Drop ".dev0" suffix from Spark SNASHOT distro builds|
|[#8604](https://github.com/NVIDIA/spark-rapids/pull/8604)|Upgrade scalatest version to 3.2.16|
|[#8555](https://github.com/NVIDIA/spark-rapids/pull/8555)|Support `flatten`  SQL function|
|[#8599](https://github.com/NVIDIA/spark-rapids/pull/8599)|Fix broken links in advanced_configs.md|
|[#8589](https://github.com/NVIDIA/spark-rapids/pull/8589)|Revert to the JVM-based Spark version extraction in pytests|
|[#8582](https://github.com/NVIDIA/spark-rapids/pull/8582)|Fix databricks shims build errors caused by DB updates|
|[#8564](https://github.com/NVIDIA/spark-rapids/pull/8564)|Fold `verify-all-modules-with-headSparkVersion` into `verify-all-modules` [skip ci]|
|[#8553](https://github.com/NVIDIA/spark-rapids/pull/8553)|Handle empty batch in ParquetCachedBatchSerializer|
|[#8575](https://github.com/NVIDIA/spark-rapids/pull/8575)|Corrected typos in CONTRIBUTING.md [skip ci]|
|[#8574](https://github.com/NVIDIA/spark-rapids/pull/8574)|Remove maxTaskFailures=4 for pre-3.1.1 Spark|
|[#8503](https://github.com/NVIDIA/spark-rapids/pull/8503)|Remove hard-coded version numbers for dependencies when building on|
|[#8544](https://github.com/NVIDIA/spark-rapids/pull/8544)|Fix auto merge conflict 8543 [skip ci]|
|[#8521](https://github.com/NVIDIA/spark-rapids/pull/8521)|List supported Spark versions when no shim found|
|[#8520](https://github.com/NVIDIA/spark-rapids/pull/8520)|Add support for first, last, nth, and collect_list aggregations for BinaryType|
|[#8509](https://github.com/NVIDIA/spark-rapids/pull/8509)|Remove legacy spark version check|
|[#8494](https://github.com/NVIDIA/spark-rapids/pull/8494)|Fix 23.08 build on Databricks 12.2|
|[#8487](https://github.com/NVIDIA/spark-rapids/pull/8487)|Move MockTaskContext to tests project|
|[#8426](https://github.com/NVIDIA/spark-rapids/pull/8426)|Pre-merge CI to support Databricks 12.2|
|[#8282](https://github.com/NVIDIA/spark-rapids/pull/8282)|Databricks 12.2 Support|
|[#8407](https://github.com/NVIDIA/spark-rapids/pull/8407)|Bump up dep version to 23.08.0-SNAPSHOT|
|[#8359](https://github.com/NVIDIA/spark-rapids/pull/8359)|Init version 23.08.0-SNAPSHOT|

## Release 23.06

### Features
|||
|:---|:---|
|[#6201](https://github.com/NVIDIA/spark-rapids/issues/6201)|[FEA] experiment with memoizing datagens in the integration_tests|
|[#8079](https://github.com/NVIDIA/spark-rapids/issues/8079)|[FEA] Release Spark 3.4 Support|
|[#7043](https://github.com/NVIDIA/spark-rapids/issues/7043)|[FEA] Support Empty2Null expression on Spark 3.4.0|
|[#8222](https://github.com/NVIDIA/spark-rapids/issues/8222)|[FEA] String Split Unsupported escaped character '.'|
|[#8211](https://github.com/NVIDIA/spark-rapids/issues/8211)|[FEA] Add tencent blob store uri to spark rapids cloudScheme defaults|
|[#4103](https://github.com/NVIDIA/spark-rapids/issues/4103)|[FEA] jdk17 support|
|[#7094](https://github.com/NVIDIA/spark-rapids/issues/7094)|[FEA] Add a shim layer for Spark 3.2.4|
|[#6202](https://github.com/NVIDIA/spark-rapids/issues/6202)|[SPARK-39528][SQL] Use V2 Filter in SupportsRuntimeFiltering|
|[#6034](https://github.com/NVIDIA/spark-rapids/issues/6034)|[FEA] Support `offset` parameter in `TakeOrderedAndProject`|
|[#8196](https://github.com/NVIDIA/spark-rapids/issues/8196)|[FEA] Add retry handling to GpuGenerateExec.fixedLenLazyArrayGenerate path|
|[#7891](https://github.com/NVIDIA/spark-rapids/issues/7891)|[FEA] Support StddevSamp with cast(col as double)  for input|
|[#62](https://github.com/NVIDIA/spark-rapids/issues/62)|[FEA] stddevsamp function|
|[#7867](https://github.com/NVIDIA/spark-rapids/issues/7867)|[FEA] support json to struct function|
|[#7883](https://github.com/NVIDIA/spark-rapids/issues/7883)|[FEA] support order by string in windowing function|
|[#7882](https://github.com/NVIDIA/spark-rapids/issues/7882)|[FEA] support StringTranslate function|
|[#7843](https://github.com/NVIDIA/spark-rapids/issues/7843)|[FEA] build with CUDA 12|
|[#8045](https://github.com/NVIDIA/spark-rapids/issues/8045)|[FEA] Support repetition in choice on regular expressions|
|[#6882](https://github.com/NVIDIA/spark-rapids/issues/6882)|[FEA] Regular expressions - support line anchors in choice|
|[#7901](https://github.com/NVIDIA/spark-rapids/issues/7901)|[FEA] better rlike function supported|
|[#7784](https://github.com/NVIDIA/spark-rapids/issues/7784)|[FEA] Add Spark 3.3.3-SNAPSHOT to shims|
|[#7260](https://github.com/NVIDIA/spark-rapids/issues/7260)|[FEA] Create a new Expression execution framework|

### Performance
|||
|:---|:---|
|[#7870](https://github.com/NVIDIA/spark-rapids/issues/7870)|[FEA] Turn on spark.rapids.sql.castDecimalToString.enabled by default|
|[#7321](https://github.com/NVIDIA/spark-rapids/issues/7321)|[FEA] Improve performance of small file ORC reads from blobstores|
|[#7672](https://github.com/NVIDIA/spark-rapids/issues/7672)|Make all buffers/columnar batches spillable by default|

### Bugs Fixed
|||
|:---|:---|
|[#6339](https://github.com/NVIDIA/spark-rapids/issues/6339)|[BUG] 0 in some cases for decimal being cast to a string returns different results.|
|[#8522](https://github.com/NVIDIA/spark-rapids/issues/8522)|[BUG] `from_json` function failed testing with input column containing empty or null string|
|[#8483](https://github.com/NVIDIA/spark-rapids/issues/8483)|[BUG] `test_read_compressed_hive_text` fails on CDH|
|[#8330](https://github.com/NVIDIA/spark-rapids/issues/8330)|[BUG] Handle Decimal128 computation with overflow of Remainder on Spark 3.4|
|[#8448](https://github.com/NVIDIA/spark-rapids/issues/8448)|[BUG] GpuRegExpReplaceWithBackref with empty string input produces incorrect result on GPU in Spark 3.1.1|
|[#8323](https://github.com/NVIDIA/spark-rapids/issues/8323)|[BUG] regexp_replace hangs with specific inputs and patterns|
|[#8473](https://github.com/NVIDIA/spark-rapids/issues/8473)|[BUG] Complete aggregation with non-trivial grouping expression fails|
|[#8440](https://github.com/NVIDIA/spark-rapids/issues/8440)|[BUG] the jar with scaladoc overwrites the jar with javadoc |
|[#8469](https://github.com/NVIDIA/spark-rapids/issues/8469)|[BUG] Multi-threaded reader can't be toggled on/off|
|[#8460](https://github.com/NVIDIA/spark-rapids/issues/8460)|[BUG] Compile failure on Databricks 11.3 with GpuHiveTableScanExec.scala|
|[#8114](https://github.com/NVIDIA/spark-rapids/issues/8114)|[BUG] [AUDIT] [SPARK-42478] Make a serializable jobTrackerId instead of a non-serializable JobID in FileWriterFactory|
|[#6786](https://github.com/NVIDIA/spark-rapids/issues/6786)|[BUG] NDS q95 fails with OOM at 10TB|
|[#8419](https://github.com/NVIDIA/spark-rapids/issues/8419)|[BUG] Hive Text reader fails for GZIP compressed input|
|[#8409](https://github.com/NVIDIA/spark-rapids/issues/8409)|[BUG] JVM agent crashed SIGFPE cudf::detail::repeat in integration tests|
|[#8411](https://github.com/NVIDIA/spark-rapids/issues/8411)|[BUG] Close called too many times in Gpu json reader|
|[#8400](https://github.com/NVIDIA/spark-rapids/issues/8400)|[BUG] Cloudera IT test failures - test_timesub_from_subquery|
|[#8240](https://github.com/NVIDIA/spark-rapids/issues/8240)|[BUG] NDS power run hits GPU OOM on Databricks.|
|[#8375](https://github.com/NVIDIA/spark-rapids/issues/8375)|[BUG] test_empty_filter[>] failed in 23.06 nightly|
|[#8363](https://github.com/NVIDIA/spark-rapids/issues/8363)|[BUG] ORC reader NullPointerExecption|
|[#8281](https://github.com/NVIDIA/spark-rapids/issues/8281)|[BUG] ParquetCachedBatchSerializer is crashing on count|
|[#8331](https://github.com/NVIDIA/spark-rapids/issues/8331)|[BUG] Filter on dates with subquery results in ArrayIndexOutOfBoundsException|
|[#8293](https://github.com/NVIDIA/spark-rapids/issues/8293)|[BUG] GpuTimeAdd throws UnsupportedOperationException takes column and interval as an argument only|
|[#8161](https://github.com/NVIDIA/spark-rapids/issues/8161)|Add support for Remainder[DecimalType] for Spark 3.4 and DB 11.3|
|[#8321](https://github.com/NVIDIA/spark-rapids/issues/8321)|[BUG] `test_read_hive_fixed_length_char` integ test fails on Spark 3.4|
|[#8225](https://github.com/NVIDIA/spark-rapids/issues/8225)|[BUG] GpuGetArrayItem only supports ints as the ordinal.|
|[#8294](https://github.com/NVIDIA/spark-rapids/issues/8294)|[BUG] ORC `CHAR(N)` columns written from Hive unreadable with RAPIDS plugin|
|[#8186](https://github.com/NVIDIA/spark-rapids/issues/8186)|[BUG] integration test test_cast_nested can fail with non-empty nulls|
|[#6190](https://github.com/NVIDIA/spark-rapids/issues/6190)|[SPARK-39731][SQL] Fix issue in CSV data sources when parsing dates in "yyyyMMdd" format with CORRECTED time parser policy|
|[#8185](https://github.com/NVIDIA/spark-rapids/issues/8185)|[BUG] Scala Test md5 can produce non-empty nulls (merge and set validity)|
|[#8235](https://github.com/NVIDIA/spark-rapids/issues/8235)|[BUG] Java agent crashed intermittently running integration tests|
|[#7485](https://github.com/NVIDIA/spark-rapids/issues/7485)|[BUG] stop using mergeAndSetValidity for any nested type|
|[#8263](https://github.com/NVIDIA/spark-rapids/issues/8263)|[BUG] Databricks 11.3 - Task failed while writing rows for Delta table -  java.lang.Integer cannot be cast to java.lang.Long|
|[#7898](https://github.com/NVIDIA/spark-rapids/issues/7898)|Override `canonicalized` method to the Expressions|
|[#8254](https://github.com/NVIDIA/spark-rapids/issues/8254)|[BUG] Unable to determine Databricks version in azure Databricks instances|
|[#6967](https://github.com/NVIDIA/spark-rapids/issues/6967)|[BUG] Parquet List corner cases fail to be parsed|
|[#6991](https://github.com/NVIDIA/spark-rapids/issues/6991)|[BUG] Integration test failures in Spark - 3.4 SNAPSHOT build|
|[#7773](https://github.com/NVIDIA/spark-rapids/issues/7773)|[BUG] udf test failed cudf-py 23.04 ENV setup on databricks 11.3 runtime|
|[#7934](https://github.com/NVIDIA/spark-rapids/issues/7934)|[BUG] User app fails with OOM - GpuOutOfCoreSortIterator|
|[#8214](https://github.com/NVIDIA/spark-rapids/issues/8214)|[BUG] Exception when counting rows in an ORC file that has no column names|
|[#8160](https://github.com/NVIDIA/spark-rapids/issues/8160)|[BUG] Arithmetic_ops_test failure for Spark 3.4|
|[#7495](https://github.com/NVIDIA/spark-rapids/issues/7495)|Update GpuDataSource to match the change in Spark 3.4|
|[#8189](https://github.com/NVIDIA/spark-rapids/issues/8189)|[BUG] test_array_element_at_zero_index_fail test failures in Spark 3.4 |
|[#8043](https://github.com/NVIDIA/spark-rapids/issues/8043)|[BUG] Host memory leak in SerializedBatchIterator|
|[#8194](https://github.com/NVIDIA/spark-rapids/issues/8194)|[BUG] JVM agent crash intermittently in CI integration test |
|[#6182](https://github.com/NVIDIA/spark-rapids/issues/6182)|[SPARK-39319][CORE][SQL] Make query contexts as a part of `SparkThrowable`|
|[#7491](https://github.com/NVIDIA/spark-rapids/issues/7491)|[AUDIT][SPARK-41448][SQL] Make consistent MR job IDs in FileBatchWriter and FileFormatWriter|
|[#8149](https://github.com/NVIDIA/spark-rapids/issues/8149)|[BUG] dataproc init script does not fail clearly with newer versions of CUDA|
|[#7624](https://github.com/NVIDIA/spark-rapids/issues/7624)|[BUG] `test_parquet_write_encryption_option_fallback` failed|
|[#8019](https://github.com/NVIDIA/spark-rapids/issues/8019)|[BUG] Spark-3.4 - Integration test failures due to GpuCreateDataSourceTableAsSelectCommand|
|[#8017](https://github.com/NVIDIA/spark-rapids/issues/8017)|[BUG]Spark-3.4 Integration tests failure  due to InsertIntoHadoopFsRelationCommand not running on GPU|
|[#7492](https://github.com/NVIDIA/spark-rapids/issues/7492)|[AUDIT][SPARK-41468][SQL][FOLLOWUP] Handle NamedLambdaVariables in EquivalentExpressions|
|[#6987](https://github.com/NVIDIA/spark-rapids/issues/6987)|[BUG] Unit Test failures in Spark-3.4 SNAPSHOT build|
|[#8171](https://github.com/NVIDIA/spark-rapids/issues/8171)|[BUG] ORC read failure when reading decimals with different precision/scale from write schema|
|[#7216](https://github.com/NVIDIA/spark-rapids/issues/7216)|[BUG] The PCBS tests fail on Spark 340|
|[#8016](https://github.com/NVIDIA/spark-rapids/issues/8016)|[BUG] Spark-3.4 -  Integration tests failure due to missing InsertIntoHiveTable operator in GPU |
|[#8166](https://github.com/NVIDIA/spark-rapids/issues/8166)|Databricks Delta defaults to LEGACY for int96RebaseModeInWrite|
|[#8147](https://github.com/NVIDIA/spark-rapids/issues/8147)|[BUG] test_substring_column failed|
|[#8164](https://github.com/NVIDIA/spark-rapids/issues/8164)|[BUG] failed AnsiCastShim build in datasbricks 11.3 runtime|
|[#7757](https://github.com/NVIDIA/spark-rapids/issues/7757)|[BUG] Unit tests failure in AnsiCastOpSuite on Spark-3.4|
|[#7756](https://github.com/NVIDIA/spark-rapids/issues/7756)|[BUG] Unit test failure in AdaptiveQueryExecSuite on Spark-3.4|
|[#8153](https://github.com/NVIDIA/spark-rapids/issues/8153)|[BUG] `get-shim-versions-from-dist` workflow failing in CI|
|[#7961](https://github.com/NVIDIA/spark-rapids/issues/7961)|[BUG] understand why unspill can throw an OutOfMemoryError and not a RetryOOM|
|[#7755](https://github.com/NVIDIA/spark-rapids/issues/7755)|[BUG] Unit tests  failures in WindowFunctionSuite and CostBasedOptimizerSuite on Spark-3.4|
|[#7752](https://github.com/NVIDIA/spark-rapids/issues/7752)|[BUG] Test in CastOpSuite fails on Spark-3.4|
|[#7754](https://github.com/NVIDIA/spark-rapids/issues/7754)|[BUG]  unit test `nz timestamp` fails on Spark-3.4|
|[#7018](https://github.com/NVIDIA/spark-rapids/issues/7018)|[BUG] The unit test `sorted partitioned write` fails on Spark 3.4|
|[#8015](https://github.com/NVIDIA/spark-rapids/issues/8015)|[BUG] Spark 3.4 - Integration tests failure due to unsupported KnownNullable operator in Window|
|[#7751](https://github.com/NVIDIA/spark-rapids/issues/7751)|[BUG]  Unit test `Write encrypted ORC fallback` fails on Spark-3.4|
|[#8117](https://github.com/NVIDIA/spark-rapids/issues/8117)|[BUG] Compile error in RapidsErrorUtils when building against Spark 3.4.0 release |
|[#5659](https://github.com/NVIDIA/spark-rapids/issues/5659)|[BUG] Minimize false positives when falling back to CPU for end of line/string anchors and newlines|
|[#8012](https://github.com/NVIDIA/spark-rapids/issues/8012)|[BUG] Integration tests failing due to CreateDataSourceTableAsSelectCommand in Spark-3.4|
|[#8061](https://github.com/NVIDIA/spark-rapids/issues/8061)|[BUG] join_test failed in integration tests|
|[#8018](https://github.com/NVIDIA/spark-rapids/issues/8018)|[BUG] Spark-3.4 - Integration test failures in window aggregations for decimal types|
|[#7581](https://github.com/NVIDIA/spark-rapids/issues/7581)|[BUG] INC AFTER CLOSE for ColumnVector during shutdown in the join code|

### PRs
|||
|:---|:---|
|[#7465](https://github.com/NVIDIA/spark-rapids/pull/7465)|Add support for arrays in hashaggregate|
|[#8584](https://github.com/NVIDIA/spark-rapids/pull/8584)|Update 23.06 changelog 6/19 [skip ci]|
|[#8581](https://github.com/NVIDIA/spark-rapids/pull/8581)|Fix 321db 330db shims build errors caused by DB updates|
|[#8570](https://github.com/NVIDIA/spark-rapids/pull/8570)|Update changelog to latest [skip ci]|
|[#8567](https://github.com/NVIDIA/spark-rapids/pull/8567)|Fixed a link in config doc[skip ci]|
|[#8562](https://github.com/NVIDIA/spark-rapids/pull/8562)|Update changelog to latest 230612 [skip ci]|
|[#8560](https://github.com/NVIDIA/spark-rapids/pull/8560)|Fix relative path in config doc [skip ci]|
|[#8557](https://github.com/NVIDIA/spark-rapids/pull/8557)|Disable `JsonToStructs` for input schema other than `Map<String, String>`|
|[#8549](https://github.com/NVIDIA/spark-rapids/pull/8549)|Revert "Handle caching empty batch (#8507)"|
|[#8507](https://github.com/NVIDIA/spark-rapids/pull/8507)|Handle caching empty batch|
|[#8528](https://github.com/NVIDIA/spark-rapids/pull/8528)|Update deps JNI and private version to 23.06.0|
|[#8492](https://github.com/NVIDIA/spark-rapids/pull/8492)|[Doc]update download docs for 2306 version[skip ci]|
|[#8510](https://github.com/NVIDIA/spark-rapids/pull/8510)|[Doc] address getting-started-on-prem document issues [skip ci]|
|[#8537](https://github.com/NVIDIA/spark-rapids/pull/8537)|Add limitation for the UCX shuffle keep_alive workaround [skip ci]|
|[#8526](https://github.com/NVIDIA/spark-rapids/pull/8526)|Fix `from_json` function failure when input contains empty or null strings|
|[#8529](https://github.com/NVIDIA/spark-rapids/pull/8529)|Init changelog 23.06 [skip ci]|
|[#8338](https://github.com/NVIDIA/spark-rapids/pull/8338)|Moved some configs to an advanced config page|
|[#8441](https://github.com/NVIDIA/spark-rapids/pull/8441)|Memoizing DataGens in integration tests|
|[#8516](https://github.com/NVIDIA/spark-rapids/pull/8516)|Avoid calling Table.merge with BinaryType columns|
|[#8515](https://github.com/NVIDIA/spark-rapids/pull/8515)|Fix warning about deprecated parquet config|
|[#8427](https://github.com/NVIDIA/spark-rapids/pull/8427)|[Doc] address Spark RAPIDS NVAIE VDR issues [skip ci]|
|[#8486](https://github.com/NVIDIA/spark-rapids/pull/8486)|Move task completion listener registration to after variables are initialized|
|[#8481](https://github.com/NVIDIA/spark-rapids/pull/8481)|Removed spark.rapids.sql.castDecimalToString.enabled and enabled GPU decimal to string by default|
|[#8485](https://github.com/NVIDIA/spark-rapids/pull/8485)|Disable `test_read_compressed_hive_text` on CDH.|
|[#8488](https://github.com/NVIDIA/spark-rapids/pull/8488)|Adds note on multi-threaded shuffle targetting <= 200 partitions and on TCP keep-alive for UCX [skip ci]|
|[#8414](https://github.com/NVIDIA/spark-rapids/pull/8414)|Add support for computing remainder with Decimal128 operands with more precision on Spark 3.4|
|[#8467](https://github.com/NVIDIA/spark-rapids/pull/8467)|Add retry support to GpuExpandExec|
|[#8433](https://github.com/NVIDIA/spark-rapids/pull/8433)|Add regression test for regexp_replace hanging with some inputs|
|[#8477](https://github.com/NVIDIA/spark-rapids/pull/8477)|Fix input binding of grouping expressions for complete aggregations|
|[#8464](https://github.com/NVIDIA/spark-rapids/pull/8464)|Remove NOP Maven javadoc plugin definition|
|[#8402](https://github.com/NVIDIA/spark-rapids/pull/8402)|Bring back UCX 1.14|
|[#8470](https://github.com/NVIDIA/spark-rapids/pull/8470)|Ensure the MT shuffle reader enables/disables with spark.rapids.shuff…|
|[#8462](https://github.com/NVIDIA/spark-rapids/pull/8462)|Fix compressed Hive text read on|
|[#8458](https://github.com/NVIDIA/spark-rapids/pull/8458)|Add check for negative id when creating new MR job id|
|[#8435](https://github.com/NVIDIA/spark-rapids/pull/8435)|Add in a few more retry improvements|
|[#8437](https://github.com/NVIDIA/spark-rapids/pull/8437)|Implement the bug fix for SPARK-41448 and shim it for Spark 3.2.4 and Spark 3.3.{2,3}|
|[#8420](https://github.com/NVIDIA/spark-rapids/pull/8420)|Fix reads for GZIP compressed Hive Text.|
|[#8445](https://github.com/NVIDIA/spark-rapids/pull/8445)|Document errors/warns in the logs during catalog shutdown [skip ci]|
|[#8438](https://github.com/NVIDIA/spark-rapids/pull/8438)|Revert "skip test_array_repeat_with_count_scalar for now (#8424)"|
|[#8385](https://github.com/NVIDIA/spark-rapids/pull/8385)|Reduce memory usage in GpuFileFormatDataWriter and GpuDynamicPartitionDataConcurrentWriter|
|[#8304](https://github.com/NVIDIA/spark-rapids/pull/8304)|Support combining small files for multi-threaded ORC reads|
|[#8413](https://github.com/NVIDIA/spark-rapids/pull/8413)|Stop double closing in json scan + skip test|
|[#8430](https://github.com/NVIDIA/spark-rapids/pull/8430)|Update docs for spark.rapids.filecache.checkStale default change [skip ci]|
|[#8424](https://github.com/NVIDIA/spark-rapids/pull/8424)|skip test_array_repeat_with_count_scalar to wait for fix #8409|
|[#8405](https://github.com/NVIDIA/spark-rapids/pull/8405)|Change TimeAdd/Sub subquery tests to use min/max|
|[#8408](https://github.com/NVIDIA/spark-rapids/pull/8408)|Document conventional dist jar layout for single-shim deployments [skip ci]|
|[#8394](https://github.com/NVIDIA/spark-rapids/pull/8394)|Removed "peak device memory" metric|
|[#8378](https://github.com/NVIDIA/spark-rapids/pull/8378)|Use spillable batch with retry in GpuCachedDoublePassWindowIterator|
|[#8392](https://github.com/NVIDIA/spark-rapids/pull/8392)|Update IDEA dev instructions [skip ci]|
|[#8387](https://github.com/NVIDIA/spark-rapids/pull/8387)|Rename inconsinstent profiles in api_validation|
|[#8374](https://github.com/NVIDIA/spark-rapids/pull/8374)|Avoid processing empty batch in ParquetCachedBatchSerializer|
|[#8386](https://github.com/NVIDIA/spark-rapids/pull/8386)|Fix check to do positional indexing in ORC|
|[#8360](https://github.com/NVIDIA/spark-rapids/pull/8360)|use matrix to combine multiple jdk* jobs in maven-verify CI [skip ci]|
|[#8371](https://github.com/NVIDIA/spark-rapids/pull/8371)|Fix V1 column name match is case-sensitive when dropping partition by columns|
|[#8368](https://github.com/NVIDIA/spark-rapids/pull/8368)|Doc Update: Clarify both line anchors ^ and $ for regular expression compatibility [skip ci]|
|[#8377](https://github.com/NVIDIA/spark-rapids/pull/8377)|Avoid a possible race in test_empty_filter|
|[#8354](https://github.com/NVIDIA/spark-rapids/pull/8354)|[DOCS] Updating tools docs in spark-rapids [skip ci]|
|[#8341](https://github.com/NVIDIA/spark-rapids/pull/8341)|Enable CachedBatchWriterSuite.testCompressColBatch|
|[#8264](https://github.com/NVIDIA/spark-rapids/pull/8264)|Make tables spillable by default|
|[#8364](https://github.com/NVIDIA/spark-rapids/pull/8364)|Fix NullPointerException in ORC multithreaded reader where we access context that could be null|
|[#8322](https://github.com/NVIDIA/spark-rapids/pull/8322)|Avoid out of bounds on GpuInMemoryTableScan when reading no columns|
|[#8342](https://github.com/NVIDIA/spark-rapids/pull/8342)|Elimnate javac warnings|
|[#8334](https://github.com/NVIDIA/spark-rapids/pull/8334)|Add in support for filter on empty batch|
|[#8355](https://github.com/NVIDIA/spark-rapids/pull/8355)|Speed up github verify checks [skip ci]|
|[#8356](https://github.com/NVIDIA/spark-rapids/pull/8356)|Enable auto-merge from branch-23.06 to branch-23.08 [skip ci]|
|[#8339](https://github.com/NVIDIA/spark-rapids/pull/8339)|Fix withResource order in GpuGenerateExec|
|[#8340](https://github.com/NVIDIA/spark-rapids/pull/8340)|Stop calling contiguousSplit without splits from GpuSortExec|
|[#8333](https://github.com/NVIDIA/spark-rapids/pull/8333)|Fix GpuTimeAdd handling both input expressions being GpuScalar|
|[#8302](https://github.com/NVIDIA/spark-rapids/pull/8302)|Add support for DecimalType in Remainder for Spark 3.4 and DB 11.3|
|[#8325](https://github.com/NVIDIA/spark-rapids/pull/8325)|Disable `test_read_hive_fixed_length_char` on Spark 3.4+.|
|[#8327](https://github.com/NVIDIA/spark-rapids/pull/8327)|Enable spark.sql.legacy.parquet.nanosAsLong for Spark 3.2.4|
|[#8328](https://github.com/NVIDIA/spark-rapids/pull/8328)|Fix Hive text file write to deal with CUDF changes|
|[#8309](https://github.com/NVIDIA/spark-rapids/pull/8309)|Fix GpuTopN with offset for multiple batches|
|[#8306](https://github.com/NVIDIA/spark-rapids/pull/8306)|Update code to deal with new retry semantics|
|[#8307](https://github.com/NVIDIA/spark-rapids/pull/8307)|Full ordinal support in GetArrayItem|
|[#8243](https://github.com/NVIDIA/spark-rapids/pull/8243)|Enable retry for Parquet writes|
|[#8295](https://github.com/NVIDIA/spark-rapids/pull/8295)|Fix ORC reader for `CHAR(N)` columns written from Hive|
|[#8298](https://github.com/NVIDIA/spark-rapids/pull/8298)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#8276](https://github.com/NVIDIA/spark-rapids/pull/8276)|Fallback to CPU for `enableDateTimeParsingFallback` configuration|
|[#8296](https://github.com/NVIDIA/spark-rapids/pull/8296)|Fix Multithreaded Readers working with Unity Catalog on Databricks|
|[#8273](https://github.com/NVIDIA/spark-rapids/pull/8273)|Add support for escaped dot in character class in regexp parser|
|[#8266](https://github.com/NVIDIA/spark-rapids/pull/8266)|Add test to confirm correct behavior for decimal average in Spark 3.4|
|[#8291](https://github.com/NVIDIA/spark-rapids/pull/8291)|Fix delta stats tracker conf|
|[#8287](https://github.com/NVIDIA/spark-rapids/pull/8287)|Fix Delta write stats if data schema is missing columns relative to table schema|
|[#8286](https://github.com/NVIDIA/spark-rapids/pull/8286)|Add Tencent cosn:// to default cloud schemes|
|[#8283](https://github.com/NVIDIA/spark-rapids/pull/8283)|Add split and retry support for filter|
|[#8290](https://github.com/NVIDIA/spark-rapids/pull/8290)|Pre-merge docker build stage to support containerd runtime [skip ci]|
|[#8257](https://github.com/NVIDIA/spark-rapids/pull/8257)|Support cuda12 jar's release [skip CI]|
|[#8274](https://github.com/NVIDIA/spark-rapids/pull/8274)|Add a unit test for reordered canonicalized expressions in BinaryComparison|
|[#8265](https://github.com/NVIDIA/spark-rapids/pull/8265)|Small code cleanup for pattern matching on Decimal type|
|[#8255](https://github.com/NVIDIA/spark-rapids/pull/8255)|Enable locals,patvars,privates unused Scalac checks|
|[#8234](https://github.com/NVIDIA/spark-rapids/pull/8234)|JDK17 build support in CI|
|[#8256](https://github.com/NVIDIA/spark-rapids/pull/8256)|Use env var with version files as fallback for IT DBR version|
|[#8239](https://github.com/NVIDIA/spark-rapids/pull/8239)|Add Spark 3.2.4 shim|
|[#8221](https://github.com/NVIDIA/spark-rapids/pull/8221)|[Doc] update getting started guide based on latest databricks env [skip ci]|
|[#8224](https://github.com/NVIDIA/spark-rapids/pull/8224)|Fix misinterpretation of Parquet's legacy ARRAY schemas.|
|[#8241](https://github.com/NVIDIA/spark-rapids/pull/8241)|Update to filecache API changes|
|[#8244](https://github.com/NVIDIA/spark-rapids/pull/8244)|Remove semicolon at the end of the package statement in Scala files|
|[#8245](https://github.com/NVIDIA/spark-rapids/pull/8245)|Remove redundant open of ORC file|
|[#8252](https://github.com/NVIDIA/spark-rapids/pull/8252)|Fix auto merge conflict 8250 [skip ci]|
|[#8170](https://github.com/NVIDIA/spark-rapids/pull/8170)|Update GpuRunningWindowExec to use OOM retry framework|
|[#8218](https://github.com/NVIDIA/spark-rapids/pull/8218)|Update to add 340 build and unit test in premerge and in JDK 11 build|
|[#8232](https://github.com/NVIDIA/spark-rapids/pull/8232)|Add integration tests for inferred schema|
|[#8223](https://github.com/NVIDIA/spark-rapids/pull/8223)|Use SupportsRuntimeV2Filtering in Spark 3.4.0|
|[#8233](https://github.com/NVIDIA/spark-rapids/pull/8233)|cudf-udf integration test against python3.9 [skip ci]|
|[#8226](https://github.com/NVIDIA/spark-rapids/pull/8226)|Offset support for TakeOrderedAndProject|
|[#8237](https://github.com/NVIDIA/spark-rapids/pull/8237)|Use weak keys in executor broadcast plan cache|
|[#8229](https://github.com/NVIDIA/spark-rapids/pull/8229)|Upgrade to jacoco 0.8.8 for JDK 17 support|
|[#8216](https://github.com/NVIDIA/spark-rapids/pull/8216)|Add oom retry handling for GpuGenerate.fixedLenLazyArrayGenerate|
|[#8191](https://github.com/NVIDIA/spark-rapids/pull/8191)|Add in retry-work to GPU OutOfCore Sort|
|[#8228](https://github.com/NVIDIA/spark-rapids/pull/8228)|Partial JDK 17 support|
|[#8227](https://github.com/NVIDIA/spark-rapids/pull/8227)|Adjust defaults for better performance out of the box|
|[#8212](https://github.com/NVIDIA/spark-rapids/pull/8212)|Add file caching|
|[#8179](https://github.com/NVIDIA/spark-rapids/pull/8179)|Fall back to CPU for try_cast in Spark 3.4.0|
|[#8220](https://github.com/NVIDIA/spark-rapids/pull/8220)|Batch install-file executions in a single JVM|
|[#8215](https://github.com/NVIDIA/spark-rapids/pull/8215)|Fix count from ORC files with no column names|
|[#8192](https://github.com/NVIDIA/spark-rapids/pull/8192)|Handle PySparkException in case of literal expressions|
|[#8190](https://github.com/NVIDIA/spark-rapids/pull/8190)|Fix element_at_index_zero integration test by using newer error message from Spark 3.4.0|
|[#8203](https://github.com/NVIDIA/spark-rapids/pull/8203)|Clean up queued batches on task failures in RapidsShuffleThreadedBlockIterator|
|[#8207](https://github.com/NVIDIA/spark-rapids/pull/8207)|Support `std` aggregation in reduction|
|[#8174](https://github.com/NVIDIA/spark-rapids/pull/8174)|[FEA] support json to struct function |
|[#8195](https://github.com/NVIDIA/spark-rapids/pull/8195)|Bump mockito to 3.12.4|
|[#8193](https://github.com/NVIDIA/spark-rapids/pull/8193)|Increase databricks cluster autotermination to 6.5 hours [skip ci]|
|[#8182](https://github.com/NVIDIA/spark-rapids/pull/8182)|Support STRING order-by columns for RANGE window functions|
|[#8167](https://github.com/NVIDIA/spark-rapids/pull/8167)|Add oom retry handling to GpuGenerateExec.doGenerate path|
|[#8183](https://github.com/NVIDIA/spark-rapids/pull/8183)|Disable asserts for non-empty nulls|
|[#8177](https://github.com/NVIDIA/spark-rapids/pull/8177)|Fix 340 shim of GpuCreateDataSourceTableAsSelectCommand and shim GpuDataSource for 3.4.0|
|[#8159](https://github.com/NVIDIA/spark-rapids/pull/8159)|Verify CPU fallback class when creating HIVE table [Databricks]|
|[#8180](https://github.com/NVIDIA/spark-rapids/pull/8180)|Follow-up for ORC Decimal read failure (#8172)|
|[#8172](https://github.com/NVIDIA/spark-rapids/pull/8172)|Fix ORC decimal read when precision/scale changes|
|[#7227](https://github.com/NVIDIA/spark-rapids/pull/7227)|Fix PCBS integration tests for Spark-3.4|
|[#8175](https://github.com/NVIDIA/spark-rapids/pull/8175)|Restore test_substring_column|
|[#8162](https://github.com/NVIDIA/spark-rapids/pull/8162)|Support Java 17 for packaging|
|[#8169](https://github.com/NVIDIA/spark-rapids/pull/8169)|Fix AnsiCastShim for 330db|
|[#8168](https://github.com/NVIDIA/spark-rapids/pull/8168)|[DOC] Updating profiling/qualification docs for usability improvements [skip ci]|
|[#8144](https://github.com/NVIDIA/spark-rapids/pull/8144)|Add 340 shim for GpuInsertIntoHiveTable|
|[#8143](https://github.com/NVIDIA/spark-rapids/pull/8143)|Add handling for SplitAndRetryOOM in nextCbFromGatherer|
|[#8102](https://github.com/NVIDIA/spark-rapids/pull/8102)|Rewrite two tests from AnsiCastOpSuite in Python and make compatible with Spark 3.4.0|
|[#8152](https://github.com/NVIDIA/spark-rapids/pull/8152)|Fix Spark-3.4 test failure in AdaptiveQueryExecSuite|
|[#8154](https://github.com/NVIDIA/spark-rapids/pull/8154)|Use repo1.maven.org/maven2 instead of default apache central url |
|[#8150](https://github.com/NVIDIA/spark-rapids/pull/8150)|xfail test_substring_column|
|[#8128](https://github.com/NVIDIA/spark-rapids/pull/8128)|Fix CastOpSuite failures with Spark 3.4|
|[#8145](https://github.com/NVIDIA/spark-rapids/pull/8145)|Fix nz timestamp unit tests|
|[#8146](https://github.com/NVIDIA/spark-rapids/pull/8146)|Set version of slf4j for Spark 3.4.0|
|[#8058](https://github.com/NVIDIA/spark-rapids/pull/8058)|Add retry to BatchByKeyIterator|
|[#8142](https://github.com/NVIDIA/spark-rapids/pull/8142)|Enable ParquetWriterSuite test 'sorted partitioned write' on Spark 3.4.0|
|[#8035](https://github.com/NVIDIA/spark-rapids/pull/8035)|[FEA] support StringTranslate function|
|[#8136](https://github.com/NVIDIA/spark-rapids/pull/8136)|Add GPU support for KnownNullable expression (Spark 3.4.0)|
|[#8096](https://github.com/NVIDIA/spark-rapids/pull/8096)|Add OOM retry handling for existence joins|
|[#8139](https://github.com/NVIDIA/spark-rapids/pull/8139)|Fix auto merge conflict 8138 [skip ci]|
|[#8135](https://github.com/NVIDIA/spark-rapids/pull/8135)|Fix Orc writer test failure with Spark 3.4|
|[#8129](https://github.com/NVIDIA/spark-rapids/pull/8129)|Fix compile error with Spark 3.4.0 release and bump to use 3.4.0 release JAR|
|[#8093](https://github.com/NVIDIA/spark-rapids/pull/8093)|Add cuda12 build support [skip ci]|
|[#8108](https://github.com/NVIDIA/spark-rapids/pull/8108)|Make Arm methods static|
|[#8060](https://github.com/NVIDIA/spark-rapids/pull/8060)|Support repetitions in regexp choice expressions|
|[#8081](https://github.com/NVIDIA/spark-rapids/pull/8081)|Re-enable empty repetition near end-of-line anchor for rlike, regexp_extract and regexp_replace|
|[#8075](https://github.com/NVIDIA/spark-rapids/pull/8075)|Update some integration tests so that they are compatible with Spark 3.4.0|
|[#8063](https://github.com/NVIDIA/spark-rapids/pull/8063)|Update docker to support integration tests against JDK17 [skip ci]|
|[#8047](https://github.com/NVIDIA/spark-rapids/pull/8047)|Enable line/string anchors in choice|
|[#7996](https://github.com/NVIDIA/spark-rapids/pull/7996)|Sub-partitioning supports repartitioning the input data multiple times|
|[#8009](https://github.com/NVIDIA/spark-rapids/pull/8009)|Add in some more retry blocks|
|[#8051](https://github.com/NVIDIA/spark-rapids/pull/8051)|MINOR: Improve assertion error in assert_py4j_exception|
|[#8020](https://github.com/NVIDIA/spark-rapids/pull/8020)|[FEA] Add Spark 3.3.3-SNAPSHOT to shims|
|[#8034](https://github.com/NVIDIA/spark-rapids/pull/8034)|Fix the check for dedicated per-shim files [skip ci]|
|[#7978](https://github.com/NVIDIA/spark-rapids/pull/7978)|Update JNI and private deps version to 23.06.0-SNAPSHOT|
|[#7965](https://github.com/NVIDIA/spark-rapids/pull/7965)|Remove stale references to the pre-shimplify dirs|
|[#7948](https://github.com/NVIDIA/spark-rapids/pull/7948)|Init plugin version 23.06.0-SNAPSHOT|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
