# Change log
Generated on 2025-08-11

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
|[#10571](https://github.com/NVIDIA/spark-rapids/issues/10571)|[AUDIT] [SPARK-46366] Implement `Between` expression GpuOverrides|
|[#13007](https://github.com/NVIDIA/spark-rapids/issues/13007)|[FEA] Look into better performance for ANSI mode SUM reduction/group by aggregation|
|[#13035](https://github.com/NVIDIA/spark-rapids/issues/13035)|[FEA] Add Debug Metrics measuring the actual parallelism of multithreaded reader|
|[#12927](https://github.com/NVIDIA/spark-rapids/issues/12927)|[FEA] optimize casting string to date|

### Bugs Fixed
|||
|:---|:---|
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
|[#12848](https://github.com/NVIDIA/spark-rapids/pull/12848)|Fix timezone bug: unsupported timezones for current JVM|
|[#12854](https://github.com/NVIDIA/spark-rapids/pull/12854)|Drop the new time based metric in commitInfo while comparing Delta Logs|
|[#12847](https://github.com/NVIDIA/spark-rapids/pull/12847)|Update spark400 to released version|
|[#12777](https://github.com/NVIDIA/spark-rapids/pull/12777)|Fix auto merge conflict 12774 [skip ci]|
|[#12761](https://github.com/NVIDIA/spark-rapids/pull/12761)|Use cuda12 as default, drop cuda11 support|
|[#12721](https://github.com/NVIDIA/spark-rapids/pull/12721)|Fix auto merge conflict 12710 [skip ci]|

## Release 25.06

### Features
|||
|:---|:---|
|[#12754](https://github.com/NVIDIA/spark-rapids/issues/12754)|[FEA] Introduce gpu delete filter test suite.|
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
|[#12519](https://github.com/NVIDIA/spark-rapids/issues/12519)|[FEA] CICD Ubuntu dockerfiles to support 24.04 LTS|
|[#6840](https://github.com/NVIDIA/spark-rapids/issues/6840)|[FEA] Support timestamp transitions to and from UTC for repeating rules (daylight savings time zones)|
|[#7035](https://github.com/NVIDIA/spark-rapids/issues/7035)|[FEA]Support function sha1|
|[#8511](https://github.com/NVIDIA/spark-rapids/issues/8511)|[FEA] Support conv function|
|[#12368](https://github.com/NVIDIA/spark-rapids/issues/12368)|[FEA] HybridScan: Add a specific SQL Hint to enable HYBRID_SCAN on certain tables|
|[#5221](https://github.com/NVIDIA/spark-rapids/issues/5221)|[FEA] Support function array_distinct|

### Performance
|||
|:---|:---|
|[#12472](https://github.com/NVIDIA/spark-rapids/issues/12472)|[FEA] Zero Config - Centralized Priority Assignment|
|[#12557](https://github.com/NVIDIA/spark-rapids/issues/12557)|[FEA] by default enable skip agg when the inputs of agg cannot reduce|
|[#12457](https://github.com/NVIDIA/spark-rapids/issues/12457)|[FEA] Add a specific physical rule for LocalAggregate|

### Bugs Fixed
|||
|:---|:---|
|[#12527](https://github.com/NVIDIA/spark-rapids/issues/12527)|[BUG][metric] Enhance the metric for GPU write|
|[#12666](https://github.com/NVIDIA/spark-rapids/issues/12666)|[BUG] Kudo serialization error on limit test|
|[#12619](https://github.com/NVIDIA/spark-rapids/issues/12619)|[BUG] delta_lake_test.py::test_delta_read integration test failure|
|[#12839](https://github.com/NVIDIA/spark-rapids/issues/12839)|XFAIL the failing tests and assert fallback in delta_lake_test.py|
|[#12855](https://github.com/NVIDIA/spark-rapids/issues/12855)|[BUG] delta_lake_test.py::test_delta_read_column_mapping fails with delta 3.3.0|
|[#12845](https://github.com/NVIDIA/spark-rapids/issues/12845)|[BUG] [test] Specify timezones in pytest according to JVM version|
|[#12852](https://github.com/NVIDIA/spark-rapids/issues/12852)|Remove Delta-3.3.0 support from the plugin for release 25.06|
|[#12856](https://github.com/NVIDIA/spark-rapids/issues/12856)|[BUG] 400+ iceberg test cases failed DATAGEN_SEED=1748978166, INJECT_OOM with spark356|
|[#12816](https://github.com/NVIDIA/spark-rapids/issues/12816)|[BUG] Iceberg nightly test failed code5 with spark355 "No tests collected"|
|[#11235](https://github.com/NVIDIA/spark-rapids/issues/11235)|[BUG] 229 Unit tests failed in Spark 400 against cuda 11 and scala 2.13|
|[#11556](https://github.com/NVIDIA/spark-rapids/issues/11556)|[BUG] [Spark 4] Exceptions from casting to `DATE`/`TIMESTAMP` do not match the Spark's exceptions, with ANSI enabled|
|[#12808](https://github.com/NVIDIA/spark-rapids/issues/12808)|[BUG] Integration test fails with undefined use_cdf|
|[#12742](https://github.com/NVIDIA/spark-rapids/issues/12742)|[BUG]  `hybrid_parquet_test.py` test fails on Spark 3.3|
|[#12015](https://github.com/NVIDIA/spark-rapids/issues/12015)|[BUG] Spark-4.0: Tests failures in WindowFunctionSuite|
|[#12022](https://github.com/NVIDIA/spark-rapids/issues/12022)|[BUG] Spark-4.0: Tests failures in HashAggregatesSuite|
|[#12013](https://github.com/NVIDIA/spark-rapids/issues/12013)|[BUG] Spark-4.0: Test failure in JoinsSuite|
|[#12009](https://github.com/NVIDIA/spark-rapids/issues/12009)|[BUG] Spark-4.0: Tests failures in CostBasedOptimizerSuite|
|[#12006](https://github.com/NVIDIA/spark-rapids/issues/12006)|[BUG] Spark-4.0: Tests failures in AdaptiveQueryExecSuite|
|[#12794](https://github.com/NVIDIA/spark-rapids/issues/12794)|[BUG] Iceberg 3.5.2 failed|
|[#12802](https://github.com/NVIDIA/spark-rapids/issues/12802)|[BUG] iceberg integration tests failed on python 3.8.|
|[#12778](https://github.com/NVIDIA/spark-rapids/issues/12778)|[BUG] getjsonobj generate different output as cpu|
|[#12012](https://github.com/NVIDIA/spark-rapids/issues/12012)|[BUG] Spark-4.0: Tests failures in ParquetWriterSuite|
|[#12758](https://github.com/NVIDIA/spark-rapids/issues/12758)|[BUG] dataframe write option for optimizeWrite for delta is ignored on databricks|
|[#12770](https://github.com/NVIDIA/spark-rapids/issues/12770)|[BUG] scala2.13 iceberg nightly test failed ClassNotFoundException: com.nvidia.spark.rapids.iceberg.IcebergProviderImpl|
|[#12004](https://github.com/NVIDIA/spark-rapids/issues/12004)|[BUG] Spark-4.0: Tests failures in StringOperatorsSuite|
|[#12014](https://github.com/NVIDIA/spark-rapids/issues/12014)|[BUG] Spark-4.0: Tests failures in TimeOperatorsSuite|
|[#12723](https://github.com/NVIDIA/spark-rapids/issues/12723)|[BUG] spark.rapids.sql.mode=explainOnly fails without GPU present|
|[#12608](https://github.com/NVIDIA/spark-rapids/issues/12608)|[BUG] Unhandled exceptions in Python worker|
|[#12711](https://github.com/NVIDIA/spark-rapids/issues/12711)|[BUG] Bad StringGen In test_formats_for_legacy_mode_other_formats_tz_rules|
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
|[#12846](https://github.com/NVIDIA/spark-rapids/pull/12846)|[DOC] update the download doc for 2506 release [skip ci]|
|[#12861](https://github.com/NVIDIA/spark-rapids/pull/12861)|Update changelog for the v25.06.0 release [skip ci]|
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

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
