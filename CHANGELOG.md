# Change log
Generated on 2024-08-06

## Release 24.08

### Features
|||
|:---|:---|
|[#9259](https://github.com/NVIDIA/spark-rapids/issues/9259)|[FEA] Create Spark 4.0.0 shim and build env|
|[#11104](https://github.com/NVIDIA/spark-rapids/issues/11104)|[FEA] Adopt UCX 1.17|
|[#10366](https://github.com/NVIDIA/spark-rapids/issues/10366)|[FEA] It would be nice if we could support Hive-style write bucketing table|
|[#10987](https://github.com/NVIDIA/spark-rapids/issues/10987)|[FEA] Implement lore framework to support all operators.|
|[#11087](https://github.com/NVIDIA/spark-rapids/issues/11087)|[FEA] Support regex pattern with brackets when rewrite to PrefixRange patten in rlike|
|[#10889](https://github.com/NVIDIA/spark-rapids/issues/10889)|[Test]Spark UT framework should support exclude a case based on JDK version and Scala version|
|[#22](https://github.com/NVIDIA/spark-rapids/issues/22)|[FEA] Add support for bucketed writes|
|[#9939](https://github.com/NVIDIA/spark-rapids/issues/9939)|[FEA] `GpuInsertIntoHiveTable` supports parquet format|
|[#10867](https://github.com/NVIDIA/spark-rapids/issues/10867)|[FEA] Update branch-24.08 JNI and private dependencies version to 24.08.0-SNAPSHOT|

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
|[#10997](https://github.com/NVIDIA/spark-rapids/pull/10997)|Fix some test issues in Spark UT and keep RapidsTestSettings update-to-date|
|[#11076](https://github.com/NVIDIA/spark-rapids/pull/11076)|Improve the diagnostics for 'conv' fallback explain|
|[#11092](https://github.com/NVIDIA/spark-rapids/pull/11092)|Add GpuBucketingUtils shim to Spark 4.0.0|
|[#11062](https://github.com/NVIDIA/spark-rapids/pull/11062)|fix duplicate counted metrics like op time for GpuCoalesceBatches|
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
|[#10963](https://github.com/NVIDIA/spark-rapids/pull/10963)|Add rapids configs to enable GPU running in Spark UT|
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
|[#10945](https://github.com/NVIDIA/spark-rapids/pull/10945)|Add Support for Multiple Filtering Keys for Subquery Broadcast|
|[#10871](https://github.com/NVIDIA/spark-rapids/pull/10871)|Add classloader diagnostics to initShuffleManager error message|
|[#10933](https://github.com/NVIDIA/spark-rapids/pull/10933)|Fixed Databricks build|

## Release 24.06

### Features
|||
|:---|:---|
|[#10850](https://github.com/NVIDIA/spark-rapids/issues/10850)|[FEA] Refine the test framework introduced in #10745|
|[#6969](https://github.com/NVIDIA/spark-rapids/issues/6969)|[FEA] Support parse_url |
|[#10496](https://github.com/NVIDIA/spark-rapids/issues/10496)|[FEA] Drop support for CentOS7|
|[#10760](https://github.com/NVIDIA/spark-rapids/issues/10760)|[FEA]Support ArrayFilter|
|[#10721](https://github.com/NVIDIA/spark-rapids/issues/10721)|[FEA] Dump the complete set of build-info properties to the Spark eventLog|
|[#10666](https://github.com/NVIDIA/spark-rapids/issues/10666)|[FEA]  Create Spark 3.4.3 shim|

### Performance
|||
|:---|:---|
|[#8963](https://github.com/NVIDIA/spark-rapids/issues/8963)|[FEA] Use custom kernel for parse_url|
|[#10817](https://github.com/NVIDIA/spark-rapids/issues/10817)|[FOLLOW ON] Combining regex parsing in transpiling and regex rewrite in `rlike`|
|[#10821](https://github.com/NVIDIA/spark-rapids/issues/10821)|Rewrite `pattern[A-B]{X,Y}` (a pattern string followed by X to Y chars in range A - B) in `RLIKE` to a custom kernel|

### Bugs Fixed
|||
|:---|:---|
|[#10928](https://github.com/NVIDIA/spark-rapids/issues/10928)|[BUG] 24.06 test_conditional_with_side_effects_case_when test failed on Scala 2.13 with DATAGEN_SEED=1716656294|
|[#10941](https://github.com/NVIDIA/spark-rapids/issues/10941)|[BUG] Failed to build on databricks due to GpuOverrides.scala:4264: not found: type GpuSubqueryBroadcastMeta|
|[#10902](https://github.com/NVIDIA/spark-rapids/issues/10902)|Spark UT failed: SPARK-37360: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ|
|[#10899](https://github.com/NVIDIA/spark-rapids/issues/10899)|[BUG] format_number Spark UT failed because Type conversion is not allowed|
|[#10913](https://github.com/NVIDIA/spark-rapids/issues/10913)|[BUG] rlike with empty pattern failed with 'NoSuchElementException' when enabling regex rewrite|
|[#10774](https://github.com/NVIDIA/spark-rapids/issues/10774)|[BUG] Issues found by Spark UT Framework on RapidsRegexpExpressionsSuite|
|[#10606](https://github.com/NVIDIA/spark-rapids/issues/10606)|[BUG] Update Plugin to use the new `getPartitionedFile` method|
|[#10806](https://github.com/NVIDIA/spark-rapids/issues/10806)|[BUG] orc_write_test.py::test_write_round_trip_corner failed with DATAGEN_SEED=1715517863|
|[#10831](https://github.com/NVIDIA/spark-rapids/issues/10831)|[BUG] Failed to read data from iceberg|
|[#10810](https://github.com/NVIDIA/spark-rapids/issues/10810)|[BUG] NPE when running `ParseUrl` tests in `RapidsStringExpressionsSuite`|
|[#10797](https://github.com/NVIDIA/spark-rapids/issues/10797)|[BUG] udf_test test_single_aggregate_udf, test_group_aggregate_udf and test_group_apply_udf_more_types failed on DB 13.3|
|[#10719](https://github.com/NVIDIA/spark-rapids/issues/10719)|[BUG] test_exact_percentile_groupby FAILED: hash_aggregate_test.py::test_exact_percentile_groupby with DATAGEN seed 1713362217|
|[#10738](https://github.com/NVIDIA/spark-rapids/issues/10738)|[BUG] test_exact_percentile_groupby_partial_fallback_to_cpu failed with DATAGEN_SEED=1713928179|
|[#10768](https://github.com/NVIDIA/spark-rapids/issues/10768)|[DOC] Dead links with tools pages|
|[#10751](https://github.com/NVIDIA/spark-rapids/issues/10751)|[BUG] Cascaded Pandas UDFs not working as expected on Databricks when plugin is enabled|
|[#10318](https://github.com/NVIDIA/spark-rapids/issues/10318)|[BUG] `fs.azure.account.keyInvalid` configuration issue while reading from Unity Catalog Tables on Azure DB|
|[#10722](https://github.com/NVIDIA/spark-rapids/issues/10722)|[BUG] "Could not find any rapids-4-spark jars in classpath" error when debugging UT in IDEA|
|[#10724](https://github.com/NVIDIA/spark-rapids/issues/10724)|[BUG] Failed to convert string with invisible characters to float|
|[#10633](https://github.com/NVIDIA/spark-rapids/issues/10633)|[BUG] ScanJson and JsonToStructs can give almost random errors|
|[#10659](https://github.com/NVIDIA/spark-rapids/issues/10659)|[BUG] from_json ArrayIndexOutOfBoundsException in 24.02|
|[#10656](https://github.com/NVIDIA/spark-rapids/issues/10656)|[BUG] Databricks cache tests failing with host memory OOM|

### PRs
|||
|:---|:---|
|[#11222](https://github.com/NVIDIA/spark-rapids/pull/11222)|Update change log for v24.06.1 release [skip ci]|
|[#11221](https://github.com/NVIDIA/spark-rapids/pull/11221)|Change cudf version back to 24.06.0-SNAPSHOT [skip ci]|
|[#11217](https://github.com/NVIDIA/spark-rapids/pull/11217)|Update latest changelog [skip ci]|
|[#11211](https://github.com/NVIDIA/spark-rapids/pull/11211)|Use fixed seed for test_from_json_struct_decimal|
|[#11203](https://github.com/NVIDIA/spark-rapids/pull/11203)|Update version to 24.06.1-SNAPSHOT|
|[#11205](https://github.com/NVIDIA/spark-rapids/pull/11205)|Update docs for 24.06.1 release [skip ci]|
|[#11056](https://github.com/NVIDIA/spark-rapids/pull/11056)|Update latest changelog [skip ci]|
|[#11052](https://github.com/NVIDIA/spark-rapids/pull/11052)|Add spark343 shim for scala2.13 dist jar|
|[#10981](https://github.com/NVIDIA/spark-rapids/pull/10981)|Update latest changelog [skip ci]|
|[#10984](https://github.com/NVIDIA/spark-rapids/pull/10984)|[DOC] Update docs for 24.06.0 release [skip ci]|
|[#10974](https://github.com/NVIDIA/spark-rapids/pull/10974)|Update rapids JNI and private dependency to 24.06.0|
|[#10830](https://github.com/NVIDIA/spark-rapids/pull/10830)|Use ErrorClass to Throw AnalysisException|
|[#10947](https://github.com/NVIDIA/spark-rapids/pull/10947)|Prevent contains-PrefixRange optimization if not preceded by wildcards|
|[#10934](https://github.com/NVIDIA/spark-rapids/pull/10934)|Revert "Add Support for Multiple Filtering Keys for Subquery Broadcast "|
|[#10870](https://github.com/NVIDIA/spark-rapids/pull/10870)|Add support for self-contained profiling|
|[#10903](https://github.com/NVIDIA/spark-rapids/pull/10903)|Use upper case for LEGACY_TIME_PARSER_POLICY to fix a spark UT|
|[#10900](https://github.com/NVIDIA/spark-rapids/pull/10900)|Fix type convert error in format_number scalar input|
|[#10868](https://github.com/NVIDIA/spark-rapids/pull/10868)|Disable default cuDF pinned pool|
|[#10914](https://github.com/NVIDIA/spark-rapids/pull/10914)|Fix NoSuchElementException when rlike with empty pattern|
|[#10858](https://github.com/NVIDIA/spark-rapids/pull/10858)|Add Support for Multiple Filtering Keys for Subquery Broadcast |
|[#10861](https://github.com/NVIDIA/spark-rapids/pull/10861)|refine ut framework including Part 1 and Part 2|
|[#10872](https://github.com/NVIDIA/spark-rapids/pull/10872)|[DOC] ignore released plugin links to reduce the bother info [skip ci]|
|[#10839](https://github.com/NVIDIA/spark-rapids/pull/10839)|Replace anonymous classes for SortOrder and FIlterExec overrides|
|[#10873](https://github.com/NVIDIA/spark-rapids/pull/10873)|Auto merge PRs to branch-24.08 from branch-24.06 [skip ci]|
|[#10860](https://github.com/NVIDIA/spark-rapids/pull/10860)|[Spark 4.0] Account for `PartitionedFileUtil.getPartitionedFile` signature change.|
|[#10822](https://github.com/NVIDIA/spark-rapids/pull/10822)|Rewrite regex pattern `literal[a-b]{x}` to custom kernel in rlike|
|[#10833](https://github.com/NVIDIA/spark-rapids/pull/10833)|Filter out unused json_path tokens|
|[#10855](https://github.com/NVIDIA/spark-rapids/pull/10855)|Fix auto merge conflict 10845 [[skip ci]]|
|[#10826](https://github.com/NVIDIA/spark-rapids/pull/10826)|Add NVTX ranges to identify Spark stages and tasks|
|[#10836](https://github.com/NVIDIA/spark-rapids/pull/10836)|Catch exceptions when trying to examine Iceberg scan for metadata queries|
|[#10824](https://github.com/NVIDIA/spark-rapids/pull/10824)|Support zstd for GPU shuffle compression|
|[#10828](https://github.com/NVIDIA/spark-rapids/pull/10828)|Added DateTimeUtilsShims [Databricks]|
|[#10829](https://github.com/NVIDIA/spark-rapids/pull/10829)|Fix `Inheritance Shadowing` to add support for Spark 4.0.0|
|[#10811](https://github.com/NVIDIA/spark-rapids/pull/10811)|Fix NPE in GpuParseUrl for null keys.|
|[#10723](https://github.com/NVIDIA/spark-rapids/pull/10723)|Implement chunked ORC reader|
|[#10715](https://github.com/NVIDIA/spark-rapids/pull/10715)|Rewrite some rlike expression to StartsWith/Contains|
|[#10820](https://github.com/NVIDIA/spark-rapids/pull/10820)|workaround #10801 temporally|
|[#10812](https://github.com/NVIDIA/spark-rapids/pull/10812)|Replace ThreadPoolExecutor creation with ThreadUtils API|
|[#10813](https://github.com/NVIDIA/spark-rapids/pull/10813)|Fix the errors for Pandas UDF tests on DB13.3|
|[#10795](https://github.com/NVIDIA/spark-rapids/pull/10795)|Remove fixed seed for exact `percentile` integration tests|
|[#10805](https://github.com/NVIDIA/spark-rapids/pull/10805)|Drop Support for CentOS 7|
|[#10800](https://github.com/NVIDIA/spark-rapids/pull/10800)|Add number normalization test and address followup for getJsonObject|
|[#10796](https://github.com/NVIDIA/spark-rapids/pull/10796)|fixing build break on DBR|
|[#10791](https://github.com/NVIDIA/spark-rapids/pull/10791)|Fix auto merge conflict 10779 [skip ci]|
|[#10636](https://github.com/NVIDIA/spark-rapids/pull/10636)|Update actions version [skip ci]|
|[#10743](https://github.com/NVIDIA/spark-rapids/pull/10743)|initial PR for the framework reusing Vanilla Spark's unit tests|
|[#10767](https://github.com/NVIDIA/spark-rapids/pull/10767)|Add rows-only batches support to RebatchingRoundoffIterator|
|[#10763](https://github.com/NVIDIA/spark-rapids/pull/10763)|Add in the GpuArrayFilter command|
|[#10766](https://github.com/NVIDIA/spark-rapids/pull/10766)|Fix dead links related to tools documentation [skip ci]|
|[#10644](https://github.com/NVIDIA/spark-rapids/pull/10644)|Add logging to Integration test runs in local and local-cluster mode|
|[#10756](https://github.com/NVIDIA/spark-rapids/pull/10756)|Fix Authorization Failure While Reading Tables From Unity Catalog|
|[#10752](https://github.com/NVIDIA/spark-rapids/pull/10752)|Add SparkRapidsBuildInfoEvent to the event log|
|[#10754](https://github.com/NVIDIA/spark-rapids/pull/10754)|Substitute whoami for $USER|
|[#10755](https://github.com/NVIDIA/spark-rapids/pull/10755)|[DOC] Update README for prioritize-commits script [skip ci]|
|[#10728](https://github.com/NVIDIA/spark-rapids/pull/10728)|Let big data gen set nullability recursively|
|[#10740](https://github.com/NVIDIA/spark-rapids/pull/10740)|Use parse_url kernel for PATH parsing|
|[#10734](https://github.com/NVIDIA/spark-rapids/pull/10734)|Add short circuit path for get-json-object when there is separate wildcard path|
|[#10725](https://github.com/NVIDIA/spark-rapids/pull/10725)|Initial definition for Spark 4.0.0 shim|
|[#10635](https://github.com/NVIDIA/spark-rapids/pull/10635)|Use new getJsonObject kernel for json_tuple|
|[#10739](https://github.com/NVIDIA/spark-rapids/pull/10739)|Use fixed seed for some random failed tests|
|[#10720](https://github.com/NVIDIA/spark-rapids/pull/10720)|Add Shims for Spark 3.4.3|
|[#10716](https://github.com/NVIDIA/spark-rapids/pull/10716)|Remove the mixedType config for JSON as it has no downsides any longer|
|[#10733](https://github.com/NVIDIA/spark-rapids/pull/10733)|Fix "Could not find any rapids-4-spark jars in classpath" error when debugging UT in IDEA|
|[#10718](https://github.com/NVIDIA/spark-rapids/pull/10718)|Change parameters for memory limit in Parquet chunked reader|
|[#10292](https://github.com/NVIDIA/spark-rapids/pull/10292)|Upgrade to UCX 1.16.0|
|[#10709](https://github.com/NVIDIA/spark-rapids/pull/10709)|Removing some authorizations for departed users [skip ci]|
|[#10726](https://github.com/NVIDIA/spark-rapids/pull/10726)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#10708](https://github.com/NVIDIA/spark-rapids/pull/10708)|Updated dump tool to verify get_json_object|
|[#10706](https://github.com/NVIDIA/spark-rapids/pull/10706)|Fix auto merge conflict 10704 [skip ci]|
|[#10675](https://github.com/NVIDIA/spark-rapids/pull/10675)|Fix merge conflict with branch-24.04 [skip ci]|
|[#10678](https://github.com/NVIDIA/spark-rapids/pull/10678)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#10662](https://github.com/NVIDIA/spark-rapids/pull/10662)|Audit script - Check commits from shuffle and storage directories [skip ci]|
|[#10655](https://github.com/NVIDIA/spark-rapids/pull/10655)|Update rapids jni/private dependency to 24.06|
|[#10652](https://github.com/NVIDIA/spark-rapids/pull/10652)|Substitute murmurHash32 for spark32BitMurmurHash3|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
