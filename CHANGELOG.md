# Change log
Generated on 2020-11-30

## Release 0.3

### Features
|||
|:---|:---|
|[#1071](https://github.com/NVIDIA/spark-rapids/issues/1071)|[FEA] Databricks 7.3 nightly build and integration testing|
|[#670](https://github.com/NVIDIA/spark-rapids/issues/670)|[FEA] Support NullType|
|[#50](https://github.com/NVIDIA/spark-rapids/issues/50)|[FEA] support `spark.sql.legacy.timeParserPolicy`|
|[#1144](https://github.com/NVIDIA/spark-rapids/issues/1144)|[FEA] Remove Databricks 3.0.0 shim layer|
|[#1096](https://github.com/NVIDIA/spark-rapids/issues/1096)|[FEA] Implement parquet CreateDataSourceTableAsSelectCommand|
|[#688](https://github.com/NVIDIA/spark-rapids/issues/688)|[FEA] udf compiler should be auto-appended to `spark.sql.extensions`|
|[#1038](https://github.com/NVIDIA/spark-rapids/issues/1038)|[FEA] Accelerate the data transfer for plan `WindowInPandasExec`|
|[#502](https://github.com/NVIDIA/spark-rapids/issues/502)|[FEA] Support Databricks 7.3 LTS Runtime|
|[#533](https://github.com/NVIDIA/spark-rapids/issues/533)|[FEA] Improve PTDS performance|
|[#764](https://github.com/NVIDIA/spark-rapids/issues/764)|[FEA] Sanity checks for cudf jar mismatch|
|[#963](https://github.com/NVIDIA/spark-rapids/issues/963)|[FEA] Audit_3.0.1: AQE changes to support Columnar exchanges|
|[#1018](https://github.com/NVIDIA/spark-rapids/issues/1018)|[FEA] Log details related to GPU memory fragmentation on GPU OOM|
|[#619](https://github.com/NVIDIA/spark-rapids/issues/619)|[FEA] log whether libcudf and libcudfjni were built for PTDS|
|[#955](https://github.com/NVIDIA/spark-rapids/issues/955)|[FEA] Audit_3.0.1: Check time stamp overflow bug fix impact on plugin side|
|[#954](https://github.com/NVIDIA/spark-rapids/issues/954)|[FEA] Audit_3.0.1: Check first/last functionality changes from d669dea|
|[#905](https://github.com/NVIDIA/spark-rapids/issues/905)|[FEA] create EMR 3.0.1 shim|
|[#989](https://github.com/NVIDIA/spark-rapids/issues/989)|[FEA] Smoke test for RAPIDS shuffle|
|[#945](https://github.com/NVIDIA/spark-rapids/issues/945)|[FEA] Refactor benchmark code to avoid code duplication|
|[#838](https://github.com/NVIDIA/spark-rapids/issues/838)|[FEA] Support window count for a column|
|[#96](https://github.com/NVIDIA/spark-rapids/issues/96)|[FEA] window integration tests|
|[#864](https://github.com/NVIDIA/spark-rapids/issues/864)|[FEA] config option to enable RMM arena memory resource|
|[#430](https://github.com/NVIDIA/spark-rapids/issues/430)|[FEA] Audit: Parquet Writer support for TIMESTAMP_MILLIS|
|[#818](https://github.com/NVIDIA/spark-rapids/issues/818)|[FEA] Create shim layer for EMR|
|[#608](https://github.com/NVIDIA/spark-rapids/issues/608)|[FEA] Parquet small file optimization improve handle merge schema|

### Performance
|||
|:---|:---|
|[#1118](https://github.com/NVIDIA/spark-rapids/issues/1118)|[FEA] Benchmark runner should set job descriptions for setup jobs versus query jobs|
|[#1027](https://github.com/NVIDIA/spark-rapids/issues/1027)|[FEA] BenchmarkRunner should produce JSON summary file even when queries fail|
|[#901](https://github.com/NVIDIA/spark-rapids/issues/901)|[FEA] Create Docker image(s) for benchmark data generation|
|[#902](https://github.com/NVIDIA/spark-rapids/issues/902)|[FEA] Benchmark CSV to Parquet conversion should have explicit partitioning|
|[#794](https://github.com/NVIDIA/spark-rapids/issues/794)|[DOC] Write benchmarking guide|
|[#896](https://github.com/NVIDIA/spark-rapids/issues/896)|[FEA] Benchmark utility should have option to capture Spark event log|
|[#795](https://github.com/NVIDIA/spark-rapids/issues/795)|[FEA] Make it easier to run TPC-* benchmarks with spark-submit|
|[#849](https://github.com/NVIDIA/spark-rapids/issues/849)|[FEA] Have GpuColumnarBatchSerializer return GpuColumnVectorFromBuffer instances|
|[#784](https://github.com/NVIDIA/spark-rapids/issues/784)|[FEA] Allow Host Spilling to be more dynamic|
|[#627](https://github.com/NVIDIA/spark-rapids/issues/627)|[FEA] Further parquet reading small file improvements|
|[#5](https://github.com/NVIDIA/spark-rapids/issues/5)|[FEA] Support Adaptive Execution|

### Bugs Fixed
|||
|:---|:---|
|[#1184](https://github.com/NVIDIA/spark-rapids/issues/1184)|[BUG] test_window_aggregate_udf_array_from_python fails on databricks 3.0.1|
|[#1151](https://github.com/NVIDIA/spark-rapids/issues/1151)|[BUG]Add databricks 3.0.1 shim layer for GpuWindowInPandasExec.|
|[#1199](https://github.com/NVIDIA/spark-rapids/issues/1199)|[BUG] No data size in Input column in Stages page from Spark UI when using Parquet as file source|
|[#1031](https://github.com/NVIDIA/spark-rapids/issues/1031)|[BUG] dependency info properties file contains error messages|
|[#1149](https://github.com/NVIDIA/spark-rapids/issues/1149)|[BUG] Scaladoc warnings in GpuDataSource|
|[#1185](https://github.com/NVIDIA/spark-rapids/issues/1185)|[BUG] test_hash_multiple_mode_query failing|
|[#724](https://github.com/NVIDIA/spark-rapids/issues/724)|[BUG] PySpark test_broadcast_nested_loop_join_special_case intermittent failure|
|[#1164](https://github.com/NVIDIA/spark-rapids/issues/1164)|[BUG] ansi_cast tests are failing in 3.1.0|
|[#1110](https://github.com/NVIDIA/spark-rapids/issues/1110)|[BUG] Special date "now" has wrong value on GPU|
|[#1139](https://github.com/NVIDIA/spark-rapids/issues/1139)|[BUG] Host columnar to GPU can be very slow|
|[#1094](https://github.com/NVIDIA/spark-rapids/issues/1094)|[BUG] unix_timestamp on GPU returns invalid data for special dates|
|[#1098](https://github.com/NVIDIA/spark-rapids/issues/1098)|[BUG] unix_timestamp on GPU returns invalid data for bad input|
|[#1082](https://github.com/NVIDIA/spark-rapids/issues/1082)|[BUG] string to timestamp conversion fails with split|
|[#1140](https://github.com/NVIDIA/spark-rapids/issues/1140)|[BUG] ConcurrentModificationException error after scala test suite completes|
|[#1073](https://github.com/NVIDIA/spark-rapids/issues/1073)|[BUG] java.lang.RuntimeException: BinaryExpressions must override either eval or nullSafeEval|
|[#975](https://github.com/NVIDIA/spark-rapids/issues/975)|[BUG] BroadcastExchangeExec fails to fall back to CPU on driver node on GCP Dataproc|
|[#773](https://github.com/NVIDIA/spark-rapids/issues/773)|[BUG] Investigate high task deserialization|
|[#1035](https://github.com/NVIDIA/spark-rapids/issues/1035)|[BUG] TPC-DS query 90 with AQE enabled fails with doExecuteBroadcast exception|
|[#825](https://github.com/NVIDIA/spark-rapids/issues/825)|[BUG] test_window_aggs_for_ranges intermittently fails|
|[#1008](https://github.com/NVIDIA/spark-rapids/issues/1008)|[BUG] limit function is producing inconsistent result when type is Byte, Long, Boolean and Timestamp|
|[#996](https://github.com/NVIDIA/spark-rapids/issues/996)|[BUG] TPC-DS benchmark via spark-submit does not provide option to disable appending .dat to path|
|[#1006](https://github.com/NVIDIA/spark-rapids/issues/1006)|[BUG] Spark3.1.0 changed BasicWriteTaskStats breaks BasicColumnarWriteTaskStatsTracker|
|[#985](https://github.com/NVIDIA/spark-rapids/issues/985)|[BUG] missing metric `dataSize`|
|[#881](https://github.com/NVIDIA/spark-rapids/issues/881)|[BUG] cannot disable Sort by itself|
|[#812](https://github.com/NVIDIA/spark-rapids/issues/812)|[BUG] Test failures for 0.2 when run with multiple executors|
|[#925](https://github.com/NVIDIA/spark-rapids/issues/925)|[BUG]Range window-functions with non-timestamp order-by expressions not falling back to CPU|
|[#852](https://github.com/NVIDIA/spark-rapids/issues/852)|[BUG] BenchUtils.compareResults cannot compare partitioned files when ignoreOrdering=false|
|[#868](https://github.com/NVIDIA/spark-rapids/issues/868)|[BUG] Rounding error when casting timestamp to string for timestamps before 1970|
|[#880](https://github.com/NVIDIA/spark-rapids/issues/880)|[BUG] doing a window operation with an orderby for a single constant crashes|
|[#776](https://github.com/NVIDIA/spark-rapids/issues/776)|[BUG] Integration test fails on spark 3.1.0-SNAPSHOT|
|[#874](https://github.com/NVIDIA/spark-rapids/issues/874)|[BUG] `RapidsConf.scala` has some un-consistency for `spark.rapids.sql.format.parquet.multiThreadedRead`|
|[#860](https://github.com/NVIDIA/spark-rapids/issues/860)|[BUG] we need to mark columns from received shuffle buffers as `GpuColumnVectorFromBuffer`|
|[#122](https://github.com/NVIDIA/spark-rapids/issues/122)|[BUG] CSV Timestamp parseing is broken for TS < 1902 and TS > 2038|
|[#810](https://github.com/NVIDIA/spark-rapids/issues/810)|[BUG] UDF Integration tests fail if pandas is not installed|
|[#750](https://github.com/NVIDIA/spark-rapids/issues/750)|[BUG] udf_cudf_test::test_with_column fails with IPC error |
|[#746](https://github.com/NVIDIA/spark-rapids/issues/746)|[BUG] cudf_udf_test.py is flakey|
|[#811](https://github.com/NVIDIA/spark-rapids/issues/811)|[BUG] 0.3 nightly is timing out |
|[#574](https://github.com/NVIDIA/spark-rapids/issues/574)|[BUG] Fix GpuTimeSub for Spark 3.1.0|

### PRs
|||
|:---|:---|
|[#1210](https://github.com/NVIDIA/spark-rapids/pull/1210)|Support auto-merge for branch-0.4 [skip ci]|
|[#1202](https://github.com/NVIDIA/spark-rapids/pull/1202)|Fix a bug with the support for java.lang.StringBuilder.append.|
|[#1213](https://github.com/NVIDIA/spark-rapids/pull/1213)|Skip casting StringType to TimestampType for Spark 310|
|[#1201](https://github.com/NVIDIA/spark-rapids/pull/1201)|Replace only window expressions on databricks.|
|[#1208](https://github.com/NVIDIA/spark-rapids/pull/1208)|[BUG] Fix GHSL2020-239 [skip ci]|
|[#1205](https://github.com/NVIDIA/spark-rapids/pull/1205)|Fix missing input bytes read metric for Parquet|
|[#1206](https://github.com/NVIDIA/spark-rapids/pull/1206)|Update Spark 3.1 shim for ShuffleOrigin shuffle parameter|
|[#1196](https://github.com/NVIDIA/spark-rapids/pull/1196)|Rename ShuffleCoalesceExec to GpuShuffleCoalesceExec|
|[#1191](https://github.com/NVIDIA/spark-rapids/pull/1191)|Skip window array tests for databricks.|
|[#1183](https://github.com/NVIDIA/spark-rapids/pull/1183)|Support for CalendarIntervalType and NullType|
|[#1188](https://github.com/NVIDIA/spark-rapids/pull/1188)|Add in tests for parquet nested pruning support|
|[#1189](https://github.com/NVIDIA/spark-rapids/pull/1189)|Enable NullType for First and Last in 3.0.1+|
|[#1181](https://github.com/NVIDIA/spark-rapids/pull/1181)|Fix resource leaks in unit tests|
|[#1186](https://github.com/NVIDIA/spark-rapids/pull/1186)|Fix compilation and scaladoc warnings|
|[#1187](https://github.com/NVIDIA/spark-rapids/pull/1187)|Updated documentation for distinct count compatibility|
|[#1182](https://github.com/NVIDIA/spark-rapids/pull/1182)|Close buffer catalog on device manager shutdown|
|[#1176](https://github.com/NVIDIA/spark-rapids/pull/1176)|Add in support for null type|
|[#1174](https://github.com/NVIDIA/spark-rapids/pull/1174)|Fix race condition in SerializeConcatHostBuffersDeserializeBatch|
|[#1175](https://github.com/NVIDIA/spark-rapids/pull/1175)|Fix leaks seen in shuffle tests|
|[#1138](https://github.com/NVIDIA/spark-rapids/pull/1138)|[REVIEW] Support decimal type for GpuProjectExec|
|[#1162](https://github.com/NVIDIA/spark-rapids/pull/1162)|Set job descriptions in benchmark runner|
|[#1172](https://github.com/NVIDIA/spark-rapids/pull/1172)|Revert "Fix race condition (#1165)"|
|[#1060](https://github.com/NVIDIA/spark-rapids/pull/1060)|Show partition metrics for custom shuffler reader|
|[#1165](https://github.com/NVIDIA/spark-rapids/pull/1165)|Fix race condition in SerializeConcatHostBuffersDeserializeBatch|
|[#1163](https://github.com/NVIDIA/spark-rapids/pull/1163)|Add in support for GetStructField|
|[#1166](https://github.com/NVIDIA/spark-rapids/pull/1166)|Fix the cast tests for 3.1.0+|
|[#1159](https://github.com/NVIDIA/spark-rapids/pull/1159)|fix bug where 'now' had same value as 'today' for timestamps|
|[#1161](https://github.com/NVIDIA/spark-rapids/pull/1161)|Fix nightly build pipeline failure.|
|[#1160](https://github.com/NVIDIA/spark-rapids/pull/1160)|Fix some performance problems with columnar to columnar conversion|
|[#1105](https://github.com/NVIDIA/spark-rapids/pull/1105)|[REVIEW] Change ColumnViewAccess usage to work with ColumnView|
|[#1148](https://github.com/NVIDIA/spark-rapids/pull/1148)|Add in tests for Maps and extend map support where possible|
|[#1154](https://github.com/NVIDIA/spark-rapids/pull/1154)|Mark test as xfail until we can get a fix in|
|[#1113](https://github.com/NVIDIA/spark-rapids/pull/1113)|Support unix_timestamp on GPU for subset of formats|
|[#1156](https://github.com/NVIDIA/spark-rapids/pull/1156)|Fix warning introduced in iterator suite|
|[#1095](https://github.com/NVIDIA/spark-rapids/pull/1095)|Dependency info|
|[#1145](https://github.com/NVIDIA/spark-rapids/pull/1145)|Remove support for databricks 7.0 runtime - shim spark300db|
|[#1147](https://github.com/NVIDIA/spark-rapids/pull/1147)|Change the assert to require for handling TIMESTAMP_MILLIS in isDateTimeRebaseNeeded |
|[#1132](https://github.com/NVIDIA/spark-rapids/pull/1132)|Add in basic support to read structs from parquet|
|[#1121](https://github.com/NVIDIA/spark-rapids/pull/1121)|Shuffle/better error handling|
|[#1134](https://github.com/NVIDIA/spark-rapids/pull/1134)|Support saveAsTable for writing orc and parquet|
|[#1124](https://github.com/NVIDIA/spark-rapids/pull/1124)|Add shim layers for GpuWindowInPandasExec.|
|[#1131](https://github.com/NVIDIA/spark-rapids/pull/1131)|Add in some basic support for Structs|
|[#1127](https://github.com/NVIDIA/spark-rapids/pull/1127)|Add in basic support for reading lists from parquet|
|[#1129](https://github.com/NVIDIA/spark-rapids/pull/1129)|Fix resource leaks with new shuffle optimization|
|[#1116](https://github.com/NVIDIA/spark-rapids/pull/1116)|Optimize normal shuffle by coalescing smaller batches on host|
|[#1102](https://github.com/NVIDIA/spark-rapids/pull/1102)|Auto-register UDF extention when main plugin is set|
|[#1108](https://github.com/NVIDIA/spark-rapids/pull/1108)|Remove integration test pipelines on NGCC|
|[#1123](https://github.com/NVIDIA/spark-rapids/pull/1123)|Mark Pandas udf over window tests as xfail on databricks until they can be fixed|
|[#1120](https://github.com/NVIDIA/spark-rapids/pull/1120)|Add in support for filtering ArrayType|
|[#1080](https://github.com/NVIDIA/spark-rapids/pull/1080)|Support for CalendarIntervalType and NullType for ParquetCachedSerializer|
|[#994](https://github.com/NVIDIA/spark-rapids/pull/994)|Packs bounce buffers for highly partitioned shuffles|
|[#1112](https://github.com/NVIDIA/spark-rapids/pull/1112)|Remove bad config from pytest setup|
|[#1107](https://github.com/NVIDIA/spark-rapids/pull/1107)|closeOnExcept -> withResources in MetaUtils|
|[#1104](https://github.com/NVIDIA/spark-rapids/pull/1104)|Support lists to/from the GPU|
|[#1106](https://github.com/NVIDIA/spark-rapids/pull/1106)|Improve mechanism for expected exceptions in tests|
|[#1069](https://github.com/NVIDIA/spark-rapids/pull/1069)|Accelerate the data transfer between JVM and Python for the plan 'GpuWindowInPandasExec'|
|[#1099](https://github.com/NVIDIA/spark-rapids/pull/1099)|Update how we deal with type checking|
|[#1077](https://github.com/NVIDIA/spark-rapids/pull/1077)|Improve AQE transitions for shuffle and coalesce batches|
|[#1097](https://github.com/NVIDIA/spark-rapids/pull/1097)|Cleanup some instances of excess closure serialization|
|[#1090](https://github.com/NVIDIA/spark-rapids/pull/1090)|Fix the integration build|
|[#1086](https://github.com/NVIDIA/spark-rapids/pull/1086)|Speed up test performance using pytest-xdist|
|[#1084](https://github.com/NVIDIA/spark-rapids/pull/1084)|Avoid issues where more scalars that expected show up in an expression|
|[#1076](https://github.com/NVIDIA/spark-rapids/pull/1076)|[FEA] Support Databricks 7.3 LTS Runtime|
|[#1083](https://github.com/NVIDIA/spark-rapids/pull/1083)|Revert "Get cudf/spark dependency from the correct .m2 dir"|
|[#1062](https://github.com/NVIDIA/spark-rapids/pull/1062)|Get cudf/spark dependency from the correct .m2 dir|
|[#1078](https://github.com/NVIDIA/spark-rapids/pull/1078)|Another round of fixes for mapping of DataType to DType|
|[#1066](https://github.com/NVIDIA/spark-rapids/pull/1066)|More fixes for conversion to ColumnarBatch|
|[#1029](https://github.com/NVIDIA/spark-rapids/pull/1029)|BenchmarkRunner should produce JSON summary file even when queries fail|
|[#1055](https://github.com/NVIDIA/spark-rapids/pull/1055)|Fix build warnings|
|[#1064](https://github.com/NVIDIA/spark-rapids/pull/1064)|Use array instead of List for from(Table, DataType)|
|[#1057](https://github.com/NVIDIA/spark-rapids/pull/1057)|Fix empty table broadcast requiring a GPU on driver node|
|[#1047](https://github.com/NVIDIA/spark-rapids/pull/1047)|Sanity checks for cudf jar mismatch|
|[#1044](https://github.com/NVIDIA/spark-rapids/pull/1044)|Accelerated row to columnar and columnar to row transitions|
|[#1056](https://github.com/NVIDIA/spark-rapids/pull/1056)|Add query number to Spark app name when running benchmarks|
|[#1054](https://github.com/NVIDIA/spark-rapids/pull/1054)|Log total RMM allocated on GPU OOM|
|[#1053](https://github.com/NVIDIA/spark-rapids/pull/1053)|Remove isGpuBroadcastNestedLoopJoin from shims|
|[#1052](https://github.com/NVIDIA/spark-rapids/pull/1052)|Allow for GPUCoalesceBatch to deal with Map|
|[#1051](https://github.com/NVIDIA/spark-rapids/pull/1051)|Add simple retry for URM dependencies [skip ci]|
|[#1046](https://github.com/NVIDIA/spark-rapids/pull/1046)|Fix broken links|
|[#1017](https://github.com/NVIDIA/spark-rapids/pull/1017)|Log whether PTDS is enabled|
|[#1040](https://github.com/NVIDIA/spark-rapids/pull/1040)|Update to cudf 0.17-SNAPSHOT and fix tests|
|[#1042](https://github.com/NVIDIA/spark-rapids/pull/1042)|Fix inconsistencies in AQE support for broadcast joins|
|[#1037](https://github.com/NVIDIA/spark-rapids/pull/1037)|Add in support for the SQL functions Least and Greatest|
|[#1036](https://github.com/NVIDIA/spark-rapids/pull/1036)|Increase number of retries when waiting for databricks cluster|
|[#1034](https://github.com/NVIDIA/spark-rapids/pull/1034)|[BUG] To honor spark.rapids.memory.gpu.pool=NONE|
|[#854](https://github.com/NVIDIA/spark-rapids/pull/854)|Arbitrary function call in UDF|
|[#1028](https://github.com/NVIDIA/spark-rapids/pull/1028)|Update to cudf-0.16|
|[#1023](https://github.com/NVIDIA/spark-rapids/pull/1023)|Add --gc-between-run flag for TPC* benchmarks.|
|[#1001](https://github.com/NVIDIA/spark-rapids/pull/1001)|ColumnarBatch to CachedBatch and back|
|[#990](https://github.com/NVIDIA/spark-rapids/pull/990)|Parquet coalesce file reader for local filesystems|
|[#1014](https://github.com/NVIDIA/spark-rapids/pull/1014)|Add --append-dat flag for TPC-DS benchmark|
|[#991](https://github.com/NVIDIA/spark-rapids/pull/991)|Updated GCP Dataproc Mortgage-ETL-GPU.ipynb|
|[#886](https://github.com/NVIDIA/spark-rapids/pull/886)|Spark BinaryType and cast to BinaryType|
|[#1016](https://github.com/NVIDIA/spark-rapids/pull/1016)|Change Hash Aggregate to allow pass-through on MapType|
|[#984](https://github.com/NVIDIA/spark-rapids/pull/984)|Add support for MapType in selected operators |
|[#1012](https://github.com/NVIDIA/spark-rapids/pull/1012)|Update for new position parameter in Spark 3.1.0 RegExpReplace|
|[#995](https://github.com/NVIDIA/spark-rapids/pull/995)|Add shim for EMR 3.0.1 and EMR 3.0.1-SNAPSHOT|
|[#998](https://github.com/NVIDIA/spark-rapids/pull/998)|Update benchmark automation script|
|[#1000](https://github.com/NVIDIA/spark-rapids/pull/1000)|Always use RAPIDS shuffle when running TPCH and Mortgage tests|
|[#981](https://github.com/NVIDIA/spark-rapids/pull/981)|Change databricks build to dynamically create a cluster|
|[#986](https://github.com/NVIDIA/spark-rapids/pull/986)|Fix missing dataSize metric when using RAPIDS shuffle|
|[#914](https://github.com/NVIDIA/spark-rapids/pull/914)|Write InternalRow to CachedBatch|
|[#934](https://github.com/NVIDIA/spark-rapids/pull/934)|Iterator to make it easier to work with a window of blocks in the RAPIDS shuffle|
|[#992](https://github.com/NVIDIA/spark-rapids/pull/992)|Skip post-clean if aborted before the image build stage in pre-merge [skip ci]|
|[#988](https://github.com/NVIDIA/spark-rapids/pull/988)|Change in Spark caused the 3.1.0 CI to fail|
|[#983](https://github.com/NVIDIA/spark-rapids/pull/983)|clean jenkins file for premerge on NGCC|
|[#964](https://github.com/NVIDIA/spark-rapids/pull/964)|Refactor TPC benchmarks to reduce duplicate code|
|[#978](https://github.com/NVIDIA/spark-rapids/pull/978)|Enable scalastyle checks for udf-compiler module|
|[#949](https://github.com/NVIDIA/spark-rapids/pull/949)|Fix GpuWindowExec to work with a CPU SortExec|
|[#973](https://github.com/NVIDIA/spark-rapids/pull/973)|Stop reporting totalTime metric for GpuShuffleExchangeExec|
|[#968](https://github.com/NVIDIA/spark-rapids/pull/968)|XFail pos_explode tests until final fix can be put in|
|[#970](https://github.com/NVIDIA/spark-rapids/pull/970)|Add legacy config to clear active Spark 3.1.0 session in tests|
|[#918](https://github.com/NVIDIA/spark-rapids/pull/918)|Benchmark runner script|
|[#915](https://github.com/NVIDIA/spark-rapids/pull/915)|Add option to control number of partitions when converting from CSV to Parquet|
|[#944](https://github.com/NVIDIA/spark-rapids/pull/944)|Fix some issues with non-determinism|
|[#935](https://github.com/NVIDIA/spark-rapids/pull/935)|Add in support/tests for a window count on a column|
|[#940](https://github.com/NVIDIA/spark-rapids/pull/940)|Fix closeOnExcept suppressed exception handling|
|[#942](https://github.com/NVIDIA/spark-rapids/pull/942)|fix github action env setup [skip ci]|
|[#933](https://github.com/NVIDIA/spark-rapids/pull/933)|Update first/last tests to avoid non-determinisim and ordering differences|
|[#931](https://github.com/NVIDIA/spark-rapids/pull/931)|Fix checking for nullable columns in window range query|
|[#924](https://github.com/NVIDIA/spark-rapids/pull/924)|Benchmark guide update for command-line interface / spark-submit|
|[#926](https://github.com/NVIDIA/spark-rapids/pull/926)|Move pandas_udf functions into the tests functions|
|[#929](https://github.com/NVIDIA/spark-rapids/pull/929)|Pick a default tableId to use that is non 0 so that flatbuffers allow…|
|[#928](https://github.com/NVIDIA/spark-rapids/pull/928)|Fix RapidsBufferStore NPE when no spillable buffers are available|
|[#820](https://github.com/NVIDIA/spark-rapids/pull/820)|Benchmarking guide|
|[#859](https://github.com/NVIDIA/spark-rapids/pull/859)|Compare partitioned files in order|
|[#916](https://github.com/NVIDIA/spark-rapids/pull/916)|create new sparkContext explicitly in CPU notebook|
|[#917](https://github.com/NVIDIA/spark-rapids/pull/917)|create new SparkContext in GPU notebook explicitly.|
|[#919](https://github.com/NVIDIA/spark-rapids/pull/919)|Add label benchmark to performance subsection in changelog|
|[#850](https://github.com/NVIDIA/spark-rapids/pull/850)| Add in basic support for lead/lag|
|[#843](https://github.com/NVIDIA/spark-rapids/pull/843)|[REVIEW] Cache plugin to handle reading CachedBatch to an InternalRow|
|[#904](https://github.com/NVIDIA/spark-rapids/pull/904)|Add command-line argument for benchmark result filename|
|[#909](https://github.com/NVIDIA/spark-rapids/pull/909)|GCP preview version image name update|
|[#903](https://github.com/NVIDIA/spark-rapids/pull/903)|update getting-started-gcp.md with new component list|
|[#900](https://github.com/NVIDIA/spark-rapids/pull/900)|Turn off CollectLimitExec replacement by default|
|[#907](https://github.com/NVIDIA/spark-rapids/pull/907)|remove configs from databricks that shouldn't be used by default|
|[#893](https://github.com/NVIDIA/spark-rapids/pull/893)|Fix rounding error when casting timestamp to string for timestamps before 1970|
|[#899](https://github.com/NVIDIA/spark-rapids/pull/899)|Mark reduction corner case tests as xfail on databricks until they can be fixed|
|[#894](https://github.com/NVIDIA/spark-rapids/pull/894)|Replace whole-buffer slicing with direct refcounting|
|[#891](https://github.com/NVIDIA/spark-rapids/pull/891)|Add config to dump heap on GPU OOM|
|[#890](https://github.com/NVIDIA/spark-rapids/pull/890)|Clean up CoalesceBatch to use withResource|
|[#892](https://github.com/NVIDIA/spark-rapids/pull/892)|Only manifest the current batch in cached block shuffle read iterator|
|[#871](https://github.com/NVIDIA/spark-rapids/pull/871)|Add support for using the arena allocator|
|[#889](https://github.com/NVIDIA/spark-rapids/pull/889)|Fix crash on scalar only orderby|
|[#879](https://github.com/NVIDIA/spark-rapids/pull/879)|Update SpillableColumnarBatch to remove buffer from catalog on close|
|[#888](https://github.com/NVIDIA/spark-rapids/pull/888)|Shrink detect scope to compile only [skip ci]|
|[#885](https://github.com/NVIDIA/spark-rapids/pull/885)|[BUG] fix IT dockerfile arguments [skip ci]|
|[#883](https://github.com/NVIDIA/spark-rapids/pull/883)|[BUG] fix IT dockerfile args ordering [skip ci]|
|[#875](https://github.com/NVIDIA/spark-rapids/pull/875)|fix the non-consistency for `spark.rapids.sql.format.parquet.multiThreadedRead` in RapidsConf.scala|
|[#862](https://github.com/NVIDIA/spark-rapids/pull/862)|Migrate nightly&integration pipelines to blossom [skip ci]|
|[#872](https://github.com/NVIDIA/spark-rapids/pull/872)|Ensure that receive-side batches use GpuColumnVectorFromBuffer to avoid|
|[#833](https://github.com/NVIDIA/spark-rapids/pull/833)|Add nvcomp LZ4 codec support|
|[#870](https://github.com/NVIDIA/spark-rapids/pull/870)|Cleaned up tests and documentation for csv timestamp parsing|
|[#823](https://github.com/NVIDIA/spark-rapids/pull/823)|Add command-line interface for TPC-* for use with spark-submit|
|[#856](https://github.com/NVIDIA/spark-rapids/pull/856)|Move GpuWindowInPandasExec in shims layers|
|[#756](https://github.com/NVIDIA/spark-rapids/pull/756)|Add stream-time metric|
|[#832](https://github.com/NVIDIA/spark-rapids/pull/832)|Skip pandas tests if pandas cannot be found|
|[#841](https://github.com/NVIDIA/spark-rapids/pull/841)|Fix a hanging issue when processing empty data.|
|[#840](https://github.com/NVIDIA/spark-rapids/pull/840)|[REVIEW] Fixed failing cache tests|
|[#848](https://github.com/NVIDIA/spark-rapids/pull/848)|Update task memory and disk spill metrics when buffer store spills|
|[#851](https://github.com/NVIDIA/spark-rapids/pull/851)|Use contiguous table when deserializing columnar batch|
|[#857](https://github.com/NVIDIA/spark-rapids/pull/857)|fix pvc scheduling issue|
|[#853](https://github.com/NVIDIA/spark-rapids/pull/853)|Remove nodeAffinity from premerge pipeline|
|[#796](https://github.com/NVIDIA/spark-rapids/pull/796)|Record spark plan SQL metrics to JSON when running benchmarks|
|[#781](https://github.com/NVIDIA/spark-rapids/pull/781)|Add AQE unit tests|
|[#824](https://github.com/NVIDIA/spark-rapids/pull/824)|Skip cudf_udf test by default|
|[#839](https://github.com/NVIDIA/spark-rapids/pull/839)|First/Last reduction and cleanup of agg APIs|
|[#827](https://github.com/NVIDIA/spark-rapids/pull/827)|Add Spark 3.0 EMR Shim layer |
|[#816](https://github.com/NVIDIA/spark-rapids/pull/816)|[BUG] fix nightly is timing out|
|[#782](https://github.com/NVIDIA/spark-rapids/pull/782)|Benchmark utility to perform diff of output from benchmark runs, allowing for precision differences|
|[#813](https://github.com/NVIDIA/spark-rapids/pull/813)|Revert "Enable tests in udf_cudf_test.py"|
|[#788](https://github.com/NVIDIA/spark-rapids/pull/788)|[FEA] Persist workspace data on PVC for premerge|
|[#805](https://github.com/NVIDIA/spark-rapids/pull/805)|[FEA] nightly build trigger both IT on spark 300 and 301|
|[#797](https://github.com/NVIDIA/spark-rapids/pull/797)|Allow host spill store to fit a buffer larger than configured max size|
|[#777](https://github.com/NVIDIA/spark-rapids/pull/777)|Enable tests in udf_cudf_test.py|
|[#790](https://github.com/NVIDIA/spark-rapids/pull/790)|CI: Update cudf python to 0.16 nightly|
|[#772](https://github.com/NVIDIA/spark-rapids/pull/772)|Add support for empty array construction.|
|[#783](https://github.com/NVIDIA/spark-rapids/pull/783)|Improved GpuArrowEvalPythonExec|
|[#771](https://github.com/NVIDIA/spark-rapids/pull/771)|Various improvements to benchmarks|
|[#763](https://github.com/NVIDIA/spark-rapids/pull/763)|[REVIEW] Allow CoalesceBatch to spill data that is not in active use|
|[#727](https://github.com/NVIDIA/spark-rapids/pull/727)|Update cudf dependency to 0.16-SNAPSHOT|
|[#726](https://github.com/NVIDIA/spark-rapids/pull/726)|parquet writer support for TIMESTAMP_MILLIS|
|[#674](https://github.com/NVIDIA/spark-rapids/pull/674)|Unit test for GPU exchange re-use with AQE|
|[#723](https://github.com/NVIDIA/spark-rapids/pull/723)|Update code coverage to find source files in new places|
|[#766](https://github.com/NVIDIA/spark-rapids/pull/766)|Update the integration Dockerfile to reduce the image size|
|[#762](https://github.com/NVIDIA/spark-rapids/pull/762)|Fixing conflicts in branch-0.3|
|[#738](https://github.com/NVIDIA/spark-rapids/pull/738)|[auto-merge] branch-0.2 to branch-0.3 - resolve conflict|
|[#722](https://github.com/NVIDIA/spark-rapids/pull/722)|Initial code changes to support spilling outside of shuffle|
|[#693](https://github.com/NVIDIA/spark-rapids/pull/693)|Update jenkins files for 0.3|
|[#692](https://github.com/NVIDIA/spark-rapids/pull/692)|Merge shims dependency to spark-3.0.1 into branch-0.3|
|[#690](https://github.com/NVIDIA/spark-rapids/pull/690)|Update the version to 0.3.0-SNAPSHOT|

## Release 0.2

### Features
|||
|:---|:---|
|[#696](https://github.com/NVIDIA/spark-rapids/issues/696)|[FEA] run integration tests against SPARK-3.0.1|
|[#455](https://github.com/NVIDIA/spark-rapids/issues/455)|[FEA] Support UCX shuffle with optimized AQE|
|[#510](https://github.com/NVIDIA/spark-rapids/issues/510)|[FEA] Investigate libcudf features needed to support struct schema pruning during loads|
|[#541](https://github.com/NVIDIA/spark-rapids/issues/541)|[FEA] Scala UDF:Support for null Value operands|
|[#542](https://github.com/NVIDIA/spark-rapids/issues/542)|[FEA] Scala UDF: Support for Date and Time |
|[#499](https://github.com/NVIDIA/spark-rapids/issues/499)|[FEA] disable any kind of warnings about ExecutedCommandExec not being on the GPU|
|[#540](https://github.com/NVIDIA/spark-rapids/issues/540)|[FEA] Scala UDF: Support for String replaceFirst()|
|[#340](https://github.com/NVIDIA/spark-rapids/issues/340)|[FEA] widen the rendered Jekyll pages|
|[#602](https://github.com/NVIDIA/spark-rapids/issues/602)|[FEA] don't release with any -SNAPSHOT dependencies|
|[#579](https://github.com/NVIDIA/spark-rapids/issues/579)|[FEA] Auto-merge between branches|
|[#515](https://github.com/NVIDIA/spark-rapids/issues/515)|[FEA] Write tests for AQE skewed join optimization|
|[#452](https://github.com/NVIDIA/spark-rapids/issues/452)|[FEA] Update HashSortOptimizerSuite to work with AQE|
|[#454](https://github.com/NVIDIA/spark-rapids/issues/454)|[FEA] Update GpuCoalesceBatchesSuite to work with AQE enabled|
|[#354](https://github.com/NVIDIA/spark-rapids/issues/354)|[FEA]Spark 3.1 FileSourceScanExec adds parameter optionalNumCoalescedBuckets|
|[#566](https://github.com/NVIDIA/spark-rapids/issues/566)|[FEA] Add support for StringSplit with an array index.|
|[#524](https://github.com/NVIDIA/spark-rapids/issues/524)|[FEA] Add GPU specific metrics to GpuFileSourceScanExec|
|[#494](https://github.com/NVIDIA/spark-rapids/issues/494)|[FEA] Add some AQE-specific tests to the PySpark test suite|
|[#146](https://github.com/NVIDIA/spark-rapids/issues/146)|[FEA] Python tests should support running with Adaptive Query Execution enabled|
|[#465](https://github.com/NVIDIA/spark-rapids/issues/465)|[FEA] Audit: Update script to audit multiple versions of Spark |
|[#488](https://github.com/NVIDIA/spark-rapids/issues/488)|[FEA] Ability to limit total GPU memory used|
|[#70](https://github.com/NVIDIA/spark-rapids/issues/70)|[FEA] Support StringSplit|
|[#403](https://github.com/NVIDIA/spark-rapids/issues/403)|[FEA] Add in support for GetArrayItem|
|[#493](https://github.com/NVIDIA/spark-rapids/issues/493)|[FEA] Implement shuffle optimization when AQE is enabled|
|[#500](https://github.com/NVIDIA/spark-rapids/issues/500)|[FEA] Add maven profiles for testing with AQE on or off|
|[#471](https://github.com/NVIDIA/spark-rapids/issues/471)|[FEA] create a formal process for updating the github-pages branch|
|[#233](https://github.com/NVIDIA/spark-rapids/issues/233)|[FEA] Audit DataWritingCommandExec |
|[#240](https://github.com/NVIDIA/spark-rapids/issues/240)|[FEA] Audit Api validation script follow on - Optimize StringToTypeTag |
|[#388](https://github.com/NVIDIA/spark-rapids/issues/388)|[FEA] Audit WindowExec|
|[#425](https://github.com/NVIDIA/spark-rapids/issues/425)|[FEA] Add tests for configs in BatchScan Readers|
|[#453](https://github.com/NVIDIA/spark-rapids/issues/453)|[FEA] Update HashAggregatesSuite to work with AQE|
|[#184](https://github.com/NVIDIA/spark-rapids/issues/184)|[FEA] Enable NoScalaDoc scalastyle rule|
|[#438](https://github.com/NVIDIA/spark-rapids/issues/438)|[FEA] Enable StringLPad|
|[#232](https://github.com/NVIDIA/spark-rapids/issues/232)|[FEA] Audit SortExec |
|[#236](https://github.com/NVIDIA/spark-rapids/issues/236)|[FEA] Audit ShuffleExchangeExec |
|[#355](https://github.com/NVIDIA/spark-rapids/issues/355)|[FEA] Support Multiple Spark versions in the same jar|
|[#385](https://github.com/NVIDIA/spark-rapids/issues/385)|[FEA] Support RangeExec on the GPU|
|[#317](https://github.com/NVIDIA/spark-rapids/issues/317)|[FEA] Write test wrapper to run SQL queries via pyspark|
|[#235](https://github.com/NVIDIA/spark-rapids/issues/235)|[FEA] Audit BroadcastExchangeExec|
|[#234](https://github.com/NVIDIA/spark-rapids/issues/234)|[FEA] Audit BatchScanExec|
|[#238](https://github.com/NVIDIA/spark-rapids/issues/238)|[FEA] Audit ShuffledHashJoinExec |
|[#237](https://github.com/NVIDIA/spark-rapids/issues/237)|[FEA] Audit BroadcastHashJoinExec |
|[#316](https://github.com/NVIDIA/spark-rapids/issues/316)|[FEA] Add some basic Dataframe tests for CoalesceExec|
|[#145](https://github.com/NVIDIA/spark-rapids/issues/145)|[FEA] Scala tests should support running with Adaptive Query Execution enabled|
|[#231](https://github.com/NVIDIA/spark-rapids/issues/231)|[FEA] Audit ProjectExec |
|[#229](https://github.com/NVIDIA/spark-rapids/issues/229)|[FEA] Audit FileSourceScanExec |

### Performance
|||
|:---|:---|
|[#326](https://github.com/NVIDIA/spark-rapids/issues/326)|[DISCUSS] Shuffle read-side error handling|
|[#601](https://github.com/NVIDIA/spark-rapids/issues/601)|[FEA] Optimize unnecessary sorts when replacing SortAggregate|
|[#333](https://github.com/NVIDIA/spark-rapids/issues/333)|[FEA] Better handling of reading lots of small Parquet files|
|[#511](https://github.com/NVIDIA/spark-rapids/issues/511)|[FEA] Connect shuffle table compression to shuffle exec metrics|
|[#15](https://github.com/NVIDIA/spark-rapids/issues/15)|[FEA] Multiple threads sharing the same GPU|
|[#272](https://github.com/NVIDIA/spark-rapids/issues/272)|[DOC] Getting started guide for UCX shuffle|

### Bugs Fixed
|||
|:---|:---|
|[#780](https://github.com/NVIDIA/spark-rapids/issues/780)|[BUG] Inner Join dropping data with bucketed Table input|
|[#569](https://github.com/NVIDIA/spark-rapids/issues/569)|[BUG] left_semi_join operation is abnormal and serious time-consuming|
|[#744](https://github.com/NVIDIA/spark-rapids/issues/744)|[BUG] TPC-DS query 6 now produces incorrect results.|
|[#718](https://github.com/NVIDIA/spark-rapids/issues/718)|[BUG] GpuBroadcastHashJoinExec ArrayIndexOutOfBoundsException|
|[#698](https://github.com/NVIDIA/spark-rapids/issues/698)|[BUG] batch coalesce can fail to appear between columnar shuffle and subsequent columnar operation|
|[#658](https://github.com/NVIDIA/spark-rapids/issues/658)|[BUG] GpuCoalesceBatches collectTime metric can be underreported|
|[#59](https://github.com/NVIDIA/spark-rapids/issues/59)|[BUG] enable tests for string literals in a select|
|[#486](https://github.com/NVIDIA/spark-rapids/issues/486)|[BUG] GpuWindowExec does not implement requiredChildOrdering|
|[#631](https://github.com/NVIDIA/spark-rapids/issues/631)|[BUG] Rows are dropped when AQE is enabled in some cases|
|[#671](https://github.com/NVIDIA/spark-rapids/issues/671)|[BUG] Databricks hash_aggregate_test fails trying to canonicalize a WrappedAggFunction|
|[#218](https://github.com/NVIDIA/spark-rapids/issues/218)|[BUG] Window function COUNT(x) includes null-values, when it shouldn't|
|[#153](https://github.com/NVIDIA/spark-rapids/issues/153)|[BUG] Incorrect output from partial-only hash aggregates with multiple distincts and non-distinct functions|
|[#656](https://github.com/NVIDIA/spark-rapids/issues/656)|[BUG] integration tests produce hive metadata files|
|[#607](https://github.com/NVIDIA/spark-rapids/issues/607)|[BUG] Fix misleading "cannot run on GPU" warnings when AQE is enabled|
|[#630](https://github.com/NVIDIA/spark-rapids/issues/630)|[BUG] GpuCustomShuffleReader metrics always show zero rows/batches output|
|[#643](https://github.com/NVIDIA/spark-rapids/issues/643)|[BUG] race condition while registering a buffer and spilling at the same time|
|[#606](https://github.com/NVIDIA/spark-rapids/issues/606)|[BUG] Multiple scans for same data source with TPC-DS query59 with delta format|
|[#626](https://github.com/NVIDIA/spark-rapids/issues/626)|[BUG] parquet_test showing leaked memory buffer|
|[#155](https://github.com/NVIDIA/spark-rapids/issues/155)|[BUG] Incorrect output from averages with filters in partial only mode|
|[#277](https://github.com/NVIDIA/spark-rapids/issues/277)|[BUG] HashAggregateSuite failure when AQE is enabled|
|[#276](https://github.com/NVIDIA/spark-rapids/issues/276)|[BUG] GpuCoalesceBatchSuite failure when AQE is enabled|
|[#598](https://github.com/NVIDIA/spark-rapids/issues/598)|[BUG] Non-deterministic output from MapOutputTracker.getStatistics() with AQE on GPU|
|[#192](https://github.com/NVIDIA/spark-rapids/issues/192)|[BUG] test_read_merge_schema fails on Databricks|
|[#341](https://github.com/NVIDIA/spark-rapids/issues/341)|[BUG] Document compression formats for readers/writers|
|[#587](https://github.com/NVIDIA/spark-rapids/issues/587)|[BUG] Spark3.1 changed FileScan which means or GpuScans need to be added to shim layer|
|[#362](https://github.com/NVIDIA/spark-rapids/issues/362)|[BUG] Implement getReaderForRange in the RapidsShuffleManager|
|[#528](https://github.com/NVIDIA/spark-rapids/issues/528)|[BUG] HashAggregateSuite "Avg Distinct with filter" no longer valid when testing against Spark 3.1.0|
|[#416](https://github.com/NVIDIA/spark-rapids/issues/416)|[BUG] Fix Spark 3.1.0 integration tests|
|[#556](https://github.com/NVIDIA/spark-rapids/issues/556)|[BUG] NPE when removing shuffle|
|[#553](https://github.com/NVIDIA/spark-rapids/issues/553)|[BUG] GpuColumnVector build warnings from raw type access|
|[#492](https://github.com/NVIDIA/spark-rapids/issues/492)|[BUG] Re-enable AQE integration tests|
|[#275](https://github.com/NVIDIA/spark-rapids/issues/275)|[BUG] TpchLike query 2 fails when AQE is enabled|
|[#508](https://github.com/NVIDIA/spark-rapids/issues/508)|[BUG] GpuUnion publishes metrics on the UI that are all 0|
|[#269](https://github.com/NVIDIA/spark-rapids/issues/269)|Needed to add `--conf spark.driver.extraClassPath=` |
|[#473](https://github.com/NVIDIA/spark-rapids/issues/473)|[BUG] PartMerge:countDistinct:sum fails sporadically|
|[#531](https://github.com/NVIDIA/spark-rapids/issues/531)|[BUG] Temporary RMM workaround needs to be removed|
|[#532](https://github.com/NVIDIA/spark-rapids/issues/532)|[BUG] NPE when enabling shuffle manager|
|[#525](https://github.com/NVIDIA/spark-rapids/issues/525)|[BUG] GpuFilterExec reports incorrect nullability of output in some cases|
|[#483](https://github.com/NVIDIA/spark-rapids/issues/483)|[BUG] Multiple scans for the same parquet data source|
|[#382](https://github.com/NVIDIA/spark-rapids/issues/382)|[BUG] Spark3.1 StringFallbackSuite regexp_replace null cpu fall back test fails.|
|[#489](https://github.com/NVIDIA/spark-rapids/issues/489)|[FEA] Fix Spark 3.1 GpuHashJoin since it now requires CodegenSupport|
|[#441](https://github.com/NVIDIA/spark-rapids/issues/441)|[BUG] test_broadcast_nested_loop_join_special_case fails on databricks|
|[#347](https://github.com/NVIDIA/spark-rapids/issues/347)|[BUG] Failed to read Parquet file generated by GPU-enabled Spark.|
|[#433](https://github.com/NVIDIA/spark-rapids/issues/433)|`InSet` operator produces an error for Strings|
|[#144](https://github.com/NVIDIA/spark-rapids/issues/144)|[BUG] spark.sql.legacy.parquet.datetimeRebaseModeInWrite is ignored|
|[#323](https://github.com/NVIDIA/spark-rapids/issues/323)|[BUG] GpuBroadcastNestedLoopJoinExec can fail if there are no columns|
|[#356](https://github.com/NVIDIA/spark-rapids/issues/356)|[BUG] Integration cache test for BroadcastNestedLoopJoin failure|
|[#280](https://github.com/NVIDIA/spark-rapids/issues/280)|[BUG] Full Outer Join does not work on nullable keys|
|[#149](https://github.com/NVIDIA/spark-rapids/issues/149)|[BUG] Spark driver fails to load native libs when running on node without CUDA|

### PRs
|||
|:---|:---|
|[#826](https://github.com/NVIDIA/spark-rapids/pull/826)|Fix link to cudf-0.15-cuda11.jar|
|[#815](https://github.com/NVIDIA/spark-rapids/pull/815)|Update documentation for Scala UDFs in 0.2 since you need two things|
|[#802](https://github.com/NVIDIA/spark-rapids/pull/802)|Update 0.2 CHANGELOG|
|[#793](https://github.com/NVIDIA/spark-rapids/pull/793)|Update Jenkins scripts for release|
|[#798](https://github.com/NVIDIA/spark-rapids/pull/798)|Fix shims provider override config not being seen by executors|
|[#785](https://github.com/NVIDIA/spark-rapids/pull/785)|Make shuffle run on CPU if we do a join where we read from bucketed table|
|[#765](https://github.com/NVIDIA/spark-rapids/pull/765)|Add config to override shims provider class|
|[#759](https://github.com/NVIDIA/spark-rapids/pull/759)|Add CHANGELOG for release 0.2|
|[#758](https://github.com/NVIDIA/spark-rapids/pull/758)|Skip the udf test fails periodically.|
|[#752](https://github.com/NVIDIA/spark-rapids/pull/752)|Fix snapshot plugin jar version in docs|
|[#751](https://github.com/NVIDIA/spark-rapids/pull/751)|Correct the channel for cudf installation|
|[#754](https://github.com/NVIDIA/spark-rapids/pull/754)|Filter nulls from joins where possible to improve performance|
|[#732](https://github.com/NVIDIA/spark-rapids/pull/732)|Add a timeout for RapidsShuffleIterator to prevent jobs to hang infin…|
|[#637](https://github.com/NVIDIA/spark-rapids/pull/637)|Documentation changes for 0.2 release |
|[#747](https://github.com/NVIDIA/spark-rapids/pull/747)|Disable udf tests that fail periodically|
|[#745](https://github.com/NVIDIA/spark-rapids/pull/745)|Revert Null Join Filter|
|[#741](https://github.com/NVIDIA/spark-rapids/pull/741)|Fix issue with parquet partitioned reads|
|[#733](https://github.com/NVIDIA/spark-rapids/pull/733)|Remove GPU Types from github|
|[#720](https://github.com/NVIDIA/spark-rapids/pull/720)|Stop removing GpuCoalesceBatches from non-AQE queries when AQE is enabled|
|[#729](https://github.com/NVIDIA/spark-rapids/pull/729)|Fix collect time metric in CoalesceBatches|
|[#640](https://github.com/NVIDIA/spark-rapids/pull/640)|Support running Pandas UDFs on GPUs in Python processes.|
|[#721](https://github.com/NVIDIA/spark-rapids/pull/721)|Add some more checks to databricks build scripts|
|[#714](https://github.com/NVIDIA/spark-rapids/pull/714)|Move spark 3.0.1-shims out of snapshot-shims|
|[#711](https://github.com/NVIDIA/spark-rapids/pull/711)|fix blossom checkout repo|
|[#709](https://github.com/NVIDIA/spark-rapids/pull/709)|[BUG] fix unexpected indentation issue in blossom yml|
|[#642](https://github.com/NVIDIA/spark-rapids/pull/642)|Init workflow for blossom-ci|
|[#705](https://github.com/NVIDIA/spark-rapids/pull/705)|Enable configuration check for cast string to timestamp|
|[#702](https://github.com/NVIDIA/spark-rapids/pull/702)|Update slack channel for Jenkins builds|
|[#701](https://github.com/NVIDIA/spark-rapids/pull/701)|fix checkout-ref for automerge|
|[#695](https://github.com/NVIDIA/spark-rapids/pull/695)|Fix spark-3.0.1 shim to be released|
|[#668](https://github.com/NVIDIA/spark-rapids/pull/668)|refactor automerge to support merge for protected branch|
|[#687](https://github.com/NVIDIA/spark-rapids/pull/687)|Include the UDF compiler in the dist jar|
|[#689](https://github.com/NVIDIA/spark-rapids/pull/689)|Change shims dependency to spark-3.0.1|
|[#677](https://github.com/NVIDIA/spark-rapids/pull/677)|Use multi-threaded parquet read with small files|
|[#638](https://github.com/NVIDIA/spark-rapids/pull/638)|Add Parquet-based cache serializer|
|[#613](https://github.com/NVIDIA/spark-rapids/pull/613)|Enable UCX + AQE|
|[#684](https://github.com/NVIDIA/spark-rapids/pull/684)|Enable test for literal string values in a select|
|[#686](https://github.com/NVIDIA/spark-rapids/pull/686)|Remove sorts when replacing sort aggregate if possible|
|[#675](https://github.com/NVIDIA/spark-rapids/pull/675)|Added TimeAdd|
|[#645](https://github.com/NVIDIA/spark-rapids/pull/645)|[window] Add GpuWindowExec requiredChildOrdering|
|[#676](https://github.com/NVIDIA/spark-rapids/pull/676)|fixUpJoinConsistency rule now works when AQE is enabled|
|[#683](https://github.com/NVIDIA/spark-rapids/pull/683)|Fix issues with cannonicalization of WrappedAggFunction|
|[#682](https://github.com/NVIDIA/spark-rapids/pull/682)|Fix path to start-slave.sh script in docs|
|[#673](https://github.com/NVIDIA/spark-rapids/pull/673)|Increase build timeouts on nightly and premerge builds|
|[#648](https://github.com/NVIDIA/spark-rapids/pull/648)|add signoff-check use github actions|
|[#593](https://github.com/NVIDIA/spark-rapids/pull/593)|Add support for isNaN and datetime related instructions in UDF compiler|
|[#666](https://github.com/NVIDIA/spark-rapids/pull/666)|[window] Disable GPU for COUNT(exp) queries|
|[#655](https://github.com/NVIDIA/spark-rapids/pull/655)|Implement AQE unit test for InsertAdaptiveSparkPlan|
|[#614](https://github.com/NVIDIA/spark-rapids/pull/614)|Fix for aggregation with multiple distinct and non distinct functions|
|[#657](https://github.com/NVIDIA/spark-rapids/pull/657)|Fix verify build after integration tests are run|
|[#660](https://github.com/NVIDIA/spark-rapids/pull/660)|Add in neverReplaceExec and several rules for it|
|[#639](https://github.com/NVIDIA/spark-rapids/pull/639)|BooleanType test shouldn't xfail|
|[#652](https://github.com/NVIDIA/spark-rapids/pull/652)|Mark UVM config as internal until supported|
|[#653](https://github.com/NVIDIA/spark-rapids/pull/653)|Move to the cudf-0.15 release|
|[#647](https://github.com/NVIDIA/spark-rapids/pull/647)|Improve warnings about AQE nodes not supported on GPU|
|[#646](https://github.com/NVIDIA/spark-rapids/pull/646)|Stop reporting zero metrics for GpuCustomShuffleReader|
|[#644](https://github.com/NVIDIA/spark-rapids/pull/644)|Small fix for race in catalog where a buffer could get spilled while …|
|[#623](https://github.com/NVIDIA/spark-rapids/pull/623)|Fix issues with canonicalization|
|[#599](https://github.com/NVIDIA/spark-rapids/pull/599)|[FEA] changelog generator|
|[#563](https://github.com/NVIDIA/spark-rapids/pull/563)|cudf and spark version info in artifacts|
|[#633](https://github.com/NVIDIA/spark-rapids/pull/633)|Fix leak if RebaseHelper throws during Parquet read|
|[#632](https://github.com/NVIDIA/spark-rapids/pull/632)|Copy function isSearchableType from Spark because signature changed in 3.0.1|
|[#583](https://github.com/NVIDIA/spark-rapids/pull/583)|Add udf compiler unit tests|
|[#617](https://github.com/NVIDIA/spark-rapids/pull/617)|Documentation updates for branch 0.2|
|[#616](https://github.com/NVIDIA/spark-rapids/pull/616)|Add config to reserve GPU memory|
|[#612](https://github.com/NVIDIA/spark-rapids/pull/612)|[REVIEW] Fix incorrect output from averages with filters in partial only mode|
|[#609](https://github.com/NVIDIA/spark-rapids/pull/609)|fix minor issues with instructions for building ucx|
|[#611](https://github.com/NVIDIA/spark-rapids/pull/611)|Added in profile to enable shims for SNAPSHOT releases|
|[#595](https://github.com/NVIDIA/spark-rapids/pull/595)|Parquet small file reading optimization|
|[#582](https://github.com/NVIDIA/spark-rapids/pull/582)|fix #579 Auto-merge between branches|
|[#536](https://github.com/NVIDIA/spark-rapids/pull/536)|Add test for skewed join optimization when AQE is enabled|
|[#603](https://github.com/NVIDIA/spark-rapids/pull/603)|Fix data size metric always 0 when using RAPIDS shuffle|
|[#600](https://github.com/NVIDIA/spark-rapids/pull/600)|Fix calculation of string data for compressed batches|
|[#597](https://github.com/NVIDIA/spark-rapids/pull/597)|Remove the xfail for parquet test_read_merge_schema on Databricks|
|[#591](https://github.com/NVIDIA/spark-rapids/pull/591)|Add ucx license in NOTICE-binary|
|[#596](https://github.com/NVIDIA/spark-rapids/pull/596)|Add Spark 3.0.2 to Shim layer|
|[#594](https://github.com/NVIDIA/spark-rapids/pull/594)|Filter nulls from joins where possible to improve performance.|
|[#590](https://github.com/NVIDIA/spark-rapids/pull/590)|Move GpuParquetScan/GpuOrcScan into Shim|
|[#588](https://github.com/NVIDIA/spark-rapids/pull/588)|xfail the tpch spark 3.1.0 tests that fail|
|[#572](https://github.com/NVIDIA/spark-rapids/pull/572)|Update buffer store to return compressed batches directly, add compression NVTX ranges|
|[#558](https://github.com/NVIDIA/spark-rapids/pull/558)|Fix unit tests when AQE is enabled|
|[#580](https://github.com/NVIDIA/spark-rapids/pull/580)|xfail the Spark 3.1.0 integration tests that fail |
|[#565](https://github.com/NVIDIA/spark-rapids/pull/565)|Minor improvements to TPC-DS benchmarking code|
|[#567](https://github.com/NVIDIA/spark-rapids/pull/567)|Explicitly disable AQE in one test|
|[#571](https://github.com/NVIDIA/spark-rapids/pull/571)|Fix Databricks shim layer for GpuFileSourceScanExec and GpuBroadcastExchangeExec|
|[#564](https://github.com/NVIDIA/spark-rapids/pull/564)|Add GPU decode time metric to scans|
|[#562](https://github.com/NVIDIA/spark-rapids/pull/562)|getCatalog can be called from the driver, and can return null|
|[#555](https://github.com/NVIDIA/spark-rapids/pull/555)|Fix build warnings for ColumnViewAccess|
|[#560](https://github.com/NVIDIA/spark-rapids/pull/560)|Fix databricks build for AQE support|
|[#557](https://github.com/NVIDIA/spark-rapids/pull/557)|Fix tests failing on Spark 3.1|
|[#547](https://github.com/NVIDIA/spark-rapids/pull/547)|Add GPU metrics to GpuFileSourceScanExec|
|[#462](https://github.com/NVIDIA/spark-rapids/pull/462)|Implement optimized AQE support so that exchanges run on GPU where possible|
|[#550](https://github.com/NVIDIA/spark-rapids/pull/550)|Document Parquet and ORC compression support|
|[#539](https://github.com/NVIDIA/spark-rapids/pull/539)|Update script to audit multiple Spark versions|
|[#543](https://github.com/NVIDIA/spark-rapids/pull/543)|Add metrics to GpuUnion operator|
|[#549](https://github.com/NVIDIA/spark-rapids/pull/549)|Move spark shim properties to top level pom|
|[#497](https://github.com/NVIDIA/spark-rapids/pull/497)|Add UDF compiler implementations|
|[#487](https://github.com/NVIDIA/spark-rapids/pull/487)|Add framework for batch compression of shuffle partitions|
|[#544](https://github.com/NVIDIA/spark-rapids/pull/544)|Add in driverExtraClassPath for standalone mode docs|
|[#546](https://github.com/NVIDIA/spark-rapids/pull/546)|Fix Spark 3.1.0 shim build error in GpuHashJoin|
|[#537](https://github.com/NVIDIA/spark-rapids/pull/537)|Use fresh SparkSession when capturing to avoid late capture of previous query|
|[#538](https://github.com/NVIDIA/spark-rapids/pull/538)|Revert "Temporary workaround for RMM initial pool size bug (#530)"|
|[#517](https://github.com/NVIDIA/spark-rapids/pull/517)|Add config to limit maximum RMM pool size|
|[#527](https://github.com/NVIDIA/spark-rapids/pull/527)|Add support for split and getArrayIndex|
|[#534](https://github.com/NVIDIA/spark-rapids/pull/534)|Fixes bugs around GpuShuffleEnv initialization|
|[#529](https://github.com/NVIDIA/spark-rapids/pull/529)|[BUG] Degenerate table metas were not getting copied to the heap|
|[#530](https://github.com/NVIDIA/spark-rapids/pull/530)|Temporary workaround for RMM initial pool size bug|
|[#526](https://github.com/NVIDIA/spark-rapids/pull/526)|Fix bug with nullability reporting in GpuFilterExec|
|[#521](https://github.com/NVIDIA/spark-rapids/pull/521)|Fix typo with databricks shim classname SparkShimServiceProvider|
|[#522](https://github.com/NVIDIA/spark-rapids/pull/522)|Use SQLConf instead of SparkConf when looking up SQL configs|
|[#518](https://github.com/NVIDIA/spark-rapids/pull/518)|Fix init order issue in GpuShuffleEnv when RAPIDS shuffle configured|
|[#514](https://github.com/NVIDIA/spark-rapids/pull/514)|Added clarification of RegExpReplace, DateDiff, made descriptive text consistent|
|[#506](https://github.com/NVIDIA/spark-rapids/pull/506)|Add in basic support for running tpcds like queries|
|[#504](https://github.com/NVIDIA/spark-rapids/pull/504)|Add ability to ignore tests depending on spark shim version|
|[#503](https://github.com/NVIDIA/spark-rapids/pull/503)|Remove unused async buffer spill support|
|[#501](https://github.com/NVIDIA/spark-rapids/pull/501)|disable codegen in 3.1 shim for hash join|
|[#466](https://github.com/NVIDIA/spark-rapids/pull/466)|Optimize and fix Api validation script|
|[#481](https://github.com/NVIDIA/spark-rapids/pull/481)|Codeowners|
|[#439](https://github.com/NVIDIA/spark-rapids/pull/439)|Check a PR has been committed using git signoff|
|[#319](https://github.com/NVIDIA/spark-rapids/pull/319)|Update partitioning logic in ShuffledBatchRDD|
|[#491](https://github.com/NVIDIA/spark-rapids/pull/491)|Temporarily ignore AQE integration tests|
|[#490](https://github.com/NVIDIA/spark-rapids/pull/490)|Fix Spark 3.1.0 build for HashJoin changes|
|[#482](https://github.com/NVIDIA/spark-rapids/pull/482)|Prevent bad practice in python tests|
|[#485](https://github.com/NVIDIA/spark-rapids/pull/485)|Show plan in assertion message if test fails|
|[#480](https://github.com/NVIDIA/spark-rapids/pull/480)|Fix link from README to getting-started.md|
|[#448](https://github.com/NVIDIA/spark-rapids/pull/448)|Preliminary support for keeping broadcast exchanges on GPU when AQE is enabled|
|[#478](https://github.com/NVIDIA/spark-rapids/pull/478)|Fall back to CPU for binary as string in parquet|
|[#477](https://github.com/NVIDIA/spark-rapids/pull/477)|Fix special case joins in broadcast nested loop join|
|[#469](https://github.com/NVIDIA/spark-rapids/pull/469)|Update HashAggregateSuite to work with AQE|
|[#475](https://github.com/NVIDIA/spark-rapids/pull/475)|Udf compiler pom followup|
|[#434](https://github.com/NVIDIA/spark-rapids/pull/434)|Add UDF compiler skeleton|
|[#474](https://github.com/NVIDIA/spark-rapids/pull/474)|Re-enable noscaladoc check|
|[#461](https://github.com/NVIDIA/spark-rapids/pull/461)|Fix comments style to pass scala style check|
|[#468](https://github.com/NVIDIA/spark-rapids/pull/468)|fix broken link|
|[#456](https://github.com/NVIDIA/spark-rapids/pull/456)|Add closeOnExcept to clean up code that closes resources only on exceptions|
|[#464](https://github.com/NVIDIA/spark-rapids/pull/464)|Turn off noscaladoc rule until codebase is fixed|
|[#449](https://github.com/NVIDIA/spark-rapids/pull/449)|Enforce NoScalaDoc rule in scalastyle checks|
|[#450](https://github.com/NVIDIA/spark-rapids/pull/450)|Enable scalastyle for shuffle plugin|
|[#451](https://github.com/NVIDIA/spark-rapids/pull/451)|Databricks remove unneeded files and fix build to not fail on rm when file missing|
|[#442](https://github.com/NVIDIA/spark-rapids/pull/442)|Shim layer support for Spark 3.0.0 Databricks|
|[#447](https://github.com/NVIDIA/spark-rapids/pull/447)|Add scalastyle plugin to shim module|
|[#426](https://github.com/NVIDIA/spark-rapids/pull/426)|Update BufferMeta to support multiple codec buffers per table|
|[#440](https://github.com/NVIDIA/spark-rapids/pull/440)|Run mortgage test both with AQE on and off|
|[#445](https://github.com/NVIDIA/spark-rapids/pull/445)|Added in StringRPad and StringLPad|
|[#422](https://github.com/NVIDIA/spark-rapids/pull/422)|Documentation updates|
|[#437](https://github.com/NVIDIA/spark-rapids/pull/437)|Fix bug with InSet and Strings|
|[#435](https://github.com/NVIDIA/spark-rapids/pull/435)|Add in checks for Parquet LEGACY date/time rebase|
|[#432](https://github.com/NVIDIA/spark-rapids/pull/432)|Fix batch use-after-close in partitioning, shuffle env init|
|[#423](https://github.com/NVIDIA/spark-rapids/pull/423)|Fix duplicates includes in assembly jar|
|[#418](https://github.com/NVIDIA/spark-rapids/pull/418)|CI Add unit tests running for Spark 3.0.1|
|[#421](https://github.com/NVIDIA/spark-rapids/pull/421)|Make it easier to run TPCxBB benchmarks from spark shell|
|[#413](https://github.com/NVIDIA/spark-rapids/pull/413)|Fix download link|
|[#414](https://github.com/NVIDIA/spark-rapids/pull/414)|Shim Layer to support multiple Spark versions |
|[#406](https://github.com/NVIDIA/spark-rapids/pull/406)|Update cast handling to deal with new libcudf casting limitations|
|[#405](https://github.com/NVIDIA/spark-rapids/pull/405)|Change slave->worker|
|[#395](https://github.com/NVIDIA/spark-rapids/pull/395)|Databricks doc updates|
|[#401](https://github.com/NVIDIA/spark-rapids/pull/401)|Extended the FAQ|
|[#398](https://github.com/NVIDIA/spark-rapids/pull/398)|Add tests for GpuPartition|
|[#352](https://github.com/NVIDIA/spark-rapids/pull/352)|Change spark tgz package name|
|[#397](https://github.com/NVIDIA/spark-rapids/pull/397)|Fix small bug in ShuffleBufferCatalog.hasActiveShuffle|
|[#286](https://github.com/NVIDIA/spark-rapids/pull/286)|[REVIEW] Updated join tests for cache|
|[#393](https://github.com/NVIDIA/spark-rapids/pull/393)|Contributor license agreement|
|[#389](https://github.com/NVIDIA/spark-rapids/pull/389)|Added in support for RangeExec|
|[#390](https://github.com/NVIDIA/spark-rapids/pull/390)|Ucx getting started|
|[#391](https://github.com/NVIDIA/spark-rapids/pull/391)|Hide slack channel in Jenkins scripts|
|[#387](https://github.com/NVIDIA/spark-rapids/pull/387)|Remove the term whitelist|
|[#365](https://github.com/NVIDIA/spark-rapids/pull/365)|[REVIEW] Timesub tests|
|[#383](https://github.com/NVIDIA/spark-rapids/pull/383)|Test utility to compare SQL query results between CPU and GPU|
|[#380](https://github.com/NVIDIA/spark-rapids/pull/380)|Fix databricks notebook link|
|[#378](https://github.com/NVIDIA/spark-rapids/pull/378)|Added in FAQ and fixed spelling|
|[#377](https://github.com/NVIDIA/spark-rapids/pull/377)|Update heading in configs.md|
|[#373](https://github.com/NVIDIA/spark-rapids/pull/373)|Modifying branch name to conform with rapidsai branch name change|
|[#376](https://github.com/NVIDIA/spark-rapids/pull/376)|Add our session extension correctly if there are other extensions configured|
|[#374](https://github.com/NVIDIA/spark-rapids/pull/374)|Fix rat issue for notebooks|
|[#364](https://github.com/NVIDIA/spark-rapids/pull/364)|Update Databricks patch for changes to GpuSortMergeJoin|
|[#371](https://github.com/NVIDIA/spark-rapids/pull/371)|fix typo and use regional bucket per GCP's update|
|[#359](https://github.com/NVIDIA/spark-rapids/pull/359)|Karthik changes|
|[#353](https://github.com/NVIDIA/spark-rapids/pull/353)|Fix broadcast nested loop join for the no column case|
|[#313](https://github.com/NVIDIA/spark-rapids/pull/313)|Additional tests for broadcast hash join|
|[#342](https://github.com/NVIDIA/spark-rapids/pull/342)|Implement build-side rules for shuffle hash join|
|[#349](https://github.com/NVIDIA/spark-rapids/pull/349)|Updated join code to treat null equality properly|
|[#335](https://github.com/NVIDIA/spark-rapids/pull/335)|Integration tests on spark 3.0.1-SNAPSHOT & 3.1.0-SNAPSHOT|
|[#346](https://github.com/NVIDIA/spark-rapids/pull/346)|Update the Title Header for Fine Tuning|
|[#344](https://github.com/NVIDIA/spark-rapids/pull/344)|Fix small typo in readme|
|[#331](https://github.com/NVIDIA/spark-rapids/pull/331)|Adds iterator and client unit tests, and prepares for more fetch failure handling|
|[#337](https://github.com/NVIDIA/spark-rapids/pull/337)|Fix Scala compile phase to allow Java classes referencing Scala classes|
|[#332](https://github.com/NVIDIA/spark-rapids/pull/332)|Match GPU overwritten functions with SQL functions from FunctionRegistry|
|[#339](https://github.com/NVIDIA/spark-rapids/pull/339)|Fix databricks build|
|[#338](https://github.com/NVIDIA/spark-rapids/pull/338)|Move GpuPartitioning to a separate file|
|[#310](https://github.com/NVIDIA/spark-rapids/pull/310)|Update release Jenkinsfile for Databricks|
|[#330](https://github.com/NVIDIA/spark-rapids/pull/330)|Hide private info in Jenkins scripts|
|[#324](https://github.com/NVIDIA/spark-rapids/pull/324)|Add in basic support for GpuCartesianProductExec|
|[#328](https://github.com/NVIDIA/spark-rapids/pull/328)|Enable slack notification for Databricks build|
|[#321](https://github.com/NVIDIA/spark-rapids/pull/321)|update databricks patch for GpuBroadcastNestedLoopJoinExec|
|[#322](https://github.com/NVIDIA/spark-rapids/pull/322)|Add oss.sonatype.org to download the cudf jar|
|[#320](https://github.com/NVIDIA/spark-rapids/pull/320)|Don't mount passwd/group to the container|
|[#258](https://github.com/NVIDIA/spark-rapids/pull/258)|Enable running TPCH tests with AQE enabled|
|[#318](https://github.com/NVIDIA/spark-rapids/pull/318)|Build docker image with Dockerfile|
|[#309](https://github.com/NVIDIA/spark-rapids/pull/309)|Update databricks patch to latest changes|
|[#312](https://github.com/NVIDIA/spark-rapids/pull/312)|Trigger branch-0.2 integration test|
|[#307](https://github.com/NVIDIA/spark-rapids/pull/307)|[Jenkins] Update the release script and Jenkinsfile|
|[#304](https://github.com/NVIDIA/spark-rapids/pull/304)|[DOC][Minor] Fix typo in spark config name.|
|[#303](https://github.com/NVIDIA/spark-rapids/pull/303)|Update compatibility doc for -0.0 issues|
|[#301](https://github.com/NVIDIA/spark-rapids/pull/301)|Add info about branches in README.md|
|[#296](https://github.com/NVIDIA/spark-rapids/pull/296)|Added in basic support for broadcast nested loop join|
|[#297](https://github.com/NVIDIA/spark-rapids/pull/297)|Databricks CI improvements and support runtime env parameter to xfail certain tests|
|[#292](https://github.com/NVIDIA/spark-rapids/pull/292)|Move artifacts version in version-def.sh|
|[#254](https://github.com/NVIDIA/spark-rapids/pull/254)|Cleanup QA tests|
|[#289](https://github.com/NVIDIA/spark-rapids/pull/289)|Clean up GpuCollectLimitMeta and add in metrics|
|[#287](https://github.com/NVIDIA/spark-rapids/pull/287)|Add in support for right join and fix issues build right|
|[#273](https://github.com/NVIDIA/spark-rapids/pull/273)|Added releases to the README.md|
|[#285](https://github.com/NVIDIA/spark-rapids/pull/285)|modify run_pyspark_from_build.sh to be bash 3 friendly|
|[#281](https://github.com/NVIDIA/spark-rapids/pull/281)|Add in support for Full Outer Join on non-null keys|
|[#274](https://github.com/NVIDIA/spark-rapids/pull/274)|Add RapidsDiskStore tests|
|[#259](https://github.com/NVIDIA/spark-rapids/pull/259)|Add RapidsHostMemoryStore tests|
|[#282](https://github.com/NVIDIA/spark-rapids/pull/282)|Update Databricks patch for 0.2 branch|
|[#261](https://github.com/NVIDIA/spark-rapids/pull/261)|Add conditional xfail test for DISTINCT aggregates with NaN|
|[#263](https://github.com/NVIDIA/spark-rapids/pull/263)|More time ops|
|[#256](https://github.com/NVIDIA/spark-rapids/pull/256)|Remove special cases for contains, startsWith, and endWith|
|[#253](https://github.com/NVIDIA/spark-rapids/pull/253)|Remove GpuAttributeReference and GpuSortOrder|
|[#271](https://github.com/NVIDIA/spark-rapids/pull/271)|Update the versions for 0.2.0 properly for the databricks build|
|[#162](https://github.com/NVIDIA/spark-rapids/pull/162)|Integration tests for corner cases in window functions.|
|[#264](https://github.com/NVIDIA/spark-rapids/pull/264)|Add a local mvn repo for nightly pipeline|
|[#262](https://github.com/NVIDIA/spark-rapids/pull/262)|Refer to branch-0.2|
|[#255](https://github.com/NVIDIA/spark-rapids/pull/255)|Revert change to make dependencies of shaded jar optional|
|[#257](https://github.com/NVIDIA/spark-rapids/pull/257)|Fix link to RAPIDS cudf in index.md|
|[#252](https://github.com/NVIDIA/spark-rapids/pull/252)|Update to 0.2.0-SNAPSHOT and cudf-0.15-SNAPSHOT|

## Release 0.1

### Features
|||
|:---|:---|
|[#74](https://github.com/NVIDIA/spark-rapids/issues/74)|[FEA] Support ToUnixTimestamp|
|[#21](https://github.com/NVIDIA/spark-rapids/issues/21)|[FEA] NormalizeNansAndZeros|
|[#105](https://github.com/NVIDIA/spark-rapids/issues/105)|[FEA] integration tests for equi-joins|

### Bugs Fixed
|||
|:---|:---|
|[#116](https://github.com/NVIDIA/spark-rapids/issues/116)|[BUG] calling replace with a NULL throws an exception|
|[#168](https://github.com/NVIDIA/spark-rapids/issues/168)|[BUG] GpuUnitTests Date tests leak column vectors|
|[#209](https://github.com/NVIDIA/spark-rapids/issues/209)|[BUG] Developers section in pom need to be updated|
|[#204](https://github.com/NVIDIA/spark-rapids/issues/204)|[BUG] Code coverage docs are out of date|
|[#154](https://github.com/NVIDIA/spark-rapids/issues/154)|[BUG] Incorrect output from partial-only averages with nulls|
|[#61](https://github.com/NVIDIA/spark-rapids/issues/61)|[BUG] Cannot disable Parquet, ORC, CSV reading when using FileSourceScanExec|

### PRs
|||
|:---|:---|
|[#249](https://github.com/NVIDIA/spark-rapids/pull/249)|Compatability -> Compatibility|
|[#247](https://github.com/NVIDIA/spark-rapids/pull/247)|Add index.md for default doc page, fix table formatting for configs|
|[#241](https://github.com/NVIDIA/spark-rapids/pull/241)|Let default branch to master per the release rule|
|[#177](https://github.com/NVIDIA/spark-rapids/pull/177)|Fixed leaks in unit test and use ColumnarBatch for testing|
|[#243](https://github.com/NVIDIA/spark-rapids/pull/243)|Jenkins file for Databricks release|
|[#225](https://github.com/NVIDIA/spark-rapids/pull/225)|Make internal project dependencies optional for shaded artifact|
|[#242](https://github.com/NVIDIA/spark-rapids/pull/242)|Add site pages|
|[#221](https://github.com/NVIDIA/spark-rapids/pull/221)|Databricks Build Support|
|[#215](https://github.com/NVIDIA/spark-rapids/pull/215)|Remove CudfColumnVector|
|[#213](https://github.com/NVIDIA/spark-rapids/pull/213)|Add RapidsDeviceMemoryStore tests|
|[#214](https://github.com/NVIDIA/spark-rapids/pull/214)|[REVIEW] Test failure to pass Attribute as GpuAttribute|
|[#211](https://github.com/NVIDIA/spark-rapids/pull/211)|Add project leads to pom developer list|
|[#210](https://github.com/NVIDIA/spark-rapids/pull/210)|Updated coverage docs|
|[#195](https://github.com/NVIDIA/spark-rapids/pull/195)|Support public release for plugin jar|
|[#208](https://github.com/NVIDIA/spark-rapids/pull/208)|Remove unneeded comment from pom.xml|
|[#191](https://github.com/NVIDIA/spark-rapids/pull/191)|WindowExec handle different spark distributions|
|[#181](https://github.com/NVIDIA/spark-rapids/pull/181)|Remove INCOMPAT for NormalizeNanAndZero, KnownFloatingPointNormalized|
|[#196](https://github.com/NVIDIA/spark-rapids/pull/196)|Update Spark dependency to the released 3.0.0 artifacts|
|[#206](https://github.com/NVIDIA/spark-rapids/pull/206)|Change groupID to 'com.nvidia' in IT scripts|
|[#202](https://github.com/NVIDIA/spark-rapids/pull/202)|Fixed issue for contains when searching for an empty string|
|[#201](https://github.com/NVIDIA/spark-rapids/pull/201)|Fix name of scan|
|[#200](https://github.com/NVIDIA/spark-rapids/pull/200)|Fix issue with GpuAttributeReference not overrideing references|
|[#197](https://github.com/NVIDIA/spark-rapids/pull/197)|Fix metrics for writes|
|[#186](https://github.com/NVIDIA/spark-rapids/pull/186)|Fixed issue with nullability on concat|
|[#193](https://github.com/NVIDIA/spark-rapids/pull/193)|Add RapidsBufferCatalog tests|
|[#188](https://github.com/NVIDIA/spark-rapids/pull/188)|rebrand to com.nvidia instead of ai.rapids|
|[#189](https://github.com/NVIDIA/spark-rapids/pull/189)|Handle AggregateExpression having resultIds parameter instead of a single resultId|
|[#190](https://github.com/NVIDIA/spark-rapids/pull/190)|FileSourceScanExec can have logicalRelation parameter on some distributions|
|[#185](https://github.com/NVIDIA/spark-rapids/pull/185)|Update type of parameter of GpuExpandExec to make it consistent|
|[#172](https://github.com/NVIDIA/spark-rapids/pull/172)|Merge qa test to integration test|
|[#180](https://github.com/NVIDIA/spark-rapids/pull/180)|Add MetaUtils unit tests|
|[#171](https://github.com/NVIDIA/spark-rapids/pull/171)|Cleanup scaladoc warnings about missing links|
|[#176](https://github.com/NVIDIA/spark-rapids/pull/176)|Updated join tests to cover more data.|
|[#169](https://github.com/NVIDIA/spark-rapids/pull/169)|Remove dependency on shaded Spark artifact|
|[#174](https://github.com/NVIDIA/spark-rapids/pull/174)|Added in fallback tests|
|[#165](https://github.com/NVIDIA/spark-rapids/pull/165)|Move input metadata tests to pyspark|
|[#173](https://github.com/NVIDIA/spark-rapids/pull/173)|Fix setting local mode for tests|
|[#160](https://github.com/NVIDIA/spark-rapids/pull/160)|Integration tests for normalizing NaN/zeroes.|
|[#163](https://github.com/NVIDIA/spark-rapids/pull/163)|Ignore the order locally for repartition tests|
|[#157](https://github.com/NVIDIA/spark-rapids/pull/157)|Add partial and final only hash aggregate tests and fix nulls corner case for Average|
|[#159](https://github.com/NVIDIA/spark-rapids/pull/159)|Add integration tests for joins|
|[#158](https://github.com/NVIDIA/spark-rapids/pull/158)|Orc merge schema fallback and FileScan format configs|
|[#164](https://github.com/NVIDIA/spark-rapids/pull/164)|Fix compiler warnings|
|[#152](https://github.com/NVIDIA/spark-rapids/pull/152)|Moved cudf to 0.14 for CI|
|[#151](https://github.com/NVIDIA/spark-rapids/pull/151)|Switch CICD pipelines to Github|
