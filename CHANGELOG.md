# Change log
Generated on 2022-04-12

## Release 22.04

### Features
|||
|:---|:---|
|[#4734](https://github.com/NVIDIA/spark-rapids/issues/4734)|[FEA] Support approx_percentile in reduction context|
|[#1922](https://github.com/NVIDIA/spark-rapids/issues/1922)|[FEA] Support ORC forced positional evolution|
|[#123](https://github.com/NVIDIA/spark-rapids/issues/123)|[FEA] add in support for dayfirst formats in the CSV parser|
|[#4863](https://github.com/NVIDIA/spark-rapids/issues/4863)|[FEA] Improve timestamp support in JSON and CSV readers|
|[#4935](https://github.com/NVIDIA/spark-rapids/issues/4935)|[FEA] Support reading Avro: primitive types|
|[#4915](https://github.com/NVIDIA/spark-rapids/issues/4915)|[FEA] Drop support for Spark 3.0.1, 3.0.2, 3.0.3, Databricks 7.3 ML LTS|
|[#4815](https://github.com/NVIDIA/spark-rapids/issues/4815)|[FEA] Support org.apache.spark.sql.catalyst.expressions.ArrayExists|
|[#3245](https://github.com/NVIDIA/spark-rapids/issues/3245)|[FEA] GpuGetMapValue should support all valid value data types and non-complex key types|
|[#4914](https://github.com/NVIDIA/spark-rapids/issues/4914)|[FEA] Support for Databricks 10.4 ML LTS|
|[#4945](https://github.com/NVIDIA/spark-rapids/issues/4945)|[FEA] Support filter and comparisons on ANSI day time interval type|
|[#4004](https://github.com/NVIDIA/spark-rapids/issues/4004)|[FEA] Add support for percent_rank|
|[#1111](https://github.com/NVIDIA/spark-rapids/issues/1111)|[FEA] support `spark.sql.legacy.timeParserPolicy` when parsing CSV files|
|[#4849](https://github.com/NVIDIA/spark-rapids/issues/4849)|[FEA] Support parsing dates in JSON reader|
|[#4789](https://github.com/NVIDIA/spark-rapids/issues/4789)|[FEA] Add Spark 3.1.4 shim|
|[#4646](https://github.com/NVIDIA/spark-rapids/issues/4646)|[FEA] Make JSON parsing of `NaN` and `Infinity` values fully compatible with Spark|
|[#4824](https://github.com/NVIDIA/spark-rapids/issues/4824)|[FEA] Support reading decimals from JSON and CSV|
|[#4814](https://github.com/NVIDIA/spark-rapids/issues/4814)|[FEA] Support element_at with non-literal index|
|[#4816](https://github.com/NVIDIA/spark-rapids/issues/4816)|[FEA] Support org.apache.spark.sql.catalyst.expressions.GetArrayStructFields|
|[#3542](https://github.com/NVIDIA/spark-rapids/issues/3542)|[FEA] Support str_to_map function|
|[#4721](https://github.com/NVIDIA/spark-rapids/issues/4721)|[FEA] Support regular expression delimiters for `str_to_map`|
|[#4791](https://github.com/NVIDIA/spark-rapids/issues/4791)|Update Spark 3.1.3 to be released|
|[#4712](https://github.com/NVIDIA/spark-rapids/issues/4712)|[FEA] Allow <WindowExec> to partition on Decimal 128 when running on the GPU|
|[#4762](https://github.com/NVIDIA/spark-rapids/issues/4762)|[FEA] Improve support for reading JSON integer types|
|[#4696](https://github.com/NVIDIA/spark-rapids/issues/4696)|[FEA] Support casting map to string|
|[#1572](https://github.com/NVIDIA/spark-rapids/issues/1572)|[FEA] Add in decimal support for pmod, remainder and divide|
|[#4763](https://github.com/NVIDIA/spark-rapids/issues/4763)|[FEA] Improve support for reading JSON boolean types|
|[#4003](https://github.com/NVIDIA/spark-rapids/issues/4003)|[FEA] Add regular expression support to GPU implementation of StringSplit|
|[#4626](https://github.com/NVIDIA/spark-rapids/issues/4626)|[FEA] <WindowExec> cannot run on GPU because unsupported data types in 'partitionSpec'|
|[#33](https://github.com/NVIDIA/spark-rapids/issues/33)|[FEA] hypot SQL function|
|[#4515](https://github.com/NVIDIA/spark-rapids/issues/4515)|[FEA] Set RMM async allocator as default|

### Performance
|||
|:---|:---|
|[#3026](https://github.com/NVIDIA/spark-rapids/issues/3026)|[FEA] [Audit]: Set the list of read columns in the task configuration to reduce reading of ORC data|
|[#4895](https://github.com/NVIDIA/spark-rapids/issues/4895)|Add support for structs in GpuScalarSubquery |
|[#4393](https://github.com/NVIDIA/spark-rapids/issues/4393)|[BUG] Columnar to Columnar transfers are very slow|
|[#589](https://github.com/NVIDIA/spark-rapids/issues/589)|[FEA] Support ExistenceJoin|
|[#4784](https://github.com/NVIDIA/spark-rapids/issues/4784)|[FEA] Improve copying decimal data from CPU columnar data |
|[#4685](https://github.com/NVIDIA/spark-rapids/issues/4685)|[FEA] Avoid regexp cost in string_split for escaped characters|
|[#4777](https://github.com/NVIDIA/spark-rapids/issues/4777)|Remove input upcast in GpuExtractChunk32|
|[#4722](https://github.com/NVIDIA/spark-rapids/issues/4722)|Optimize DECIMAL128 average aggregations|
|[#4645](https://github.com/NVIDIA/spark-rapids/issues/4645)|[FEA] Investigate ASYNC allocator performance with additional queries|
|[#4539](https://github.com/NVIDIA/spark-rapids/issues/4539)|[FEA] semaphore optimization in shuffled hash join|
|[#2441](https://github.com/NVIDIA/spark-rapids/issues/2441)|[FEA] Use AST for filter in join APIs|

### Bugs Fixed
|||
|:---|:---|
|[#5180](https://github.com/NVIDIA/spark-rapids/issues/5180)|[BUG] create_map failed with java.lang.IllegalStateException: This is not supported yet|
|[#5181](https://github.com/NVIDIA/spark-rapids/issues/5181)|[BUG] Dataproc tests failing when trying to detect for accelerated row conversions|
|[#5154](https://github.com/NVIDIA/spark-rapids/issues/5154)|[BUG] build failed in databricks 10.4 runtime (updated recently)|
|[#5159](https://github.com/NVIDIA/spark-rapids/issues/5159)|[BUG] Approx percentile query fails with UnsupportedOperationException|
|[#5164](https://github.com/NVIDIA/spark-rapids/issues/5164)|[BUG] Databricks 9.1ML failed with "java.lang.NoSuchMethodError: org.apache.spark.sql.execution.metric.SQLMetrics$.createSizeMetric"|
|[#5125](https://github.com/NVIDIA/spark-rapids/issues/5125)|[BUG] GpuCast.hasSideEffects does not check if child expression has side effects|
|[#5091](https://github.com/NVIDIA/spark-rapids/issues/5091)|[BUG] Profiling tool fails process custom task accumulators of type CollectionAccumulator|
|[#5050](https://github.com/NVIDIA/spark-rapids/issues/5050)|[BUG] Release build of v22.04.0 FAILED on "Execution attach-javadoc failed: NullPointerException" with maven option '-P source-javadoc'|
|[#5035](https://github.com/NVIDIA/spark-rapids/issues/5035)|[BUG] Different CSV parsing behavior between 22.04 and 22.02|
|[#5065](https://github.com/NVIDIA/spark-rapids/issues/5065)|[BUG] spark330+ build error due to SPARK-37463|
|[#5019](https://github.com/NVIDIA/spark-rapids/issues/5019)|[BUG] udf compiler failed to translate UDF in spark-shell |
|[#5048](https://github.com/NVIDIA/spark-rapids/issues/5048)|[BUG] OOM for q18 of TPC-DS benchmark testing on Spark2a|
|[#5038](https://github.com/NVIDIA/spark-rapids/issues/5038)|[BUG] When spark.rapids.sql.regexp.enabled is on in 22.04 snapshot jars, Reading a Delta table in Databricks may cause driver error|
|[#5023](https://github.com/NVIDIA/spark-rapids/issues/5023)|[BUG] When+sequence could trigger "Illegal sequence boundaries" error|
|[#5021](https://github.com/NVIDIA/spark-rapids/issues/5021)|[BUG] test_cache_reverse_order failed|
|[#5003](https://github.com/NVIDIA/spark-rapids/issues/5003)|[BUG] Cloudera 3.1.1 tests fail due to ClouderaShimVersion|
|[#4960](https://github.com/NVIDIA/spark-rapids/issues/4960)|[BUG] Spark 3.3 IT cache_test:test_passing_gpuExpr_as_Expr failure|
|[#4913](https://github.com/NVIDIA/spark-rapids/issues/4913)|[BUG] Fall back to the CPU if we see a scale on Ceil or Floor|
|[#4806](https://github.com/NVIDIA/spark-rapids/issues/4806)|[BUG] When running xgboost training, if PCBS is enabled, it fails with java.lang.AssertionError|
|[#4542](https://github.com/NVIDIA/spark-rapids/issues/4542)|[BUG] test_write_round_trip failed Maximum pool size exceeded |
|[#4911](https://github.com/NVIDIA/spark-rapids/issues/4911)|[BUG][Audit] [SPARK-38314] - Fail to read parquet files after writing the hidden file metadata|
|[#4936](https://github.com/NVIDIA/spark-rapids/issues/4936)|[BUG] databricks nightly window_function_test failures|
|[#4931](https://github.com/NVIDIA/spark-rapids/issues/4931)|[BUG] Spark 3.3 IT test cache_test.py::test_passing_gpuExpr_as_Expr  fails with IllegalArgumentException|
|[#4710](https://github.com/NVIDIA/spark-rapids/issues/4710)|[BUG] cudaErrorIllegalAddress for q95 (3TB) on GCP with ASYNC allocator|
|[#4918](https://github.com/NVIDIA/spark-rapids/issues/4918)|[BUG] databricks nightly build failed|
|[#4826](https://github.com/NVIDIA/spark-rapids/issues/4826)|[BUG] cache_test failures when testing with 128-bit decimal|
|[#4855](https://github.com/NVIDIA/spark-rapids/issues/4855)|[BUG] Shim tests in sql-plugin module are not running|
|[#4487](https://github.com/NVIDIA/spark-rapids/issues/4487)|[BUG] regexp_find hangs with some patterns|
|[#4486](https://github.com/NVIDIA/spark-rapids/issues/4486)|[BUG] Regular expressions with hex digits not working as expected|
|[#4879](https://github.com/NVIDIA/spark-rapids/issues/4879)|[BUG] [SPARK-38237][SQL] ClusteredDistribution clustering keys break build with wrong arguments|
|[#4883](https://github.com/NVIDIA/spark-rapids/issues/4883)|[BUG] row-based_udf_test.py::test_hive_empty_* fail nightly tests|
|[#4876](https://github.com/NVIDIA/spark-rapids/issues/4876)|[BUG] Nightly build failed on Databricks with "pip: No such file or directory"|
|[#4739](https://github.com/NVIDIA/spark-rapids/issues/4739)|[BUG] Plugin will crash with query > 100 columns on pascal GPU|
|[#4840](https://github.com/NVIDIA/spark-rapids/issues/4840)|[BUG] test_dpp_via_aggregate_subquery_aqe_off failed with table already exists|
|[#4841](https://github.com/NVIDIA/spark-rapids/issues/4841)|[BUG] test_compress_write_round_trip failed on Spark 3.3|
|[#4668](https://github.com/NVIDIA/spark-rapids/issues/4668)|[FEA][Audit] - [SPARK-37750][SQL] ANSI mode: optionally return null result if element not exists in array/map|
|[#3971](https://github.com/NVIDIA/spark-rapids/issues/3971)|[BUG] udf-examples dependencies are incorrect|
|[#4022](https://github.com/NVIDIA/spark-rapids/issues/4022)|[BUG] Ensure shims.v2.ParquetCachedBatchSerializer and similar classes are at most package-private  |
|[#4526](https://github.com/NVIDIA/spark-rapids/issues/4526)|[BUG] Short circuit AND/OR in ANSI mode|
|[#4787](https://github.com/NVIDIA/spark-rapids/issues/4787)|[BUG] Dataproc notebook IT test failure - NoSuchMethodError: org.apache.spark.network.util.ByteUnit.toBytes|
|[#4704](https://github.com/NVIDIA/spark-rapids/issues/4704)|[BUG] Update the premerge and nightly tests after moving the UDF example to external repository|
|[#4795](https://github.com/NVIDIA/spark-rapids/issues/4795)|[BUG] Read ORC does not ignoreCorruptFiles|
|[#4802](https://github.com/NVIDIA/spark-rapids/issues/4802)|[BUG] GPU CSV read does not honor ignoreCorruptFiles or ignoreMissingFiles|
|[#4803](https://github.com/NVIDIA/spark-rapids/issues/4803)|[BUG] GPU JSON read does not honor ignoreCorruptFiles or ignoreMissingFiles|
|[#1986](https://github.com/NVIDIA/spark-rapids/issues/1986)|[BUG] CSV reading null inconsistent between spark.rapids.sql.format.csv.enabled=true&false|
|[#126](https://github.com/NVIDIA/spark-rapids/issues/126)|[BUG] CSV parsing large number values overflow|
|[#4759](https://github.com/NVIDIA/spark-rapids/issues/4759)|[BUG] Profiling tool can miss datasources when they are GPU reads|
|[#4798](https://github.com/NVIDIA/spark-rapids/issues/4798)|[BUG] Integration test builds failing with worker_id not found|
|[#4727](https://github.com/NVIDIA/spark-rapids/issues/4727)|[BUG] Read Parquet does not ignoreCorruptFiles|
|[#4744](https://github.com/NVIDIA/spark-rapids/issues/4744)|[BUG] test_groupby_std_variance_partial_replace_fallback failed|
|[#4761](https://github.com/NVIDIA/spark-rapids/issues/4761)|[BUG] test_simple_partitioned_read failed on Spark 3.3|
|[#2071](https://github.com/NVIDIA/spark-rapids/issues/2071)|[BUG] parsing invalid boolean CSV values return true instead of null|
|[#4749](https://github.com/NVIDIA/spark-rapids/issues/4749)|[BUG] test_write_empty_parquet_round_trip failed|
|[#4730](https://github.com/NVIDIA/spark-rapids/issues/4730)|[BUG] python UDF tests are leaking|
|[#4290](https://github.com/NVIDIA/spark-rapids/issues/4290)|[BUG] Investigate q32 and q67 for decimals potential regression|
|[#4409](https://github.com/NVIDIA/spark-rapids/issues/4409)|[BUG] Possible race condition in regular expression support for octal digits|
|[#4728](https://github.com/NVIDIA/spark-rapids/issues/4728)|[BUG] test_mixed_compress_read orc_test.py failures|
|[#4736](https://github.com/NVIDIA/spark-rapids/issues/4736)|[BUG] buildall --profile=321 fails on missing spark301 rapids-4-spark-sql dependency |
|[#4702](https://github.com/NVIDIA/spark-rapids/issues/4702)|[BUG] cache_test.py failed w/ cache.serializer in spark 3.3.0|
|[#4031](https://github.com/NVIDIA/spark-rapids/issues/4031)|[BUG] Spark 3.3.0 test failure: NoSuchMethodError org.apache.orc.TypeDescription.getAttributeValue|
|[#4664](https://github.com/NVIDIA/spark-rapids/issues/4664)|[BUG] MortgageAdaptiveSparkSuite failed with duplicate buffer exception|
|[#4564](https://github.com/NVIDIA/spark-rapids/issues/4564)|[BUG] map_test ansi failed in spark330|
|[#119](https://github.com/NVIDIA/spark-rapids/issues/119)|[BUG] LIKE does not work if null chars are in the string|
|[#124](https://github.com/NVIDIA/spark-rapids/issues/124)|[BUG] CSV/JSON Parsing some float values results in overflow|
|[#4045](https://github.com/NVIDIA/spark-rapids/issues/4045)|[BUG] q93 failed in this week's NDS runs|
|[#4488](https://github.com/NVIDIA/spark-rapids/issues/4488)|[BUG] isCastingStringToNegDecimalScaleSupported seems set wrong for some Spark versions|

### PRs
|||
|:---|:---|
|[#5190](https://github.com/NVIDIA/spark-rapids/pull/5190)|Fix column->row conversion GPU check:|
|[#5184](https://github.com/NVIDIA/spark-rapids/pull/5184)|Fix CPU fallback for Map lookup|
|[#5191](https://github.com/NVIDIA/spark-rapids/pull/5191)|Update version-def to use released cudfjni 22.04.0 [skip ci]|
|[#5167](https://github.com/NVIDIA/spark-rapids/pull/5167)|Update cudfjni version to released 22.04.0|
|[#5169](https://github.com/NVIDIA/spark-rapids/pull/5169)|Terminate test earlier if pytest ENV issue [skip ci]|
|[#5160](https://github.com/NVIDIA/spark-rapids/pull/5160)|Fix approximate percentile reduction UnsupportedOperationException|
|[#5165](https://github.com/NVIDIA/spark-rapids/pull/5165)|Update Databricks 10.4 for changes to the QueryStageExec and ClusteredDistribution|
|[#4997](https://github.com/NVIDIA/spark-rapids/pull/4997)|Update docs for the 22.04 release[skip ci]|
|[#5146](https://github.com/NVIDIA/spark-rapids/pull/5146)|Support env var INTEGRATION_TEST_VERSION to override shim version|
|[#5103](https://github.com/NVIDIA/spark-rapids/pull/5103)|Init 22.04 changelog [skip ci]|
|[#5122](https://github.com/NVIDIA/spark-rapids/pull/5122)|Disable GPU accelerated row-column transpose for Pascal GPUs:|
|[#5127](https://github.com/NVIDIA/spark-rapids/pull/5127)|GpuCast.hasSideEffects now checks to see if the child expression has side-effects|
|[#5118](https://github.com/NVIDIA/spark-rapids/pull/5118)|On task failure catch some CUDA exceptions and kill executor|
|[#5069](https://github.com/NVIDIA/spark-rapids/pull/5069)|Update for the public release [skip ci]|
|[#5097](https://github.com/NVIDIA/spark-rapids/pull/5097)|Implement hasSideEffects for GpuGetArrayItem, GpuElementAt, GpuGetMapValue, GpuUnaryMinus, and GpuAbs|
|[#5079](https://github.com/NVIDIA/spark-rapids/pull/5079)|Disable spark snapshot shims pre-merge build in 22.04|
|[#5094](https://github.com/NVIDIA/spark-rapids/pull/5094)|Fix profiling tool reading collectionAccumulator|
|[#5078](https://github.com/NVIDIA/spark-rapids/pull/5078)|Disable JSON and CSV floating-point reads by default|
|[#4961](https://github.com/NVIDIA/spark-rapids/pull/4961)|Support approx_percentile in reduction context|
|[#5062](https://github.com/NVIDIA/spark-rapids/pull/5062)|Update Spark 2.x explain API with changes in 22.04|
|[#5066](https://github.com/NVIDIA/spark-rapids/pull/5066)|Add getOrcSchemaString for OrcShims|
|[#5030](https://github.com/NVIDIA/spark-rapids/pull/5030)|Fix regression from 21.12 where udfs defined in repl no longer worked|
|[#5051](https://github.com/NVIDIA/spark-rapids/pull/5051)|Revert "Replace ParquetFileReader.readFooter with open() and getFooter "|
|[#5052](https://github.com/NVIDIA/spark-rapids/pull/5052)|Work around incompatibility between Databricks Delta loads and GpuRegExpExtract|
|[#4972](https://github.com/NVIDIA/spark-rapids/pull/4972)|Add support for ORC forced positional evolution|
|[#5042](https://github.com/NVIDIA/spark-rapids/pull/5042)|Implement hasSideEffects for GpuSequence|
|[#5040](https://github.com/NVIDIA/spark-rapids/pull/5040)|Fix missing imports for 321db shim|
|[#5033](https://github.com/NVIDIA/spark-rapids/pull/5033)|Removed limit from the test|
|[#4938](https://github.com/NVIDIA/spark-rapids/pull/4938)|Improve compatibility when reading timestamps from JSON and CSV sources|
|[#5026](https://github.com/NVIDIA/spark-rapids/pull/5026)|Update RoCE doc URL [skip ci]|
|[#4976](https://github.com/NVIDIA/spark-rapids/pull/4976)|Replace ParquetFileReader.readFooter with open() and getFooter|
|[#4989](https://github.com/NVIDIA/spark-rapids/pull/4989)|Use conf.useCompression config to decide if we should be compressing the cache|
|[#4956](https://github.com/NVIDIA/spark-rapids/pull/4956)|Add avro reader support|
|[#5009](https://github.com/NVIDIA/spark-rapids/pull/5009)|Remove references of `shims` folder in docs [skip ci]|
|[#5004](https://github.com/NVIDIA/spark-rapids/pull/5004)|Add ClouderaShimVersion to unshimmed files|
|[#4971](https://github.com/NVIDIA/spark-rapids/pull/4971)|Fall back to the CPU for non-zero scale on Ceil or Floor functions|
|[#4996](https://github.com/NVIDIA/spark-rapids/pull/4996)|Fix collect_set on struct type|
|[#4998](https://github.com/NVIDIA/spark-rapids/pull/4998)|Added the id back for struct children to make them unique|
|[#4995](https://github.com/NVIDIA/spark-rapids/pull/4995)|Include 321db shim in distribution build [skip ci]|
|[#4981](https://github.com/NVIDIA/spark-rapids/pull/4981)|Update doc for CSV reading interval|
|[#4973](https://github.com/NVIDIA/spark-rapids/pull/4973)|Implement support for ArrayExists expression|
|[#4988](https://github.com/NVIDIA/spark-rapids/pull/4988)|Remove support for Spark 3.0.x|
|[#4955](https://github.com/NVIDIA/spark-rapids/pull/4955)|Add UDT support to ParquetCachedBatchSerializer (CPU)|
|[#4994](https://github.com/NVIDIA/spark-rapids/pull/4994)|Add databricks 10.4 build in pre-merge|
|[#4990](https://github.com/NVIDIA/spark-rapids/pull/4990)|Remove 30X permerge support for version 22.04 and above [skip ci]|
|[#4958](https://github.com/NVIDIA/spark-rapids/pull/4958)|Add independent mvn verify check [skip ci]|
|[#4933](https://github.com/NVIDIA/spark-rapids/pull/4933)|Set OrcConf.INCLUDE_COLUMNS for ORC reading|
|[#4944](https://github.com/NVIDIA/spark-rapids/pull/4944)|Support for non-string key-types for `GetMapValue` and `element_at()`|
|[#4974](https://github.com/NVIDIA/spark-rapids/pull/4974)|Add shim for Databricks 10.4|
|[#4907](https://github.com/NVIDIA/spark-rapids/pull/4907)|Add markdown check action|
|[#4977](https://github.com/NVIDIA/spark-rapids/pull/4977)|Add missing 314 to buildall script|
|[#4927](https://github.com/NVIDIA/spark-rapids/pull/4927)|Support reading ANSI day time interval type from CSV source|
|[#4965](https://github.com/NVIDIA/spark-rapids/pull/4965)|Documentation: add example python api call for ExplainPlan.explainPotentialGpuPlan [skip ci]|
|[#4957](https://github.com/NVIDIA/spark-rapids/pull/4957)|Document agg pushdown on ORC file limitation [skip ci]|
|[#4946](https://github.com/NVIDIA/spark-rapids/pull/4946)|Support predictors on ANSI day time interval type|
|[#4952](https://github.com/NVIDIA/spark-rapids/pull/4952)|Have a fixed GPU memory size for integration tests|
|[#4954](https://github.com/NVIDIA/spark-rapids/pull/4954)|Fix of failing to read parquet files after writing the hidden file metadata in|
|[#4953](https://github.com/NVIDIA/spark-rapids/pull/4953)|Add Decimal 128 as a supported type in partition by for databricks running window|
|[#4941](https://github.com/NVIDIA/spark-rapids/pull/4941)|Use new list reduction API to improve performance|
|[#4926](https://github.com/NVIDIA/spark-rapids/pull/4926)|Support `DayTimeIntervalType` in `ParquetCachedBatchSerializer`|
|[#4947](https://github.com/NVIDIA/spark-rapids/pull/4947)|Fallback to ARENA if ASYNC configured and driver < 11.5.0|
|[#4934](https://github.com/NVIDIA/spark-rapids/pull/4934)|Replace MetadataAttribute with FileSourceMetadataAttribute to follow the update in Spark for 3.3.0+|
|[#4942](https://github.com/NVIDIA/spark-rapids/pull/4942)|Fix window rank integration tests on|
|[#4928](https://github.com/NVIDIA/spark-rapids/pull/4928)|Disable regular expressions on GPU by default|
|[#4923](https://github.com/NVIDIA/spark-rapids/pull/4923)|Support GpuScalarSubquery on nested types|
|[#4924](https://github.com/NVIDIA/spark-rapids/pull/4924)|Implement `percent_rank()` on GPU|
|[#4853](https://github.com/NVIDIA/spark-rapids/pull/4853)|Improve date support in JSON and CSV readers|
|[#4930](https://github.com/NVIDIA/spark-rapids/pull/4930)|Add in support for sorting arrays with structs in sort_array|
|[#4861](https://github.com/NVIDIA/spark-rapids/pull/4861)|Add Apache Spark 3.1.4-SNAPSHOT Shims|
|[#4925](https://github.com/NVIDIA/spark-rapids/pull/4925)|Remove unused Spark322PlusShims|
|[#4921](https://github.com/NVIDIA/spark-rapids/pull/4921)|Add DatabricksShimVersion to unshimmed class list|
|[#4917](https://github.com/NVIDIA/spark-rapids/pull/4917)|Default some configs to protect against cluster settings in integration tests|
|[#4922](https://github.com/NVIDIA/spark-rapids/pull/4922)|Add support for decimal 128 for db and spark 320+|
|[#4919](https://github.com/NVIDIA/spark-rapids/pull/4919)|Case-insensitive PR title check [skip ci]|
|[#4796](https://github.com/NVIDIA/spark-rapids/pull/4796)|Implement ExistenceJoin Iterator using an auxiliary left semijoin |
|[#4857](https://github.com/NVIDIA/spark-rapids/pull/4857)|Transition to v2 shims [Databricks]|
|[#4899](https://github.com/NVIDIA/spark-rapids/pull/4899)|Fixed Decimal 128 bug in ParquetCachedBatchSerializer|
|[#4810](https://github.com/NVIDIA/spark-rapids/pull/4810)|Support ANSI intervals to/from Parquet|
|[#4909](https://github.com/NVIDIA/spark-rapids/pull/4909)|Make ARENA the default allocator for 22.04|
|[#4856](https://github.com/NVIDIA/spark-rapids/pull/4856)|Enable shim tests in sql-plugin module|
|[#4880](https://github.com/NVIDIA/spark-rapids/pull/4880)|Bump hadoop-client dependency to 3.1.4|
|[#4825](https://github.com/NVIDIA/spark-rapids/pull/4825)|Initial support for reading decimal types from JSON and CSV|
|[#4859](https://github.com/NVIDIA/spark-rapids/pull/4859)|Fallback to CPU when Spark pushes down Aggregates (Min/Max/Count) for ORC|
|[#4872](https://github.com/NVIDIA/spark-rapids/pull/4872)|Speed up copying decimal column from parquet buffer to GPU buffer|
|[#4904](https://github.com/NVIDIA/spark-rapids/pull/4904)|Relocate Hive UDF Classes|
|[#4871](https://github.com/NVIDIA/spark-rapids/pull/4871)|Minor changes to print revision differences when building shims|
|[#4882](https://github.com/NVIDIA/spark-rapids/pull/4882)|Disable write/read Parquet when Parquet field IDs are used|
|[#4858](https://github.com/NVIDIA/spark-rapids/pull/4858)|Support non-literal index for `GpuElementAt` and `GpuGetArrayItem`|
|[#4875](https://github.com/NVIDIA/spark-rapids/pull/4875)|Support running `GetArrayStructFields` on GPU|
|[#4885](https://github.com/NVIDIA/spark-rapids/pull/4885)|Enable fuzz testing for Regular Expression repetitions and move remaining edge cases to CPU|
|[#4869](https://github.com/NVIDIA/spark-rapids/pull/4869)|Support for hexadecimal digits in regular expressions on the GPU|
|[#4854](https://github.com/NVIDIA/spark-rapids/pull/4854)|Avoid regexp_cost with stringSplit on the GPU using transpilation|
|[#4888](https://github.com/NVIDIA/spark-rapids/pull/4888)|Clean up leak detection code|
|[#4901](https://github.com/NVIDIA/spark-rapids/pull/4901)|fix a broken link in CONTRIBUTING.md[skip ci]|
|[#4891](https://github.com/NVIDIA/spark-rapids/pull/4891)|update getting started doc because aws-emr 6.5.0 released[skip ci]|
|[#4881](https://github.com/NVIDIA/spark-rapids/pull/4881)|Fix compilation error caused by ClusteredDistribution parameters|
|[#4890](https://github.com/NVIDIA/spark-rapids/pull/4890)|Integration-test tests jar for hive UDF tests|
|[#4878](https://github.com/NVIDIA/spark-rapids/pull/4878)|Set conda/mamba default to Python version to 3.8 [skip ci]|
|[#4874](https://github.com/NVIDIA/spark-rapids/pull/4874)|Fix spark-tests syntax issue [skip ci]|
|[#4850](https://github.com/NVIDIA/spark-rapids/pull/4850)|Also check cuda runtime version when using the ASYNC allocator|
|[#4851](https://github.com/NVIDIA/spark-rapids/pull/4851)|Add worker ID to temporary table names in tests|
|[#4847](https://github.com/NVIDIA/spark-rapids/pull/4847)|Fix test_compress_write_round_trip failure on Spark 3.3|
|[#4848](https://github.com/NVIDIA/spark-rapids/pull/4848)|Profile tool: fix printing of task failed reason|
|[#4636](https://github.com/NVIDIA/spark-rapids/pull/4636)|Support `str_to_map`|
|[#4835](https://github.com/NVIDIA/spark-rapids/pull/4835)|Trim parquet_write_test to reduce integration test runtime|
|[#4819](https://github.com/NVIDIA/spark-rapids/pull/4819)|Throw exception if casting from double to datetime |
|[#4838](https://github.com/NVIDIA/spark-rapids/pull/4838)|Trim cache tests to improve integration test time|
|[#4839](https://github.com/NVIDIA/spark-rapids/pull/4839)|Optionally return null if element not exists map/array|
|[#4822](https://github.com/NVIDIA/spark-rapids/pull/4822)|Push decimal workarounds to cuDF|
|[#4619](https://github.com/NVIDIA/spark-rapids/pull/4619)|Move the udf-examples module to the external repository spark-rapids-examples|
|[#4844](https://github.com/NVIDIA/spark-rapids/pull/4844)|Update spark313 dep to released one|
|[#4827](https://github.com/NVIDIA/spark-rapids/pull/4827)|Make InternalExclusiveModeGpuDiscoveryPlugin and ExplainPlanImpl as protected class.|
|[#4836](https://github.com/NVIDIA/spark-rapids/pull/4836)|Support WindowExec partitioning by Decimal 128 on the GPU|
|[#4760](https://github.com/NVIDIA/spark-rapids/pull/4760)|Short circuit AND/OR in ANSI mode|
|[#4829](https://github.com/NVIDIA/spark-rapids/pull/4829)|Make bloopInstall version configurable in buildall|
|[#4823](https://github.com/NVIDIA/spark-rapids/pull/4823)|Reduce redundancy of decimal testing|
|[#4715](https://github.com/NVIDIA/spark-rapids/pull/4715)|Patterns such (3?)+ should now fall back to CPU|
|[#4809](https://github.com/NVIDIA/spark-rapids/pull/4809)|Add ignoreCorruptFiles for ORC readers|
|[#4790](https://github.com/NVIDIA/spark-rapids/pull/4790)|Improve JSON and CSV parsing of integer values|
|[#4812](https://github.com/NVIDIA/spark-rapids/pull/4812)|Default integration test configs to allow negative decimal scale|
|[#4805](https://github.com/NVIDIA/spark-rapids/pull/4805)|Avoid output cast by using unsigned type output for GpuExtractChunk32|
|[#4804](https://github.com/NVIDIA/spark-rapids/pull/4804)|Profiling tool can miss datasources when they are GPU reads|
|[#4797](https://github.com/NVIDIA/spark-rapids/pull/4797)|Do not check for metadata during schema comparison|
|[#4785](https://github.com/NVIDIA/spark-rapids/pull/4785)|Support casting Map to String|
|[#4794](https://github.com/NVIDIA/spark-rapids/pull/4794)|Decimal-128 support for mod and pmod|
|[#4799](https://github.com/NVIDIA/spark-rapids/pull/4799)|Fix failure to generate worker_id when xdist is not present|
|[#4742](https://github.com/NVIDIA/spark-rapids/pull/4742)|Add ignoreCorruptFiles feature for Parquet reader|
|[#4792](https://github.com/NVIDIA/spark-rapids/pull/4792)|Ensure GpuM2 merge aggregation does not produce a null mean or m2|
|[#4770](https://github.com/NVIDIA/spark-rapids/pull/4770)|Improve columnarCopy for HostColumnarToGpu|
|[#4776](https://github.com/NVIDIA/spark-rapids/pull/4776)|Improve aggregation performance of average on DECIMAL128 columns|
|[#4786](https://github.com/NVIDIA/spark-rapids/pull/4786)|Add shims to compare ORC TypeDescription|
|[#4780](https://github.com/NVIDIA/spark-rapids/pull/4780)|Improve JSON and CSV support for boolean values|
|[#4778](https://github.com/NVIDIA/spark-rapids/pull/4778)|Decrease chance of random collisions in test temporary paths|
|[#4782](https://github.com/NVIDIA/spark-rapids/pull/4782)|Check in host leak detection code|
|[#4781](https://github.com/NVIDIA/spark-rapids/pull/4781)|Add Spark properties table to profiling tool output|
|[#4714](https://github.com/NVIDIA/spark-rapids/pull/4714)|Add regular expression support to string_split|
|[#4754](https://github.com/NVIDIA/spark-rapids/pull/4754)|Close SpillableBatch to avoid leaks|
|[#4758](https://github.com/NVIDIA/spark-rapids/pull/4758)|Fix merge conflict with branch-22.02 [skip ci]|
|[#4694](https://github.com/NVIDIA/spark-rapids/pull/4694)|Add clarifications and details to integration-tests README [skip ci]|
|[#4740](https://github.com/NVIDIA/spark-rapids/pull/4740)|Enable regular expressions on GPU by default|
|[#4735](https://github.com/NVIDIA/spark-rapids/pull/4735)|Re-enables partial regex support for octal digits on the GPU|
|[#4737](https://github.com/NVIDIA/spark-rapids/pull/4737)|Check for a null compression codec when creating ORC OutStream|
|[#4738](https://github.com/NVIDIA/spark-rapids/pull/4738)|Change resume-from to aggregator in buildall [skip ci]|
|[#4698](https://github.com/NVIDIA/spark-rapids/pull/4698)|Add tests for few json options|
|[#4731](https://github.com/NVIDIA/spark-rapids/pull/4731)|Trim join tests to improve runtime of tests|
|[#4732](https://github.com/NVIDIA/spark-rapids/pull/4732)|Fix failing serializer tests on Spark 3.3.0|
|[#4709](https://github.com/NVIDIA/spark-rapids/pull/4709)|Update centos 8 dockerfile to handle EOL issue [skip ci]|
|[#4724](https://github.com/NVIDIA/spark-rapids/pull/4724)|Debug dump to Parquet support for DECIMAL128 columns|
|[#4688](https://github.com/NVIDIA/spark-rapids/pull/4688)|Optimize DECIMAL128 sum aggregations|
|[#4692](https://github.com/NVIDIA/spark-rapids/pull/4692)|Add FAQ entry to discuss executor task concurrency configuration [skip ci]|
|[#4588](https://github.com/NVIDIA/spark-rapids/pull/4588)|Optimize semaphore acquisition in GpuShuffledHashJoinExec|
|[#4697](https://github.com/NVIDIA/spark-rapids/pull/4697)|Add preliminary test and test framework changes for ExistanceJoin|
|[#4716](https://github.com/NVIDIA/spark-rapids/pull/4716)|`GpuStringSplit` should return an array on not-null elements|
|[#4611](https://github.com/NVIDIA/spark-rapids/pull/4611)|Support BitLength and OctetLength|
|[#4408](https://github.com/NVIDIA/spark-rapids/pull/4408)|Use the ORC version that corresponds to the Spark version|
|[#4686](https://github.com/NVIDIA/spark-rapids/pull/4686)|Fall back to CPU for queries referencing hidden metadata columns|
|[#4669](https://github.com/NVIDIA/spark-rapids/pull/4669)|Prevent deadlock between RapidsBufferStore and RapidsBufferBase on close|
|[#4707](https://github.com/NVIDIA/spark-rapids/pull/4707)|Fix auto merge conflict 4705 [skip ci]|
|[#4690](https://github.com/NVIDIA/spark-rapids/pull/4690)|Fix map_test ANSI failure in Spark 3.3.0|
|[#4681](https://github.com/NVIDIA/spark-rapids/pull/4681)|Reimplement check for non-regexp strings using RegexParser|
|[#4683](https://github.com/NVIDIA/spark-rapids/pull/4683)|Fix documentation link, clarify documentation [skip ci]|
|[#4677](https://github.com/NVIDIA/spark-rapids/pull/4677)|Make Collect, first and last as deterministic aggregate functions for Spark-3.3|
|[#4682](https://github.com/NVIDIA/spark-rapids/pull/4682)|Enable test for LIKE with embedded null character|
|[#4673](https://github.com/NVIDIA/spark-rapids/pull/4673)|Allow GpuWindowExec to partition on structs|
|[#4637](https://github.com/NVIDIA/spark-rapids/pull/4637)|Improve support for reading CSV and JSON floating-point values|
|[#4629](https://github.com/NVIDIA/spark-rapids/pull/4629)|Remove shims module|
|[#4648](https://github.com/NVIDIA/spark-rapids/pull/4648)|Append new authorized user to blossom-ci safelist|
|[#4623](https://github.com/NVIDIA/spark-rapids/pull/4623)|Fallback to CPU when aggregate push down used for parquet|
|[#4606](https://github.com/NVIDIA/spark-rapids/pull/4606)|Set default RMM pool to ASYNC for cuda 11.2+|
|[#4531](https://github.com/NVIDIA/spark-rapids/pull/4531)|Use libcudf mixed joins for conditional hash semi and anti joins|
|[#4624](https://github.com/NVIDIA/spark-rapids/pull/4624)|Enable integration test results report on Jenkins [skip ci]|
|[#4597](https://github.com/NVIDIA/spark-rapids/pull/4597)|Update plugin version to 22.04.0-SNAPSHOT|
|[#4592](https://github.com/NVIDIA/spark-rapids/pull/4592)|Adds SQL function HYPOT using the GPU|
|[#4504](https://github.com/NVIDIA/spark-rapids/pull/4504)|Implement AST-based regular expression fuzz tests|
|[#4560](https://github.com/NVIDIA/spark-rapids/pull/4560)|Make shims.v2.ParquetCachedBatchSerializer as protected|

## Release 22.02

### Features
|||
|:---|:---|
|[#4305](https://github.com/NVIDIA/spark-rapids/issues/4305)|[FEA] write nvidia tool wrappers to allow old YARN versions to work with MIG|
|[#4410](https://github.com/NVIDIA/spark-rapids/issues/4410)|[FEA] ReplicateRows - Support ReplicateRows for decimal 128 type|
|[#4360](https://github.com/NVIDIA/spark-rapids/issues/4360)|[FEA] Add explain api for Spark 2.X|
|[#3541](https://github.com/NVIDIA/spark-rapids/issues/3541)|[FEA] Support max on single-level struct in aggregation context|
|[#4238](https://github.com/NVIDIA/spark-rapids/issues/4238)|[FEA] Add a Spark 3.X Explain only mode to the plugin|
|[#3952](https://github.com/NVIDIA/spark-rapids/issues/3952)|[Audit] [FEA][SPARK-32986][SQL] Add bucketed scan info in query plan of data source v1|
|[#4412](https://github.com/NVIDIA/spark-rapids/issues/4412)|[FEA] Improve support for \A, \Z, and \z in regular expressions|
|[#3979](https://github.com/NVIDIA/spark-rapids/issues/3979)|[FEA] Improvements for  CPU(Row) based UDF|
|[#4467](https://github.com/NVIDIA/spark-rapids/issues/4467)|[FEA] Add support for regular expression with repeated digits (`\d+`, `\d*`, `\d?`)|
|[#4439](https://github.com/NVIDIA/spark-rapids/issues/4439)|[FEA] Enable GPU broadcast exchange reuse for DPP when AQE enabled|
|[#3512](https://github.com/NVIDIA/spark-rapids/issues/3512)|[FEA] Support org.apache.spark.sql.catalyst.expressions.Sequence|
|[#3475](https://github.com/NVIDIA/spark-rapids/issues/3475)|[FEA] Spark 3.2.0 reads Parquet unsigned int64(UINT64) as Decimal(20,0) but CUDF does not support it |
|[#4091](https://github.com/NVIDIA/spark-rapids/issues/4091)|[FEA] regexp_replace: Improve support for ^ and $|
|[#4104](https://github.com/NVIDIA/spark-rapids/issues/4104)|[FEA] Support org.apache.spark.sql.catalyst.expressions.ReplicateRows|
|[#4027](https://github.com/NVIDIA/spark-rapids/issues/4027)|[FEA]  Support SubqueryBroadcast on GPU to enable exchange reuse during DPP|
|[#4284](https://github.com/NVIDIA/spark-rapids/issues/4284)|[FEA] Support idx = 0 in GpuRegExpExtract|
|[#4002](https://github.com/NVIDIA/spark-rapids/issues/4002)|[FEA] Implement regexp_extract on GPU|
|[#3221](https://github.com/NVIDIA/spark-rapids/issues/3221)|[FEA] Support GpuFirst and GpuLast on nested types under reduction aggregations|
|[#3944](https://github.com/NVIDIA/spark-rapids/issues/3944)|[FEA] Full support for sum with overflow on Decimal 128|
|[#4028](https://github.com/NVIDIA/spark-rapids/issues/4028)|[FEA] support GpuCast from non-nested ArrayType to StringType|
|[#3250](https://github.com/NVIDIA/spark-rapids/issues/3250)|[FEA] Make CreateMap duplicate key handling compatible with Spark and enable CreateMap by default|
|[#4170](https://github.com/NVIDIA/spark-rapids/issues/4170)|[FEA] Make regular expression behavior with `$` and `\r` consistent with CPU|
|[#4001](https://github.com/NVIDIA/spark-rapids/issues/4001)|[FEA] Add regexp support to regexp_replace|
|[#3962](https://github.com/NVIDIA/spark-rapids/issues/3962)|[FEA] Support null characters in regular expressions in RLIKE|
|[#3797](https://github.com/NVIDIA/spark-rapids/issues/3797)|[FEA] Make RLike support consistent with Apache Spark|

### Performance
|||
|:---|:---|
|[#4392](https://github.com/NVIDIA/spark-rapids/issues/4392)|[FEA] could the parquet scan code avoid acquiring the semaphore for an empty batch?|
|[#679](https://github.com/NVIDIA/spark-rapids/issues/679)|[FEA] move some deserialization code out of the scope of the gpu-semaphore to increase cpu concurrent|
|[#4350](https://github.com/NVIDIA/spark-rapids/issues/4350)|[FEA] Optimize the all-true and all-false cases in GPU `If` and `CaseWhen` |
|[#4309](https://github.com/NVIDIA/spark-rapids/issues/4309)|[FEA] Leverage cudf conditional nested loop join to implement semi/anti hash join with condition|
|[#4395](https://github.com/NVIDIA/spark-rapids/issues/4395)|[FEA] acquire the semaphore after concatToHost in GpuShuffleCoalesceIterator|
|[#4134](https://github.com/NVIDIA/spark-rapids/issues/4134)|[FEA] Allow `EliminateJoinToEmptyRelation` in `GpuBroadcastExchangeExec` |
|[#4189](https://github.com/NVIDIA/spark-rapids/issues/4189)|[FEA] understand why between is so expensive|

### Bugs Fixed
|||
|:---|:---|
|[#4725](https://github.com/NVIDIA/spark-rapids/issues/4725)|[DOC] Broken links in guide doc|
|[#4675](https://github.com/NVIDIA/spark-rapids/issues/4675)|[BUG] Jenkins integration build timed out at 10 hours|
|[#4665](https://github.com/NVIDIA/spark-rapids/issues/4665)|[BUG] Spark321Shims.getParquetFilters failed with NoSuchMethodError|
|[#4635](https://github.com/NVIDIA/spark-rapids/issues/4635)|[BUG] nvidia-smi wrapper script ignores ENABLE_NON_MIG_GPUS=1 on a heterogeneous multi-GPU machine|
|[#4500](https://github.com/NVIDIA/spark-rapids/issues/4500)|[BUG] Build failures against Spark 3.2.1 rc1 and make 3.2.1 non snapshot|
|[#4631](https://github.com/NVIDIA/spark-rapids/issues/4631)|[BUG] Release build with mvn option `-P source-javadoc` FAILED|
|[#4625](https://github.com/NVIDIA/spark-rapids/issues/4625)|[BUG] NDS query 5 fails with AdaptiveSparkPlanExec assertion|
|[#4632](https://github.com/NVIDIA/spark-rapids/issues/4632)|[BUG] Build failing for Spark 3.3.0 due to deprecated method warnings|
|[#4599](https://github.com/NVIDIA/spark-rapids/issues/4599)|[BUG] test_group_apply_udf and test_group_apply_udf_more_types hangs on Databricks 9.1|
|[#4600](https://github.com/NVIDIA/spark-rapids/issues/4600)|[BUG] crash if we have a decimal128 in a struct in an array |
|[#4581](https://github.com/NVIDIA/spark-rapids/issues/4581)|[BUG] Build error "GpuOverrides.scala:924: wrong number of arguments" on DB9.1.x spark-3.1.2 |
|[#4593](https://github.com/NVIDIA/spark-rapids/issues/4593)|[BUG] dup GpuHashJoin.diff case-folding issue|
|[#4559](https://github.com/NVIDIA/spark-rapids/issues/4559)|[BUG] regexp_replace with replacement string containing `\` can produce incorrect results|
|[#4503](https://github.com/NVIDIA/spark-rapids/issues/4503)|[BUG] regexp_replace with back references produces incorrect results on GPU|
|[#4567](https://github.com/NVIDIA/spark-rapids/issues/4567)|[BUG] Profile tool hangs in compare mode|
|[#4315](https://github.com/NVIDIA/spark-rapids/issues/4315)|[BUG] test_hash_reduction_decimal_overflow_sum[30] failed OOM in integration tests|
|[#4551](https://github.com/NVIDIA/spark-rapids/issues/4551)|[BUG] protobuf-java version changed to 3.x|
|[#4499](https://github.com/NVIDIA/spark-rapids/issues/4499)|[BUG]GpuSequence blows up when nulls exist in any of the inputs (start, stop, step)|
|[#4454](https://github.com/NVIDIA/spark-rapids/issues/4454)|[BUG] Shade warnings when building the tools artifact|
|[#4541](https://github.com/NVIDIA/spark-rapids/issues/4541)|[BUG] Column vector leak in conditionals_test.py|
|[#4514](https://github.com/NVIDIA/spark-rapids/issues/4514)|[BUG] test_hash_reduction_pivot_without_nans failed|
|[#4521](https://github.com/NVIDIA/spark-rapids/issues/4521)|[BUG] Inconsistencies in handling of newline characters and string and line anchors|
|[#4548](https://github.com/NVIDIA/spark-rapids/issues/4548)|[BUG] ai.rapids.cudf.CudaException: an illegal instruction was encountered in databricks 9.1|
|[#4475](https://github.com/NVIDIA/spark-rapids/issues/4475)|[BUG] `\D` and `\W` match newline in Spark but not in cuDF|
|[#1866](https://github.com/NVIDIA/spark-rapids/issues/1866)|[BUG] GpuFileFormatWriter does not close the data writer|
|[#4524](https://github.com/NVIDIA/spark-rapids/issues/4524)|[BUG] RegExp transpiler fails to detect some choice expressions that cuDF cannot compile|
|[#3226](https://github.com/NVIDIA/spark-rapids/issues/3226)|[BUG]OOM happened when do cube operations|
|[#2504](https://github.com/NVIDIA/spark-rapids/issues/2504)|[BUG] OOM when running NDS queries with UCX and GDS|
|[#4273](https://github.com/NVIDIA/spark-rapids/issues/4273)|[BUG] Rounding past the size that can be stored in a type produces incorrect results|
|[#4060](https://github.com/NVIDIA/spark-rapids/issues/4060)|[BUG] test_hash_groupby_approx_percentile_long_repeated_keys failed intermittently|
|[#4039](https://github.com/NVIDIA/spark-rapids/issues/4039)|[BUG] Spark 3.3.0 IT Array test failures|
|[#3849](https://github.com/NVIDIA/spark-rapids/issues/3849)|[BUG] In ANSI mode we can fail in cases Spark would not due to conditionals|
|[#4445](https://github.com/NVIDIA/spark-rapids/issues/4445)|[BUG] mvn clean prints an error message on a clean dir|
|[#4421](https://github.com/NVIDIA/spark-rapids/issues/4421)|[BUG] the driver is trying to load CUDA with latest 22.02 |
|[#4455](https://github.com/NVIDIA/spark-rapids/issues/4455)|[BUG] join_test.py::test_struct_self_join[IGNORE_ORDER({'local': True})] failed in spark330|
|[#4442](https://github.com/NVIDIA/spark-rapids/issues/4442)|[BUG] mvn build FAILED with option `-P noSnapshotsWithDatabricks`|
|[#4281](https://github.com/NVIDIA/spark-rapids/issues/4281)|[BUG] q9 regression between 21.10 and 21.12|
|[#4280](https://github.com/NVIDIA/spark-rapids/issues/4280)|[BUG] q88 regression between 21.10 and 21.12|
|[#4422](https://github.com/NVIDIA/spark-rapids/issues/4422)|[BUG] Host column vectors are being leaked during tests|
|[#4446](https://github.com/NVIDIA/spark-rapids/issues/4446)|[BUG] GpuCast crashes when casting from Array with unsupportable child type|
|[#4432](https://github.com/NVIDIA/spark-rapids/issues/4432)|[BUG] nightly build 3.3.0 failed: HashClusteredDistribution is not a member of org.apache.spark.sql.catalyst.plans.physical|
|[#4443](https://github.com/NVIDIA/spark-rapids/issues/4443)|[BUG] SPARK-37705 breaks parquet filters from Spark 3.3.0 and Spark 3.2.2 onwards|
|[#4316](https://github.com/NVIDIA/spark-rapids/issues/4316)|[BUG] Exception: Unable to find py4j, your SPARK_HOME may not be configured correctly intermittently|
|[#4378](https://github.com/NVIDIA/spark-rapids/issues/4378)|[BUG] udf_test udf_cudf_test failed require_minimum_pandas_version check in spark 320+|
|[#4423](https://github.com/NVIDIA/spark-rapids/issues/4423)|[BUG] Build is failing due to FileScanRDD changes in Spark 3.3.0-SNAPSHOT|
|[#4401](https://github.com/NVIDIA/spark-rapids/issues/4401)|[BUG]array_test.py::test_array_contains failures|
|[#4403](https://github.com/NVIDIA/spark-rapids/issues/4403)|[BUG] NDS query 72 logs codegen fallback exception and produces incorrect results|
|[#4386](https://github.com/NVIDIA/spark-rapids/issues/4386)|[BUG] conditionals_test.py FAILED with side_effects_cast[Integer/Long] on Databricks 9.1 Runtime|
|[#3934](https://github.com/NVIDIA/spark-rapids/issues/3934)|[BUG] Dependencies of published integration tests jar are missing|
|[#4341](https://github.com/NVIDIA/spark-rapids/issues/4341)|[BUG] GpuCast.scala:nnn warning: discarding unmoored doc comment|
|[#4356](https://github.com/NVIDIA/spark-rapids/issues/4356)|[BUG] nightly spark303 deploy pulling spark301 aggregator|
|[#4347](https://github.com/NVIDIA/spark-rapids/issues/4347)|[BUG] Dist jar pom lists aggregator jar as dependency|
|[#4176](https://github.com/NVIDIA/spark-rapids/issues/4176)|[BUG] ParseDateTimeSuite UT failed|
|[#4292](https://github.com/NVIDIA/spark-rapids/issues/4292)|[BUG] no meaningful message is surfaced to maven when binary-dedupe fails|
|[#4351](https://github.com/NVIDIA/spark-rapids/issues/4351)|[BUG] Tests FAILED On SPARK-3.2.0, com.nvidia.spark.rapids.SerializedTableColumn cannot be cast to com.nvidia.spark.rapids.GpuColumnVector|
|[#4346](https://github.com/NVIDIA/spark-rapids/issues/4346)|[BUG] q73 decimal was twice as slow in weekly results|
|[#4334](https://github.com/NVIDIA/spark-rapids/issues/4334)|[BUG] GpuColumnarToRowExec will always be tagged False for exportColumnarRdd after Spark311 |
|[#4339](https://github.com/NVIDIA/spark-rapids/issues/4339)|The parameter `dataType` is not necessary in `resolveColumnVector` method.|
|[#4275](https://github.com/NVIDIA/spark-rapids/issues/4275)|[BUG] Row-based Hive UDF will fail if arguments contain a foldable expression.|
|[#4229](https://github.com/NVIDIA/spark-rapids/issues/4229)|[BUG] regexp_replace `[^a]` has different behavior between CPU and GPU for multiline strings|
|[#4294](https://github.com/NVIDIA/spark-rapids/issues/4294)|[BUG] parquet_write_test.py::test_ts_write_fails_datetime_exception failed in spark 3.1.1 and 3.1.2|
|[#4205](https://github.com/NVIDIA/spark-rapids/issues/4205)|[BUG] Get different results when casting from timestamp to string|
|[#4277](https://github.com/NVIDIA/spark-rapids/issues/4277)|[BUG] cudf_udf nightly cudf import rmm failed|
|[#4246](https://github.com/NVIDIA/spark-rapids/issues/4246)|[BUG] Regression in CastOpSuite due to cuDF change in parsing NaN|
|[#4243](https://github.com/NVIDIA/spark-rapids/issues/4243)|[BUG] test_regexp_replace_null_pattern_fallback[ALLOW_NON_GPU(ProjectExec,RegExpReplace)] failed in databricks|
|[#4244](https://github.com/NVIDIA/spark-rapids/issues/4244)|[BUG] Cast from string to float using hand-picked values failed|
|[#4227](https://github.com/NVIDIA/spark-rapids/issues/4227)|[BUG] RAPIDS Shuffle Manager doesn't fallback given encryption settings|
|[#3374](https://github.com/NVIDIA/spark-rapids/issues/3374)|[BUG] minor deprecation warnings in a 3.2 shim build|
|[#3613](https://github.com/NVIDIA/spark-rapids/issues/3613)|[BUG] release312db profile pulls in 311until320-apache|
|[#4213](https://github.com/NVIDIA/spark-rapids/issues/4213)|[BUG] unused method with a misleading outdated comment in ShimLoader |
|[#3609](https://github.com/NVIDIA/spark-rapids/issues/3609)|[BUG] GpuShuffleExchangeExec in v2 shims has inconsistent packaging|
|[#4127](https://github.com/NVIDIA/spark-rapids/issues/4127)|[BUG] CUDF 22.02 nightly test failure|

### PRs
|||
|:---|:---|
|[#4773](https://github.com/NVIDIA/spark-rapids/pull/4773)|Update 22.02 changelog to latest [skip ci]|
|[#4771](https://github.com/NVIDIA/spark-rapids/pull/4771)|revert cudf api links from legacy to stable[skip ci]|
|[#4767](https://github.com/NVIDIA/spark-rapids/pull/4767)|Update 22.02 changelog to latest [skip ci]|
|[#4750](https://github.com/NVIDIA/spark-rapids/pull/4750)|Updated doc for decimal support|
|[#4757](https://github.com/NVIDIA/spark-rapids/pull/4757)|Update qualification tool to remove DECIMAL 128 as potential problem|
|[#4755](https://github.com/NVIDIA/spark-rapids/pull/4755)|Fix databricks doc for limitations.[skip ci]|
|[#4751](https://github.com/NVIDIA/spark-rapids/pull/4751)|Fix broken hyperlinks in documentation [skip ci]|
|[#4706](https://github.com/NVIDIA/spark-rapids/pull/4706)|Update 22.02 changelog to latest [skip ci]|
|[#4700](https://github.com/NVIDIA/spark-rapids/pull/4700)|Update cudfjni version to released 22.02.0|
|[#4701](https://github.com/NVIDIA/spark-rapids/pull/4701)|Decrease nighlty tests upper limitation to 7 [skip ci]|
|[#4639](https://github.com/NVIDIA/spark-rapids/pull/4639)|Update changelog for 22.02 and archive info of some older releases [skip ci]|
|[#4572](https://github.com/NVIDIA/spark-rapids/pull/4572)|Add download page for 22.02 [skip ci]|
|[#4672](https://github.com/NVIDIA/spark-rapids/pull/4672)|Revert "Disable 311cdh build due to missing dependency (#4659)"|
|[#4662](https://github.com/NVIDIA/spark-rapids/pull/4662)|Update the deploy script [skip ci]|
|[#4657](https://github.com/NVIDIA/spark-rapids/pull/4657)|Upmerge spark2 directory to the latest 22.02 changes|
|[#4659](https://github.com/NVIDIA/spark-rapids/pull/4659)|Disable 311cdh build by default because of a missing dependency|
|[#4508](https://github.com/NVIDIA/spark-rapids/pull/4508)|Fix Spark 3.2.1 build failures and make it non-snapshot|
|[#4652](https://github.com/NVIDIA/spark-rapids/pull/4652)|Remove non-deterministic test order in nightly [skip ci]|
|[#4643](https://github.com/NVIDIA/spark-rapids/pull/4643)|Add profile release301 when mvn help:evaluate|
|[#4630](https://github.com/NVIDIA/spark-rapids/pull/4630)|Fix the incomplete capture of SubqueryBroadcast |
|[#4633](https://github.com/NVIDIA/spark-rapids/pull/4633)|Suppress newTaskTempFile method warnings for Spark 3.3.0 build|
|[#4618](https://github.com/NVIDIA/spark-rapids/pull/4618)|[DB31x] Pick the correct Python runner for flatmap-group Pandas UDF|
|[#4622](https://github.com/NVIDIA/spark-rapids/pull/4622)|Fallback to CPU when encoding is not supported for JSON reader|
|[#4470](https://github.com/NVIDIA/spark-rapids/pull/4470)|Add in HashPartitioning support for decimal 128|
|[#4535](https://github.com/NVIDIA/spark-rapids/pull/4535)|Revert "Disable orc write by default because of https://issues.apache.org/jira/browse/ORC-1075 (#4471)"|
|[#4583](https://github.com/NVIDIA/spark-rapids/pull/4583)|Avoid unapply on PromotePrecision|
|[#4573](https://github.com/NVIDIA/spark-rapids/pull/4573)|Correct version from 21.12 to 22.02[skip ci]|
|[#4575](https://github.com/NVIDIA/spark-rapids/pull/4575)|Correct and update links in UDF doc[skip ci]|
|[#4501](https://github.com/NVIDIA/spark-rapids/pull/4501)|Switch and/or to use new cudf binops to improve performance|
|[#4594](https://github.com/NVIDIA/spark-rapids/pull/4594)|Resolve case-folding issue [skip ci]|
|[#4585](https://github.com/NVIDIA/spark-rapids/pull/4585)|Spark2 module upmerge, deploy script, and updates for Jenkins|
|[#4589](https://github.com/NVIDIA/spark-rapids/pull/4589)|Increase premerge databricks IDLE_TIMEOUT to 4 hours [skip ci]|
|[#4485](https://github.com/NVIDIA/spark-rapids/pull/4485)|Add json reader support|
|[#4556](https://github.com/NVIDIA/spark-rapids/pull/4556)|regexp_replace with back-references should fall back to CPU|
|[#4569](https://github.com/NVIDIA/spark-rapids/pull/4569)|Fix infinite loop with Profiling tool compare mode and app with no sql ids|
|[#4529](https://github.com/NVIDIA/spark-rapids/pull/4529)|Add support for Spark 2.x Explain Api|
|[#4577](https://github.com/NVIDIA/spark-rapids/pull/4577)|Revert "Fix CVE-2021-22569 (#4545)"|
|[#4520](https://github.com/NVIDIA/spark-rapids/pull/4520)|GpuSequence refactor|
|[#4570](https://github.com/NVIDIA/spark-rapids/pull/4570)|A few quick fixes to try to reduce max memory usage in the tests|
|[#4477](https://github.com/NVIDIA/spark-rapids/pull/4477)|Use libcudf mixed joins for conditional hash joins|
|[#4566](https://github.com/NVIDIA/spark-rapids/pull/4566)|remove scala-library from combined tools jar|
|[#4552](https://github.com/NVIDIA/spark-rapids/pull/4552)|Fix resource leak in GpuCaseWhen|
|[#4553](https://github.com/NVIDIA/spark-rapids/pull/4553)|Reenable test_hash_reduction_pivot_without_nans|
|[#4530](https://github.com/NVIDIA/spark-rapids/pull/4530)|Fix correctness issues in regexp and add `\r` and `\n` to fuzz tests|
|[#4549](https://github.com/NVIDIA/spark-rapids/pull/4549)|Fix typos in integration tests README [skip ci]|
|[#4545](https://github.com/NVIDIA/spark-rapids/pull/4545)|Fix CVE-2021-22569|
|[#4543](https://github.com/NVIDIA/spark-rapids/pull/4543)|Enable auto-merge from branch-22.02 to branch-22.04 [skip ci]|
|[#4540](https://github.com/NVIDIA/spark-rapids/pull/4540)|Remove user kuhushukla|
|[#4434](https://github.com/NVIDIA/spark-rapids/pull/4434)|Support max on single-level struct in aggregation context|
|[#4534](https://github.com/NVIDIA/spark-rapids/pull/4534)|Temporarily disable integration test - test_hash_reduction_pivot_without_nans|
|[#4322](https://github.com/NVIDIA/spark-rapids/pull/4322)|Add an explain only mode to the plugin|
|[#4497](https://github.com/NVIDIA/spark-rapids/pull/4497)|Make better use of pinned memory pool|
|[#4512](https://github.com/NVIDIA/spark-rapids/pull/4512)|remove hadoop version requirement[skip ci]|
|[#4527](https://github.com/NVIDIA/spark-rapids/pull/4527)|Fall back to CPU for regular expressions containing \D or \W|
|[#4525](https://github.com/NVIDIA/spark-rapids/pull/4525)|Properly close data writer in GpuFileFormatWriter|
|[#4502](https://github.com/NVIDIA/spark-rapids/pull/4502)|Removed the redundant test for element_at and fixed the failing one|
|[#4523](https://github.com/NVIDIA/spark-rapids/pull/4523)|Add more integration tests for decimal 128|
|[#3762](https://github.com/NVIDIA/spark-rapids/pull/3762)|Call the right method to convert table from row major <=> col major|
|[#4482](https://github.com/NVIDIA/spark-rapids/pull/4482)|Simplified the construction of zero scalar in GpuUnaryMinus|
|[#4510](https://github.com/NVIDIA/spark-rapids/pull/4510)|Update copyright in NOTICE [skip ci]|
|[#4484](https://github.com/NVIDIA/spark-rapids/pull/4484)|Update GpuFileFormatWriter to stay in sync with recent Spark changes, but still not support writing Hive bucketed table on GPU.|
|[#4492](https://github.com/NVIDIA/spark-rapids/pull/4492)|Fall back to CPU for regular expressions containing hex digits|
|[#4495](https://github.com/NVIDIA/spark-rapids/pull/4495)|Enable approx_percentile by default|
|[#4420](https://github.com/NVIDIA/spark-rapids/pull/4420)|Fix up incorrect results of rounding past the max digits of data type|
|[#4483](https://github.com/NVIDIA/spark-rapids/pull/4483)|Update test case of reading nested unsigned parquet file|
|[#4490](https://github.com/NVIDIA/spark-rapids/pull/4490)|Remove warning about RMM default allocator|
|[#4461](https://github.com/NVIDIA/spark-rapids/pull/4461)|[Audit] Add bucketed scan info in query plan of data source v1|
|[#4489](https://github.com/NVIDIA/spark-rapids/pull/4489)|Add arrays of decimal128 to join tests|
|[#4476](https://github.com/NVIDIA/spark-rapids/pull/4476)|Don't acquire the semaphore for empty input while scanning|
|[#4424](https://github.com/NVIDIA/spark-rapids/pull/4424)|Improve support for regular expression string anchors `\A`, `\Z`, and `\z`|
|[#4491](https://github.com/NVIDIA/spark-rapids/pull/4491)|Skip the test for spark versions 3.1.1, 3.1.2 and 3.2.0 only|
|[#4459](https://github.com/NVIDIA/spark-rapids/pull/4459)|Use merge sort for struct types in non-key columns|
|[#4494](https://github.com/NVIDIA/spark-rapids/pull/4494)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#4400](https://github.com/NVIDIA/spark-rapids/pull/4400)|Enable approx percentile tests|
|[#4471](https://github.com/NVIDIA/spark-rapids/pull/4471)|Disable orc write by default because of https://issues.apache.org/jira/browse/ORC-1075|
|[#4462](https://github.com/NVIDIA/spark-rapids/pull/4462)|Rename DECIMAL_128_FULL and rework usage of TypeSig.gpuNumeric|
|[#4479](https://github.com/NVIDIA/spark-rapids/pull/4479)|Change signoff check image to slim-buster [skip ci]|
|[#4464](https://github.com/NVIDIA/spark-rapids/pull/4464)|Throw SparkArrayIndexOutOfBoundsException for Spark 3.3.0+|
|[#4469](https://github.com/NVIDIA/spark-rapids/pull/4469)|Support repetition of \d and \D in regexp functions|
|[#4472](https://github.com/NVIDIA/spark-rapids/pull/4472)|Modify docs for 22.02 to address issue-4319[skip ci]|
|[#4440](https://github.com/NVIDIA/spark-rapids/pull/4440)|Enable GPU broadcast exchange reuse for DPP when AQE enabled|
|[#4376](https://github.com/NVIDIA/spark-rapids/pull/4376)|Add sequence support|
|[#4460](https://github.com/NVIDIA/spark-rapids/pull/4460)|Abstract the text based PartitionReader|
|[#4383](https://github.com/NVIDIA/spark-rapids/pull/4383)|Fix correctness issue with CASE WHEN with expressions that have side-effects|
|[#4465](https://github.com/NVIDIA/spark-rapids/pull/4465)|Refactor for shims 320+|
|[#4463](https://github.com/NVIDIA/spark-rapids/pull/4463)|Avoid replacing a hash join if build side is unsupported by the join type|
|[#4456](https://github.com/NVIDIA/spark-rapids/pull/4456)|Fix build issues: 1 clean non-exists target dirs; 2 remove duplicated plugin|
|[#4416](https://github.com/NVIDIA/spark-rapids/pull/4416)|Unshim join execs|
|[#4172](https://github.com/NVIDIA/spark-rapids/pull/4172)|Support String to Decimal 128|
|[#4458](https://github.com/NVIDIA/spark-rapids/pull/4458)|Exclude some metadata operators when checking GPU replacement|
|[#4451](https://github.com/NVIDIA/spark-rapids/pull/4451)|Some metrics improvements and timeline reporting|
|[#4435](https://github.com/NVIDIA/spark-rapids/pull/4435)|Disable add profile src execution by default to make the build log clean|
|[#4436](https://github.com/NVIDIA/spark-rapids/pull/4436)|Print error log to stderr output|
|[#4155](https://github.com/NVIDIA/spark-rapids/pull/4155)|Add partial support for line begin and end anchors in regexp_replace|
|[#4428](https://github.com/NVIDIA/spark-rapids/pull/4428)|Exhaustively iterate ColumnarToRow iterator to avoid leaks|
|[#4430](https://github.com/NVIDIA/spark-rapids/pull/4430)|update pca example link in ml-integration.md[skip ci]|
|[#4452](https://github.com/NVIDIA/spark-rapids/pull/4452)|Limit parallelism of nightly tests [skip ci]|
|[#4449](https://github.com/NVIDIA/spark-rapids/pull/4449)|Add recursive type checking and fallback tests for casting array with unsupported element types to string|
|[#4437](https://github.com/NVIDIA/spark-rapids/pull/4437)|Change logInfo to logWarning|
|[#4447](https://github.com/NVIDIA/spark-rapids/pull/4447)|Fix 330 build error and add 322 shims layer|
|[#4417](https://github.com/NVIDIA/spark-rapids/pull/4417)|Fix an Intellij debug issue|
|[#4431](https://github.com/NVIDIA/spark-rapids/pull/4431)|Add DateType support for AST expressions|
|[#4433](https://github.com/NVIDIA/spark-rapids/pull/4433)|Import the right pandas from conda [skip ci]|
|[#4419](https://github.com/NVIDIA/spark-rapids/pull/4419)|Import the right pandas from conda|
|[#4427](https://github.com/NVIDIA/spark-rapids/pull/4427)|Update getFileScanRDD shim for recent changes in Spark 3.3.0|
|[#4397](https://github.com/NVIDIA/spark-rapids/pull/4397)|Ignore cufile.log|
|[#4388](https://github.com/NVIDIA/spark-rapids/pull/4388)|Add support for ReplicateRows|
|[#4399](https://github.com/NVIDIA/spark-rapids/pull/4399)|Update docs for Profiling and Qualification tool to change wording|
|[#4407](https://github.com/NVIDIA/spark-rapids/pull/4407)|Fix GpuSubqueryBroadcast on multi-fields relation|
|[#4396](https://github.com/NVIDIA/spark-rapids/pull/4396)|GpuShuffleCoalesceIterator acquire semaphore after host concat|
|[#4361](https://github.com/NVIDIA/spark-rapids/pull/4361)|Accommodate altered semantics of `cudf::lists::contains()`|
|[#4394](https://github.com/NVIDIA/spark-rapids/pull/4394)|Use correct column name in GpuIf test|
|[#4385](https://github.com/NVIDIA/spark-rapids/pull/4385)|Add missing GpuSubqueryBroadcast replacement rule for spark31x |
|[#4387](https://github.com/NVIDIA/spark-rapids/pull/4387)|Fix auto merge conflict 4384[skip ci]|
|[#4374](https://github.com/NVIDIA/spark-rapids/pull/4374)|Fix the IT module depends on the tests module|
|[#4365](https://github.com/NVIDIA/spark-rapids/pull/4365)|Not publishing integration_tests jar to Maven Central [skip ci]|
|[#4358](https://github.com/NVIDIA/spark-rapids/pull/4358)|Update GpuIf to support expressions with side effects|
|[#4382](https://github.com/NVIDIA/spark-rapids/pull/4382)|Remove unused scallop dependency from integration_tests|
|[#4364](https://github.com/NVIDIA/spark-rapids/pull/4364)|Replace Scala document with Scala comment for inner functions|
|[#4373](https://github.com/NVIDIA/spark-rapids/pull/4373)|Add pytest tags for nightly test parallel run [skip ci]|
|[#4150](https://github.com/NVIDIA/spark-rapids/pull/4150)|Support GpuSubqueryBroadcast for DPP|
|[#4372](https://github.com/NVIDIA/spark-rapids/pull/4372)|Move casting to string tests from array_test.py and struct_test.py to cast_test.py|
|[#4371](https://github.com/NVIDIA/spark-rapids/pull/4371)|Fix typo in skipTestsFor330 calculation [skip ci]|
|[#4355](https://github.com/NVIDIA/spark-rapids/pull/4355)|Dedicated deploy-file with reduced pom in nightly build [skip ci]|
|[#4352](https://github.com/NVIDIA/spark-rapids/pull/4352)|Revert "Ignore failing string to timestamp tests temporarily (#4197)"|
|[#4359](https://github.com/NVIDIA/spark-rapids/pull/4359)|Audit - SPARK-37268 - Remove unused variable in GpuFileScanRDD [Databricks]|
|[#4327](https://github.com/NVIDIA/spark-rapids/pull/4327)|Print meaningful message when calling scripts in maven|
|[#4354](https://github.com/NVIDIA/spark-rapids/pull/4354)|Fix regression in AQE optimizations|
|[#4343](https://github.com/NVIDIA/spark-rapids/pull/4343)|Fix issue with binding to hash agg columns with computation|
|[#4285](https://github.com/NVIDIA/spark-rapids/pull/4285)|Add support for regexp_extract on the GPU|
|[#4349](https://github.com/NVIDIA/spark-rapids/pull/4349)|Fix PYTHONPATH in pre-merge|
|[#4269](https://github.com/NVIDIA/spark-rapids/pull/4269)|The option for the nightly script not deploying jars [skip ci]|
|[#4335](https://github.com/NVIDIA/spark-rapids/pull/4335)|Fix the issue of exporting Column RDD|
|[#4336](https://github.com/NVIDIA/spark-rapids/pull/4336)|Split expensive pytest files in cases level [skip ci]|
|[#4328](https://github.com/NVIDIA/spark-rapids/pull/4328)|Change the explanation of why the operator will not work on GPU|
|[#4338](https://github.com/NVIDIA/spark-rapids/pull/4338)|Use scala Int.box instead of Integer constructors |
|[#4340](https://github.com/NVIDIA/spark-rapids/pull/4340)|Remove the unnecessary parameter `dataType` in `resolveColumnVector` method|
|[#4256](https://github.com/NVIDIA/spark-rapids/pull/4256)|Allow returning an EmptyHashedRelation when a broadcast result is empty|
|[#4333](https://github.com/NVIDIA/spark-rapids/pull/4333)|Add tests about writing empty table to ORC/PAQUET|
|[#4337](https://github.com/NVIDIA/spark-rapids/pull/4337)|Support GpuFirst and GpuLast on nested types under reduction aggregations|
|[#4331](https://github.com/NVIDIA/spark-rapids/pull/4331)|Fix parquet options builder calls|
|[#4310](https://github.com/NVIDIA/spark-rapids/pull/4310)|Fix typo in shim class name|
|[#4326](https://github.com/NVIDIA/spark-rapids/pull/4326)|Fix 4315 decrease concurrentGpuTasks to avoid sum test OOM|
|[#4266](https://github.com/NVIDIA/spark-rapids/pull/4266)|Check revisions for all shim jars while build all|
|[#4282](https://github.com/NVIDIA/spark-rapids/pull/4282)|Use data type to create an inspector for a foldable GPU expression.|
|[#3144](https://github.com/NVIDIA/spark-rapids/pull/3144)|Optimize AQE with Spark 3.2+ to avoid redundant transitions|
|[#4317](https://github.com/NVIDIA/spark-rapids/pull/4317)|[BUG] Update nightly test script to dynamically set mem_fraction [skip ci]|
|[#4206](https://github.com/NVIDIA/spark-rapids/pull/4206)|Porting GpuRowToColumnar converters to InternalColumnarRDDConverter|
|[#4272](https://github.com/NVIDIA/spark-rapids/pull/4272)|Full support for SUM overflow detection on decimal|
|[#4255](https://github.com/NVIDIA/spark-rapids/pull/4255)|Make regexp pattern `[^a]` consistent with Spark for multiline strings|
|[#4306](https://github.com/NVIDIA/spark-rapids/pull/4306)|Revert commonizing the int96ParquetRebase* functions |
|[#4299](https://github.com/NVIDIA/spark-rapids/pull/4299)|Fix auto merge conflict 4298 [skip ci]|
|[#4159](https://github.com/NVIDIA/spark-rapids/pull/4159)|Optimize sample perf|
|[#4235](https://github.com/NVIDIA/spark-rapids/pull/4235)|Commonize v2 shim|
|[#4274](https://github.com/NVIDIA/spark-rapids/pull/4274)|Add tests for timestamps that overflowed before.|
|[#4271](https://github.com/NVIDIA/spark-rapids/pull/4271)|Skip test_regexp_replace_null_pattern_fallback on Spark 3.1.1 and later|
|[#4278](https://github.com/NVIDIA/spark-rapids/pull/4278)|Use mamba for cudf conda install [skip ci]|
|[#4270](https://github.com/NVIDIA/spark-rapids/pull/4270)|Document exponent differences when casting floating point to string [skip ci]|
|[#4268](https://github.com/NVIDIA/spark-rapids/pull/4268)|Fix merge conflict with branch-21.12|
|[#4093](https://github.com/NVIDIA/spark-rapids/pull/4093)|Add tests for regexp() and regexp_like()|
|[#4259](https://github.com/NVIDIA/spark-rapids/pull/4259)|fix regression in cast from string to float that caused signed NaN to be considered valid|
|[#4241](https://github.com/NVIDIA/spark-rapids/pull/4241)|fix bug in parsing regex character classes that start with `^` and contain an unescaped `]`|
|[#4224](https://github.com/NVIDIA/spark-rapids/pull/4224)|Support row-based Hive UDFs|
|[#4221](https://github.com/NVIDIA/spark-rapids/pull/4221)|GpuCast from ArrayType to StringType|
|[#4007](https://github.com/NVIDIA/spark-rapids/pull/4007)|Implement duplicate key handling for GpuCreateMap|
|[#4251](https://github.com/NVIDIA/spark-rapids/pull/4251)|Skip test_regexp_replace_null_pattern_fallback on Databricks|
|[#4247](https://github.com/NVIDIA/spark-rapids/pull/4247)|Disable failing CastOpSuite test|
|[#4239](https://github.com/NVIDIA/spark-rapids/pull/4239)|Make EOL anchor behavior match CPU for strings ending with newline|
|[#4153](https://github.com/NVIDIA/spark-rapids/pull/4153)|Regexp: Only transpile once per expression rather than once per batch|
|[#4230](https://github.com/NVIDIA/spark-rapids/pull/4230)|Change to build tools module with all the versions by default|
|[#4223](https://github.com/NVIDIA/spark-rapids/pull/4223)|Fixes a minor deprecation warning|
|[#4215](https://github.com/NVIDIA/spark-rapids/pull/4215)|Rebalance testing load|
|[#4214](https://github.com/NVIDIA/spark-rapids/pull/4214)|Fix pre_merge ci_2 [skip ci]|
|[#4212](https://github.com/NVIDIA/spark-rapids/pull/4212)|Remove an unused method with its outdated comment|
|[#4211](https://github.com/NVIDIA/spark-rapids/pull/4211)|Update test_floor_ceil_overflow to be more lenient on exception type|
|[#4203](https://github.com/NVIDIA/spark-rapids/pull/4203)|Move all the GpuShuffleExchangeExec shim v2 classes to org.apache.spark|
|[#4193](https://github.com/NVIDIA/spark-rapids/pull/4193)|Rename 311until320-apache to 311until320-noncdh|
|[#4197](https://github.com/NVIDIA/spark-rapids/pull/4197)|Ignore failing string to timestamp tests temporarily|
|[#4160](https://github.com/NVIDIA/spark-rapids/pull/4160)|Fix merge issues for branch 22.02|
|[#4081](https://github.com/NVIDIA/spark-rapids/pull/4081)|Convert String to DecimalType without casting to FloatType|
|[#4132](https://github.com/NVIDIA/spark-rapids/pull/4132)|Fix auto merge conflict 4131 [skip ci]|
|[#4099](https://github.com/NVIDIA/spark-rapids/pull/4099)|[REVIEW] Init version 22.02.0|
|[#4113](https://github.com/NVIDIA/spark-rapids/pull/4113)|Fix pre-merge CI 2 conditions [skip ci]|
|[#4064](https://github.com/NVIDIA/spark-rapids/pull/4064)|Regex: transpile `.` to `[^\r\n]` in cuDF|
|[#4044](https://github.com/NVIDIA/spark-rapids/pull/4044)|RLike: Fall back to CPU for regex that would produce incorrect results|

## Release 21.12

### Features
|||
|:---|:---|
|[#1571](https://github.com/NVIDIA/spark-rapids/issues/1571)|[FEA] Better precision range for decimal multiply, and possibly others|
|[#3953](https://github.com/NVIDIA/spark-rapids/issues/3953)|[FEA] Audit: Add array support to union by name |
|[#4085](https://github.com/NVIDIA/spark-rapids/issues/4085)|[FEA] Decimal 128 Support: Concat|
|[#4073](https://github.com/NVIDIA/spark-rapids/issues/4073)|[FEA] Decimal 128 Support: MapKeys, MapValues, MapEntries|
|[#3432](https://github.com/NVIDIA/spark-rapids/issues/3432)|[FEA] Qualification tool checks if there is any "Scan JDBCRelation" and count it as "problematic"|
|[#3824](https://github.com/NVIDIA/spark-rapids/issues/3824)|[FEA] Support MapType in ParquetCachedBatchSerializer|
|[#4048](https://github.com/NVIDIA/spark-rapids/issues/4048)|[FEA] WindowExpression support for Decimal 128 in Spark 320|
|[#4047](https://github.com/NVIDIA/spark-rapids/issues/4047)|[FEA] Literal support for Decimal 128 in Spark 320|
|[#3863](https://github.com/NVIDIA/spark-rapids/issues/3863)|[FEA] Add Spark 3.3.0-SNAPSHOT Shim |
|[#3814](https://github.com/NVIDIA/spark-rapids/issues/3814)|[FEA] stddev stddev_samp and std should be supported over a window|
|[#3370](https://github.com/NVIDIA/spark-rapids/issues/3370)|[FEA] Add support for Databricks 9.1 runtime|
|[#3876](https://github.com/NVIDIA/spark-rapids/issues/3876)|[FEA] Support REGEXP_REPLACE to replace null values|
|[#3784](https://github.com/NVIDIA/spark-rapids/issues/3784)|[FEA] Support ORC write Map column(single level)|
|[#3470](https://github.com/NVIDIA/spark-rapids/issues/3470)|[FEA] Add shims for 3.2.1-SNAPSHOT|
|[#3855](https://github.com/NVIDIA/spark-rapids/issues/3855)|[FEA] CPU based UDF to run efficiently and transfer data back to GPU for supported operations|
|[#3739](https://github.com/NVIDIA/spark-rapids/issues/3739)|[FEA] Provide an explicit config for fallback on CPU if plan rewrite fails|
|[#3888](https://github.com/NVIDIA/spark-rapids/issues/3888)|[FEA] Decimal 128 Support: Add a "Trust me I know it will not overflow config"|
|[#3088](https://github.com/NVIDIA/spark-rapids/issues/3088)|[FEA] Profile tool print problematic operations|
|[#3886](https://github.com/NVIDIA/spark-rapids/issues/3886)|[FEA] Decimal 128 Support: Extend the range for Decimal Multiply and Divide|
|[#79](https://github.com/NVIDIA/spark-rapids/issues/79)|[FEA] Support Size operation|
|[#3880](https://github.com/NVIDIA/spark-rapids/issues/3880)|[FEA] Decimal 128 Support: Average aggregation|
|[#3659](https://github.com/NVIDIA/spark-rapids/issues/3659)|[FEA] External tool integration with Qualification tool|
|[#2](https://github.com/NVIDIA/spark-rapids/issues/2)|[FEA] RLIKE support|
|[#3192](https://github.com/NVIDIA/spark-rapids/issues/3192)|[FEA] Support decimal type in ORC writer|
|[#3419](https://github.com/NVIDIA/spark-rapids/issues/3419)|[FEA] Add support for org.apache.spark.sql.execution.SampleExec|
|[#3535](https://github.com/NVIDIA/spark-rapids/issues/3535)|[FEA] Qualification tool can detect RDD APIs in SQL plan|
|[#3494](https://github.com/NVIDIA/spark-rapids/issues/3494)|[FEA] Support structs in ORC writer|
|[#3514](https://github.com/NVIDIA/spark-rapids/issues/3514)|[FEA] Support collect_set on struct in aggregation context|
|[#3515](https://github.com/NVIDIA/spark-rapids/issues/3515)|[FEA] Support CreateArray to produce array(struct)|
|[#3116](https://github.com/NVIDIA/spark-rapids/issues/3116)|[FEA] Support Maps, Lists, and Structs as non-key columns on joins|
|[#2054](https://github.com/NVIDIA/spark-rapids/issues/2054)|[FEA] Add support for Arrays to ParquetCachedBatchSerializer|
|[#3573](https://github.com/NVIDIA/spark-rapids/issues/3573)|[FEA] Support Cache(PCBS) Array-of-Struct|

### Performance
|||
|:---|:---|
|[#3768](https://github.com/NVIDIA/spark-rapids/issues/3768)|[DOC] document databricks init script required for UCX|
|[#2867](https://github.com/NVIDIA/spark-rapids/issues/2867)|[FEA] Make LZ4_CHUNK_SIZE configurable|
|[#3832](https://github.com/NVIDIA/spark-rapids/issues/3832)|[FEA] AST enabled GpuBroadcastNestedLoopJoin left side can't be small|
|[#3798](https://github.com/NVIDIA/spark-rapids/issues/3798)|[FEA]  bounds checking in joins can be expensive|
|[#3603](https://github.com/NVIDIA/spark-rapids/issues/3603)|[FEA] Allocate UCX bounce buffers outside of RMM if ASYNC allocator is enabled|

### Bugs Fixed
|||
|:---|:---|
|[#4253](https://github.com/NVIDIA/spark-rapids/issues/4253)|[BUG] Dependencies missing of spark-rapids v21.12.0 release jars|
|[#4216](https://github.com/NVIDIA/spark-rapids/issues/4216)|[BUG] AQE Crashing Spark RAPIDS when using filter() and union()|
|[#4188](https://github.com/NVIDIA/spark-rapids/issues/4188)|[BUG] data corruption in GpuBroadcastNestedLoopJoin with empty relations edge case|
|[#4191](https://github.com/NVIDIA/spark-rapids/issues/4191)|[BUG] failed to read DECIMAL128 within MapType from ORC|
|[#4175](https://github.com/NVIDIA/spark-rapids/issues/4175)|[BUG] arithmetic_ops_test failed in spark 3.2.0|
|[#4162](https://github.com/NVIDIA/spark-rapids/issues/4162)|[BUG] isCastDecimalToStringEnabled is never called|
|[#3894](https://github.com/NVIDIA/spark-rapids/issues/3894)|[BUG] test_pandas_scalar_udf and test_pandas_map_udf failed in UCX standalone CI run|
|[#3970](https://github.com/NVIDIA/spark-rapids/issues/3970)|[BUG] mismatching timezone settings on executor and driver can cause ORC read data corruption|
|[#4141](https://github.com/NVIDIA/spark-rapids/issues/4141)|[BUG] Unable to start the RapidsShuffleManager in databricks 9.1|
|[#4102](https://github.com/NVIDIA/spark-rapids/issues/4102)|[BUG] udf-example build failed: Unknown CMake command "cpm_check_if_package_already_added".|
|[#4084](https://github.com/NVIDIA/spark-rapids/issues/4084)|[BUG] window on unbounded preceeding and unbounded following can produce incorrect results.|
|[#3990](https://github.com/NVIDIA/spark-rapids/issues/3990)|[BUG] Scaladoc link warnings in ParquetCachedBatchSerializer and ExplainPlan|
|[#4108](https://github.com/NVIDIA/spark-rapids/issues/4108)|[BUG] premerge fails due to Spark 3.3.0 HadoopFsRelation after SPARK-37289|
|[#4042](https://github.com/NVIDIA/spark-rapids/issues/4042)|[BUG] cudf_udf tests fail on nightly Integration test run|
|[#3743](https://github.com/NVIDIA/spark-rapids/issues/3743)|[BUG] Implicitly catching all exceptions warning in GpuOverrides|
|[#4069](https://github.com/NVIDIA/spark-rapids/issues/4069)|[BUG] parquet_test.py pytests FAILED on Databricks-9.1-ML-spark-3.1.2|
|[#3461](https://github.com/NVIDIA/spark-rapids/issues/3461)|[BUG] Cannot build project from a sub-directory|
|[#4053](https://github.com/NVIDIA/spark-rapids/issues/4053)|[BUG] buildall uses a stale aggregator dependency during test compilation|
|[#3703](https://github.com/NVIDIA/spark-rapids/issues/3703)|[BUG] test_hash_groupby_approx_percentile_long_repeated_keys failed with TypeError|
|[#3706](https://github.com/NVIDIA/spark-rapids/issues/3706)|[BUG] approx_percentile returns array of zero percentiles instead of null in some cases|
|[#4017](https://github.com/NVIDIA/spark-rapids/issues/4017)|[BUG] Why is the hash aggregate not handling empty result expressions|
|[#3994](https://github.com/NVIDIA/spark-rapids/issues/3994)|[BUG] can't open notebook 'docs/demo/GCP/mortgage-xgboost4j-gpu-scala.ipynb'|
|[#3996](https://github.com/NVIDIA/spark-rapids/issues/3996)|[BUG] Exception happened when getting a null row|
|[#3999](https://github.com/NVIDIA/spark-rapids/issues/3999)|[BUG] Integration cache_test failures - ArrayIndexOutOfBoundsException|
|[#3532](https://github.com/NVIDIA/spark-rapids/issues/3532)|[BUG] DatabricksShimVersion must carry runtime version info|
|[#3834](https://github.com/NVIDIA/spark-rapids/issues/3834)|[BUG] Approx_percentile deserialize error when calling "show" rather than "collect"|
|[#3992](https://github.com/NVIDIA/spark-rapids/issues/3992)|[BUG] failed create-parallel-world in databricks build|
|[#3987](https://github.com/NVIDIA/spark-rapids/issues/3987)|[BUG] "mvn clean package -DskipTests" is no longer working|
|[#3866](https://github.com/NVIDIA/spark-rapids/issues/3866)|[BUG] RLike integration tests failing on Azure Databricks 7.3|
|[#3980](https://github.com/NVIDIA/spark-rapids/issues/3980)|[BUG] udf-example build failed due to maven-antrun-plugin upgrade|
|[#3966](https://github.com/NVIDIA/spark-rapids/issues/3966)|[BUG] udf-examples module fails on `mvn compile` and `mvn test`|
|[#3977](https://github.com/NVIDIA/spark-rapids/issues/3977)|[BUG] databricks aggregator jar deployed failed|
|[#3915](https://github.com/NVIDIA/spark-rapids/issues/3915)|[BUG] typo in verify_same_sha_for_unshimmed prevents the offending class file name from being logged. |
|[#1304](https://github.com/NVIDIA/spark-rapids/issues/1304)|[BUG] Query fails with HostColumnarToGpu doesn't support Structs|
|[#3924](https://github.com/NVIDIA/spark-rapids/issues/3924)|[BUG] ExpressionEncoder does not work for input in `GpuScalaUDF` |
|[#3911](https://github.com/NVIDIA/spark-rapids/issues/3911)|[BUG] CI fails on an inconsistent set of partial builds|
|[#2896](https://github.com/NVIDIA/spark-rapids/issues/2896)|[BUG] Extra GpuColumnarToRow when using ParquetCachedBatchSerializer on databricks|
|[#3864](https://github.com/NVIDIA/spark-rapids/issues/3864)|[BUG] test_sample_produce_empty_batch failed in dataproc|
|[#3823](https://github.com/NVIDIA/spark-rapids/issues/3823)|[BUG]binary-dedup.sh script fails on mac|
|[#3658](https://github.com/NVIDIA/spark-rapids/issues/3658)|[BUG] DataFrame actions failing with error: Error : java.lang.NoClassDefFoundError: Could not initialize class com.nvidia.spark.rapids.GpuOverrides withlatest 21.10 jars|
|[#3857](https://github.com/NVIDIA/spark-rapids/issues/3857)|[BUG] nightly build push dist packge w/ single version of spark|
|[#3854](https://github.com/NVIDIA/spark-rapids/issues/3854)|[BUG] not found: type PoissonDistribution in databricks build|
|[#3852](https://github.com/NVIDIA/spark-rapids/issues/3852)|spark-nightly-build deploys all modules due to typo in `-pl`|
|[#3844](https://github.com/NVIDIA/spark-rapids/issues/3844)|[BUG] nightly spark311cdh build failed|
|[#3843](https://github.com/NVIDIA/spark-rapids/issues/3843)|[BUG] databricks nightly deploy failed|
|[#3705](https://github.com/NVIDIA/spark-rapids/issues/3705)|[BUG] Change `nullOnDivideByZero` from runtime parameter to aggregate expression for `stddev` and `variance` aggregation families|
|[#3614](https://github.com/NVIDIA/spark-rapids/issues/3614)|[BUG] ParquetMaterializer.scala appears in both v1 and v2 shims|
|[#3430](https://github.com/NVIDIA/spark-rapids/issues/3430)|[BUG] Profiling tool silently stops without producing any output on a Synapse Spark event log|
|[#3311](https://github.com/NVIDIA/spark-rapids/issues/3311)|[BUG] cache_test.py failed w/ cache.serializer in spark 3.1.2|
|[#3710](https://github.com/NVIDIA/spark-rapids/issues/3710)|[BUG] Usage of Class.forName without specifying a classloader|
|[#3462](https://github.com/NVIDIA/spark-rapids/issues/3462)|[BUG] IDE complains about duplicate ShimBasePythonRunner instances|
|[#3476](https://github.com/NVIDIA/spark-rapids/issues/3476)|[BUG] test_non_empty_ctas fails on yarn|

### PRs
|||
|:---|:---|
|[#4362](https://github.com/NVIDIA/spark-rapids/pull/4362)|Decimal128 support for Parquet|
|[#4391](https://github.com/NVIDIA/spark-rapids/pull/4391)|update gcp custom dataproc image version to avoid log4j issue[skip ci]|
|[#4379](https://github.com/NVIDIA/spark-rapids/pull/4379)|update hot fix cudf link v21.12.2|
|[#4367](https://github.com/NVIDIA/spark-rapids/pull/4367)|update 21.12 branch for doc [skip ci]|
|[#4245](https://github.com/NVIDIA/spark-rapids/pull/4245)|Update changelog 21.12 to latest [skip ci]|
|[#4258](https://github.com/NVIDIA/spark-rapids/pull/4258)|Sanitize column names in ParquetCachedBatchSerializer before writing to Parquet|
|[#4308](https://github.com/NVIDIA/spark-rapids/pull/4308)|Bump up GPU reserve memory to 640MB|
|[#4307](https://github.com/NVIDIA/spark-rapids/pull/4307)|Update Download page for 21.12 [skip ci]|
|[#4261](https://github.com/NVIDIA/spark-rapids/pull/4261)|Update cudfjni version to released 21.12.0|
|[#4265](https://github.com/NVIDIA/spark-rapids/pull/4265)|Remove aggregator dependency before deploying dist artifact|
|[#4030](https://github.com/NVIDIA/spark-rapids/pull/4030)|Support code coverage report with single version jar [skip ci]|
|[#4287](https://github.com/NVIDIA/spark-rapids/pull/4287)|Update 21.12 compatibility guide for known regexp issue [skip ci]|
|[#4242](https://github.com/NVIDIA/spark-rapids/pull/4242)|Fix indentation issue in getting-started-k8s guide [skip ci]|
|[#4263](https://github.com/NVIDIA/spark-rapids/pull/4263)|Add missing ORC write tests on Map of Decimal|
|[#4257](https://github.com/NVIDIA/spark-rapids/pull/4257)|Implement getShuffleRDD and fixup mismatched output types on shuffle reuse|
|[#4250](https://github.com/NVIDIA/spark-rapids/pull/4250)|Update the release script [skip ci]|
|[#4222](https://github.com/NVIDIA/spark-rapids/pull/4222)|Add arguments support to 'databricks/run-tests.py'|
|[#4233](https://github.com/NVIDIA/spark-rapids/pull/4233)|Add databricks init script for UCX|
|[#4231](https://github.com/NVIDIA/spark-rapids/pull/4231)|RAPIDS Shuffle Manager fallback if security is enabled|
|[#4228](https://github.com/NVIDIA/spark-rapids/pull/4228)|Fix unconditional nested loop joins on empty tables|
|[#4217](https://github.com/NVIDIA/spark-rapids/pull/4217)|Enable event log for qualification & profiling tools testing from IT|
|[#4202](https://github.com/NVIDIA/spark-rapids/pull/4202)|Parameter for the Databricks zone-id [skip ci]|
|[#4199](https://github.com/NVIDIA/spark-rapids/pull/4199)|modify some words for synapse getting started guide[skip ci]|
|[#4200](https://github.com/NVIDIA/spark-rapids/pull/4200)|Disable approx percentile tests that intermittently fail|
|[#4187](https://github.com/NVIDIA/spark-rapids/pull/4187)|Added a getting started guide for Synapse[skip ci]|
|[#4192](https://github.com/NVIDIA/spark-rapids/pull/4192)|Fix ORC read DECIMAL128 inside MapType|
|[#4173](https://github.com/NVIDIA/spark-rapids/pull/4173)|Update approx percentile docs to link to issue 4060 [skip ci]|
|[#4174](https://github.com/NVIDIA/spark-rapids/pull/4174)|Document Bloop, Metals and VS code as an IDE option [skip ci]|
|[#4181](https://github.com/NVIDIA/spark-rapids/pull/4181)|Fix element_at for 3.2.0 and array/struct cast|
|[#4110](https://github.com/NVIDIA/spark-rapids/pull/4110)|Add a getting started guide on workload qualification [skip ci]|
|[#4106](https://github.com/NVIDIA/spark-rapids/pull/4106)|Add docs for MIG on YARN [skip ci]|
|[#4100](https://github.com/NVIDIA/spark-rapids/pull/4100)|Add PCA example to ml-integration page [skip ci]|
|[#4177](https://github.com/NVIDIA/spark-rapids/pull/4177)|Decimal128: added missing decimal128 signature on Spark 32X|
|[#4161](https://github.com/NVIDIA/spark-rapids/pull/4161)|More integration tests with decimal128|
|[#4165](https://github.com/NVIDIA/spark-rapids/pull/4165)|Fix type checks for get array item in 3.2.0|
|[#4163](https://github.com/NVIDIA/spark-rapids/pull/4163)|Enable config to check for casting decimals to strings|
|[#4154](https://github.com/NVIDIA/spark-rapids/pull/4154)|Use num_slices to guarantee partition shape in the pandas udf tests|
|[#4129](https://github.com/NVIDIA/spark-rapids/pull/4129)|Check executor timezone is same as driver timezone when running on GPU|
|[#4139](https://github.com/NVIDIA/spark-rapids/pull/4139)|Decimal128 Support|
|[#4128](https://github.com/NVIDIA/spark-rapids/pull/4128)|Fix build errors in udf-examples native build|
|[#4063](https://github.com/NVIDIA/spark-rapids/pull/4063)|Regexp_replace support regexp|
|[#4125](https://github.com/NVIDIA/spark-rapids/pull/4125)|Remove unused imports|
|[#4052](https://github.com/NVIDIA/spark-rapids/pull/4052)|Support null safe host column vector|
|[#4116](https://github.com/NVIDIA/spark-rapids/pull/4116)|Add in tests to check for overflow in unbounded window|
|[#4111](https://github.com/NVIDIA/spark-rapids/pull/4111)|Added external doc links for JRE and Spark|
|[#4105](https://github.com/NVIDIA/spark-rapids/pull/4105)|Enforce checks for unused imports and missed interpolation|
|[#4107](https://github.com/NVIDIA/spark-rapids/pull/4107)|Set the task context in background reader threads|
|[#4114](https://github.com/NVIDIA/spark-rapids/pull/4114)|Refactoring cudf_udf test setup|
|[#4109](https://github.com/NVIDIA/spark-rapids/pull/4109)|Stop using redundant partitionSchemaOption dropped in 3.3.0|
|[#4097](https://github.com/NVIDIA/spark-rapids/pull/4097)|Enable auto-merge from branch-21.12 to branch-22.02 [skip ci]|
|[#4094](https://github.com/NVIDIA/spark-rapids/pull/4094)|Remove spark311db shim layer|
|[#4082](https://github.com/NVIDIA/spark-rapids/pull/4082)|Add abfs and abfss to the cloud scheme|
|[#4071](https://github.com/NVIDIA/spark-rapids/pull/4071)|Treat scalac warnings as errors|
|[#4043](https://github.com/NVIDIA/spark-rapids/pull/4043)|Promote cudf as dist direct dependency, mark aggregator provided|
|[#4076](https://github.com/NVIDIA/spark-rapids/pull/4076)|Sets the GPU device id in the UCX early start thread|
|[#4087](https://github.com/NVIDIA/spark-rapids/pull/4087)|Regex parser improvements and bug fixes|
|[#4079](https://github.com/NVIDIA/spark-rapids/pull/4079)|verify "Add array support to union by name " by adding an integration test|
|[#4090](https://github.com/NVIDIA/spark-rapids/pull/4090)|Update pre-merge expression for 2022+ CI [skip ci]|
|[#4049](https://github.com/NVIDIA/spark-rapids/pull/4049)|Change Databricks image from 8.2 to 9.1 [skip ci]|
|[#4051](https://github.com/NVIDIA/spark-rapids/pull/4051)|Upgrade ORC version from 1.5.8 to 1.5.10|
|[#4080](https://github.com/NVIDIA/spark-rapids/pull/4080)|Add case insensitive when clipping parquet blocks|
|[#4083](https://github.com/NVIDIA/spark-rapids/pull/4083)|Fix compiler warning in regex transpiler|
|[#4070](https://github.com/NVIDIA/spark-rapids/pull/4070)|Support building from sub directory|
|[#4072](https://github.com/NVIDIA/spark-rapids/pull/4072)|Fix overflow checking on optimized decimal sum|
|[#4067](https://github.com/NVIDIA/spark-rapids/pull/4067)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#4066](https://github.com/NVIDIA/spark-rapids/pull/4066)|Temply disable cudf_udf test|
|[#4057](https://github.com/NVIDIA/spark-rapids/pull/4057)|Restore original ASL 2.0 license text|
|[#3937](https://github.com/NVIDIA/spark-rapids/pull/3937)|Qualification tool: Detect JDBCRelation in eventlog|
|[#3925](https://github.com/NVIDIA/spark-rapids/pull/3925)|verify AQE and DPP both on|
|[#3982](https://github.com/NVIDIA/spark-rapids/pull/3982)|Fix the issue of parquet reading with case insensitive schema|
|[#4054](https://github.com/NVIDIA/spark-rapids/pull/4054)|Use install for the base version build thread [skip ci]|
|[#4008](https://github.com/NVIDIA/spark-rapids/pull/4008)|[Doc] Update the getting started guide for databricks: Change from 8.2 to 9.1 runtime [skip ci]|
|[#4010](https://github.com/NVIDIA/spark-rapids/pull/4010)|Enable MapType for ParquetCachedBatchSerializer|
|[#4046](https://github.com/NVIDIA/spark-rapids/pull/4046)|lower GPU memory reserve to 256MB|
|[#3770](https://github.com/NVIDIA/spark-rapids/pull/3770)|Enable approx percentile tests|
|[#4038](https://github.com/NVIDIA/spark-rapids/pull/4038)|Change the `catalystConverter` to be a Scala `val`.|
|[#4035](https://github.com/NVIDIA/spark-rapids/pull/4035)|Hash aggregate fix empty resultExpressions|
|[#3998](https://github.com/NVIDIA/spark-rapids/pull/3998)|Check for CPU cores and free memory in IT script|
|[#3984](https://github.com/NVIDIA/spark-rapids/pull/3984)|Check for data write command before inserting hash sort optimization|
|[#4019](https://github.com/NVIDIA/spark-rapids/pull/4019)|initialize RMM with a single pool size|
|[#3993](https://github.com/NVIDIA/spark-rapids/pull/3993)|Qualification tool: Remove "unsupported" word for nested complex types|
|[#4033](https://github.com/NVIDIA/spark-rapids/pull/4033)|skip spark 330 tests temporarily in nightly [skip ci]|
|[#4029](https://github.com/NVIDIA/spark-rapids/pull/4029)|Update buildall script and the build doc [skip ci]|
|[#4014](https://github.com/NVIDIA/spark-rapids/pull/4014)|fix can't open notebook 'docs/demo/GCP/mortgage-xgboost4j-gpu-scala.ipynb'[skip ci]|
|[#4024](https://github.com/NVIDIA/spark-rapids/pull/4024)|Allow using a custom Spark Resource Name for a GPU|
|[#4012](https://github.com/NVIDIA/spark-rapids/pull/4012)|Add Apache Spark 3.3.0-SNAPSHOT Shims|
|[#4021](https://github.com/NVIDIA/spark-rapids/pull/4021)|Explicitly use the public version of ParquetCachedBatchSerializer|
|[#3869](https://github.com/NVIDIA/spark-rapids/pull/3869)|Add Std dev samp for windowing|
|[#3960](https://github.com/NVIDIA/spark-rapids/pull/3960)|Use a fixed RMM pool size|
|[#3767](https://github.com/NVIDIA/spark-rapids/pull/3767)|Add shim for Databricks 9.1|
|[#3862](https://github.com/NVIDIA/spark-rapids/pull/3862)|Prevent approx_percentile aggregate from being split between CPU and GPU|
|[#3871](https://github.com/NVIDIA/spark-rapids/pull/3871)|Add integration test for RLike with embedded null in input|
|[#3968](https://github.com/NVIDIA/spark-rapids/pull/3968)|Allow null character in regexp_replace pattern|
|[#3821](https://github.com/NVIDIA/spark-rapids/pull/3821)|Support ORC write Map column|
|[#3991](https://github.com/NVIDIA/spark-rapids/pull/3991)|Fix aggregator jar copy logic|
|[#3973](https://github.com/NVIDIA/spark-rapids/pull/3973)|Add shims for Apache Spark 3.2.1-SNAPSHOT builds|
|[#3967](https://github.com/NVIDIA/spark-rapids/pull/3967)|Bring back AST support for BNLJ inner joins|
|[#3947](https://github.com/NVIDIA/spark-rapids/pull/3947)|Enable rlike tests on databricks|
|[#3981](https://github.com/NVIDIA/spark-rapids/pull/3981)|Replace tasks w/ target of maven-antrun-plugin in udf-example|
|[#3976](https://github.com/NVIDIA/spark-rapids/pull/3976)|Replace long artifact lists with an ant loop|
|[#3972](https://github.com/NVIDIA/spark-rapids/pull/3972)|Revert udf-examples dependency change to restore test build phase|
|[#3978](https://github.com/NVIDIA/spark-rapids/pull/3978)|Update aggregator jar name in databricks deploy script|
|[#3965](https://github.com/NVIDIA/spark-rapids/pull/3965)|Add how-to resolve auto-merge conflict [skip ci]|
|[#3963](https://github.com/NVIDIA/spark-rapids/pull/3963)|Add a dedicated RapidsConf option to tolerate GpuOverrides apply failures|
|[#3923](https://github.com/NVIDIA/spark-rapids/pull/3923)|Prepare for 3.2.1 shim, various shim build fixes and improvements|
|[#3969](https://github.com/NVIDIA/spark-rapids/pull/3969)|add doc on using compute-sanitizer|
|[#3964](https://github.com/NVIDIA/spark-rapids/pull/3964)|Qualification tool: Catch exception for invalid regex patterns|
|[#3961](https://github.com/NVIDIA/spark-rapids/pull/3961)|Avoid using HostColumnarToGpu for nested types|
|[#3910](https://github.com/NVIDIA/spark-rapids/pull/3910)|Refactor the aggregate API|
|[#3897](https://github.com/NVIDIA/spark-rapids/pull/3897)|Support running CPU based UDF efficiently|
|[#3950](https://github.com/NVIDIA/spark-rapids/pull/3950)|Fix failed auto-merge #3939|
|[#3946](https://github.com/NVIDIA/spark-rapids/pull/3946)|Document compatability of operations with side effects.|
|[#3945](https://github.com/NVIDIA/spark-rapids/pull/3945)|Update udf-examples dependencies to use dist jar|
|[#3938](https://github.com/NVIDIA/spark-rapids/pull/3938)|remove GDS alignment code|
|[#3943](https://github.com/NVIDIA/spark-rapids/pull/3943)|Add artifact revisions check for nightly tests [skip ci]|
|[#3933](https://github.com/NVIDIA/spark-rapids/pull/3933)|Profiling tool: Print potential problems|
|[#3926](https://github.com/NVIDIA/spark-rapids/pull/3926)|Add zip unzip to integration tests dockerfiles [skip ci]|
|[#3757](https://github.com/NVIDIA/spark-rapids/pull/3757)|Update to nvcomp-2.x JNI APIs|
|[#3922](https://github.com/NVIDIA/spark-rapids/pull/3922)|Stop using -U in build merges aggregator jars of nightly [skip ci]|
|[#3907](https://github.com/NVIDIA/spark-rapids/pull/3907)|Add version properties to integration tests modules|
|[#3912](https://github.com/NVIDIA/spark-rapids/pull/3912)|Stop using -U in the build that merges all aggregator jars|
|[#3909](https://github.com/NVIDIA/spark-rapids/pull/3909)|Fix warning when catching all throwables in GpuOverrides|
|[#3766](https://github.com/NVIDIA/spark-rapids/pull/3766)|Use JCudfSerialization to deserialize a table to host columns|
|[#3820](https://github.com/NVIDIA/spark-rapids/pull/3820)|Advertise CPU orderingSatisfies|
|[#3858](https://github.com/NVIDIA/spark-rapids/pull/3858)|update emr 6.4 getting started doc and pic[skip ci]|
|[#3899](https://github.com/NVIDIA/spark-rapids/pull/3899)|Fix sample test cases|
|[#3896](https://github.com/NVIDIA/spark-rapids/pull/3896)|Xfail the sample tests temporarily|
|[#3848](https://github.com/NVIDIA/spark-rapids/pull/3848)|Fix binary-dedupe failures and improve its performance on macOS|
|[#3867](https://github.com/NVIDIA/spark-rapids/pull/3867)|Disable rlike integration tests on Databricks|
|[#3850](https://github.com/NVIDIA/spark-rapids/pull/3850)|Add explain Plugin API for CPU plan|
|[#3868](https://github.com/NVIDIA/spark-rapids/pull/3868)|Fix incorrect schema of nested types of union - audit SPARK-36673|
|[#3860](https://github.com/NVIDIA/spark-rapids/pull/3860)|Add unit test for GpuKryoRegistrator|
|[#3847](https://github.com/NVIDIA/spark-rapids/pull/3847)|Add Running Qualification App API|
|[#3861](https://github.com/NVIDIA/spark-rapids/pull/3861)|Revert "Fix typo in nightly deploy project list (#3853)" [skip ci]|
|[#3796](https://github.com/NVIDIA/spark-rapids/pull/3796)|Add Rlike support|
|[#3856](https://github.com/NVIDIA/spark-rapids/pull/3856)|Fix not found: type PoissonDistribution in databricks build|
|[#3853](https://github.com/NVIDIA/spark-rapids/pull/3853)|Fix typo in nightly deploy project list|
|[#3831](https://github.com/NVIDIA/spark-rapids/pull/3831)|Support decimal type in ORC writer|
|[#3789](https://github.com/NVIDIA/spark-rapids/pull/3789)|GPU sample exec|
|[#3846](https://github.com/NVIDIA/spark-rapids/pull/3846)|Include pluginRepository for cdh build|
|[#3819](https://github.com/NVIDIA/spark-rapids/pull/3819)|Qualification tool: Detect RDD Api's in SQL plan|
|[#3835](https://github.com/NVIDIA/spark-rapids/pull/3835)|Minor cleanup: do not set cuda stream to null|
|[#3845](https://github.com/NVIDIA/spark-rapids/pull/3845)|Include 'DB_SHIM_NAME' from Databricks jar path to fix nightly deploy [skip ci]|
|[#3523](https://github.com/NVIDIA/spark-rapids/pull/3523)|Interpolate spark.version.classifier in build.dir|
|[#3813](https://github.com/NVIDIA/spark-rapids/pull/3813)|Change `nullOnDivideByZero` from runtime parameter to aggregate expression for `stddev` and `variance` aggregations|
|[#3791](https://github.com/NVIDIA/spark-rapids/pull/3791)|Add audit script to get list of commits from Apache Spark master branch|
|[#3744](https://github.com/NVIDIA/spark-rapids/pull/3744)|Add developer documentation for setting up Microk8s [skip ci]|
|[#3817](https://github.com/NVIDIA/spark-rapids/pull/3817)|Fix auto-merge conflict 3816 [skip ci]|
|[#3804](https://github.com/NVIDIA/spark-rapids/pull/3804)|Missing statistics in GpuBroadcastNestedLoopJoin|
|[#3799](https://github.com/NVIDIA/spark-rapids/pull/3799)|Optimize out bounds checking for joins when the gather map has only valid entries|
|[#3801](https://github.com/NVIDIA/spark-rapids/pull/3801)|Update premerge to use the combined snapshots jar |
|[#3696](https://github.com/NVIDIA/spark-rapids/pull/3696)|Support nested types in ORC writer|
|[#3790](https://github.com/NVIDIA/spark-rapids/pull/3790)|Fix overflow when casting integral to neg scale decimal|
|[#3779](https://github.com/NVIDIA/spark-rapids/pull/3779)|Enable some union of structs tests that were marked xfail|
|[#3787](https://github.com/NVIDIA/spark-rapids/pull/3787)|Fix auto-merge conflict 3786 from branch-21.10 [skip ci]|
|[#3782](https://github.com/NVIDIA/spark-rapids/pull/3782)|Fix auto-merge conflict 3781 [skip ci]|
|[#3778](https://github.com/NVIDIA/spark-rapids/pull/3778)|Remove extra ParquetMaterializer.scala file|
|[#3773](https://github.com/NVIDIA/spark-rapids/pull/3773)|Restore disabled ORC and Parquet tests|
|[#3714](https://github.com/NVIDIA/spark-rapids/pull/3714)|Qualification tool: Error handling while processing large event logs|
|[#3758](https://github.com/NVIDIA/spark-rapids/pull/3758)|Temporarily disable timestamp read tests for Parquet and ORC|
|[#3748](https://github.com/NVIDIA/spark-rapids/pull/3748)|Fix merge conflict with branch-21.10|
|[#3700](https://github.com/NVIDIA/spark-rapids/pull/3700)|CollectSet supports structs|
|[#3740](https://github.com/NVIDIA/spark-rapids/pull/3740)|Throw Exception if failure to load ParquetCachedBatchSerializer class|
|[#3726](https://github.com/NVIDIA/spark-rapids/pull/3726)|Replace Class.forName with ShimLoader.loadClass|
|[#3690](https://github.com/NVIDIA/spark-rapids/pull/3690)|Added support for Array[Struct] to GpuCreateArray|
|[#3728](https://github.com/NVIDIA/spark-rapids/pull/3728)|Qualification tool: Fix bug to process correct listeners|
|[#3734](https://github.com/NVIDIA/spark-rapids/pull/3734)|Fix squashed merge from #3725|
|[#3725](https://github.com/NVIDIA/spark-rapids/pull/3725)|Fix merge conflict with branch-21.10|
|[#3680](https://github.com/NVIDIA/spark-rapids/pull/3680)|cudaMalloc UCX bounce buffers when async allocator is used|
|[#3681](https://github.com/NVIDIA/spark-rapids/pull/3681)|Clean up and document metrics|
|[#3674](https://github.com/NVIDIA/spark-rapids/pull/3674)|Move file TestingV2Source.Scala|
|[#3617](https://github.com/NVIDIA/spark-rapids/pull/3617)|Update Version to 21.12.0-SNAPSHOT|
|[#3612](https://github.com/NVIDIA/spark-rapids/pull/3612)|Add support for nested types as non-key columns on joins |
|[#3619](https://github.com/NVIDIA/spark-rapids/pull/3619)|Added support for Array of Structs|

## Release 21.10

### Features
|||
|:---|:---|
|[#1601](https://github.com/NVIDIA/spark-rapids/issues/1601)|[FEA] Support AggregationFunction StddevSamp|
|[#3223](https://github.com/NVIDIA/spark-rapids/issues/3223)|[FEA] Rework the shim layer to robustly handle ABI and API incompatibilities across Spark releases|
|[#13](https://github.com/NVIDIA/spark-rapids/issues/13)|[FEA] Percentile support|
|[#3606](https://github.com/NVIDIA/spark-rapids/issues/3606)|[FEA] Support approx_percentile on GPU with decimal type|
|[#3552](https://github.com/NVIDIA/spark-rapids/issues/3552)|[FEA] extend allowed datatypes for add and multiply in ANSI mode |
|[#3450](https://github.com/NVIDIA/spark-rapids/issues/3450)|[FEA] test the UCX shuffle with the new build changes|
|[#3043](https://github.com/NVIDIA/spark-rapids/issues/3043)|[FEA] Qualification tool: Add support to filter specific configuration values|
|[#3413](https://github.com/NVIDIA/spark-rapids/issues/3413)|[FEA] Add in support for transform_keys|
|[#3297](https://github.com/NVIDIA/spark-rapids/issues/3297)|[FEA] ORC reader supports reading Map columns.|
|[#3367](https://github.com/NVIDIA/spark-rapids/issues/3367)|[FEA] Support GpuRowToColumnConverter on BinaryType|
|[#3380](https://github.com/NVIDIA/spark-rapids/issues/3380)|[FEA] Support CollectList/CollectSet on nested input types in GroupBy aggregation|
|[#1923](https://github.com/NVIDIA/spark-rapids/issues/1923)|[FEA] Fall back to the CPU when LEAD/LAG wants to IGNORE NULLS|
|[#3044](https://github.com/NVIDIA/spark-rapids/issues/3044)|[FEA] Qualification tool: Report the nested data types|
|[#3045](https://github.com/NVIDIA/spark-rapids/issues/3045)|[FEA] Qualification tool: Report the write data formats.|
|[#3224](https://github.com/NVIDIA/spark-rapids/issues/3224)|[FEA] Add maven compile/package plugin executions, one for each supported Spark dependency version|
|[#3047](https://github.com/NVIDIA/spark-rapids/issues/3047)|[FEA] Profiling tool: Structured output format|
|[#2877](https://github.com/NVIDIA/spark-rapids/issues/2877)|[FEA] Support HashAggregate on struct and nested struct|
|[#2916](https://github.com/NVIDIA/spark-rapids/issues/2916)|[FEA] Support GpuCollectList and GpuCollectSet as TypedImperativeAggregate|
|[#463](https://github.com/NVIDIA/spark-rapids/issues/463)|[FEA] Support NESTED_SCHEMA_PRUNING_ENABLED for ORC|
|[#1481](https://github.com/NVIDIA/spark-rapids/issues/1481)|[FEA] ORC Predicate pushdown for Nested fields|
|[#2879](https://github.com/NVIDIA/spark-rapids/issues/2879)|[FEA] ORC reader supports reading Struct columns.|
|[#27](https://github.com/NVIDIA/spark-rapids/issues/27)|[FEA] test current_date and current_timestamp|
|[#3229](https://github.com/NVIDIA/spark-rapids/issues/3229)|[FEA] Improve CreateMap to support multiple key and value expressions|
|[#3111](https://github.com/NVIDIA/spark-rapids/issues/3111)|[FEA] Support conditional nested loop joins|
|[#3177](https://github.com/NVIDIA/spark-rapids/issues/3177)|[FEA] Support decimal type in ORC reader|
|[#3014](https://github.com/NVIDIA/spark-rapids/issues/3014)|[FEA] Add initial support for CreateMap|
|[#3110](https://github.com/NVIDIA/spark-rapids/issues/3110)|[FEA] Support Map as input to explode and pos_explode|
|[#3046](https://github.com/NVIDIA/spark-rapids/issues/3046)|[FEA] Profiling tool: Scale to run large number of event logs.|
|[#3156](https://github.com/NVIDIA/spark-rapids/issues/3156)|[FEA] Support casting struct to struct|
|[#2876](https://github.com/NVIDIA/spark-rapids/issues/2876)|[FEA] Support joins(SHJ and BHJ) on struct as join key with nested struct in the selected column list|
|[#68](https://github.com/NVIDIA/spark-rapids/issues/68)|[FEA] support StringRepeat|
|[#3042](https://github.com/NVIDIA/spark-rapids/issues/3042)|[FEA] Qualification tool: Add conjunction and disjunction filters.|
|[#2615](https://github.com/NVIDIA/spark-rapids/issues/2615)|[FEA] support collect_list and collect_set as groupby aggregation|
|[#2943](https://github.com/NVIDIA/spark-rapids/issues/2943)|[FEA] Support PreciseTimestampConversion when using windowing function|
|[#2878](https://github.com/NVIDIA/spark-rapids/issues/2878)|[FEA] Support Sort on nested struct|
|[#2133](https://github.com/NVIDIA/spark-rapids/issues/2133)|[FEA] Join support for passing MapType columns along when not join keys|
|[#3041](https://github.com/NVIDIA/spark-rapids/issues/3041)|[FEA] Qualification tool: Add filters based on Regex and user name.|
|[#576](https://github.com/NVIDIA/spark-rapids/issues/576)|[FEA] Spark 3.1 orc nested predicate pushdown support|

### Performance
|||
|:---|:---|
|[#3651](https://github.com/NVIDIA/spark-rapids/issues/3651)|[DOC] Point users to UCX 1.11.2|
|[#2370](https://github.com/NVIDIA/spark-rapids/issues/2370)|[FEA] RAPIDS Shuffle Manager enable/disable config|
|[#2923](https://github.com/NVIDIA/spark-rapids/issues/2923)|[FEA] Move to dispatched binops instead of JIT binops|

### Bugs Fixed
|||
|:---|:---|
|[#3929](https://github.com/NVIDIA/spark-rapids/issues/3929)|[BUG] published rapids-4-spark dist artifact references aggregator|
|[#3837](https://github.com/NVIDIA/spark-rapids/issues/3837)|[BUG] Spark-rapids v21.10.0 release candidate jars failed on the OSS validation check.|
|[#3769](https://github.com/NVIDIA/spark-rapids/issues/3769)|[BUG] dedupe fails with  find: './parallel-world/spark301/ ...' No such file or directory|
|[#3783](https://github.com/NVIDIA/spark-rapids/issues/3783)|[BUG] spark-rapids v21.10.0 release build failed on script "dist/scripts/binary-dedupe.sh"|
|[#3775](https://github.com/NVIDIA/spark-rapids/issues/3775)|[BUG] Hash aggregate with structs crashes with IllegalArgumentException|
|[#3704](https://github.com/NVIDIA/spark-rapids/issues/3704)|[BUG] Executor-side ClassCastException when testing with Spark 3.2.1-SNAPSHOT in k8s environment|
|[#3760](https://github.com/NVIDIA/spark-rapids/issues/3760)|[BUG] Databricks class cast exception failure |
|[#3736](https://github.com/NVIDIA/spark-rapids/issues/3736)|[BUG] Crossjoin performance degraded a lot on RAPIDS 21.10 snapshot|
|[#3369](https://github.com/NVIDIA/spark-rapids/issues/3369)|[BUG] UDF compiler can cause crashes with unexpected class input|
|[#3713](https://github.com/NVIDIA/spark-rapids/issues/3713)|[BUG] AQE shuffle coalesce optimization is broken with Spark 3.2|
|[#3720](https://github.com/NVIDIA/spark-rapids/issues/3720)|[BUG] Qualification tool warnings|
|[#3718](https://github.com/NVIDIA/spark-rapids/issues/3718)|[BUG] plugin failing to build for CDH due to missing dependency|
|[#3653](https://github.com/NVIDIA/spark-rapids/issues/3653)|[BUG] Issue seen with AQE on in Q5 (possibly others) using Spark 3.2 rc3|
|[#3686](https://github.com/NVIDIA/spark-rapids/issues/3686)|[BUG] binary-dedupe doesn't fail the build on errors|
|[#3520](https://github.com/NVIDIA/spark-rapids/issues/3520)|[BUG] Scaladoc warnings emitted during build|
|[#3516](https://github.com/NVIDIA/spark-rapids/issues/3516)|[BUG] MultiFileParquetPartitionReader can fail while trying to write the footer|
|[#3648](https://github.com/NVIDIA/spark-rapids/issues/3648)|[BUG] test_cast_decimal_to failing in databricks 7.3|
|[#3670](https://github.com/NVIDIA/spark-rapids/issues/3670)|[BUG] mvn test failed compiling rapids-4-spark-tests-next-spark_2.12|
|[#3640](https://github.com/NVIDIA/spark-rapids/issues/3640)|[BUG] q82 regression after #3288|
|[#3642](https://github.com/NVIDIA/spark-rapids/issues/3642)|[BUG] Shims improperly overridden|
|[#3611](https://github.com/NVIDIA/spark-rapids/issues/3611)|[BUG] test_no_fallback_when_ansi_enabled failed in databricks|
|[#3601](https://github.com/NVIDIA/spark-rapids/issues/3601)|[BUG] Latest 21.10 snapshot jars failing with java.lang.ClassNotFoundException: com.nvidia.spark.rapids.ColumnarRdd with XGBoost|
|[#3589](https://github.com/NVIDIA/spark-rapids/issues/3589)|[BUG] Latest 21.10 snapshot jars failing with java.lang.ClassNotFoundException: com.nvidia.spark.ExclusiveModeGpuDiscoveryPlugin|
|[#3424](https://github.com/NVIDIA/spark-rapids/issues/3424)|[BUG] Aggregations in ANSI mode do not detect overflows|
|[#3592](https://github.com/NVIDIA/spark-rapids/issues/3592)|[BUG] Failed to find data source: com.nvidia.spark.rapids.tests.datasourcev2.parquet.ArrowColumnarDataSourceV2|
|[#3580](https://github.com/NVIDIA/spark-rapids/issues/3580)|[BUG] Class deduplication pulls wrong class for ProxyRapidsShuffleInternalManagerBase|
|[#3331](https://github.com/NVIDIA/spark-rapids/issues/3331)|[BUG] Failed to read file into buffer in `CuFile.readFromFile` in gds standalone test|
|[#3376](https://github.com/NVIDIA/spark-rapids/issues/3376)|[BUG] Unit test failures in Spark 3.2 shim build|
|[#3382](https://github.com/NVIDIA/spark-rapids/issues/3382)|[BUG] Support years with up to 7 digits when casting from String to Date in Spark 3.2|
|[#3266](https://github.com/NVIDIA/spark-rapids/issues/3266)|CDP - Flakiness in JoinSuite in Integration tests|
|[#3415](https://github.com/NVIDIA/spark-rapids/issues/3415)|[BUG] Fix regressions in WindowFunctionSuite with Spark 3.2.0|
|[#3548](https://github.com/NVIDIA/spark-rapids/issues/3548)|[BUG] GpuSum overflow  on 3.2.0+|
|[#3472](https://github.com/NVIDIA/spark-rapids/issues/3472)|[BUG] GpuAdd and GpuMultiply do not include failOnError|
|[#3502](https://github.com/NVIDIA/spark-rapids/issues/3502)|[BUG] Spark 3.2.0 TimeAdd/TimeSub fail due to new DayTimeIntervalType|
|[#3511](https://github.com/NVIDIA/spark-rapids/issues/3511)|[BUG] "Sequence" function fails with "java.lang.UnsupportedOperationException: Not supported on UnsafeArrayData"|
|[#3518](https://github.com/NVIDIA/spark-rapids/issues/3518)|[BUG] Nightly tests failed with RMM outstanding allocations on shutdown|
|[#3383](https://github.com/NVIDIA/spark-rapids/issues/3383)|[BUG] ParseDateTime should not support special dates with Spark 3.2|
|[#3384](https://github.com/NVIDIA/spark-rapids/issues/3384)|[BUG] AQE does not work with Spark 3.2 due to unrecognized GPU partitioning|
|[#3478](https://github.com/NVIDIA/spark-rapids/issues/3478)|[BUG] CastOpSuite and ParseDateTimeSuite failures spark 302 and others|
|[#3495](https://github.com/NVIDIA/spark-rapids/issues/3495)|Fix shim override config|
|[#3482](https://github.com/NVIDIA/spark-rapids/issues/3482)|[BUG] ClassNotFound error when running a job|
|[#1867](https://github.com/NVIDIA/spark-rapids/issues/1867)|[BUG] In Spark 3.2.0 and above dynamic partition pruning and AQE are not mutually exclusive|
|[#3468](https://github.com/NVIDIA/spark-rapids/issues/3468)|[BUG] GpuKryoRegistrator ClassNotFoundException  |
|[#3488](https://github.com/NVIDIA/spark-rapids/issues/3488)|[BUG] databricks 8.2 runtime build failed|
|[#3429](https://github.com/NVIDIA/spark-rapids/issues/3429)|[BUG] test_sortmerge_join_struct_mixed_key_with_null_filter LeftSemi/LeftAnti fails|
|[#3400](https://github.com/NVIDIA/spark-rapids/issues/3400)|[BUG] Canonicalized GPU plans sometimes not consistent when using Spark 3.2|
|[#3440](https://github.com/NVIDIA/spark-rapids/issues/3440)|[BUG] Followup comments from PR3411|
|[#3372](https://github.com/NVIDIA/spark-rapids/issues/3372)|[BUG] 3.2.0 shim: ShuffledBatchRDD.scala:141: match may not be exhaustive.|
|[#3434](https://github.com/NVIDIA/spark-rapids/issues/3434)|[BUG] Fix the unit test failure of KnownNotNull in Scala UDF for Spark 3.2|
|[#3084](https://github.com/NVIDIA/spark-rapids/issues/3084)|[AUDIT] [SPARK-32484][SQL] Fix log info BroadcastExchangeExec.scala|
|[#3463](https://github.com/NVIDIA/spark-rapids/issues/3463)|[BUG] 301+-nondb is named incorrectly|
|[#3435](https://github.com/NVIDIA/spark-rapids/issues/3435)|[BUG] tools - test dsv1 complex and decimal test fails|
|[#3388](https://github.com/NVIDIA/spark-rapids/issues/3388)|[BUG] maven scalastyle checks don't appear to work for alterneate source directories|
|[#3416](https://github.com/NVIDIA/spark-rapids/issues/3416)|[BUG] Resource cleanup issues with Spark 3.2|
|[#3339](https://github.com/NVIDIA/spark-rapids/issues/3339)|[BUG] Databricks test fails test_hash_groupby_collect_partial_replace_fallback|
|[#3375](https://github.com/NVIDIA/spark-rapids/issues/3375)|[BUG]  SPARK-35742 Replace semanticEquals with canonicalize|
|[#3334](https://github.com/NVIDIA/spark-rapids/issues/3334)|[BUG] UCX join_test FAILED on spark standalone |
|[#3058](https://github.com/NVIDIA/spark-rapids/issues/3058)|[BUG] GPU ORC reader complains errors when specifying columns that do not exist in file schema.|
|[#3385](https://github.com/NVIDIA/spark-rapids/issues/3385)|[BUG] misc_expr_test FAILED on Dataproc|
|[#2052](https://github.com/NVIDIA/spark-rapids/issues/2052)|[BUG] Spark 3.2.0 test fails due to SPARK-34906 Refactor TreeNode's children handling methods into specialized traits|
|[#3401](https://github.com/NVIDIA/spark-rapids/issues/3401)|[BUG] Qualification tool failed with java.lang.ArrayIndexOutOfBoundsException|
|[#3333](https://github.com/NVIDIA/spark-rapids/issues/3333)|[BUG]Mortgage ETL input_file_name is not correct when using CPU's CsvScan|
|[#3391](https://github.com/NVIDIA/spark-rapids/issues/3391)|[BUG] UDF example build fail|
|[#3379](https://github.com/NVIDIA/spark-rapids/issues/3379)|[BUG] q93 failed w/ UCX|
|[#3364](https://github.com/NVIDIA/spark-rapids/issues/3364)|[BUG] analysis tool cannot handle a job with no tasks.|
|[#3235](https://github.com/NVIDIA/spark-rapids/issues/3235)|Classes directly in Apache Spark packages|
|[#3237](https://github.com/NVIDIA/spark-rapids/issues/3237)|BasicColumnWriteJobStatsTracker might be affected by spark change SPARK-34399|
|[#3134](https://github.com/NVIDIA/spark-rapids/issues/3134)|[BUG] Add more checkings before coalescing ORC files|
|[#3324](https://github.com/NVIDIA/spark-rapids/issues/3324)|[BUG] Databricks builds failing with missing dependency issue|
|[#3244](https://github.com/NVIDIA/spark-rapids/issues/3244)|[BUG] join_test LeftAnti failing on Databricks|
|[#3268](https://github.com/NVIDIA/spark-rapids/issues/3268)|[BUG] CDH ParquetCachedBatchSerializer fails to build due to api change in VectorizedColumnReader|
|[#3305](https://github.com/NVIDIA/spark-rapids/issues/3305)|[BUG] test_case_when failed on Databricks 7.3 nightly build|
|[#3139](https://github.com/NVIDIA/spark-rapids/issues/3139)|[BUG] case when on some nested types can produce a crash|
|[#3253](https://github.com/NVIDIA/spark-rapids/issues/3253)|[BUG] ClassCastException for unsupported TypedImperativeAggregate functions|
|[#3256](https://github.com/NVIDIA/spark-rapids/issues/3256)|[BUG] udf-examples native build broken |
|[#3271](https://github.com/NVIDIA/spark-rapids/issues/3271)|[BUG] Databricks 301 shim compilation error|
|[#3255](https://github.com/NVIDIA/spark-rapids/issues/3255)|[BUG] GpuRunningWindowExecMeta is missing ExecChecks for partitionSpec in databricks runtime|
|[#3222](https://github.com/NVIDIA/spark-rapids/issues/3222)|[BUG] `test_running_window_function_exec_for_all_aggs` failed in the UCX EGX run|
|[#3195](https://github.com/NVIDIA/spark-rapids/issues/3195)|[BUG] failures parquet_test test:read_round_trip|
|[#3176](https://github.com/NVIDIA/spark-rapids/issues/3176)|[BUG] test_window_aggs_for_rows_collect_list[IGNORE_ORDER({'local': True})] FAILED on EGX Yarn cluster|
|[#3187](https://github.com/NVIDIA/spark-rapids/issues/3187)|[BUG] NullPointerException in SLF4J on startup|
|[#3166](https://github.com/NVIDIA/spark-rapids/issues/3166)|[BUG] Unable to build rapids-4-spark jar from source due to missing 3.0.3-SNAPSHOT for spark-sql|
|[#3131](https://github.com/NVIDIA/spark-rapids/issues/3131)|[BUG] hash_aggregate_test TypedImperativeAggregate tests failed|
|[#3147](https://github.com/NVIDIA/spark-rapids/issues/3147)|[BUG] window_function_test.py::test_window_ride_along failed in databricks runtime|
|[#3094](https://github.com/NVIDIA/spark-rapids/issues/3094)|[BUG] join_test.py::test_sortmerge_join_with_conditionals failed in databricks 8.2 runtime|
|[#3078](https://github.com/NVIDIA/spark-rapids/issues/3078)|[BUG] test_hash_join_map, test_sortmerge_join_map failed in databricks runtime|
|[#3059](https://github.com/NVIDIA/spark-rapids/issues/3059)|[BUG] orc_test:test_pred_push_round_trip failed|

### PRs
|||
|:---|:---|
|[#3940](https://github.com/NVIDIA/spark-rapids/pull/3940)|Update changelog [skip ci]|
|[#3930](https://github.com/NVIDIA/spark-rapids/pull/3930)|Dist artifact with provided aggregator dependency|
|[#3918](https://github.com/NVIDIA/spark-rapids/pull/3918)|Update changelog [skip ci]|
|[#3906](https://github.com/NVIDIA/spark-rapids/pull/3906)|Doc updated for v2110[skip ci]|
|[#3840](https://github.com/NVIDIA/spark-rapids/pull/3840)|Update changelog [skip ci]|
|[#3838](https://github.com/NVIDIA/spark-rapids/pull/3838)|Update deploy script [skip ci]|
|[#3827](https://github.com/NVIDIA/spark-rapids/pull/3827)|Update changelog 21.10 to latest [skip ci]|
|[#3808](https://github.com/NVIDIA/spark-rapids/pull/3808)|Rewording qualification and profiling tools doc files[skip ci]|
|[#3815](https://github.com/NVIDIA/spark-rapids/pull/3815)|Correct 21.10 docs such as PCBS related FAQ [skip ci]|
|[#3807](https://github.com/NVIDIA/spark-rapids/pull/3807)|Update 21.10.0 release doc [skip ci]|
|[#3800](https://github.com/NVIDIA/spark-rapids/pull/3800)|Update approximate percentile documentation|
|[#3810](https://github.com/NVIDIA/spark-rapids/pull/3810)|Update to include Spark 3.2.0 in nosnapshots target so it gets released officially.|
|[#3806](https://github.com/NVIDIA/spark-rapids/pull/3806)|Update spark320.version to 3.2.0|
|[#3795](https://github.com/NVIDIA/spark-rapids/pull/3795)|Reduce usage of escaping in xargs|
|[#3785](https://github.com/NVIDIA/spark-rapids/pull/3785)|[BUG] Update cudf version in version-dev script [skip ci]|
|[#3771](https://github.com/NVIDIA/spark-rapids/pull/3771)|Update cudfjni version to 21.10.0|
|[#3777](https://github.com/NVIDIA/spark-rapids/pull/3777)|Ignore nullability when checking for need to cast aggregation input|
|[#3763](https://github.com/NVIDIA/spark-rapids/pull/3763)|Force parallel world in Shim caller's classloader|
|[#3756](https://github.com/NVIDIA/spark-rapids/pull/3756)|Simplify shim classloader logic|
|[#3746](https://github.com/NVIDIA/spark-rapids/pull/3746)|Avoid using AST on inner joins and avoid coalesce after nested loop join filter|
|[#3719](https://github.com/NVIDIA/spark-rapids/pull/3719)|Advertise CPU sort order and partitioning expressions to Catalyst|
|[#3737](https://github.com/NVIDIA/spark-rapids/pull/3737)|Add note referencing known issues in approx_percentile implementation|
|[#3729](https://github.com/NVIDIA/spark-rapids/pull/3729)|Update to ucx 1.11.2 for 21.10|
|[#3711](https://github.com/NVIDIA/spark-rapids/pull/3711)|Surface problems with overrides and fallback|
|[#3722](https://github.com/NVIDIA/spark-rapids/pull/3722)|CDH build stopped working due to missing jars in maven repo|
|[#3691](https://github.com/NVIDIA/spark-rapids/pull/3691)|Fix issues with AQE and DPP enabled on Spark 3.2|
|[#3373](https://github.com/NVIDIA/spark-rapids/pull/3373)|Support `stddev` and `variance` aggregations families|
|[#3708](https://github.com/NVIDIA/spark-rapids/pull/3708)|disable percentile approx tests|
|[#3695](https://github.com/NVIDIA/spark-rapids/pull/3695)|Remove duplicated data types for collect_list tests|
|[#3687](https://github.com/NVIDIA/spark-rapids/pull/3687)|Improve dedupe script|
|[#3646](https://github.com/NVIDIA/spark-rapids/pull/3646)|Debug utility method to dump a table or columnar batch to Parquet|
|[#3683](https://github.com/NVIDIA/spark-rapids/pull/3683)|Change deploy scripts for new build system|
|[#3301](https://github.com/NVIDIA/spark-rapids/pull/3301)|Approx Percentile|
|[#3673](https://github.com/NVIDIA/spark-rapids/pull/3673)|Add the Scala jar as an external lib for a linkage warning|
|[#3668](https://github.com/NVIDIA/spark-rapids/pull/3668)|Improve the diagnostics in udf compiler for try-and-catch.|
|[#3666](https://github.com/NVIDIA/spark-rapids/pull/3666)|Recompute Parquet block metadata when estimating footer from multiple file input|
|[#3671](https://github.com/NVIDIA/spark-rapids/pull/3671)|Fix tests-spark310+ dependency|
|[#3663](https://github.com/NVIDIA/spark-rapids/pull/3663)|Add back the tests-spark310+|
|[#3657](https://github.com/NVIDIA/spark-rapids/pull/3657)|Revert "Use cudf to compute exact hash join output row sizes (#3288)"|
|[#3643](https://github.com/NVIDIA/spark-rapids/pull/3643)|Properly override Shims for int96Rebase|
|[#3645](https://github.com/NVIDIA/spark-rapids/pull/3645)|Verify unshimmed classes are bitwise-identical|
|[#3650](https://github.com/NVIDIA/spark-rapids/pull/3650)|Fix dist copy dependencies|
|[#3649](https://github.com/NVIDIA/spark-rapids/pull/3649)|Add ignore_order to other fallback tests for the aggregate|
|[#3631](https://github.com/NVIDIA/spark-rapids/pull/3631)|Change premerge to build all Spark versions|
|[#3630](https://github.com/NVIDIA/spark-rapids/pull/3630)|Fix CDH Build |
|[#3636](https://github.com/NVIDIA/spark-rapids/pull/3636)|Change nightly build to not deploy dist for each classifier version [skip ci]|
|[#3632](https://github.com/NVIDIA/spark-rapids/pull/3632)|Revert disabling of ctas test|
|[#3628](https://github.com/NVIDIA/spark-rapids/pull/3628)|Fix 313 ShuffleManager build|
|[#3618](https://github.com/NVIDIA/spark-rapids/pull/3618)|Update changelog script to strip ambiguous annotation [skip ci]|
|[#3626](https://github.com/NVIDIA/spark-rapids/pull/3626)|Add in support for casting decimal to other number types|
|[#3615](https://github.com/NVIDIA/spark-rapids/pull/3615)|Ignore order for the test_no_fallback_when_ansi_enabled|
|[#3602](https://github.com/NVIDIA/spark-rapids/pull/3602)|Dedupe proxy rapids shuffle manager byte code|
|[#3330](https://github.com/NVIDIA/spark-rapids/pull/3330)|Support `int96RebaseModeInWrite` and `int96RebaseModeInRead`|
|[#3438](https://github.com/NVIDIA/spark-rapids/pull/3438)|Parquet read unsigned int: uint8, uin16, uint32|
|[#3607](https://github.com/NVIDIA/spark-rapids/pull/3607)|com.nvidia.spark.rapids.ColumnarRdd not exposed to user for XGBoost|
|[#3566](https://github.com/NVIDIA/spark-rapids/pull/3566)|Enable String Array Max and Min|
|[#3590](https://github.com/NVIDIA/spark-rapids/pull/3590)|Unshim ExclusiveModeGpuDiscoveryPlugin|
|[#3597](https://github.com/NVIDIA/spark-rapids/pull/3597)|ANSI check for aggregates|
|[#3595](https://github.com/NVIDIA/spark-rapids/pull/3595)|Update the overflow check algorithm for Subtract|
|[#3588](https://github.com/NVIDIA/spark-rapids/pull/3588)|Disable test_non_empty_ctas test|
|[#3577](https://github.com/NVIDIA/spark-rapids/pull/3577)|Commonize more shim module files|
|[#3594](https://github.com/NVIDIA/spark-rapids/pull/3594)|Fix nightly integration test script for specfic artifacts|
|[#3544](https://github.com/NVIDIA/spark-rapids/pull/3544)|Add test for nested grouping sets, rollup, cube|
|[#3587](https://github.com/NVIDIA/spark-rapids/pull/3587)|Revert shared class list modifications in PR#3545|
|[#3570](https://github.com/NVIDIA/spark-rapids/pull/3570)|ANSI Support for Abs, UnaryMinus, and Subtract|
|[#3574](https://github.com/NVIDIA/spark-rapids/pull/3574)|Add in ANSI date time fallback|
|[#3578](https://github.com/NVIDIA/spark-rapids/pull/3578)|Deploy all of the classifier versions of the jars [skip ci]|
|[#3569](https://github.com/NVIDIA/spark-rapids/pull/3569)|Add commons-lang3 dependency to tests|
|[#3568](https://github.com/NVIDIA/spark-rapids/pull/3568)|Enable 3.2.0 unit test in premerge and nightly|
|[#3559](https://github.com/NVIDIA/spark-rapids/pull/3559)|Commonize shim module join and shuffle files|
|[#3565](https://github.com/NVIDIA/spark-rapids/pull/3565)|Auto-dedupe ASM-relocated shim dependencies|
|[#3531](https://github.com/NVIDIA/spark-rapids/pull/3531)|Fall back to the CPU for date/time parsing we cannot support yet|
|[#3561](https://github.com/NVIDIA/spark-rapids/pull/3561)|Follow on to ANSI Add|
|[#3557](https://github.com/NVIDIA/spark-rapids/pull/3557)|Add IDEA profile switch workarounds|
|[#3504](https://github.com/NVIDIA/spark-rapids/pull/3504)|Fix reserialization of broadcasted tables|
|[#3556](https://github.com/NVIDIA/spark-rapids/pull/3556)|Fix databricks test.sh script for passing spark shim version|
|[#3545](https://github.com/NVIDIA/spark-rapids/pull/3545)|Dynamic class file deduplication across shims in dist jar build |
|[#3551](https://github.com/NVIDIA/spark-rapids/pull/3551)|Fix window sum overflow for 3.2.0+|
|[#3537](https://github.com/NVIDIA/spark-rapids/pull/3537)|GpuAdd supports ANSI mode.|
|[#3533](https://github.com/NVIDIA/spark-rapids/pull/3533)|Define a SPARK_SHIM_VER to pick up specific rapids-4-spark-integration-tests jars|
|[#3547](https://github.com/NVIDIA/spark-rapids/pull/3547)|Range window supports DayTime on 3.2+|
|[#3534](https://github.com/NVIDIA/spark-rapids/pull/3534)|Fix package name and sql string issue for GpuTimeAdd|
|[#3536](https://github.com/NVIDIA/spark-rapids/pull/3536)|Enable auto-merge from branch 21.10 to 21.12 [skip ci]|
|[#3521](https://github.com/NVIDIA/spark-rapids/pull/3521)|Qualification tool: Report nested complex types in Potential Problems and improve write csv identification.|
|[#3507](https://github.com/NVIDIA/spark-rapids/pull/3507)|TimeAdd supports DayTimeIntervalType|
|[#3529](https://github.com/NVIDIA/spark-rapids/pull/3529)|Support UnsafeArrayData in scalars|
|[#3528](https://github.com/NVIDIA/spark-rapids/pull/3528)|Update NOTICE copyrights to 2021|
|[#3527](https://github.com/NVIDIA/spark-rapids/pull/3527)|Ignore CBO tests that fail against Spark 3.2.0|
|[#3439](https://github.com/NVIDIA/spark-rapids/pull/3439)|Stop parsing special dates for Spark 3.2+|
|[#3524](https://github.com/NVIDIA/spark-rapids/pull/3524)|Update hashing to normalize -0.0 on 3.2+|
|[#3508](https://github.com/NVIDIA/spark-rapids/pull/3508)|Auto abort dup pre-merge builds [skip ci]|
|[#3501](https://github.com/NVIDIA/spark-rapids/pull/3501)|Add limitations for Databricks doc|
|[#3517](https://github.com/NVIDIA/spark-rapids/pull/3517)|Update empty CTAS testing to avoid Hive if possible|
|[#3513](https://github.com/NVIDIA/spark-rapids/pull/3513)|Allow spark320 tests to run with 320 or 321|
|[#3493](https://github.com/NVIDIA/spark-rapids/pull/3493)|Initialze RAPIDS Shuffle Manager at driver/executor startup|
|[#3496](https://github.com/NVIDIA/spark-rapids/pull/3496)|Update parse date to leverage cuDF support for single digit components|
|[#3454](https://github.com/NVIDIA/spark-rapids/pull/3454)|Catch UDF compiler exceptions and fallback to CPU|
|[#3505](https://github.com/NVIDIA/spark-rapids/pull/3505)|Remove doc references to cudf JIT|
|[#3503](https://github.com/NVIDIA/spark-rapids/pull/3503)|Have average support nulls for 3.2.0|
|[#3500](https://github.com/NVIDIA/spark-rapids/pull/3500)|Fix GpuSum type to match resultType|
|[#3485](https://github.com/NVIDIA/spark-rapids/pull/3485)|Fix regressions in cast from string to date and timestamp|
|[#3487](https://github.com/NVIDIA/spark-rapids/pull/3487)|Add databricks build tests to pre-merge CI [skip ci]|
|[#3497](https://github.com/NVIDIA/spark-rapids/pull/3497)|Re-enable spark.rapids.shims-provider-override|
|[#3499](https://github.com/NVIDIA/spark-rapids/pull/3499)|Fix Spark 3.2.0 test_div_by_zero_ansi failures|
|[#3418](https://github.com/NVIDIA/spark-rapids/pull/3418)|Qualification tool: Add filtering based on configuration parameters|
|[#3498](https://github.com/NVIDIA/spark-rapids/pull/3498)|Update the scala repl loader to avoid issues with broadcast.|
|[#3479](https://github.com/NVIDIA/spark-rapids/pull/3479)|Test with Spark 3.2.1-SNAPSHOT|
|[#3474](https://github.com/NVIDIA/spark-rapids/pull/3474)|Build fixes and IDE instructions|
|[#3460](https://github.com/NVIDIA/spark-rapids/pull/3460)|Add DayTimeIntervalType/YearMonthIntervalType support|
|[#3491](https://github.com/NVIDIA/spark-rapids/pull/3491)|Shim GpuKryoRegistrator|
|[#3489](https://github.com/NVIDIA/spark-rapids/pull/3489)|Fix 311 databricks shim for AnsiCastOpSuite failures|
|[#3456](https://github.com/NVIDIA/spark-rapids/pull/3456)|Fallback to CPU when datasource v2 enables RuntimeFiltering|
|[#3417](https://github.com/NVIDIA/spark-rapids/pull/3417)|Adds pre/post steps for merge and update aggregate|
|[#3431](https://github.com/NVIDIA/spark-rapids/pull/3431)|Reinstate test_sortmerge_join_struct_mixed_key_with_null_filter|
|[#3477](https://github.com/NVIDIA/spark-rapids/pull/3477)|Update supported docs to clarify casting floating point to string|
|[#3447](https://github.com/NVIDIA/spark-rapids/pull/3447)|Add CUDA async memory resource as an option|
|[#3473](https://github.com/NVIDIA/spark-rapids/pull/3473)|Create non-shim specific version of ParquetCachedBatchSerializer|
|[#3471](https://github.com/NVIDIA/spark-rapids/pull/3471)|Fix canonicalization of GpuScalarSubquery|
|[#3480](https://github.com/NVIDIA/spark-rapids/pull/3480)|Temporarily disable failing cast string to date tests|
|[#3377](https://github.com/NVIDIA/spark-rapids/pull/3377)|Fix AnsiCastOpSuite failures with Spark 3.2|
|[#3467](https://github.com/NVIDIA/spark-rapids/pull/3467)|Update docs to better describe support for floating point aggregation and NaNs|
|[#3459](https://github.com/NVIDIA/spark-rapids/pull/3459)|Use Shims v2 for ShuffledBatchRDD|
|[#3457](https://github.com/NVIDIA/spark-rapids/pull/3457)|Update the children unpacking pattern for GpuIf.|
|[#3464](https://github.com/NVIDIA/spark-rapids/pull/3464)|Add test for empty relation propagation|
|[#3458](https://github.com/NVIDIA/spark-rapids/pull/3458)|Fix log info GPU BroadcastExchangeExec|
|[#3466](https://github.com/NVIDIA/spark-rapids/pull/3466)|Databricks build fixes for missing shouldFailDivOverflow and removal of needed imports|
|[#3465](https://github.com/NVIDIA/spark-rapids/pull/3465)|Fix name of 301+-nondb directory to stop at Spark 3.2.0|
|[#3452](https://github.com/NVIDIA/spark-rapids/pull/3452)|Enable AQE/DPP test for Spark 3.2|
|[#3436](https://github.com/NVIDIA/spark-rapids/pull/3436)|Qualification tool: Update expected result for test|
|[#3455](https://github.com/NVIDIA/spark-rapids/pull/3455)|Decrease pre_merge_ci parallelism to 4 and reordering time-consuming tests|
|[#3420](https://github.com/NVIDIA/spark-rapids/pull/3420)|`IntegralDivide` throws an exception on overflow in ANSI mode|
|[#3433](https://github.com/NVIDIA/spark-rapids/pull/3433)|Batch scalastyle checks across all modules upfront|
|[#3453](https://github.com/NVIDIA/spark-rapids/pull/3453)|Fix spark-tests script for classifier|
|[#3445](https://github.com/NVIDIA/spark-rapids/pull/3445)|Update nightly build to pull Databricks jars|
|[#3446](https://github.com/NVIDIA/spark-rapids/pull/3446)|Format aggregator pom and commonize some configuration|
|[#3444](https://github.com/NVIDIA/spark-rapids/pull/3444)|Add in tests for unaligned parquet pages|
|[#3451](https://github.com/NVIDIA/spark-rapids/pull/3451)|Fix typo in spark-tests.sh|
|[#3443](https://github.com/NVIDIA/spark-rapids/pull/3443)|Remove 301emr shim|
|[#3441](https://github.com/NVIDIA/spark-rapids/pull/3441)|update deploy script for Databricks|
|[#3414](https://github.com/NVIDIA/spark-rapids/pull/3414)|Add in support for transform_keys|
|[#3320](https://github.com/NVIDIA/spark-rapids/pull/3320)|Add AST support for logical AND and logical OR|
|[#3425](https://github.com/NVIDIA/spark-rapids/pull/3425)|Throw an error by default if CREATE TABLE AS SELECT overwrites data|
|[#3422](https://github.com/NVIDIA/spark-rapids/pull/3422)|Stop double closing SerializeBatchDeserializeHostBuffer host buffers when running with Spark 3.2|
|[#3411](https://github.com/NVIDIA/spark-rapids/pull/3411)|Make new build default and combine into dist package|
|[#3368](https://github.com/NVIDIA/spark-rapids/pull/3368)|Extend TagForReplaceMode to adapt Databricks runtime |
|[#3428](https://github.com/NVIDIA/spark-rapids/pull/3428)|Remove commented-out semanticEquals overrides|
|[#3421](https://github.com/NVIDIA/spark-rapids/pull/3421)|Revert to CUDA runtime image for build|
|[#3381](https://github.com/NVIDIA/spark-rapids/pull/3381)|Implement per-shim parallel world jar classloader|
|[#3303](https://github.com/NVIDIA/spark-rapids/pull/3303)|Update to cudf conditional join change that removes null equality argument|
|[#3408](https://github.com/NVIDIA/spark-rapids/pull/3408)|Add leafNodeDefaultParallelism support|
|[#3426](https://github.com/NVIDIA/spark-rapids/pull/3426)|Correct grammar in qualification tool doc|
|[#3423](https://github.com/NVIDIA/spark-rapids/pull/3423)|Fix hash_aggregate tests that leaked configs|
|[#3412](https://github.com/NVIDIA/spark-rapids/pull/3412)|Restore AST conditional join tests|
|[#3403](https://github.com/NVIDIA/spark-rapids/pull/3403)|Fix canonicalization regression with Spark 3.2|
|[#3394](https://github.com/NVIDIA/spark-rapids/pull/3394)|Orc read map|
|[#3392](https://github.com/NVIDIA/spark-rapids/pull/3392)|Support transforming BinaryType between Row and Columnar|
|[#3393](https://github.com/NVIDIA/spark-rapids/pull/3393)|Fill with null columns for the names exist only in read schema in ORC reader|
|[#3399](https://github.com/NVIDIA/spark-rapids/pull/3399)|Fix collect_list test so it covers nested types properly|
|[#3410](https://github.com/NVIDIA/spark-rapids/pull/3410)|Specify number of RDD slices for ID tests|
|[#3363](https://github.com/NVIDIA/spark-rapids/pull/3363)|Add AST support for null literals|
|[#3396](https://github.com/NVIDIA/spark-rapids/pull/3396)|Throw exception on parse error in ANSI mode when casting String to Date|
|[#3315](https://github.com/NVIDIA/spark-rapids/pull/3315)|Add in reporting of time taken to transition plan to GPU|
|[#3409](https://github.com/NVIDIA/spark-rapids/pull/3409)|Use devel cuda image for premerge CI|
|[#3405](https://github.com/NVIDIA/spark-rapids/pull/3405)|Qualification tool: Filter empty strings from Read Schema|
|[#3387](https://github.com/NVIDIA/spark-rapids/pull/3387)|Fallback to the CPU for IGNORE NULLS on lead and lag|
|[#3398](https://github.com/NVIDIA/spark-rapids/pull/3398)|Fix NPE on string repeat when there is no data buffer|
|[#3366](https://github.com/NVIDIA/spark-rapids/pull/3366)|Fix input_file_xxx issue when FileScan is running on CPU|
|[#3397](https://github.com/NVIDIA/spark-rapids/pull/3397)|Add tests for GpuInSet|
|[#3395](https://github.com/NVIDIA/spark-rapids/pull/3395)|Fix UDF native example build|
|[#3389](https://github.com/NVIDIA/spark-rapids/pull/3389)|Bring back setRapidsShuffleManager in the driver side|
|[#3263](https://github.com/NVIDIA/spark-rapids/pull/3263)|Qualification tool: Report write data format and nested types|
|[#3378](https://github.com/NVIDIA/spark-rapids/pull/3378)|Make Dockerfile.cuda consistent with getting-started-kubernetes.md|
|[#3359](https://github.com/NVIDIA/spark-rapids/pull/3359)|UnionExec array and nested array support|
|[#3342](https://github.com/NVIDIA/spark-rapids/pull/3342)|Profiling tool add CSV output option and add new combined mode|
|[#3365](https://github.com/NVIDIA/spark-rapids/pull/3365)|fix databricks builds|
|[#3323](https://github.com/NVIDIA/spark-rapids/pull/3323)|Enable optional Spark 3.2.0 shim build|
|[#3361](https://github.com/NVIDIA/spark-rapids/pull/3361)|Fix databricks 3.1.1 arrow dependency version|
|[#3354](https://github.com/NVIDIA/spark-rapids/pull/3354)|Support HashAggregate on struct and nested struct|
|[#3341](https://github.com/NVIDIA/spark-rapids/pull/3341)|ArrayMax and ArrayMin support plus map_entries, map_keys, map_values|
|[#3356](https://github.com/NVIDIA/spark-rapids/pull/3356)|Support Databricks 3.0.1 with new build profiles|
|[#3344](https://github.com/NVIDIA/spark-rapids/pull/3344)|Move classes out of Apache Spark packages|
|[#3345](https://github.com/NVIDIA/spark-rapids/pull/3345)|Add job commit time to task tracker stats|
|[#3357](https://github.com/NVIDIA/spark-rapids/pull/3357)|Avoid RAT checks on any CSV file|
|[#3355](https://github.com/NVIDIA/spark-rapids/pull/3355)|Add new authorized user to blossom-ci whitelist [skip ci]|
|[#3340](https://github.com/NVIDIA/spark-rapids/pull/3340)|xfail AST nested loop join tests until cudf empty left table bug is fixed|
|[#3276](https://github.com/NVIDIA/spark-rapids/pull/3276)|Use child type in some places to make it more clear|
|[#3346](https://github.com/NVIDIA/spark-rapids/pull/3346)|Mark more tests as premerge_ci_1|
|[#3353](https://github.com/NVIDIA/spark-rapids/pull/3353)|Fix automerge conflict 3349 [skip ci]|
|[#3335](https://github.com/NVIDIA/spark-rapids/pull/3335)|Support Databricks 3.1.1 in new build profiles|
|[#3317](https://github.com/NVIDIA/spark-rapids/pull/3317)|Adds in support for the transform_values SQL function|
|[#3299](https://github.com/NVIDIA/spark-rapids/pull/3299)|Insert buffer converters for TypedImperativeAggregate|
|[#3325](https://github.com/NVIDIA/spark-rapids/pull/3325)|Fix spark version classifier being applied properly|
|[#3288](https://github.com/NVIDIA/spark-rapids/pull/3288)|Use cudf to compute exact hash join output row sizes|
|[#3318](https://github.com/NVIDIA/spark-rapids/pull/3318)|Fix LeftAnti nested loop join missing condition case|
|[#3316](https://github.com/NVIDIA/spark-rapids/pull/3316)|Fix GpuProjectAstExec when projecting only literals|
|[#3262](https://github.com/NVIDIA/spark-rapids/pull/3262)|Re-enable the struct support for the ORC reader.|
|[#3312](https://github.com/NVIDIA/spark-rapids/pull/3312)|Fix inconsistent function name and add backward compatibility support for premerge job [skip ci]|
|[#3319](https://github.com/NVIDIA/spark-rapids/pull/3319)|Temporarily disable cache test except for spark 3.1.1|
|[#3308](https://github.com/NVIDIA/spark-rapids/pull/3308)|Branch 21.10 FAQ update forward compatibility, update Spark and CUDA versions|
|[#3309](https://github.com/NVIDIA/spark-rapids/pull/3309)|Prepare Spark 3.2.0 related changes|
|[#3289](https://github.com/NVIDIA/spark-rapids/pull/3289)|Support for ArrayTransform|
|[#3307](https://github.com/NVIDIA/spark-rapids/pull/3307)|Fix generation of null scalars in tests|
|[#3306](https://github.com/NVIDIA/spark-rapids/pull/3306)|Update guava to be 30.0-jre|
|[#3304](https://github.com/NVIDIA/spark-rapids/pull/3304)|Fix nested cast type checks|
|[#3302](https://github.com/NVIDIA/spark-rapids/pull/3302)|Fix shim aggregator dependencies when snapshot-shims profile provided|
|[#3291](https://github.com/NVIDIA/spark-rapids/pull/3291)|Bump guava from 28.0-jre to 29.0-jre in /tests|
|[#3292](https://github.com/NVIDIA/spark-rapids/pull/3292)|Bump guava from 28.0-jre to 29.0-jre in /integration_tests|
|[#3293](https://github.com/NVIDIA/spark-rapids/pull/3293)|Bump guava from 28.0-jre to 29.0-jre in /udf-compiler|
|[#3294](https://github.com/NVIDIA/spark-rapids/pull/3294)|Update Qualification and Profiling tool documentation for gh-pages|
|[#3282](https://github.com/NVIDIA/spark-rapids/pull/3282)|Test for `current_date`, `current_timestamp` and `now`|
|[#3298](https://github.com/NVIDIA/spark-rapids/pull/3298)|Minor parent pom fixes|
|[#3296](https://github.com/NVIDIA/spark-rapids/pull/3296)|Support map type in case when expression|
|[#3295](https://github.com/NVIDIA/spark-rapids/pull/3295)|Rename pytest 'slow_test' tag as 'premerge_ci_1' to avoid confusion|
|[#3274](https://github.com/NVIDIA/spark-rapids/pull/3274)|Add m2 cache to fast premerge build|
|[#3283](https://github.com/NVIDIA/spark-rapids/pull/3283)|Fix ClassCastException for unsupported TypedImperativeAggregate functions|
|[#3251](https://github.com/NVIDIA/spark-rapids/pull/3251)|CreateMap support for multiple key-value pairs|
|[#3234](https://github.com/NVIDIA/spark-rapids/pull/3234)|Parquet support for MapType|
|[#3277](https://github.com/NVIDIA/spark-rapids/pull/3277)|Build changes for Spark 3.0.3, 3.0.4, 3.1.1, 3.1.2, 3.1.3, 3.1.1cdh and 3.0.1emr|
|[#3275](https://github.com/NVIDIA/spark-rapids/pull/3275)|Improve over-estimating for ORC coalescing reading|
|[#3280](https://github.com/NVIDIA/spark-rapids/pull/3280)|Update project URL to the public doc website|
|[#3285](https://github.com/NVIDIA/spark-rapids/pull/3285)|Qualification tool: Check for metadata being null|
|[#3281](https://github.com/NVIDIA/spark-rapids/pull/3281)|Decrease parallelism for pre-merge pod to avoid potential OOM kill|
|[#3264](https://github.com/NVIDIA/spark-rapids/pull/3264)|Add parallel support to nightly spark standalone tests|
|[#3257](https://github.com/NVIDIA/spark-rapids/pull/3257)|Add maven compile/package plugin executions for Spark302 and Spark301|
|[#3272](https://github.com/NVIDIA/spark-rapids/pull/3272)|Fix Databricks shim build|
|[#3270](https://github.com/NVIDIA/spark-rapids/pull/3270)|Remove reference to old maven-scala-plugin|
|[#3259](https://github.com/NVIDIA/spark-rapids/pull/3259)|Generate docs for AST from checks|
|[#3164](https://github.com/NVIDIA/spark-rapids/pull/3164)|Support Union on Map types|
|[#3261](https://github.com/NVIDIA/spark-rapids/pull/3261)|Fix some typos[skip ci]|
|[#3242](https://github.com/NVIDIA/spark-rapids/pull/3242)|Support for LeftOuter/BuildRight and RightOuter/BuildLeft nested loop joins|
|[#3239](https://github.com/NVIDIA/spark-rapids/pull/3239)|Support decimal type in orc reader|
|[#3258](https://github.com/NVIDIA/spark-rapids/pull/3258)|Add ExecChecks to Databricks shims for RunningWindowFunctionExec|
|[#3230](https://github.com/NVIDIA/spark-rapids/pull/3230)|Initial support for CreateMap on GPU|
|[#3252](https://github.com/NVIDIA/spark-rapids/pull/3252)|Update to new cudf AST API|
|[#3249](https://github.com/NVIDIA/spark-rapids/pull/3249)|Fix typo in Spark311dbShims|
|[#3183](https://github.com/NVIDIA/spark-rapids/pull/3183)|Add TypeSig checks for join keys and other special cases|
|[#3246](https://github.com/NVIDIA/spark-rapids/pull/3246)|Disable test_broadcast_nested_loop_join_condition_missing_count on Databricks|
|[#3241](https://github.com/NVIDIA/spark-rapids/pull/3241)|Split pytest by 'slow_test' tag and run from different k8s pods to reduce premerge job duration|
|[#3184](https://github.com/NVIDIA/spark-rapids/pull/3184)|Support broadcast nested loop join for LeftSemi and LeftAnti|
|[#3236](https://github.com/NVIDIA/spark-rapids/pull/3236)|Fix Scaladoc warnings in GpuScalaUDF and BufferSendState|
|[#2846](https://github.com/NVIDIA/spark-rapids/pull/2846)|default rmm alloc fraction to the max to avoid unnecessary fragmentation|
|[#3231](https://github.com/NVIDIA/spark-rapids/pull/3231)|Fix some resource leaks in GpuCast and RapidsShuffleServerSuite|
|[#3179](https://github.com/NVIDIA/spark-rapids/pull/3179)|Support GpuFirst/GpuLast on more data types|
|[#3228](https://github.com/NVIDIA/spark-rapids/pull/3228)|Fix unreachable code warnings in GpuCast|
|[#3200](https://github.com/NVIDIA/spark-rapids/pull/3200)|Enable a smoke test for UCX in pre-merge|
|[#3203](https://github.com/NVIDIA/spark-rapids/pull/3203)|Fix Parquet test_round_trip to avoid CPU write exception|
|[#3220](https://github.com/NVIDIA/spark-rapids/pull/3220)|Use LongRangeGen instead of IntegerGen|
|[#3218](https://github.com/NVIDIA/spark-rapids/pull/3218)|Add UCX 1.11.0 to the pre-merge Docker image|
|[#3204](https://github.com/NVIDIA/spark-rapids/pull/3204)|Decrease parallelism for pre-merge integration tests|
|[#3212](https://github.com/NVIDIA/spark-rapids/pull/3212)|Fix merge conflict 3211 [skip ci]|
|[#3188](https://github.com/NVIDIA/spark-rapids/pull/3188)|Exclude slf4j classes from the spark-rapids jar|
|[#3189](https://github.com/NVIDIA/spark-rapids/pull/3189)|Disable snapshot shims by default|
|[#3178](https://github.com/NVIDIA/spark-rapids/pull/3178)|Fix hash_aggregate test failures due to TypedImperativeAggregate|
|[#3190](https://github.com/NVIDIA/spark-rapids/pull/3190)|Update GpuInSet for SPARK-35422 changes|
|[#3193](https://github.com/NVIDIA/spark-rapids/pull/3193)|Append  res-life to blossom-ci whitelist [skip ci]|
|[#3175](https://github.com/NVIDIA/spark-rapids/pull/3175)|Add in support for explode on maps|
|[#3171](https://github.com/NVIDIA/spark-rapids/pull/3171)|Refine upload log stage naming in workflow file [skip ci]|
|[#3173](https://github.com/NVIDIA/spark-rapids/pull/3173)|Profile tool: Fix reporting app contains Dataset|
|[#3165](https://github.com/NVIDIA/spark-rapids/pull/3165)|Add optional projection via AST expression evaluation|
|[#3113](https://github.com/NVIDIA/spark-rapids/pull/3113)|Fix order of operations when using mkString in typeConversionInfo|
|[#3161](https://github.com/NVIDIA/spark-rapids/pull/3161)|Rework Profile tool to not require Spark to run and process files faster|
|[#3169](https://github.com/NVIDIA/spark-rapids/pull/3169)|Fix auto-merge conflict 3167 [skip ci]|
|[#3162](https://github.com/NVIDIA/spark-rapids/pull/3162)|Add in more generalized support for casting nested types|
|[#3158](https://github.com/NVIDIA/spark-rapids/pull/3158)|Enable joins on nested structs|
|[#3099](https://github.com/NVIDIA/spark-rapids/pull/3099)|Decimal_128 type checks|
|[#3155](https://github.com/NVIDIA/spark-rapids/pull/3155)|Simple nested additions v2|
|[#2728](https://github.com/NVIDIA/spark-rapids/pull/2728)|Support string `repeat` SQL|
|[#3148](https://github.com/NVIDIA/spark-rapids/pull/3148)|Updated RunningWindow to support extended types too|
|[#3112](https://github.com/NVIDIA/spark-rapids/pull/3112)|Qualification tool: Add conjunction and disjunction filters|
|[#3117](https://github.com/NVIDIA/spark-rapids/pull/3117)|First pass at enabling structs, arrays, and maps for more parts of the plan|
|[#3109](https://github.com/NVIDIA/spark-rapids/pull/3109)|Cudf agg type changes|
|[#2971](https://github.com/NVIDIA/spark-rapids/pull/2971)|Support GpuCollectList and GpuCollectSet as TypedImperativeAggregate|
|[#3107](https://github.com/NVIDIA/spark-rapids/pull/3107)|Add setting to enable/disable RAPIDS Shuffle Manager dynamically|
|[#3105](https://github.com/NVIDIA/spark-rapids/pull/3105)|Add filter in query plan for conditional nested loop and cartesian joins|
|[#3096](https://github.com/NVIDIA/spark-rapids/pull/3096)|add spark311db GpuSortMergeJoinExec conditional joins filter|
|[#3086](https://github.com/NVIDIA/spark-rapids/pull/3086)|Fix Support of MapType in joins on Databricks|
|[#3089](https://github.com/NVIDIA/spark-rapids/pull/3089)|Add filter node in the query plan for conditional joins|
|[#3074](https://github.com/NVIDIA/spark-rapids/pull/3074)|Partial support for time windows|
|[#3061](https://github.com/NVIDIA/spark-rapids/pull/3061)|Support Union on Struct of Map|
|[#3034](https://github.com/NVIDIA/spark-rapids/pull/3034)| Support Sort on nested struct |
|[#3011](https://github.com/NVIDIA/spark-rapids/pull/3011)|Support MapType in joins|
|[#3031](https://github.com/NVIDIA/spark-rapids/pull/3031)|add doc for PR status checks [skip ci]|
|[#3028](https://github.com/NVIDIA/spark-rapids/pull/3028)|Enable parallel build for pre-merge job to reduce overall duration [skip ci]|
|[#3025](https://github.com/NVIDIA/spark-rapids/pull/3025)|Qualification tool: Add regex and username filters.|
|[#2980](https://github.com/NVIDIA/spark-rapids/pull/2980)|Init version 21.10.0|
|[#3000](https://github.com/NVIDIA/spark-rapids/pull/3000)|Merge branch-21.08 to branch-21.10|

## Release 21.08.1

### Bugs Fixed
|||
|:---|:---|
|[#3350](https://github.com/NVIDIA/spark-rapids/issues/3350)|[BUG] Qualification tool: check for metadata being null|

### PRs
|||
|:---|:---|
|[#3351](https://github.com/NVIDIA/spark-rapids/pull/3351)|Update changelog for tools v21.08.1 release [skip CI]|
|[#3348](https://github.com/NVIDIA/spark-rapids/pull/3348)|Change tool version to 21.08.1 [skip ci]|
|[#3343](https://github.com/NVIDIA/spark-rapids/pull/3343)|Qualification tool backport: Check for metadata being null (#3285)|

## Release 21.08

### Features
|||
|:---|:---|
|[#1584](https://github.com/NVIDIA/spark-rapids/issues/1584)|[FEA] Support rank as window function|
|[#1859](https://github.com/NVIDIA/spark-rapids/issues/1859)|[FEA] Optimize row_number/rank for memory usage|
|[#2976](https://github.com/NVIDIA/spark-rapids/issues/2976)|[FEA] support for arrays in BroadcastNestedLoopJoinExec and CartesianProductExec|
|[#2398](https://github.com/NVIDIA/spark-rapids/issues/2398)|[FEA] `GpuIf ` and `GpuCoalesce` supports `ArrayType`|
|[#2445](https://github.com/NVIDIA/spark-rapids/issues/2445)|[FEA] Support literal arrays in case/when statements|
|[#2757](https://github.com/NVIDIA/spark-rapids/issues/2757)|[FEA] Profiling tool display input data types|
|[#2860](https://github.com/NVIDIA/spark-rapids/issues/2860)|[FEA] Minimal support for LEGACY timeParserPolicy|
|[#2693](https://github.com/NVIDIA/spark-rapids/issues/2693)|[FEA] Profiling Tool: Print GDS + UCX related parameters |
|[#2334](https://github.com/NVIDIA/spark-rapids/issues/2334)|[FEA] Record GPU time and Fetch time separately, instead of recording Total Time|
|[#2685](https://github.com/NVIDIA/spark-rapids/issues/2685)|[FEA] Profiling compare mode for table SQL Duration and Executor CPU Time Percent|
|[#2742](https://github.com/NVIDIA/spark-rapids/issues/2742)|[FEA] include App Name from profiling tool output|
|[#2712](https://github.com/NVIDIA/spark-rapids/issues/2712)|[FEA] Display job and stage info in the dot graph for profiling tool|
|[#2562](https://github.com/NVIDIA/spark-rapids/issues/2562)|[FEA] Implement KnownNotNull on the GPU|
|[#2557](https://github.com/NVIDIA/spark-rapids/issues/2557)|[FEA] support sort_array on GPU|
|[#2307](https://github.com/NVIDIA/spark-rapids/issues/2307)|[FEA] Enable Parquet writing for arrays|
|[#1856](https://github.com/NVIDIA/spark-rapids/issues/1856)|[FEA] Create a batch chunking iterator and integrate it with GpuWindowExec|

### Performance
|||
|:---|:---|
|[#866](https://github.com/NVIDIA/spark-rapids/issues/866)|[FEA] combine window operations into single call|
|[#2800](https://github.com/NVIDIA/spark-rapids/issues/2800)|[FEA] Support ORC small files coalescing reading|
|[#737](https://github.com/NVIDIA/spark-rapids/issues/737)|[FEA] handle peer timeouts in shuffle|
|[#1590](https://github.com/NVIDIA/spark-rapids/issues/1590)|Rapids Shuffle - UcpListener|
|[#2275](https://github.com/NVIDIA/spark-rapids/issues/2275)|[FEA] UCP error callback deal with cleanup|
|[#2799](https://github.com/NVIDIA/spark-rapids/issues/2799)|[FEA] Support ORC multi-file cloud reading|

### Bugs Fixed
|||
|:---|:---|
|[#3135](https://github.com/NVIDIA/spark-rapids/issues/3135)|[BUG] Regression seen in `concatenate` in NDS with RAPIDS Shuffle Manager enabled|
|[#3017](https://github.com/NVIDIA/spark-rapids/issues/3017)|[BUG] orc_write_test failed in databricks runtime|
|[#3060](https://github.com/NVIDIA/spark-rapids/issues/3060)|[BUG] ORC read can corrupt data when specified schema does not match file schema ordering|
|[#3065](https://github.com/NVIDIA/spark-rapids/issues/3065)|[BUG] window exec tries to do too much on the GPU|
|[#3066](https://github.com/NVIDIA/spark-rapids/issues/3066)|[BUG] Profiling tool generate dot file fails to convert|
|[#3038](https://github.com/NVIDIA/spark-rapids/issues/3038)|[BUG] leak in `getDeviceMemoryBuffer` for the unspill case|
|[#3007](https://github.com/NVIDIA/spark-rapids/issues/3007)|[BUG] data mess up reading from ORC|
|[#3029](https://github.com/NVIDIA/spark-rapids/issues/3029)|[BUG] udf_test failed in ucx standalone env|
|[#2723](https://github.com/NVIDIA/spark-rapids/issues/2723)|[BUG] test failures in CI build (observed in UCX job) after starting to use 21.08|
|[#3016](https://github.com/NVIDIA/spark-rapids/issues/3016)|[BUG] databricks script failed to return correct exit code|
|[#3002](https://github.com/NVIDIA/spark-rapids/issues/3002)|[BUG] writing parquet with partitionBy() loses sort order|
|[#2959](https://github.com/NVIDIA/spark-rapids/issues/2959)|[BUG] Resolve common code source incompatibility with supported Spark versions|
|[#2589](https://github.com/NVIDIA/spark-rapids/issues/2589)|[BUG] RapidsShuffleHeartbeatManager needs to remove executors that are stale|
|[#2964](https://github.com/NVIDIA/spark-rapids/issues/2964)|[BUG] IGNORE ORDER, WITH DECIMALS: [Window] [MIXED WINDOW SPECS]  FAILED in spark 3.0.3+|
|[#2942](https://github.com/NVIDIA/spark-rapids/issues/2942)|[BUG] Cache of Array using ParquetCachedBatchSerializer failed with "DATA ACCESS MUST BE ON A HOST VECTOR"|
|[#2965](https://github.com/NVIDIA/spark-rapids/issues/2965)|[BUG] test_round_robin_sort_fallback failed with ValueError: 'a_1' is not in list|
|[#2891](https://github.com/NVIDIA/spark-rapids/issues/2891)|[BUG] Discrepancy in getting count before and after caching|
|[#2972](https://github.com/NVIDIA/spark-rapids/issues/2972)|[BUG] When using timeout option(-t) of qualification tool, it does not print anything in output after timeout.|
|[#2958](https://github.com/NVIDIA/spark-rapids/issues/2958)|[BUG] When AQE=on, SMJ with a Map in SELECTed list fails with "key not found: numPartitions"|
|[#2929](https://github.com/NVIDIA/spark-rapids/issues/2929)|[BUG] No validation of format strings when formatting dates in legacy timeParserPolicy mode|
|[#2900](https://github.com/NVIDIA/spark-rapids/issues/2900)|[BUG] CAST string to float/double produces incorrect results in some cases|
|[#2957](https://github.com/NVIDIA/spark-rapids/issues/2957)|[BUG] Builds failing due to breaking changes in SPARK-36034|
|[#2901](https://github.com/NVIDIA/spark-rapids/issues/2901)|[BUG] `GpuCompressedColumnVector` cannot be cast to `GpuColumnVector` with AQE|
|[#2899](https://github.com/NVIDIA/spark-rapids/issues/2899)|[BUG] CAST string to integer produces incorrect results in some cases|
|[#2937](https://github.com/NVIDIA/spark-rapids/issues/2937)|[BUG] Fix more edge cases when parsing dates in legacy timeParserPolicy|
|[#2939](https://github.com/NVIDIA/spark-rapids/issues/2939)|[BUG] Window integration tests failing with `Lead expected at least 3 but found 0`|
|[#2912](https://github.com/NVIDIA/spark-rapids/issues/2912)|[BUG] Profiling compare mode fails when comparing spark 2 eventlog to spark 3 event log|
|[#2892](https://github.com/NVIDIA/spark-rapids/issues/2892)|[BUG] UCX error `Message truncated` observed with UCX 1.11 RC in Q77 NDS|
|[#2807](https://github.com/NVIDIA/spark-rapids/issues/2807)|[BUG] Use UCP_AM_FLAG_WHOLE_MSG and UCP_AM_FLAG_PERSISTENT_DATA for receive handlers|
|[#2930](https://github.com/NVIDIA/spark-rapids/issues/2930)|[BUG] Profiling tool does not show "Potential Problems" for dataset API in section "SQL Duration and Executor CPU Time Percent"|
|[#2902](https://github.com/NVIDIA/spark-rapids/issues/2902)|[BUG] CAST string to bool produces incorrect results in some cases|
|[#2850](https://github.com/NVIDIA/spark-rapids/issues/2850)|[BUG] "java.io.InterruptedIOException: getFileStatus on s3a://xxx" for ORC reading in Databricks 8.2 runtime|
|[#2856](https://github.com/NVIDIA/spark-rapids/issues/2856)|[BUG] cache of struct does not work on databricks 8.2ML|
|[#2790](https://github.com/NVIDIA/spark-rapids/issues/2790)|[BUG] In Comparison mode health check does not show the application id|
|[#2713](https://github.com/NVIDIA/spark-rapids/issues/2713)|[BUG] profiling tool does not error or warn if incompatible options are given|
|[#2477](https://github.com/NVIDIA/spark-rapids/issues/2477)|[BUG] test_single_sort_in_part is failed in nightly UCX and AQE (no UCX) integration |
|[#2868](https://github.com/NVIDIA/spark-rapids/issues/2868)|[BUG] to_date produces wrong value on GPU for some corner cases|
|[#2907](https://github.com/NVIDIA/spark-rapids/issues/2907)|[BUG] incorrect expression to detect previously set `--master`|
|[#2893](https://github.com/NVIDIA/spark-rapids/issues/2893)|[BUG] TransferRequest request transactions are getting leaked|
|[#120](https://github.com/NVIDIA/spark-rapids/issues/120)|[BUG] GPU InitCap supports too much white space.|
|[#2786](https://github.com/NVIDIA/spark-rapids/issues/2786)|[BUG][initCap function]There is an issue converting the uppercase character to lowercase on GPU.|
|[#2754](https://github.com/NVIDIA/spark-rapids/issues/2754)|[BUG] cudf_udf tests failed w/ 21.08|
|[#2820](https://github.com/NVIDIA/spark-rapids/issues/2820)|[BUG] Metrics are inconsistent for GpuRowToColumnarToExec|
|[#2710](https://github.com/NVIDIA/spark-rapids/issues/2710)|[BUG] dot file generation can go over the limits of dot|
|[#2772](https://github.com/NVIDIA/spark-rapids/issues/2772)|[BUG] new integration test failures w/ maxFailures=1|
|[#2739](https://github.com/NVIDIA/spark-rapids/issues/2739)|[BUG] CBO causes less efficient plan for NDS q84|
|[#2717](https://github.com/NVIDIA/spark-rapids/issues/2717)|[BUG] CBO forces joins back onto CPU in some cases|
|[#2718](https://github.com/NVIDIA/spark-rapids/issues/2718)|[BUG] CBO falls back to CPU to write to Parquet in some cases|
|[#2692](https://github.com/NVIDIA/spark-rapids/issues/2692)|[BUG] Profiling tool: Add error handling for comparison functions |
|[#2711](https://github.com/NVIDIA/spark-rapids/issues/2711)|[BUG] reused stages should not appear multiple times in dot|
|[#2746](https://github.com/NVIDIA/spark-rapids/issues/2746)|[BUG] test_single_nested_sort_in_part integration test failure 21.08|
|[#2690](https://github.com/NVIDIA/spark-rapids/issues/2690)|[BUG] Profiling tool doesn't properly read rolled log files|
|[#2546](https://github.com/NVIDIA/spark-rapids/issues/2546)|[BUG] Build Failure when building from source|
|[#2750](https://github.com/NVIDIA/spark-rapids/issues/2750)|[BUG] nightly test failed with lists: `testStringReplaceWithBackrefs`|
|[#2644](https://github.com/NVIDIA/spark-rapids/issues/2644)|[BUG] test event logs should be compressed|
|[#2725](https://github.com/NVIDIA/spark-rapids/issues/2725)|[BUG] Heartbeat from unknown executor when running with UCX shuffle in local mode|
|[#2715](https://github.com/NVIDIA/spark-rapids/issues/2715)|[BUG] Part of the plan is not columnar class com.databricks.sql.execution.window.RunningWindowFunc|
|[#2521](https://github.com/NVIDIA/spark-rapids/issues/2521)|[BUG] cudf_udf failed in all spark release intermittently|
|[#1712](https://github.com/NVIDIA/spark-rapids/issues/1712)|[BUG] Scala UDF compiler can decompile UDFs with RAPIDS implementation|

### PRs
|||
|:---|:---|
|[#3216](https://github.com/NVIDIA/spark-rapids/pull/3216)|Update changelog to include download doc update [skip ci]|
|[#3214](https://github.com/NVIDIA/spark-rapids/pull/3214)|Update download and databricks doc for 21.06.2 [skip ci]|
|[#3210](https://github.com/NVIDIA/spark-rapids/pull/3210)|Update 21.08.0 changelog to latest [skip ci]|
|[#3197](https://github.com/NVIDIA/spark-rapids/pull/3197)|Databricks parquetFilters api change in db 8.2 runtime|
|[#3168](https://github.com/NVIDIA/spark-rapids/pull/3168)|Update 21.08 changelog to latest [skip ci]|
|[#3146](https://github.com/NVIDIA/spark-rapids/pull/3146)|update cudf Java binding version to 21.08.2|
|[#3080](https://github.com/NVIDIA/spark-rapids/pull/3080)|Update docs for 21.08 release|
|[#3136](https://github.com/NVIDIA/spark-rapids/pull/3136)|Update tool docs to explain default filesystem [skip ci]|
|[#3128](https://github.com/NVIDIA/spark-rapids/pull/3128)|Fix merge conflict 3126 from branch-21.06 [skip ci]|
|[#3124](https://github.com/NVIDIA/spark-rapids/pull/3124)|Fix merge conflict 3122 from branch-21.06 [skip ci]|
|[#3100](https://github.com/NVIDIA/spark-rapids/pull/3100)|Update databricks 3.0.1 shim to new ParquetFilter api|
|[#3083](https://github.com/NVIDIA/spark-rapids/pull/3083)|Initial CHANGELOG.md update for 21.08|
|[#3079](https://github.com/NVIDIA/spark-rapids/pull/3079)|Remove the struct support in ORC reader|
|[#3062](https://github.com/NVIDIA/spark-rapids/pull/3062)|Fix ORC read corruption when specified schema does not match file order|
|[#3064](https://github.com/NVIDIA/spark-rapids/pull/3064)|Tweak scaladoc to callout the GDS+unspill case in copyBuffer|
|[#3049](https://github.com/NVIDIA/spark-rapids/pull/3049)|Handle mmap exception more gracefully in RapidsShuffleServer|
|[#3067](https://github.com/NVIDIA/spark-rapids/pull/3067)|Update to UCX 1.11.0|
|[#3024](https://github.com/NVIDIA/spark-rapids/pull/3024)|Check validity of any() or all() results that could be null|
|[#3069](https://github.com/NVIDIA/spark-rapids/pull/3069)|Fall back to the CPU on window partition by struct or array|
|[#3068](https://github.com/NVIDIA/spark-rapids/pull/3068)|Profiling tool generate dot file fails on unescaped html characters|
|[#3048](https://github.com/NVIDIA/spark-rapids/pull/3048)|Apply unique committer job ID fix from SPARK-33230|
|[#3050](https://github.com/NVIDIA/spark-rapids/pull/3050)|Updates for google analytics [skip ci]|
|[#3015](https://github.com/NVIDIA/spark-rapids/pull/3015)|Fix ORC read error when read schema reorders file schema columns|
|[#3053](https://github.com/NVIDIA/spark-rapids/pull/3053)|cherry-pick #3028 [skip ci]|
|[#2887](https://github.com/NVIDIA/spark-rapids/pull/2887)|ORC reader supports struct|
|[#3032](https://github.com/NVIDIA/spark-rapids/pull/3032)|Add disorder read schema test case for Parquet|
|[#3022](https://github.com/NVIDIA/spark-rapids/pull/3022)|Add in docs to describe window performance|
|[#3018](https://github.com/NVIDIA/spark-rapids/pull/3018)|[BUG] fix db script hides error issue|
|[#2953](https://github.com/NVIDIA/spark-rapids/pull/2953)|Add in support for rank and dense_rank|
|[#3009](https://github.com/NVIDIA/spark-rapids/pull/3009)|Propagate child output ordering in GpuCoalesceBatches|
|[#2989](https://github.com/NVIDIA/spark-rapids/pull/2989)|Re-enable Array support in Cartesian Joins, Broadcast Nested Loop Joins|
|[#2999](https://github.com/NVIDIA/spark-rapids/pull/2999)|Remove unused configuration setting spark.rapids.sql.castStringToInteger.enabled|
|[#2967](https://github.com/NVIDIA/spark-rapids/pull/2967)|Resolve hidden source incompatibility between Spark30x and Spark31x  Shims|
|[#2982](https://github.com/NVIDIA/spark-rapids/pull/2982)|Add FAQ entry for timezone error|
|[#2839](https://github.com/NVIDIA/spark-rapids/pull/2839)|GpuIf and GpuCoalesce support array and struct types|
|[#2987](https://github.com/NVIDIA/spark-rapids/pull/2987)|Update documentation for unsupported edge cases when casting from string to timestamp|
|[#2977](https://github.com/NVIDIA/spark-rapids/pull/2977)|Expire executors from the RAPIDS shuffle heartbeat manager on timeout|
|[#2985](https://github.com/NVIDIA/spark-rapids/pull/2985)|Move tools README to docs/additional-functionality/qualification-profiling-tools.md with some modification|
|[#2992](https://github.com/NVIDIA/spark-rapids/pull/2992)|Remove commented/redundant window-function tests.|
|[#2994](https://github.com/NVIDIA/spark-rapids/pull/2994)|Tweak RAPIDS Shuffle Manager configs for 21.08|
|[#2984](https://github.com/NVIDIA/spark-rapids/pull/2984)|Avoid comparing window range canonicalized plans on Spark 3.0.x|
|[#2970](https://github.com/NVIDIA/spark-rapids/pull/2970)|Put the GPU data back on host before processing cache on CPU|
|[#2986](https://github.com/NVIDIA/spark-rapids/pull/2986)|Avoid struct aliasing in test_round_robin_sort_fallback|
|[#2935](https://github.com/NVIDIA/spark-rapids/pull/2935)|Read the complete batch before returning when selectedAttributes is empty|
|[#2826](https://github.com/NVIDIA/spark-rapids/pull/2826)|CaseWhen supports scalar of list and struct|
|[#2978](https://github.com/NVIDIA/spark-rapids/pull/2978)|enable auto-merge from branch 21.08 to 21.10 [skip ci]|
|[#2946](https://github.com/NVIDIA/spark-rapids/pull/2946)|ORC reader supports list|
|[#2947](https://github.com/NVIDIA/spark-rapids/pull/2947)|Qualification tool: Filter based on timestamp in event logs|
|[#2973](https://github.com/NVIDIA/spark-rapids/pull/2973)|Assert that CPU and GPU row fields match when present|
|[#2974](https://github.com/NVIDIA/spark-rapids/pull/2974)|Qualification tool: fix performance regression|
|[#2948](https://github.com/NVIDIA/spark-rapids/pull/2948)|Remove unnecessary copies of ParquetCachedBatchSerializer|
|[#2968](https://github.com/NVIDIA/spark-rapids/pull/2968)|Fix AQE CustomShuffleReaderExec not seeing ShuffleQueryStageExec|
|[#2969](https://github.com/NVIDIA/spark-rapids/pull/2969)|Make the dir for spark301 shuffle shim match package name|
|[#2933](https://github.com/NVIDIA/spark-rapids/pull/2933)|Improve CAST string to float implementation to handle more edge cases|
|[#2963](https://github.com/NVIDIA/spark-rapids/pull/2963)|Add override getParquetFilters for shim 304|
|[#2956](https://github.com/NVIDIA/spark-rapids/pull/2956)|Profile Tool: make order consistent between runs|
|[#2924](https://github.com/NVIDIA/spark-rapids/pull/2924)|Fix bug when collecting directly from a GPU shuffle query stage with AQE on|
|[#2950](https://github.com/NVIDIA/spark-rapids/pull/2950)|Fix shutdown bugs in the RAPIDS Shuffle Manager|
|[#2922](https://github.com/NVIDIA/spark-rapids/pull/2922)|Improve UCX assertion to show the failed assertion|
|[#2961](https://github.com/NVIDIA/spark-rapids/pull/2961)|Fix ParquetFilters issue|
|[#2951](https://github.com/NVIDIA/spark-rapids/pull/2951)|Qualification tool: Allow app start and app name filtering and test with filesystem filters|
|[#2941](https://github.com/NVIDIA/spark-rapids/pull/2941)|Make test event log compression codec configurable|
|[#2919](https://github.com/NVIDIA/spark-rapids/pull/2919)|Fix bugs in CAST string to integer|
|[#2944](https://github.com/NVIDIA/spark-rapids/pull/2944)|Fix childExprs list for GpuWindowExpression, for Spark 3.1.x.|
|[#2917](https://github.com/NVIDIA/spark-rapids/pull/2917)|Refine GpuHashAggregateExec.setupReference|
|[#2909](https://github.com/NVIDIA/spark-rapids/pull/2909)|Support orc coalescing reading|
|[#2938](https://github.com/NVIDIA/spark-rapids/pull/2938)|Qualification tool: Add negation filter|
|[#2940](https://github.com/NVIDIA/spark-rapids/pull/2940)|qualification tool: add filtering by app start time|
|[#2928](https://github.com/NVIDIA/spark-rapids/pull/2928)|Qualification tool support recognizing decimal operations|
|[#2934](https://github.com/NVIDIA/spark-rapids/pull/2934)|Qualification tool: Add filter based on appName|
|[#2904](https://github.com/NVIDIA/spark-rapids/pull/2904)|Qualification and Profiling tool handle Read formats and datatypes|
|[#2927](https://github.com/NVIDIA/spark-rapids/pull/2927)|Restore aggregation sorted data hint|
|[#2932](https://github.com/NVIDIA/spark-rapids/pull/2932)|Profiling tool: Fix comparing spark2 and spark3 event logs|
|[#2926](https://github.com/NVIDIA/spark-rapids/pull/2926)|GPU Active Messages for all buffer types|
|[#2888](https://github.com/NVIDIA/spark-rapids/pull/2888)|Type check with the information from RapidsMeta|
|[#2903](https://github.com/NVIDIA/spark-rapids/pull/2903)|Fix cast string to bool|
|[#2895](https://github.com/NVIDIA/spark-rapids/pull/2895)|Add in running window optimization using scan|
|[#2859](https://github.com/NVIDIA/spark-rapids/pull/2859)|Add spillable batch caching and sort fallback to hash aggregation|
|[#2898](https://github.com/NVIDIA/spark-rapids/pull/2898)|Add fuzz tests for cast from string to other types|
|[#2881](https://github.com/NVIDIA/spark-rapids/pull/2881)|fix orc readers leak issue for ORC PERFILE type|
|[#2842](https://github.com/NVIDIA/spark-rapids/pull/2842)|Support STRUCT/STRING for LEAD()/LAG()|
|[#2880](https://github.com/NVIDIA/spark-rapids/pull/2880)|Added ParquetCachedBatchSerializer support for Databricks|
|[#2911](https://github.com/NVIDIA/spark-rapids/pull/2911)|Add in ID as sort for Job + Stage level aggregated task metrics|
|[#2914](https://github.com/NVIDIA/spark-rapids/pull/2914)|Profiling tool: add app index to tables that don't have it|
|[#2906](https://github.com/NVIDIA/spark-rapids/pull/2906)|Fix compiler warning|
|[#2890](https://github.com/NVIDIA/spark-rapids/pull/2890)|Fix cast to date bug|
|[#2908](https://github.com/NVIDIA/spark-rapids/pull/2908)|Fixes bad string contains in run_pyspark_from_build|
|[#2886](https://github.com/NVIDIA/spark-rapids/pull/2886)|Use UCP Listener for UCX connections and enable peer error handling|
|[#2875](https://github.com/NVIDIA/spark-rapids/pull/2875)|Add support for timeParserPolicy=LEGACY|
|[#2894](https://github.com/NVIDIA/spark-rapids/pull/2894)|Fixes a JVM leak for UCX TransactionRequests|
|[#2854](https://github.com/NVIDIA/spark-rapids/pull/2854)|Qualification Tool to output only the 'k' highest-ranked or 'k' lowest-ranked applications	|
|[#2873](https://github.com/NVIDIA/spark-rapids/pull/2873)|Fix infinite loop in MultiFileCloudPartitionReaderBase|
|[#2838](https://github.com/NVIDIA/spark-rapids/pull/2838)|Replace `toTitle` with `capitalize` for GpuInitCap|
|[#2870](https://github.com/NVIDIA/spark-rapids/pull/2870)|Avoid readers acquiring GPU on next batch query if not first batch|
|[#2882](https://github.com/NVIDIA/spark-rapids/pull/2882)|Refactor window operations to do them in the exec|
|[#2874](https://github.com/NVIDIA/spark-rapids/pull/2874)|Update audit script to clone branch-3.2 instead of master|
|[#2843](https://github.com/NVIDIA/spark-rapids/pull/2843)|Qualification/Profiling tool add tests for Spark2 event logs|
|[#2828](https://github.com/NVIDIA/spark-rapids/pull/2828)|add cloud reading for orc|
|[#2721](https://github.com/NVIDIA/spark-rapids/pull/2721)|Check-list for corner cases in testing.|
|[#2675](https://github.com/NVIDIA/spark-rapids/pull/2675)|Support for Decimals with negative scale for Parquet Cached Batch Serializer|
|[#2849](https://github.com/NVIDIA/spark-rapids/pull/2849)|Update release notes to include qualification and profiling tool|
|[#2852](https://github.com/NVIDIA/spark-rapids/pull/2852)|Fix hash aggregate tests leaking configs into other tests|
|[#2845](https://github.com/NVIDIA/spark-rapids/pull/2845)|Split window exec into multiple stages if needed|
|[#2853](https://github.com/NVIDIA/spark-rapids/pull/2853)|Tag last batch when coalescing|
|[#2851](https://github.com/NVIDIA/spark-rapids/pull/2851)|Fix build failure - update ucx profiling test to fix parameter type to getEventLogInfo|
|[#2785](https://github.com/NVIDIA/spark-rapids/pull/2785)|Profiling tool: Print UCX and GDS parameters|
|[#2840](https://github.com/NVIDIA/spark-rapids/pull/2840)|Fix Gpu -> GPU|
|[#2844](https://github.com/NVIDIA/spark-rapids/pull/2844)|Document Qualification tool Spark requirements|
|[#2787](https://github.com/NVIDIA/spark-rapids/pull/2787)|Add metrics definition link to tool README.md[skip ci] |
|[#2841](https://github.com/NVIDIA/spark-rapids/pull/2841)|Add a threadpool to Qualification tool to process logs in parallel|
|[#2833](https://github.com/NVIDIA/spark-rapids/pull/2833)|Stop running so many versions of Spark unit tests for premerge|
|[#2837](https://github.com/NVIDIA/spark-rapids/pull/2837)|Append new authorized user to blossom-ci whitelist [skip ci]|
|[#2822](https://github.com/NVIDIA/spark-rapids/pull/2822)|Rewrite Qualification tool for better performance|
|[#2823](https://github.com/NVIDIA/spark-rapids/pull/2823)|Add semaphoreWaitTime and gpuOpTime for GpuRowToColumnarExec|
|[#2829](https://github.com/NVIDIA/spark-rapids/pull/2829)|Fix filtering directories on compression extension match|
|[#2720](https://github.com/NVIDIA/spark-rapids/pull/2720)|Add metrics documentation to the tuning guide|
|[#2816](https://github.com/NVIDIA/spark-rapids/pull/2816)|Improve some existing collectTime handling|
|[#2821](https://github.com/NVIDIA/spark-rapids/pull/2821)|Truncate long plan labels and refer to "print-plans"|
|[#2827](https://github.com/NVIDIA/spark-rapids/pull/2827)|Update cmake to build udf native [skip ci]|
|[#2793](https://github.com/NVIDIA/spark-rapids/pull/2793)|Report equivilant stages/sql ids as a part of compare|
|[#2810](https://github.com/NVIDIA/spark-rapids/pull/2810)|Use SecureRandom for UCPListener TCP port choice|
|[#2798](https://github.com/NVIDIA/spark-rapids/pull/2798)|Mirror apache repos to urm|
|[#2788](https://github.com/NVIDIA/spark-rapids/pull/2788)|Update the type signatures for some expressions|
|[#2792](https://github.com/NVIDIA/spark-rapids/pull/2792)|Automatically set spark.task.maxFailures and local[*, maxFailures]|
|[#2805](https://github.com/NVIDIA/spark-rapids/pull/2805)|Revert "Use UCX Active Messages for all shuffle transfers (#2735)"|
|[#2796](https://github.com/NVIDIA/spark-rapids/pull/2796)|show disk bytes spilled when GDS spill is enabled|
|[#2801](https://github.com/NVIDIA/spark-rapids/pull/2801)|Update pre-merge to use reserved_pool [skip ci]|
|[#2795](https://github.com/NVIDIA/spark-rapids/pull/2795)|Improve CBO debug logging|
|[#2794](https://github.com/NVIDIA/spark-rapids/pull/2794)|Prevent integer overflow when estimating data sizes in cost-based optimizer|
|[#2784](https://github.com/NVIDIA/spark-rapids/pull/2784)|Make spark303 shim version w/o snapshot and add shim layer for spark304|
|[#2744](https://github.com/NVIDIA/spark-rapids/pull/2744)|Cost-based optimizer: Implement simple cost model that demonstrates benefits with NDS queries|
|[#2762](https://github.com/NVIDIA/spark-rapids/pull/2762)| Profiling tool: Update comparison mode output format and add error handling|
|[#2761](https://github.com/NVIDIA/spark-rapids/pull/2761)|Update dot graph to include stages and remove some duplication|
|[#2760](https://github.com/NVIDIA/spark-rapids/pull/2760)|Add in application timeline to profiling tool|
|[#2735](https://github.com/NVIDIA/spark-rapids/pull/2735)|Use UCX Active Messages for all shuffle transfers|
|[#2732](https://github.com/NVIDIA/spark-rapids/pull/2732)|qualification and profiling tool support rolled and compressed event logs for CSPs and Apache Spark|
|[#2768](https://github.com/NVIDIA/spark-rapids/pull/2768)|Make window function test results deterministic.|
|[#2769](https://github.com/NVIDIA/spark-rapids/pull/2769)|Add developer documentation for Adaptive Query Execution|
|[#2532](https://github.com/NVIDIA/spark-rapids/pull/2532)|date_format should not suggest enabling incompatibleDateFormats for formats we cannot support|
|[#2743](https://github.com/NVIDIA/spark-rapids/pull/2743)|Disable dynamicAllocation and set maxFailures to 1 in integration tests|
|[#2749](https://github.com/NVIDIA/spark-rapids/pull/2749)|Revert "Add in support for lists in some joins (#2702)"|
|[#2181](https://github.com/NVIDIA/spark-rapids/pull/2181)|abstract the parquet coalescing reading|
|[#2753](https://github.com/NVIDIA/spark-rapids/pull/2753)|Merge branch-21.06 to branch-21.08 [skip ci]|
|[#2751](https://github.com/NVIDIA/spark-rapids/pull/2751)|remove invalid blossom-ci users [skip ci]|
|[#2707](https://github.com/NVIDIA/spark-rapids/pull/2707)|Support `KnownNotNull` running on GPU|
|[#2747](https://github.com/NVIDIA/spark-rapids/pull/2747)|Fix num_slices for test_single_nested_sort_in_part|
|[#2729](https://github.com/NVIDIA/spark-rapids/pull/2729)|fix 301db-shim typecheck typo|
|[#2726](https://github.com/NVIDIA/spark-rapids/pull/2726)|Fix local mode starting RAPIDS shuffle heartbeats|
|[#2722](https://github.com/NVIDIA/spark-rapids/pull/2722)|Support aggregation on NullType in RunningWindowExec|
|[#2719](https://github.com/NVIDIA/spark-rapids/pull/2719)|Avoid executing child plan twice in CoalesceExec|
|[#2586](https://github.com/NVIDIA/spark-rapids/pull/2586)|Update metrics use in GpuUnionExec and GpuCoalesceExec|
|[#2716](https://github.com/NVIDIA/spark-rapids/pull/2716)|Add file size check to pre-merge CI|
|[#2554](https://github.com/NVIDIA/spark-rapids/pull/2554)|Upload build failure log to Github for external contributors access|
|[#2596](https://github.com/NVIDIA/spark-rapids/pull/2596)|Initial running window memory optimization|
|[#2702](https://github.com/NVIDIA/spark-rapids/pull/2702)|Add in support for arrays in BroadcastNestedLoopJoinExec and CartesianProductExec|
|[#2699](https://github.com/NVIDIA/spark-rapids/pull/2699)|Add a pre-commit hook to reject large files|
|[#2700](https://github.com/NVIDIA/spark-rapids/pull/2700)|Set numSlices and use parallelize to build dataframe for partition-se…|
|[#2548](https://github.com/NVIDIA/spark-rapids/pull/2548)|support collect_set in rolling window|
|[#2661](https://github.com/NVIDIA/spark-rapids/pull/2661)|Make tools inherit common dependency versions from parent pom|
|[#2668](https://github.com/NVIDIA/spark-rapids/pull/2668)|Remove CUDA 10.x from getting started guide [skip ci]|
|[#2676](https://github.com/NVIDIA/spark-rapids/pull/2676)|Profiling tool: Print Job Information in compare mode|
|[#2679](https://github.com/NVIDIA/spark-rapids/pull/2679)|Merge branch-21.06 to branch-21.08 [skip ci]|
|[#2677](https://github.com/NVIDIA/spark-rapids/pull/2677)|Add pre-merge independent stage timeout [skip ci]|
|[#2616](https://github.com/NVIDIA/spark-rapids/pull/2616)|support GpuSortArray|
|[#2582](https://github.com/NVIDIA/spark-rapids/pull/2582)|support parquet write arrays|
|[#2609](https://github.com/NVIDIA/spark-rapids/pull/2609)|Fix automerge failure from branch-21.06 to branch-21.08|
|[#2570](https://github.com/NVIDIA/spark-rapids/pull/2570)|Added nested structs to UnionExec|
|[#2581](https://github.com/NVIDIA/spark-rapids/pull/2581)|Fix merge conflict 2580 [skip ci]|
|[#2458](https://github.com/NVIDIA/spark-rapids/pull/2458)|Split batch by key for window operations|
|[#2565](https://github.com/NVIDIA/spark-rapids/pull/2565)|Merge branch-21.06 into branch-21.08|
|[#2563](https://github.com/NVIDIA/spark-rapids/pull/2563)|Document: git commit twice when copyright year updated by hook|
|[#2561](https://github.com/NVIDIA/spark-rapids/pull/2561)|Fixing the merge of 21.06 to 21.08 for comment changes in Profiling tool|
|[#2558](https://github.com/NVIDIA/spark-rapids/pull/2558)|Fix cdh shim version in 21.08 [skip ci]|
|[#2543](https://github.com/NVIDIA/spark-rapids/pull/2543)|Init branch-21.08|

## Release 21.06.2

### Bugs Fixed
|||
|:---|:---|
|[#3191](https://github.com/NVIDIA/spark-rapids/issues/3191)|[BUG] Databricks parquetFilters build failure in db 8.2 runtime|

### PRs
|||
|:---|:---|
|[#3209](https://github.com/NVIDIA/spark-rapids/pull/3209)|Update 21.06.2 changelog [skip ci]|
|[#3208](https://github.com/NVIDIA/spark-rapids/pull/3208)|Update rapids plugin version to 21.06.2 [skip ci]|
|[#3207](https://github.com/NVIDIA/spark-rapids/pull/3207)|Disable auto-merge from 21.06 to 21.08 [skip ci]|
|[#3205](https://github.com/NVIDIA/spark-rapids/pull/3205)|Branch 21.06 databricks update [skip ci]|
|[#3198](https://github.com/NVIDIA/spark-rapids/pull/3198)|Databricks parquetFilters api change in db 8.2 runtime|

## Release 21.06.1

### Bugs Fixed
|||
|:---|:---|
|[#3098](https://github.com/NVIDIA/spark-rapids/issues/3098)|[BUG] Databricks parquetFilters build failure|

### PRs
|||
|:---|:---|
|[#3127](https://github.com/NVIDIA/spark-rapids/pull/3127)|Update CHANGELOG for the release v21.06.1 [skip ci]|
|[#3123](https://github.com/NVIDIA/spark-rapids/pull/3123)|Update rapids plugin version to 21.06.1 [skip ci]|
|[#3118](https://github.com/NVIDIA/spark-rapids/pull/3118)|Fix databricks 3.0.1 for ParquetFilters api change|
|[#3119](https://github.com/NVIDIA/spark-rapids/pull/3119)|Branch 21.06 databricks update [skip ci]|

## Release 21.06

### Features
|||
|:---|:---|
|[#2483](https://github.com/NVIDIA/spark-rapids/issues/2483)|[FEA] Profiling and qualification tool|
|[#951](https://github.com/NVIDIA/spark-rapids/issues/951)|[FEA] Create Cloudera shim layer|
|[#2481](https://github.com/NVIDIA/spark-rapids/issues/2481)|[FEA] Support Spark 3.1.2|
|[#2530](https://github.com/NVIDIA/spark-rapids/issues/2530)|[FEA] Add support for Struct columns in CoalesceExec|
|[#2512](https://github.com/NVIDIA/spark-rapids/issues/2512)|[FEA] Report gpuOpTime not totalTime for expand, generate, and range execs|
|[#63](https://github.com/NVIDIA/spark-rapids/issues/63)|[FEA] support ConcatWs sql function|
|[#2501](https://github.com/NVIDIA/spark-rapids/issues/2501)|[FEA] Add support for scalar structs to named_struct|
|[#2286](https://github.com/NVIDIA/spark-rapids/issues/2286)|[FEA] update UCX documentation for branch 21.06|
|[#2436](https://github.com/NVIDIA/spark-rapids/issues/2436)|[FEA] Support nested types in CreateNamedStruct|
|[#2461](https://github.com/NVIDIA/spark-rapids/issues/2461)|[FEA] Report gpuOpTime instead of totalTime for project, filter, window, limit|
|[#2465](https://github.com/NVIDIA/spark-rapids/issues/2465)|[FEA] GpuFilterExec should report gpuOpTime not totalTime|
|[#2013](https://github.com/NVIDIA/spark-rapids/issues/2013)|[FEA] Support concatenating ArrayType columns|
|[#2425](https://github.com/NVIDIA/spark-rapids/issues/2425)|[FEA] Support for casting array of floats to array of doubles|
|[#2012](https://github.com/NVIDIA/spark-rapids/issues/2012)|[FEA] Support Window functions(lead & lag) for ArrayType|
|[#2011](https://github.com/NVIDIA/spark-rapids/issues/2011)|[FEA] Support creation of 2D array type|
|[#1582](https://github.com/NVIDIA/spark-rapids/issues/1582)|[FEA] Allow StructType as input and output type to InMemoryTableScan and InMemoryRelation|
|[#216](https://github.com/NVIDIA/spark-rapids/issues/216)|[FEA] Range window-functions must support non-timestamp order-by expressions|
|[#2390](https://github.com/NVIDIA/spark-rapids/issues/2390)|[FEA] CI/CD for databricks 8.2 runtime|
|[#2273](https://github.com/NVIDIA/spark-rapids/issues/2273)|[FEA] Enable struct type columns for GpuHashAggregateExec|
|[#20](https://github.com/NVIDIA/spark-rapids/issues/20)|[FEA] Support out of core joins|
|[#2160](https://github.com/NVIDIA/spark-rapids/issues/2160)|[FEA] Support Databricks 8.2 ML Runtime|
|[#2330](https://github.com/NVIDIA/spark-rapids/issues/2330)|[FEA] Enable hash partitioning with arrays|
|[#1103](https://github.com/NVIDIA/spark-rapids/issues/1103)|[FEA] Support date_format on GPU|
|[#1125](https://github.com/NVIDIA/spark-rapids/issues/1125)|[FEA] explode() can take expressions that generate arrays|
|[#1605](https://github.com/NVIDIA/spark-rapids/issues/1605)|[FEA] Support sorting on struct type keys|

### Performance
|||
|:---|:---|
|[#1445](https://github.com/NVIDIA/spark-rapids/issues/1445)|[FEA] GDS Integration|
|[#1588](https://github.com/NVIDIA/spark-rapids/issues/1588)|Rapids shuffle - UCX active messages|
|[#2367](https://github.com/NVIDIA/spark-rapids/issues/2367)|[FEA] CBO: Implement costs for memory access and launching kernels|
|[#2431](https://github.com/NVIDIA/spark-rapids/issues/2431)|[FEA] CBO should show benefits with q24b with decimals enabled|

### Bugs Fixed
|||
|:---|:---|
|[#2652](https://github.com/NVIDIA/spark-rapids/issues/2652)|[BUG] No Job Found. Exiting.|
|[#2659](https://github.com/NVIDIA/spark-rapids/issues/2659)|[FEA] Group profiling tool "Potential Problems"|
|[#2680](https://github.com/NVIDIA/spark-rapids/issues/2680)|[BUG] cast can throw NPE|
|[#2628](https://github.com/NVIDIA/spark-rapids/issues/2628)|[BUG] failed to build plugin in databricks runtime 8.2|
|[#2605](https://github.com/NVIDIA/spark-rapids/issues/2605)|[BUG] test_pandas_map_udf_nested_type failed in Yarn integration|
|[#2622](https://github.com/NVIDIA/spark-rapids/issues/2622)|[BUG] compressed event logs are not processed|
|[#2478](https://github.com/NVIDIA/spark-rapids/issues/2478)|[BUG] When tasks complete, cancel pending UCX requests|
|[#1953](https://github.com/NVIDIA/spark-rapids/issues/1953)|[BUG] Could not allocate native memory when running DLRM ETL with --output_ordering input on A100|
|[#2495](https://github.com/NVIDIA/spark-rapids/issues/2495)|[BUG] scaladoc warning  GpuParquetScan.scala:727 "discarding unmoored doc comment"|
|[#2368](https://github.com/NVIDIA/spark-rapids/issues/2368)|[BUG] Mismatched number of columns while performing `GpuSort`|
|[#2407](https://github.com/NVIDIA/spark-rapids/issues/2407)|[BUG] test_round_robin_sort_fallback failed|
|[#2497](https://github.com/NVIDIA/spark-rapids/issues/2497)|[BUG] GpuExec failed to find metric totalTime in databricks env|
|[#2473](https://github.com/NVIDIA/spark-rapids/issues/2473)|[BUG] enable test_window_aggs_for_rows_lead_lag_on_arrays and make the order unambiguous|
|[#2489](https://github.com/NVIDIA/spark-rapids/issues/2489)|[BUG] Queries with window expressions fail when cost-based optimizer is enabled|
|[#2457](https://github.com/NVIDIA/spark-rapids/issues/2457)|[BUG] test_window_aggs_for_rows_lead_lag_on_arrays failed|
|[#2371](https://github.com/NVIDIA/spark-rapids/issues/2371)|[BUG] Performance regression for crossjoin on 0.6 comparing to 0.5|
|[#2372](https://github.com/NVIDIA/spark-rapids/issues/2372)|[BUG] FAILED ../../src/main/python/udf_cudf_test.py::test_window|
|[#2404](https://github.com/NVIDIA/spark-rapids/issues/2404)|[BUG] test_hash_pivot_groupby_nan_fallback failed on Dataproc |
|[#2474](https://github.com/NVIDIA/spark-rapids/issues/2474)|[BUG] when ucp listener enabled we bind 16 times always|
|[#2427](https://github.com/NVIDIA/spark-rapids/issues/2427)|[BUG] test_union_struct_missing_children[(Struct(not_null) failed in databricks310 and spark 311|
|[#2455](https://github.com/NVIDIA/spark-rapids/issues/2455)|[BUG] CaseWhen crashes on literal arrays|
|[#2421](https://github.com/NVIDIA/spark-rapids/issues/2421)|[BUG] NPE when running mapInPandas Pandas UDF in 0.5GA|
|[#2428](https://github.com/NVIDIA/spark-rapids/issues/2428)|[BUG] Intermittent ValueError in test_struct_groupby_count|
|[#1628](https://github.com/NVIDIA/spark-rapids/issues/1628)|[BUG] TPC-DS-like query 24a and 24b at scale=3TB fails with OOM|
|[#2276](https://github.com/NVIDIA/spark-rapids/issues/2276)|[BUG] SPARK-33386 - ansi-mode changed ElementAt/Elt/GetArray behavior in Spark 3.1.1 - fallback to cpu|
|[#2309](https://github.com/NVIDIA/spark-rapids/issues/2309)|[BUG] legacy cast of a struct column to string with a single nested null column yields null instead of '[]' |
|[#2315](https://github.com/NVIDIA/spark-rapids/issues/2315)|[BUG] legacy struct cast to string crashes on a two field struct|
|[#2406](https://github.com/NVIDIA/spark-rapids/issues/2406)|[BUG] test_struct_groupby_count failed|
|[#2378](https://github.com/NVIDIA/spark-rapids/issues/2378)|[BUG] java.lang.ClassCastException: GpuCompressedColumnVector cannot be cast to GpuColumnVector|
|[#2355](https://github.com/NVIDIA/spark-rapids/issues/2355)|[BUG] convertDecimal64ToDecimal32Wrapper leaks ColumnView instances|
|[#2346](https://github.com/NVIDIA/spark-rapids/issues/2346)|[BUG] segfault when using `UcpListener` in TCP-only setup|
|[#2364](https://github.com/NVIDIA/spark-rapids/issues/2364)|[BUG]  qa_nightly_select_test.py::test_select integration test fails |
|[#2302](https://github.com/NVIDIA/spark-rapids/issues/2302)|[BUG] Int96 are not being written as expected|
|[#2359](https://github.com/NVIDIA/spark-rapids/issues/2359)|[BUG] Alias is different in spark 3.1.0 but our canonicalization code doesn't handle|
|[#2277](https://github.com/NVIDIA/spark-rapids/issues/2277)|[BUG] spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED or LEGACY still fails to read LEGACY date from parquet|
|[#2320](https://github.com/NVIDIA/spark-rapids/issues/2320)|[BUG] TypeChecks diagnostics outputs column ids instead of unsupported types |
|[#2238](https://github.com/NVIDIA/spark-rapids/issues/2238)|[BUG] Unnecessary to cache the batches that will be sent to Python in `FlatMapGroupInPandas`.|
|[#1811](https://github.com/NVIDIA/spark-rapids/issues/1811)|[BUG] window_function_test.py::test_multi_types_window_aggs_for_rows_lead_lag[partBy failed|

### PRs
|||
|:---|:---|
|[#2817](https://github.com/NVIDIA/spark-rapids/pull/2817)|Update changelog for v21.06.0 release [skip ci]||
|[#2806](https://github.com/NVIDIA/spark-rapids/pull/2806)|Noted testing for A10, noted that min driver ver is HW specific|
|[#2797](https://github.com/NVIDIA/spark-rapids/pull/2797)|Update documentation for InitCap incompatibility|
|[#2774](https://github.com/NVIDIA/spark-rapids/pull/2774)|Update changelog for 21.06 release [skip ci]|
|[#2770](https://github.com/NVIDIA/spark-rapids/pull/2770)|[Doc] add more for Alluxio page [skip ci]|
|[#2745](https://github.com/NVIDIA/spark-rapids/pull/2745)|Add link to Mellanox RoCE documentation and mention --without-ucx installation option|
|[#2740](https://github.com/NVIDIA/spark-rapids/pull/2740)|Update cudf Java bindings to 21.06.1|
|[#2664](https://github.com/NVIDIA/spark-rapids/pull/2664)|Update changelog for 21.06 release [skip ci]|
|[#2697](https://github.com/NVIDIA/spark-rapids/pull/2697)|fix GDS spill bug when copying from the batch write buffer|
|[#2691](https://github.com/NVIDIA/spark-rapids/pull/2691)|Update properties to check if table there|
|[#2687](https://github.com/NVIDIA/spark-rapids/pull/2687)|Remove CUDA 10.x from getting started guide (#2668)|
|[#2686](https://github.com/NVIDIA/spark-rapids/pull/2686)|Profiling tool: Print Job Information in compare mode|
|[#2657](https://github.com/NVIDIA/spark-rapids/pull/2657)|Print CPU and GPU output when _assert_equal fails to help debug given…|
|[#2681](https://github.com/NVIDIA/spark-rapids/pull/2681)|Avoid NPE when casting empty strings to ints|
|[#2669](https://github.com/NVIDIA/spark-rapids/pull/2669)|Fix multiple problems reported and improve error handling|
|[#2666](https://github.com/NVIDIA/spark-rapids/pull/2666)|[DOC]Update custom image guide in GCP dataproc to reduce cluster startup time|
|[#2665](https://github.com/NVIDIA/spark-rapids/pull/2665)|Update docs to move RAPIDS Shuffle out of beta [skip ci]|
|[#2671](https://github.com/NVIDIA/spark-rapids/pull/2671)|Clean profiling&qualification tool README|
|[#2673](https://github.com/NVIDIA/spark-rapids/pull/2673)|Profiling tool: Enable tests and update compressed event log|
|[#2672](https://github.com/NVIDIA/spark-rapids/pull/2672)|Update cudfjni dependency version to 21.06.0|
|[#2663](https://github.com/NVIDIA/spark-rapids/pull/2663)|Qualification tool - add in estimating the App end time when the event log missing application end event|
|[#2600](https://github.com/NVIDIA/spark-rapids/pull/2600)|Accelerate `RunningWindow` queries on GPU|
|[#2651](https://github.com/NVIDIA/spark-rapids/pull/2651)|Profiling tool - fix reporting contains dataset when sql time 0|
|[#2623](https://github.com/NVIDIA/spark-rapids/pull/2623)|Fixed minor mistakes in documentation|
|[#2631](https://github.com/NVIDIA/spark-rapids/pull/2631)|Update docs for Databricks 8.2 ML|
|[#2638](https://github.com/NVIDIA/spark-rapids/pull/2638)|Add an init script for databricks 7.3ML with CUDA11.0 installed|
|[#2643](https://github.com/NVIDIA/spark-rapids/pull/2643)|Profiling tool: Health check follow on|
|[#2640](https://github.com/NVIDIA/spark-rapids/pull/2640)|Add physical plan to the dot file as the graph label|
|[#2637](https://github.com/NVIDIA/spark-rapids/pull/2637)|Fix databricks for 3.1.1|
|[#2577](https://github.com/NVIDIA/spark-rapids/pull/2577)|Update download.md and FAQ.md for 21.06.0|
|[#2636](https://github.com/NVIDIA/spark-rapids/pull/2636)|Profiling tool - Fix file writer for generating dot graphs, supporting writing sql plans to a file, change output to subdirectory|
|[#2625](https://github.com/NVIDIA/spark-rapids/pull/2625)|Exclude failed jobs/queries from Qualification tool output|
|[#2626](https://github.com/NVIDIA/spark-rapids/pull/2626)|Enable processing of compressed Spark event logs|
|[#2632](https://github.com/NVIDIA/spark-rapids/pull/2632)|Profiling tool: Add support for health check.|
|[#2627](https://github.com/NVIDIA/spark-rapids/pull/2627)|Ignore order for map udf test|
|[#2620](https://github.com/NVIDIA/spark-rapids/pull/2620)|Change aggregation of executor CPU and run time for Qualification tool to speed up query|
|[#2618](https://github.com/NVIDIA/spark-rapids/pull/2618)|Correct an issue for README for tools and also correct s3 solution in Args.scala|
|[#2612](https://github.com/NVIDIA/spark-rapids/pull/2612)|Profiling tool, add in job to stage, duration, executor cpu time, fix writing to HDFS|
|[#2614](https://github.com/NVIDIA/spark-rapids/pull/2614)|change rapids-4-spark-tools directory to tools in deploy script [skip ci]|
|[#2611](https://github.com/NVIDIA/spark-rapids/pull/2611)|Revert "disable cudf_udf tests for #2521"|
|[#2604](https://github.com/NVIDIA/spark-rapids/pull/2604)|Profile/qualification tool error handling improvements and support spark < 3.1.1|
|[#2598](https://github.com/NVIDIA/spark-rapids/pull/2598)|Rename rapids-4-spark-tools directory to tools|
|[#2576](https://github.com/NVIDIA/spark-rapids/pull/2576)|Add filter support for qualification and profiling tool.|
|[#2603](https://github.com/NVIDIA/spark-rapids/pull/2603)|Add the doc for -g option of the profiling tool.|
|[#2594](https://github.com/NVIDIA/spark-rapids/pull/2594)|Change the README of the qualification and profiling tool to match the current version.|
|[#2591](https://github.com/NVIDIA/spark-rapids/pull/2591)|Implement test for qualification tool sql metric aggregates|
|[#2590](https://github.com/NVIDIA/spark-rapids/pull/2590)|Profiling tool support for collection and analysis|
|[#2587](https://github.com/NVIDIA/spark-rapids/pull/2587)|Handle UCX connection timeouts from heartbeats more gracefully|
|[#2588](https://github.com/NVIDIA/spark-rapids/pull/2588)|Fix package name|
|[#2574](https://github.com/NVIDIA/spark-rapids/pull/2574)|Add Qualification tool support|
|[#2571](https://github.com/NVIDIA/spark-rapids/pull/2571)|Change test_single_sort_in_part to print source data frame on failure|
|[#2569](https://github.com/NVIDIA/spark-rapids/pull/2569)|Remove -SNAPSHOT in documentation in preparation for release|
|[#2429](https://github.com/NVIDIA/spark-rapids/pull/2429)|Change RMM_ALLOC_FRACTION to represent percentage of available memory, rather than total memory, for initial allocation|
|[#2553](https://github.com/NVIDIA/spark-rapids/pull/2553)|Cancel requests that are queued for a client/handler on error|
|[#2566](https://github.com/NVIDIA/spark-rapids/pull/2566)|expose unspill config option|
|[#2460](https://github.com/NVIDIA/spark-rapids/pull/2460)|align GDS reads/writes to 4 KiB|
|[#2515](https://github.com/NVIDIA/spark-rapids/pull/2515)|Remove fetchTime and standardize on collectTime|
|[#2523](https://github.com/NVIDIA/spark-rapids/pull/2523)|Not compile RapidsUDF when udf compiler is enabled|
|[#2538](https://github.com/NVIDIA/spark-rapids/pull/2538)|Fixed code indentation in ParquetCachedBatchSerializer|
|[#2559](https://github.com/NVIDIA/spark-rapids/pull/2559)|Release profiling tool jar to maven central|
|[#2423](https://github.com/NVIDIA/spark-rapids/pull/2423)|Add cloudera shim layer|
|[#2520](https://github.com/NVIDIA/spark-rapids/pull/2520)|Add event logs for integration tests|
|[#2525](https://github.com/NVIDIA/spark-rapids/pull/2525)|support interval.microseconds for range window TimeStampType|
|[#2536](https://github.com/NVIDIA/spark-rapids/pull/2536)|Don't do an extra shuffle in some TopN cases|
|[#2508](https://github.com/NVIDIA/spark-rapids/pull/2508)|Refactor the code for conditional expressions|
|[#2542](https://github.com/NVIDIA/spark-rapids/pull/2542)|enable auto-merge from 21.06 to 21.08 [skip ci]|
|[#2540](https://github.com/NVIDIA/spark-rapids/pull/2540)|Update spark 312 shim, and Add spark 313-SNAPSHOT shim|
|[#2539](https://github.com/NVIDIA/spark-rapids/pull/2539)|disable cudf_udf tests for #2521|
|[#2514](https://github.com/NVIDIA/spark-rapids/pull/2514)|Add Struct support for ParquetWriter|
|[#2534](https://github.com/NVIDIA/spark-rapids/pull/2534)|Remove scaladoc on an internal method to avoid warning during build|
|[#2537](https://github.com/NVIDIA/spark-rapids/pull/2537)|Add CentOS documentation and improve dockerfiles for UCX|
|[#2531](https://github.com/NVIDIA/spark-rapids/pull/2531)|Add  nested types and decimals to CoalesceExec|
|[#2513](https://github.com/NVIDIA/spark-rapids/pull/2513)|Report opTime not totalTime for expand, range, and generate execs|
|[#2533](https://github.com/NVIDIA/spark-rapids/pull/2533)|Fix concat_ws test specifying only a separator for databricks|
|[#2528](https://github.com/NVIDIA/spark-rapids/pull/2528)|Make GenerateDot test more robust|
|[#2529](https://github.com/NVIDIA/spark-rapids/pull/2529)|Change Databricks 310 shim to be 311 to match reported spark.version|
|[#2479](https://github.com/NVIDIA/spark-rapids/pull/2479)|Support concat with separator on GPU|
|[#2507](https://github.com/NVIDIA/spark-rapids/pull/2507)|Improve test coverage for sorting structs|
|[#2526](https://github.com/NVIDIA/spark-rapids/pull/2526)|Improve debug print to include addresses and null counts|
|[#2463](https://github.com/NVIDIA/spark-rapids/pull/2463)|Add EMR 6.3 documentation|
|[#2516](https://github.com/NVIDIA/spark-rapids/pull/2516)|Avoid listener race collecting wrong plan in assert_gpu_fallback_collect|
|[#2505](https://github.com/NVIDIA/spark-rapids/pull/2505)|Qualification tool updates for datasets, udf, and misc fixes|
|[#2509](https://github.com/NVIDIA/spark-rapids/pull/2509)|Added in basic support for scalar structs to named_struct|
|[#2449](https://github.com/NVIDIA/spark-rapids/pull/2449)|Add code for generating dot file visualizations|
|[#2475](https://github.com/NVIDIA/spark-rapids/pull/2475)|Update shuffle documentation for branch-21.06 and UCX 1.10.1|
|[#2500](https://github.com/NVIDIA/spark-rapids/pull/2500)|Update Dockerfile for native UDF|
|[#2506](https://github.com/NVIDIA/spark-rapids/pull/2506)|Support creating Scalars/ColumnVectors from utf8 strings directly.|
|[#2502](https://github.com/NVIDIA/spark-rapids/pull/2502)|Remove work around for nulls in semi-anti joins|
|[#2503](https://github.com/NVIDIA/spark-rapids/pull/2503)|Remove temporary logging and adjust test column names|
|[#2499](https://github.com/NVIDIA/spark-rapids/pull/2499)|Fix regression in TOTAL_TIME metrics for Databricks|
|[#2498](https://github.com/NVIDIA/spark-rapids/pull/2498)|Add in basic support for scalar maps and allow nesting in named_struct|
|[#2496](https://github.com/NVIDIA/spark-rapids/pull/2496)|Add comments for lazy binding in WindowInPandas|
|[#2493](https://github.com/NVIDIA/spark-rapids/pull/2493)|improve window agg test for range numeric types|
|[#2491](https://github.com/NVIDIA/spark-rapids/pull/2491)|Fix regression in cost-based optimizer when calculating cost for Window operations|
|[#2482](https://github.com/NVIDIA/spark-rapids/pull/2482)|Window tests with smaller batches|
|[#2490](https://github.com/NVIDIA/spark-rapids/pull/2490)|Add temporary logging for Dataproc round robin fallback issue|
|[#2486](https://github.com/NVIDIA/spark-rapids/pull/2486)|Remove the null replacement in `computePredicate`|
|[#2469](https://github.com/NVIDIA/spark-rapids/pull/2469)|Adding additional functionalities to profiling tool |
|[#2462](https://github.com/NVIDIA/spark-rapids/pull/2462)|Report gpuOpTime instead of totalTime for project, filter, limit, and window|
|[#2484](https://github.com/NVIDIA/spark-rapids/pull/2484)|Fix the failing test `test_window` on Databricks|
|[#2472](https://github.com/NVIDIA/spark-rapids/pull/2472)|Fix hash_aggregate_test|
|[#2476](https://github.com/NVIDIA/spark-rapids/pull/2476)|Fix for UCP Listener created spark.port.maxRetries times|
|[#2471](https://github.com/NVIDIA/spark-rapids/pull/2471)|skip test_window_aggs_for_rows_lead_lag_on_arrays|
|[#2446](https://github.com/NVIDIA/spark-rapids/pull/2446)|Update plugin version to 21.06.0|
|[#2409](https://github.com/NVIDIA/spark-rapids/pull/2409)|Change shuffle metadata messages to use UCX Active Messages|
|[#2397](https://github.com/NVIDIA/spark-rapids/pull/2397)|Include memory access costs in cost models (cost-based optimizer)|
|[#2442](https://github.com/NVIDIA/spark-rapids/pull/2442)|fix GpuCreateNamedStruct not serializable issue|
|[#2379](https://github.com/NVIDIA/spark-rapids/pull/2379)|support GpuConcat on ArrayType|
|[#2456](https://github.com/NVIDIA/spark-rapids/pull/2456)|Fall back to the CPU for literal array values on case/when|
|[#2447](https://github.com/NVIDIA/spark-rapids/pull/2447)|Filter out the nulls after slicing the batches.|
|[#2426](https://github.com/NVIDIA/spark-rapids/pull/2426)|Implement cast of nested arrays|
|[#2299](https://github.com/NVIDIA/spark-rapids/pull/2299)|support creating array of array|
|[#2451](https://github.com/NVIDIA/spark-rapids/pull/2451)|Update tuning docs to add batch size recommendations.|
|[#2435](https://github.com/NVIDIA/spark-rapids/pull/2435)|support lead/lag on arrays|
|[#2448](https://github.com/NVIDIA/spark-rapids/pull/2448)|support creating list ColumnVector for Literal(ArrayType(NullType))|
|[#2402](https://github.com/NVIDIA/spark-rapids/pull/2402)|Add profiling tool|
|[#2313](https://github.com/NVIDIA/spark-rapids/pull/2313)|Supports `GpuLiteral` of array type|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
