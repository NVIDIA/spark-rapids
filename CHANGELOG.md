# Change log
Generated on 2023-04-18

## Release 23.04

### Features
|||
|:---|:---|
|[#7985](https://github.com/NVIDIA/spark-rapids/issues/7985)|[FEA] Expose Alluxio master URL to support K8s Env|
|[#7880](https://github.com/NVIDIA/spark-rapids/issues/7880)|[FEA] retry framework task level metrics|
|[#7394](https://github.com/NVIDIA/spark-rapids/issues/7394)|[FEA] Support Delta Lake auto compaction|
|[#7463](https://github.com/NVIDIA/spark-rapids/issues/7463)|[FEA] Drop support for Databricks-9.1 ML LTS|
|[#7253](https://github.com/NVIDIA/spark-rapids/issues/7253)|[FEA] Implement OOM retry framework|
|[#7042](https://github.com/NVIDIA/spark-rapids/issues/7042)|[FEA] Add support in the tools event parsing for ML functions, libraries, and expressions|

### Performance
|||
|:---|:---|
|[#7907](https://github.com/NVIDIA/spark-rapids/issues/7907)|[FEA] Optimize regexp_replace in multi-replace scenarios|
|[#7691](https://github.com/NVIDIA/spark-rapids/issues/7691)|[FEA] Upgrade and document UCX 1.14|
|[#6516](https://github.com/NVIDIA/spark-rapids/issues/6516)|[FEA] Enable RAPIDS Shuffle Manager smoke testing for the databricks environment|
|[#7695](https://github.com/NVIDIA/spark-rapids/issues/7695)|[FEA] Transpile regexp_extract expression to only have the single capture group that is needed|
|[#7393](https://github.com/NVIDIA/spark-rapids/issues/7393)|[FEA] Support Delta Lake optimized write|
|[#6561](https://github.com/NVIDIA/spark-rapids/issues/6561)|[FEA] Make SpillableColumnarBatch inform Spill code of actual usage of the batch|
|[#6864](https://github.com/NVIDIA/spark-rapids/issues/6864)|[BUG] Spilling logic can spill data that cannot be freed|

### Bugs Fixed
|||
|:---|:---|
|[#8111](https://github.com/NVIDIA/spark-rapids/issues/8111)|[BUG] test_delta_delete_entire_table failed in databricks 10.4 runtime|
|[#8074](https://github.com/NVIDIA/spark-rapids/issues/8074)|[BUG] test_parquet_read_nano_as_longs_31x failed on Dataproc|
|[#7997](https://github.com/NVIDIA/spark-rapids/issues/7997)|[BUG] executors died with too much off heap in yarn UCX CI `udf_test`|
|[#8067](https://github.com/NVIDIA/spark-rapids/issues/8067)|[BUG] extras jar sometimes fails to load|
|[#8038](https://github.com/NVIDIA/spark-rapids/issues/8038)|[BUG] vector leaked when running NDS 3TB with memory restricted|
|[#8030](https://github.com/NVIDIA/spark-rapids/issues/8030)|[BUG] test_re_replace_no_unicode_fallback test failes on integratoin tests Yarn|
|[#7971](https://github.com/NVIDIA/spark-rapids/issues/7971)|[BUG] withRestoreOnRetry should look at Throwable causes in addition to retry OOMs|
|[#6990](https://github.com/NVIDIA/spark-rapids/issues/6990)|[BUG] Several integration test failures in Spark-3.4 SNAPSHOT build|
|[#7924](https://github.com/NVIDIA/spark-rapids/issues/7924)|[BUG] Physical plan for regexp_extract does not escape newlines|
|[#7341](https://github.com/NVIDIA/spark-rapids/issues/7341)|[BUG] Leverage OOM retry framework for ORC writes|
|[#7921](https://github.com/NVIDIA/spark-rapids/issues/7921)|[BUG] ORC writes with bloom filters enabled do not fall back to the CPU|
|[#7818](https://github.com/NVIDIA/spark-rapids/issues/7818)|[BUG] Reuse of broadcast exchange can lead to unnecessary CPU fallback|
|[#7904](https://github.com/NVIDIA/spark-rapids/issues/7904)|[BUG] test_write_sql_save_table sporadically fails on Pascal|
|[#7922](https://github.com/NVIDIA/spark-rapids/issues/7922)|[BUG] YARN IT test test_optimized_hive_ctas_basic failures|
|[#7933](https://github.com/NVIDIA/spark-rapids/issues/7933)|[BUG] NDS running hits DPP error on Databricks 10.4 when enable Alluxio cache.|
|[#7850](https://github.com/NVIDIA/spark-rapids/issues/7850)|[BUG] nvcomp usage for the UCX mode of the shuffle manager is broken|
|[#7927](https://github.com/NVIDIA/spark-rapids/issues/7927)|[BUG] Shimplify adding new shim layer fails|
|[#6138](https://github.com/NVIDIA/spark-rapids/issues/6138)|[BUG] cast timezone-awareness check positive for date/time-unrelated types|
|[#7914](https://github.com/NVIDIA/spark-rapids/issues/7914)|[BUG] Parquet read with integer upcast crashes|
|[#6961](https://github.com/NVIDIA/spark-rapids/issues/6961)|[BUG] Using `\d` (or others) inside a character class results in "Unsupported escape character" |
|[#7908](https://github.com/NVIDIA/spark-rapids/issues/7908)|[BUG] Interpolate spark.version.classifier into scala:compile `secondaryCacheDir`|
|[#7707](https://github.com/NVIDIA/spark-rapids/issues/7707)|[BUG] IndexOutOfBoundsException when joining on 2 integer columns with DPP|
|[#7892](https://github.com/NVIDIA/spark-rapids/issues/7892)|[BUG] Invalid or unsupported escape character `t` when trying to use tab in regexp_replace|
|[#7640](https://github.com/NVIDIA/spark-rapids/issues/7640)|[BUG] GPU OOM using GpuRegExpExtract|
|[#7814](https://github.com/NVIDIA/spark-rapids/issues/7814)|[BUG] GPU's output differs from CPU's for big decimals when joining by sub-partitioning algorithm|
|[#7796](https://github.com/NVIDIA/spark-rapids/issues/7796)|[BUG] Parquet chunked reader size of output exceeds column size limit|
|[#7833](https://github.com/NVIDIA/spark-rapids/issues/7833)|[BUG] run_pyspark_from_build computes 5 MiB per runner instead of 5 GiB|
|[#7855](https://github.com/NVIDIA/spark-rapids/issues/7855)|[BUG] shuffle_test test_hash_grpby_sum failed OOM in premerge CI|
|[#7858](https://github.com/NVIDIA/spark-rapids/issues/7858)|[BUG] HostToGpuCoalesceIterator leaks all host batches|
|[#7826](https://github.com/NVIDIA/spark-rapids/issues/7826)|[BUG] buildall dist jar contains aggregator dependency|
|[#7729](https://github.com/NVIDIA/spark-rapids/issues/7729)|[BUG] Active GPU thread not holding the semaphore|
|[#7820](https://github.com/NVIDIA/spark-rapids/issues/7820)|[BUG] Restore pandas require_minimum_pandas_version() check|
|[#7829](https://github.com/NVIDIA/spark-rapids/issues/7829)|[BUG] Parquet buffer time not correct with multithreaded combining reader|
|[#7819](https://github.com/NVIDIA/spark-rapids/issues/7819)|[BUG] GpuDeviceManager allows setting UVM regardless of other RMM configs|
|[#7643](https://github.com/NVIDIA/spark-rapids/issues/7643)|[BUG] Databricks init scripts can fail silently|
|[#7799](https://github.com/NVIDIA/spark-rapids/issues/7799)|[BUG] Cannot lexicographic compare a table with a LIST of STRUCT column at ai.rapids.cudf.Table.sortOrder|
|[#7767](https://github.com/NVIDIA/spark-rapids/issues/7767)|[BUG] VS Code / Metals / Bloop integration fails with java.lang.RuntimeException: 'boom' |
|[#6383](https://github.com/NVIDIA/spark-rapids/issues/6383)|[SPARK-40066][SQL] ANSI mode: always return null on invalid access to map column|
|[#7093](https://github.com/NVIDIA/spark-rapids/issues/7093)|[BUG] Spark-3.4 - Integration test failures in map_test|
|[#7779](https://github.com/NVIDIA/spark-rapids/issues/7779)|[BUG] AlluxioUtilsSuite uses illegal character underscore in URI scheme|
|[#7725](https://github.com/NVIDIA/spark-rapids/issues/7725)|[BUG] cache_test failed w/ ParquetCachedBatchSerializer in spark 3.3.2-SNAPSHOT|
|[#7639](https://github.com/NVIDIA/spark-rapids/issues/7639)|[BUG] Databricks premerge failing with cannot find pytest|
|[#7694](https://github.com/NVIDIA/spark-rapids/issues/7694)|[BUG] Spark-3.4 build breaks due to removing InternalRowSet|
|[#6598](https://github.com/NVIDIA/spark-rapids/issues/6598)|[BUG] CUDA error when casting large column vector from long to string|
|[#7739](https://github.com/NVIDIA/spark-rapids/issues/7739)|[BUG] udf_test failed in databricks 11.3 ENV|
|[#5748](https://github.com/NVIDIA/spark-rapids/issues/5748)|[BUG] 3 cast tests fails on Spark 3.4.0|
|[#7688](https://github.com/NVIDIA/spark-rapids/issues/7688)|[BUG] GpuParquetScan fails with NullPointerException - Delta CDF query|
|[#7648](https://github.com/NVIDIA/spark-rapids/issues/7648)|[BUG] java.lang.ClassCastException: SerializeConcatHostBuffersDeserializeBatch cannot be cast to.HashedRelation|
|[#6988](https://github.com/NVIDIA/spark-rapids/issues/6988)|[BUG] Integration test failures with DecimalType on Spark-3.4 SNAPSHOT build|
|[#7615](https://github.com/NVIDIA/spark-rapids/issues/7615)|[BUG] Build fails on Spark 3.4|
|[#7557](https://github.com/NVIDIA/spark-rapids/issues/7557)|[AUDIT][SPARK-41970] Introduce SparkPath for typesafety|
|[#7617](https://github.com/NVIDIA/spark-rapids/issues/7617)|[BUG] Build 340 failed due to miss shim code for GpuShuffleMeta|

### PRs
|||
|:---|:---|
|[#8109](https://github.com/NVIDIA/spark-rapids/pull/8109)|Bump up JNI and private version to released 23.04.0|
|[#7939](https://github.com/NVIDIA/spark-rapids/pull/7939)|[Doc]update download docs for 2304 version[skip ci]|
|[#8127](https://github.com/NVIDIA/spark-rapids/pull/8127)|Avoid SQL result check of Delta Lake full delete on Databricks|
|[#8098](https://github.com/NVIDIA/spark-rapids/pull/8098)|Fix loading of ORC files with missing column names|
|[#8110](https://github.com/NVIDIA/spark-rapids/pull/8110)|Update ML integration page docs page [skip ci]|
|[#8103](https://github.com/NVIDIA/spark-rapids/pull/8103)|Add license of spark-rapids private in NOTICE-binary[skip ci]|
|[#8100](https://github.com/NVIDIA/spark-rapids/pull/8100)|Update/improve EMR getting started documentation [skip ci]|
|[#8101](https://github.com/NVIDIA/spark-rapids/pull/8101)|Improve OOM exception messages|
|[#8087](https://github.com/NVIDIA/spark-rapids/pull/8087)|Add an FAQ entry on encryption support [skip ci]|
|[#8076](https://github.com/NVIDIA/spark-rapids/pull/8076)|Add in docs about RetryOOM [skip ci]|
|[#8077](https://github.com/NVIDIA/spark-rapids/pull/8077)|Temporarily skip `test_parquet_read_nano_as_longs_31x` on dataproc|
|[#8071](https://github.com/NVIDIA/spark-rapids/pull/8071)|Fix error in deploy script [skip ci]|
|[#8070](https://github.com/NVIDIA/spark-rapids/pull/8070)|Fixes closed RapidsShuffleHandleImpl leak in ShuffleBufferCatalog|
|[#8069](https://github.com/NVIDIA/spark-rapids/pull/8069)|Fix loading extra jar|
|[#8044](https://github.com/NVIDIA/spark-rapids/pull/8044)|Fall back to CPU if  `spark.sql.legacy.parquet.nanosAsLong` is set|
|[#8049](https://github.com/NVIDIA/spark-rapids/pull/8049)|[DOC] Adding user tool info to main qualification docs page [skip ci]|
|[#8040](https://github.com/NVIDIA/spark-rapids/pull/8040)|Fix device vector leak in RmmRetryIterator.splitSpillableInHalfByRows|
|[#8031](https://github.com/NVIDIA/spark-rapids/pull/8031)|Fix regexp_replace integration test that should fallback when unicode is disabled|
|[#7828](https://github.com/NVIDIA/spark-rapids/pull/7828)|Fallback to arena allocator if RMM failed to initialize with async allocator|
|[#8006](https://github.com/NVIDIA/spark-rapids/pull/8006)|Handle caused-by retry exceptions in withRestoreOnRetry|
|[#8013](https://github.com/NVIDIA/spark-rapids/pull/8013)|[Doc] Adding user tools info into EMR getting started guide [skip ci]|
|[#8007](https://github.com/NVIDIA/spark-rapids/pull/8007)|Fix leak where RapidsShuffleIterator for a completed task was kept alive|
|[#8010](https://github.com/NVIDIA/spark-rapids/pull/8010)|Specify that UCX should be 1.12.1 only [skip ci]|
|[#7967](https://github.com/NVIDIA/spark-rapids/pull/7967)|Transpile simple choice-type regular expressions into lists of choices to use with string replace multi|
|[#7902](https://github.com/NVIDIA/spark-rapids/pull/7902)|Add oom retry handling for createGatherer in gpu hash joins|
|[#7986](https://github.com/NVIDIA/spark-rapids/pull/7986)|Provides a config to expose Alluxio master URL to support K8s Env|
|[#7936](https://github.com/NVIDIA/spark-rapids/pull/7936)|Stop showing internal details of ternary expressions in SparkPlan.toString|
|[#7972](https://github.com/NVIDIA/spark-rapids/pull/7972)|Add in retry for ORC writes|
|[#7975](https://github.com/NVIDIA/spark-rapids/pull/7975)|Publish documentation for private configs|
|[#7976](https://github.com/NVIDIA/spark-rapids/pull/7976)|Disable GPU write for ORC and Parquet, if bloom-filters are enabled.|
|[#7925](https://github.com/NVIDIA/spark-rapids/pull/7925)|Inject RetryOOM in CI where retry iterator is used|
|[#7970](https://github.com/NVIDIA/spark-rapids/pull/7970)|[DOCS] Updating qual tool docs from latest in tools repo|
|[#7952](https://github.com/NVIDIA/spark-rapids/pull/7952)|Add in minimal retry metrics|
|[#7884](https://github.com/NVIDIA/spark-rapids/pull/7884)|Add Python requirements file for integration tests|
|[#7958](https://github.com/NVIDIA/spark-rapids/pull/7958)|Add CheckpointRestore trait and withRestoreOnRetry|
|[#7849](https://github.com/NVIDIA/spark-rapids/pull/7849)|Fix CPU broadcast exchanges being left unreplaced due to AQE and reuse|
|[#7944](https://github.com/NVIDIA/spark-rapids/pull/7944)|Fix issue with dynamicpruning filters used in converted GPU scans when S3 paths are replaced with alluxio|
|[#7949](https://github.com/NVIDIA/spark-rapids/pull/7949)|Lazily unspill the stream batches for joins by sub-partitioning|
|[#7951](https://github.com/NVIDIA/spark-rapids/pull/7951)|Fix PMD docs URL [skip ci]|
|[#7945](https://github.com/NVIDIA/spark-rapids/pull/7945)|Enable automerge from 2304 to 2306 [skip ci]|
|[#7935](https://github.com/NVIDIA/spark-rapids/pull/7935)|Add GPU level task metrics|
|[#7930](https://github.com/NVIDIA/spark-rapids/pull/7930)|Add OOM Retry handling for join gather next|
|[#7942](https://github.com/NVIDIA/spark-rapids/pull/7942)|Revert "Upgrade to UCX 1.14.0 (#7877)"|
|[#7889](https://github.com/NVIDIA/spark-rapids/pull/7889)|Support auto-compaction for Delta tables on|
|[#7937](https://github.com/NVIDIA/spark-rapids/pull/7937)|Support hashing different types for sub-partitioning|
|[#7877](https://github.com/NVIDIA/spark-rapids/pull/7877)|Upgrade to UCX 1.14.0|
|[#7926](https://github.com/NVIDIA/spark-rapids/pull/7926)|Fixes issue where UCX compressed tables would be decompressed multiple times|
|[#7928](https://github.com/NVIDIA/spark-rapids/pull/7928)|Adjust assert for SparkShims: no longer a per-shim file [skip ci]|
|[#7895](https://github.com/NVIDIA/spark-rapids/pull/7895)|Some refactor of shuffled hash join|
|[#7894](https://github.com/NVIDIA/spark-rapids/pull/7894)|Support tagging `Cast` for timezone conditionally|
|[#7915](https://github.com/NVIDIA/spark-rapids/pull/7915)|Fix upcast of signed integral values when reading from Parquet|
|[#7879](https://github.com/NVIDIA/spark-rapids/pull/7879)|Retry for file read operations|
|[#7905](https://github.com/NVIDIA/spark-rapids/pull/7905)|[Doc] Fix some documentation issue based on VPR feedback on 23.04 branch (new PR) [skip CI] |
|[#7912](https://github.com/NVIDIA/spark-rapids/pull/7912)|[Doc] Hotfix gh-pages for compatibility page format issue [skip ci]|
|[#7913](https://github.com/NVIDIA/spark-rapids/pull/7913)|Fix resolution of GpuRapidsProcessDeltaMergeJoinExec expressions|
|[#7916](https://github.com/NVIDIA/spark-rapids/pull/7916)|Add clarification for Delta Lake optimized write fallback due to sorting [skip ci]|
|[#7906](https://github.com/NVIDIA/spark-rapids/pull/7906)|ColumnarToRowIterator should release the semaphore if parent is empty|
|[#7909](https://github.com/NVIDIA/spark-rapids/pull/7909)|Interpolate buildver into secondaryCacheDir|
|[#7844](https://github.com/NVIDIA/spark-rapids/pull/7844)|Update alluxio version to 2.9.0|
|[#7896](https://github.com/NVIDIA/spark-rapids/pull/7896)|Update regular expression parser to handle escape character sequences|
|[#7885](https://github.com/NVIDIA/spark-rapids/pull/7885)|Add Join Reordering Integration Test|
|[#7862](https://github.com/NVIDIA/spark-rapids/pull/7862)|Reduce shimming of GpuFlatMapGroupsInPandasExec|
|[#7859](https://github.com/NVIDIA/spark-rapids/pull/7859)|Remove 3.1.4-SNAPSHOT shim code|
|[#7835](https://github.com/NVIDIA/spark-rapids/pull/7835)|Update to pull the rapids spark extra plugin jar|
|[#7863](https://github.com/NVIDIA/spark-rapids/pull/7863)|[Doc] Address document issues [skip ci]|
|[#7794](https://github.com/NVIDIA/spark-rapids/pull/7794)|Implement sub partitioning for large/skewed hash joins|
|[#7864](https://github.com/NVIDIA/spark-rapids/pull/7864)|Add in basic support for OOM retry for project and filter|
|[#7878](https://github.com/NVIDIA/spark-rapids/pull/7878)|Fixing host memory calculation to properly be 5GiB|
|[#7860](https://github.com/NVIDIA/spark-rapids/pull/7860)|Enable manual copy-and-paste code detection [skip ci]|
|[#7852](https://github.com/NVIDIA/spark-rapids/pull/7852)|Use withRetry in GpuCoalesceBatches|
|[#7857](https://github.com/NVIDIA/spark-rapids/pull/7857)|Unshim getSparkShimVersion|
|[#7854](https://github.com/NVIDIA/spark-rapids/pull/7854)|Optimize `regexp_extract*` by transpiling capture groups to non-capturing groups so that only the required capturing group is manifested|
|[#7853](https://github.com/NVIDIA/spark-rapids/pull/7853)|Remove support for Databricks-9.1 ML LTS|
|[#7856](https://github.com/NVIDIA/spark-rapids/pull/7856)|Update references to reduced dependencies pom [skip ci]|
|[#7848](https://github.com/NVIDIA/spark-rapids/pull/7848)|Initialize only sql-plugin  to prevent missing submodule artifacts in buildall [skip ci]|
|[#7839](https://github.com/NVIDIA/spark-rapids/pull/7839)|Add reduced pom to dist jar in the packaging phase|
|[#7822](https://github.com/NVIDIA/spark-rapids/pull/7822)|Add in support for OOM retry|
|[#7846](https://github.com/NVIDIA/spark-rapids/pull/7846)|Stop releasing semaphore in GpuUserDefinedFunction|
|[#7840](https://github.com/NVIDIA/spark-rapids/pull/7840)|Execute mvn initialize before parallel build [skip ci]|
|[#7222](https://github.com/NVIDIA/spark-rapids/pull/7222)|Automatic conversion to shimplified directory structure|
|[#7824](https://github.com/NVIDIA/spark-rapids/pull/7824)|Use withRetryNoSplit in BasicWindowCalc|
|[#7842](https://github.com/NVIDIA/spark-rapids/pull/7842)|Try fix broken blackduck scan [skip ci]|
|[#7841](https://github.com/NVIDIA/spark-rapids/pull/7841)|Hardcode scan projects [skip ci]|
|[#7830](https://github.com/NVIDIA/spark-rapids/pull/7830)|Fix buffer and Filter time with Parquet multithreaded combine reader|
|[#7678](https://github.com/NVIDIA/spark-rapids/pull/7678)|Premerge CI to drop support for Databricks-9.1 ML LTS|
|[#7823](https://github.com/NVIDIA/spark-rapids/pull/7823)|[BUG] Enable managed memory only if async allocator is not used|
|[#7821](https://github.com/NVIDIA/spark-rapids/pull/7821)|Restore pandas import check in db113 runtime|
|[#7810](https://github.com/NVIDIA/spark-rapids/pull/7810)|UnXfail large decimal window range queries|
|[#7771](https://github.com/NVIDIA/spark-rapids/pull/7771)|Add withRetry and withRetryNoSplit and PoC with hash aggregate|
|[#7815](https://github.com/NVIDIA/spark-rapids/pull/7815)|Fix the hyperlink to shimplify.py [skip ci]|
|[#7812](https://github.com/NVIDIA/spark-rapids/pull/7812)|Fallback Delta Lake optimized writes if GPU cannot support partitioning|
|[#7791](https://github.com/NVIDIA/spark-rapids/pull/7791)|Doc changes for new nested JSON reader [skip ci]|
|[#7797](https://github.com/NVIDIA/spark-rapids/pull/7797)|Add GPU support for EphemeralSubstring|
|[#7561](https://github.com/NVIDIA/spark-rapids/pull/7561)|Ant task to automatically convert to a simple shim layout|
|[#7789](https://github.com/NVIDIA/spark-rapids/pull/7789)|Update script for integration tests on Databricks|
|[#7798](https://github.com/NVIDIA/spark-rapids/pull/7798)|Do not error out DB IT test script when pytest code 5 [skip ci]|
|[#7787](https://github.com/NVIDIA/spark-rapids/pull/7787)|Document a workaround to RuntimeException 'boom' [skip ci]|
|[#7786](https://github.com/NVIDIA/spark-rapids/pull/7786)|Fix nested loop joins when there's no build-side columns|
|[#7730](https://github.com/NVIDIA/spark-rapids/pull/7730)|[FEA] Switch to `regex_program` APIs|
|[#7788](https://github.com/NVIDIA/spark-rapids/pull/7788)|Support released spark 3.3.2|
|[#7095](https://github.com/NVIDIA/spark-rapids/pull/7095)|Fix the failure in `map_test.py` on Spark 3.4|
|[#7769](https://github.com/NVIDIA/spark-rapids/pull/7769)|Fix issue where GpuSemaphore can throw NPE when logDebug is on|
|[#7780](https://github.com/NVIDIA/spark-rapids/pull/7780)|Make AlluxioUtilsSuite pass for 340|
|[#7772](https://github.com/NVIDIA/spark-rapids/pull/7772)|Fix cache test for Spark 3.3.2|
|[#7717](https://github.com/NVIDIA/spark-rapids/pull/7717)|Move Databricks variables into blossom-lib|
|[#7749](https://github.com/NVIDIA/spark-rapids/pull/7749)|Support Delta Lake optimized write on Databricks|
|[#7696](https://github.com/NVIDIA/spark-rapids/pull/7696)|Create new version of  GpuBatchScanExec to fix Spark-3.4 build|
|[#7747](https://github.com/NVIDIA/spark-rapids/pull/7747)|batched full join tracking batch does not need to be lazy|
|[#7758](https://github.com/NVIDIA/spark-rapids/pull/7758)|Hardcode python 3.8 to be used in databricks runtime for cudf_udf ENV|
|[#7716](https://github.com/NVIDIA/spark-rapids/pull/7716)|Clean the code of `GpuMetrics`|
|[#7746](https://github.com/NVIDIA/spark-rapids/pull/7746)|Merge branch-23.02 into branch-23.04 [skip ci]|
|[#7740](https://github.com/NVIDIA/spark-rapids/pull/7740)|Revert 7737 workaround for cudf setup in databricks 11.3 runtime [skip ci]|
|[#7737](https://github.com/NVIDIA/spark-rapids/pull/7737)|Workaround for cudf setup in databricks 11.3 runtime|
|[#7734](https://github.com/NVIDIA/spark-rapids/pull/7734)|Temporarily skip the test_parquet_read_ignore_missing on Databricks|
|[#7728](https://github.com/NVIDIA/spark-rapids/pull/7728)|Fix estimatedNumBatches in case of OOM for Full Outer Join|
|[#7718](https://github.com/NVIDIA/spark-rapids/pull/7718)|GpuParquetScan fails with NullPointerException during combining|
|[#7712](https://github.com/NVIDIA/spark-rapids/pull/7712)|Enable Dynamic FIle Pruning on|
|[#7702](https://github.com/NVIDIA/spark-rapids/pull/7702)|Merge 23.02 into 23.04|
|[#7572](https://github.com/NVIDIA/spark-rapids/pull/7572)|Enables spillable/unspillable state for RapidsBuffer and allow buffer sharing|
|[#7687](https://github.com/NVIDIA/spark-rapids/pull/7687)|Fix window tests for Spark-3.4|
|[#7667](https://github.com/NVIDIA/spark-rapids/pull/7667)|Reenable tests originally bypassed for 3.4|
|[#7542](https://github.com/NVIDIA/spark-rapids/pull/7542)|Support WriteFilesExec in Spark-3.4 to fix several tests|
|[#7673](https://github.com/NVIDIA/spark-rapids/pull/7673)|Add missing spark shim test suites |
|[#7655](https://github.com/NVIDIA/spark-rapids/pull/7655)|Fix Spark 3.4 build|
|[#7621](https://github.com/NVIDIA/spark-rapids/pull/7621)|Document GNU sed for macOS auto-copyrighter users [skip ci]|
|[#7618](https://github.com/NVIDIA/spark-rapids/pull/7618)|Update JNI to 23.04.0-SNAPSHOT and update new delta-stub ver to 23.04|
|[#7541](https://github.com/NVIDIA/spark-rapids/pull/7541)|Init version 23.04.0-SNAPSHOT|

## Release 23.02

### Features
|||
|:---|:---|
|[#6420](https://github.com/NVIDIA/spark-rapids/issues/6420)|[FEA]Support HiveTableScanExec to scan a Hive text table|
|[#4897](https://github.com/NVIDIA/spark-rapids/issues/4897)|Profiling tool: create a section to focus on I/O metrics|
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
|[#5618](https://github.com/NVIDIA/spark-rapids/issues/5618)|Qualification tool use expressions parsed in duration and speedup factors|

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
|[#7069](https://github.com/NVIDIA/spark-rapids/issues/7069)|[BUG] GPU Hive Text Reader reads empty strings as null|
|[#7068](https://github.com/NVIDIA/spark-rapids/issues/7068)|[BUG] GPU Hive Text Reader skips empty lines|
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
|[#7763](https://github.com/NVIDIA/spark-rapids/pull/7763)|23.02 changelog update 2/14 [skip ci]|
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
|[#7075](https://github.com/NVIDIA/spark-rapids/pull/7075)|Fix the failure of `test_array_element_at_zero_index_fail` on Spark3.4|
|[#7126](https://github.com/NVIDIA/spark-rapids/pull/7126)|Fix support for binary encoded decimal for parquet|
|[#7113](https://github.com/NVIDIA/spark-rapids/pull/7113)|Use an improved API for appending binary to host vector|
|[#7130](https://github.com/NVIDIA/spark-rapids/pull/7130)|Enable chunked parquet reads by default|
|[#7074](https://github.com/NVIDIA/spark-rapids/pull/7074)|Update JNI and cudf-py version to 23.02|
|[#7063](https://github.com/NVIDIA/spark-rapids/pull/7063)|Init version 23.02.0|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
