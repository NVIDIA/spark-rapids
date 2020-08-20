# Change log
Generated on 2020-08-21

## Release 0.2

### Features
|||
|:---|:---|
|[#452](https://github.com/NVIDIA/spark-rapids/issues/452)|[FEA] Update HashSortOptimizerSuite to work with AQE|
|[#454](https://github.com/NVIDIA/spark-rapids/issues/454)|[FEA] Update GpuCoalesceBatchesSuite to work with AQE enabled|
|[#566](https://github.com/NVIDIA/spark-rapids/issues/566)|[FEA] Add support for StringSplit with an array index.|
|[#524](https://github.com/NVIDIA/spark-rapids/issues/524)|[FEA] Add GPU specific metrics to GpuFileSourceScanExec|
|[#494](https://github.com/NVIDIA/spark-rapids/issues/494)|[FEA] Add some AQE-specific tests to the PySpark test suite|
|[#146](https://github.com/NVIDIA/spark-rapids/issues/146)|[FEA] Python tests should support running with Adaptive Query Execution enabled|
|[#488](https://github.com/NVIDIA/spark-rapids/issues/488)|[FEA] Ability to limit total GPU memory used|
|[#70](https://github.com/NVIDIA/spark-rapids/issues/70)|[FEA] Support StringSplit|
|[#403](https://github.com/NVIDIA/spark-rapids/issues/403)|[FEA] Add in support for GetArrayItem|
|[#493](https://github.com/NVIDIA/spark-rapids/issues/493)|[FEA] Implement shuffle optimization when AQE is enabled|
|[#500](https://github.com/NVIDIA/spark-rapids/issues/500)|[FEA] Add maven profiles for testing with AQE on or off|
|[#471](https://github.com/NVIDIA/spark-rapids/issues/471)|[FEA] create a formal process for updateing the github-pages branch|
|[#479](https://github.com/NVIDIA/spark-rapids/issues/479)|[FEA] Please consider to support spark.sql.extensions=com.nvidia.spark.rapids.SQLExecPlugin|
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
|[#317](https://github.com/NVIDIA/spark-rapids/issues/317)|[FEA] Write test wrapper to run SQL queries via pyspark|
|[#235](https://github.com/NVIDIA/spark-rapids/issues/235)|[FEA] Audit BroadcastExchangeExec|
|[#234](https://github.com/NVIDIA/spark-rapids/issues/234)|[FEA] Audit BatchScanExec|
|[#238](https://github.com/NVIDIA/spark-rapids/issues/238)|[FEA]  Audit ShuffledHashJoinExec |
|[#237](https://github.com/NVIDIA/spark-rapids/issues/237)|[FEA] Audit BroadcastHashJoinExec |
|[#316](https://github.com/NVIDIA/spark-rapids/issues/316)|[FEA] Add some basic Dataframe tests for CoalesceExec|
|[#145](https://github.com/NVIDIA/spark-rapids/issues/145)|[FEA] Scala tests should support running with Adaptive Query Execution enabled|
|[#231](https://github.com/NVIDIA/spark-rapids/issues/231)|[FEA] Audit ProjectExec |
|[#229](https://github.com/NVIDIA/spark-rapids/issues/229)|[FEA] Audit FileSourceScanExec |
### Performance
|||
|:---|:---|
|[#15](https://github.com/NVIDIA/spark-rapids/issues/15)|[FEA] Multiple threads shareing the same GPU|
|[#272](https://github.com/NVIDIA/spark-rapids/issues/272)|[DOC] Getting started guide for UCX shuffle|
### Bugs Fixed
|||
|:---|:---|
|[#569](https://github.com/NVIDIA/spark-rapids/issues/569)|[BUG] left_semi_join operation is abnormal and serious time-consuming|
|[#341](https://github.com/NVIDIA/spark-rapids/issues/341)|[BUG] Document compression formats for readers/writers|
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
|[#525](https://github.com/NVIDIA/spark-rapids/issues/525)|[BUG] GpuFilterExec reports incorrect nullability of output in some cases|
|[#382](https://github.com/NVIDIA/spark-rapids/issues/382)|[BUG] Spark3.1 StringFallbackSuite regexp_replace null cpu fall back test fails.|
|[#441](https://github.com/NVIDIA/spark-rapids/issues/441)|[BUG] test_broadcast_nested_loop_join_special_case fails on databricks|
|[#347](https://github.com/NVIDIA/spark-rapids/issues/347)|[BUG] Failed to read Parquet file generated by GPU-enabled Spark.|
|[#433](https://github.com/NVIDIA/spark-rapids/issues/433)|`InSet` operator produces an error for Strings|
|[#144](https://github.com/NVIDIA/spark-rapids/issues/144)|[BUG] spark.sql.legacy.parquet.datetimeRebaseModeInWrite is ignored|
|[#323](https://github.com/NVIDIA/spark-rapids/issues/323)|[BUG] GpuBroadcastNestedLoopJoinExec can fail if there are no columns|
|[#280](https://github.com/NVIDIA/spark-rapids/issues/280)|[BUG] Full Outer Join does not work on nullable keys|
|[#149](https://github.com/NVIDIA/spark-rapids/issues/149)|[BUG] Spark driver fails to load native libs when running on node without CUDA|
### PRs
|||
|:---|:---|
|[#594](https://github.com/NVIDIA/spark-rapids/pull/594)|Filter nulls from joins where possible to improve performance.|
|[#590](https://github.com/NVIDIA/spark-rapids/pull/590)|Move GpuParquetScan/GpuOrcScan into Shim|
|[#572](https://github.com/NVIDIA/spark-rapids/pull/572)|Update buffer store to return compressed batches directly, add compression NVTX ranges|
|[#564](https://github.com/NVIDIA/spark-rapids/pull/564)|Add GPU decode time metric to scans|
|[#562](https://github.com/NVIDIA/spark-rapids/pull/562)|getCatalog can be called from the driver, and can return null|
|[#555](https://github.com/NVIDIA/spark-rapids/pull/555)|Fix build warnings for ColumnViewAccess|
|[#547](https://github.com/NVIDIA/spark-rapids/pull/547)|Add GPU metrics to GpuFileSourceScanExec|
|[#462](https://github.com/NVIDIA/spark-rapids/pull/462)|Implement optimized AQE support so that exchanges run on GPU where possible|
|[#539](https://github.com/NVIDIA/spark-rapids/pull/539)|Update script to audit multiple Spark versions|
|[#543](https://github.com/NVIDIA/spark-rapids/pull/543)|Add metrics to GpuUnion operator|
|[#497](https://github.com/NVIDIA/spark-rapids/pull/497)|Add UDF compiler implementations|
|[#487](https://github.com/NVIDIA/spark-rapids/pull/487)|Add framework for batch compression of shuffle partitions|
|[#537](https://github.com/NVIDIA/spark-rapids/pull/537)|Use fresh SparkSession when capturing to avoid late capture of previous query|
|[#538](https://github.com/NVIDIA/spark-rapids/pull/538)|Revert "Temporary workaround for RMM initial pool size bug (#530)"|
|[#517](https://github.com/NVIDIA/spark-rapids/pull/517)|Add config to limit maximum RMM pool size|
|[#527](https://github.com/NVIDIA/spark-rapids/pull/527)|Add support for split and getArrayIndex|
|[#534](https://github.com/NVIDIA/spark-rapids/pull/534)|Fixes bugs around GpuShuffleEnv initialization|
|[#529](https://github.com/NVIDIA/spark-rapids/pull/529)|[BUG] Degenerate table metas were not getting copied to the heap|
|[#530](https://github.com/NVIDIA/spark-rapids/pull/530)|Temporary workaround for RMM initial pool size bug|
|[#526](https://github.com/NVIDIA/spark-rapids/pull/526)|Fix bug with nullability reporting in GpuFilterExec|
|[#522](https://github.com/NVIDIA/spark-rapids/pull/522)|Use SQLConf instead of SparkConf when looking up SQL configs|
|[#518](https://github.com/NVIDIA/spark-rapids/pull/518)|Fix init order issue in GpuShuffleEnv when RAPIDS shuffle configured|
|[#503](https://github.com/NVIDIA/spark-rapids/pull/503)|Remove unused async buffer spill support|
|[#466](https://github.com/NVIDIA/spark-rapids/pull/466)|Optimize and fix Api validation script|
|[#319](https://github.com/NVIDIA/spark-rapids/pull/319)|Update partitioning logic in ShuffledBatchRDD|
|[#448](https://github.com/NVIDIA/spark-rapids/pull/448)|Preliminary support for keeping broadcast exchanges on GPU when AQE is enabled|
|[#478](https://github.com/NVIDIA/spark-rapids/pull/478)|Fall back to CPU for binary as string in parquet|
|[#477](https://github.com/NVIDIA/spark-rapids/pull/477)|Fix special case joins in broadcast nested loop join|
|[#434](https://github.com/NVIDIA/spark-rapids/pull/434)|Add UDF compiler skeleton|
|[#456](https://github.com/NVIDIA/spark-rapids/pull/456)|Add closeOnExcept to clean up code that closes resources only on exceptions|
|[#426](https://github.com/NVIDIA/spark-rapids/pull/426)|Update BufferMeta to support multiple codec buffers per table|
|[#445](https://github.com/NVIDIA/spark-rapids/pull/445)|Added in StringRPad and StringLPad|
|[#437](https://github.com/NVIDIA/spark-rapids/pull/437)|Fix bug with InSet and Strings|
|[#435](https://github.com/NVIDIA/spark-rapids/pull/435)|Add in checks for Parquet LEGACY date/time rebase|
|[#432](https://github.com/NVIDIA/spark-rapids/pull/432)|Fix batch use-after-close in partitioning, shuffle env init|
|[#406](https://github.com/NVIDIA/spark-rapids/pull/406)|Update cast handling to deal with new libcudf casting limitations|
|[#397](https://github.com/NVIDIA/spark-rapids/pull/397)|Fix small bug in ShuffleBufferCatalog.hasActiveShuffle|
|[#389](https://github.com/NVIDIA/spark-rapids/pull/389)|Added in support for RangeExec|
|[#390](https://github.com/NVIDIA/spark-rapids/pull/390)|Ucx getting started|
|[#387](https://github.com/NVIDIA/spark-rapids/pull/387)|Remove the term whitelist|
|[#376](https://github.com/NVIDIA/spark-rapids/pull/376)|Add our session extension correctly if there are other extensions configured|
|[#374](https://github.com/NVIDIA/spark-rapids/pull/374)|Fix rat issue for notebooks|
|[#364](https://github.com/NVIDIA/spark-rapids/pull/364)|Update Databricks patch for changes to GpuSortMergeJoin|
|[#353](https://github.com/NVIDIA/spark-rapids/pull/353)|Fix broadcast nested loop join for the no column case|
|[#342](https://github.com/NVIDIA/spark-rapids/pull/342)|Implement build-side rules for shuffle hash join|
|[#349](https://github.com/NVIDIA/spark-rapids/pull/349)|Updated join code to treat null equality properly|
|[#331](https://github.com/NVIDIA/spark-rapids/pull/331)|Adds iterator and client unit tests, and prepares for more fetch failure handling|
|[#338](https://github.com/NVIDIA/spark-rapids/pull/338)|Move GpuPartitioning to a separate file|
|[#324](https://github.com/NVIDIA/spark-rapids/pull/324)|Add in basic support for GpuCartesianProductExec|
|[#321](https://github.com/NVIDIA/spark-rapids/pull/321)|update databricks patch for GpuBroadcastNestedLoopJoinExec|
|[#258](https://github.com/NVIDIA/spark-rapids/pull/258)|Enable running TPCH tests with AQE enabled|
|[#309](https://github.com/NVIDIA/spark-rapids/pull/309)|Update databricks patch to latest changes|
|[#296](https://github.com/NVIDIA/spark-rapids/pull/296)|Added in basic support for broadcast nested loop join|
|[#289](https://github.com/NVIDIA/spark-rapids/pull/289)|Clean up GpuCollectLimitMeta and add in metrics|
|[#287](https://github.com/NVIDIA/spark-rapids/pull/287)|Add in support for right join and fix issues build right|
|[#281](https://github.com/NVIDIA/spark-rapids/pull/281)|Add in support for Full Outer Join on non-null keys|
|[#263](https://github.com/NVIDIA/spark-rapids/pull/263)|More time ops|
|[#256](https://github.com/NVIDIA/spark-rapids/pull/256)|Remove special cases for contains, startsWith, and endWith|
|[#253](https://github.com/NVIDIA/spark-rapids/pull/253)|Remove GpuAttributeReference and GpuSortOrder|
|[#162](https://github.com/NVIDIA/spark-rapids/pull/162)|Integration tests for corner cases in window functions.|
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
|[#177](https://github.com/NVIDIA/spark-rapids/pull/177)|Fixed leaks in unit test and use ColumnarBatch for testing|
|[#202](https://github.com/NVIDIA/spark-rapids/pull/202)|Fixed issue for contains when searching for an empty string|
|[#201](https://github.com/NVIDIA/spark-rapids/pull/201)|Fix name of scan|
|[#200](https://github.com/NVIDIA/spark-rapids/pull/200)|Fix issue with GpuAttributeReference not overrideing references|
|[#197](https://github.com/NVIDIA/spark-rapids/pull/197)|Fix metrics for writes|
|[#186](https://github.com/NVIDIA/spark-rapids/pull/186)|Fixed issue with nullability on concat|
|[#193](https://github.com/NVIDIA/spark-rapids/pull/193)|Add RapidsBufferCatalog tests|
|[#189](https://github.com/NVIDIA/spark-rapids/pull/189)|Handle AggregateExpression having resultIds parameter instead of a single resultId|
|[#190](https://github.com/NVIDIA/spark-rapids/pull/190)|FileSourceScanExec can have logicalRelation parameter on some distributions|
|[#185](https://github.com/NVIDIA/spark-rapids/pull/185)|Update type of parameter of GpuExpandExec to make it consistent|
|[#180](https://github.com/NVIDIA/spark-rapids/pull/180)|Add MetaUtils unit tests|
|[#157](https://github.com/NVIDIA/spark-rapids/pull/157)|Add partial and final only hash aggregate tests and fix nulls corner case for Average|
|[#158](https://github.com/NVIDIA/spark-rapids/pull/158)|Orc merge schema fallback and FileScan format configs|
