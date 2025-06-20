# Change log
Generated on 2025-06-04
## Release 25.02

### Features
|||
|:---|:---|
|[#12225](https://github.com/NVIDIA/spark-rapids/issues/12225)|[FEA] Support Spark 3.5.5|
|[#11648](https://github.com/NVIDIA/spark-rapids/issues/11648)|[FEA] it would be nice if we could support org.apache.spark.sql.catalyst.expressions.Bin|
|[#11891](https://github.com/NVIDIA/spark-rapids/issues/11891)|[FEA] Support Spark 3.5.4 release|
|[#11928](https://github.com/NVIDIA/spark-rapids/issues/11928)|[FEA] make maxCpuBatchSize in GpuPartitioning configurable|
|[#10505](https://github.com/NVIDIA/spark-rapids/issues/10505)|[FOLLOW UP] Support `row_number()` filters for `GpuWindowGroupLimitExec`|
|[#11853](https://github.com/NVIDIA/spark-rapids/issues/11853)|[FEA] Ability to dump tables on a write|
|[#11804](https://github.com/NVIDIA/spark-rapids/issues/11804)|[FEA] Support TruncDate expression|
|[#11674](https://github.com/NVIDIA/spark-rapids/issues/11674)|[FEA] HiveHash supports nested types|

### Performance
|||
|:---|:---|
|[#11342](https://github.com/NVIDIA/spark-rapids/issues/11342)|[FEA] Put file writes in a background thread|
|[#11729](https://github.com/NVIDIA/spark-rapids/issues/11729)|[FEA] optimize the multi-contains generated by rlike|
|[#11860](https://github.com/NVIDIA/spark-rapids/issues/11860)|[FEA] kernel for date_trunc and trunc that has a scalar format|
|[#11812](https://github.com/NVIDIA/spark-rapids/issues/11812)|[FEA] Support escape characters in search list when rewrite `regexp_replace` to string replace|

### Bugs Fixed
|||
|:---|:---|
|[#12230](https://github.com/NVIDIA/spark-rapids/issues/12230)|[BUG] parquet_test.py::test_many_column_project test fails on GB100 and cuda12.8|
|[#12231](https://github.com/NVIDIA/spark-rapids/issues/12231)|[BUG] hash_aggregate_test.py::test_hash_multiple_grpby_pivot and row_conversion_test.py::test_row_conversions_fixed_width_wide fails on GB100 and cuda12.8|
|[#12152](https://github.com/NVIDIA/spark-rapids/issues/12152)|[BUG] mvn-verify-check failed cache non-snapshot deps|
|[#11835](https://github.com/NVIDIA/spark-rapids/issues/11835)|[BUG] Intermittent result discrepancy for NDS SF3K query86 on L40S|
|[#12111](https://github.com/NVIDIA/spark-rapids/issues/12111)|[BUG] Hit velox runtime error when filtering df with timestamp column inside when enabling hybrid|
|[#12091](https://github.com/NVIDIA/spark-rapids/issues/12091)|[BUG]An assertion error in the sized hash join|
|[#12113](https://github.com/NVIDIA/spark-rapids/issues/12113)|[BUG] HybridParquetScan fails on the `select count(1)` case|
|[#12096](https://github.com/NVIDIA/spark-rapids/issues/12096)|[BUG] CI_PART1 for DBR 14.3 hangs in the nightly pre-release pipeline|
|[#12076](https://github.com/NVIDIA/spark-rapids/issues/12076)|[BUG] ExtraPlugins might be loaded duplicated|
|[#11433](https://github.com/NVIDIA/spark-rapids/issues/11433)|[BUG] Spark UT framework: SPARK-34212 Parquet should read decimals correctly|
|[#12038](https://github.com/NVIDIA/spark-rapids/issues/12038)|[BUG] spark321 failed core dump in nightly|
|[#12046](https://github.com/NVIDIA/spark-rapids/issues/12046)|[BUG] orc_test fail non-UTC cases with Part of the plan is not columnar class org.apache.spark.sql.execution.FileSourceScanExec|
|[#12036](https://github.com/NVIDIA/spark-rapids/issues/12036)|[BUG] The check in assertIsOnTheGpu method to test if a plan is on the GPU is not accurate|
|[#11989](https://github.com/NVIDIA/spark-rapids/issues/11989)|[BUG] ParquetCachedBatchSerializer does not grab the GPU semaphore and does not have retry blocks|
|[#11651](https://github.com/NVIDIA/spark-rapids/issues/11651)|[BUG] Parse regular expressions using JDK to make error behavior more consistent between CPU and GPU|
|[#11628](https://github.com/NVIDIA/spark-rapids/issues/11628)|[BUG] Spark UT framework: select one deep nested complex field after join, IOException parsing parquet|
|[#11629](https://github.com/NVIDIA/spark-rapids/issues/11629)|[BUG] Spark UT framework: select one deep nested complex field after outer join, IOException parsing parquet|
|[#11620](https://github.com/NVIDIA/spark-rapids/issues/11620)|[BUG] Spark UT framework: "select a single complex field and partition column" causes java.lang.IndexOutOfBoundsException|
|[#11621](https://github.com/NVIDIA/spark-rapids/issues/11621)|[BUG] Spark UT framework: partial schema intersection - select missing subfield causes java.lang.IndexOutOfBoundsException|
|[#11619](https://github.com/NVIDIA/spark-rapids/issues/11619)|[BUG] Spark UT framework: "select a single complex field" causes java.lang.IndexOutOfBoundsException|
|[#11975](https://github.com/NVIDIA/spark-rapids/issues/11975)|[FOLLOWUP] We should have a separate version definition for the  rapids-4-spark-hybrid dependency|
|[#11976](https://github.com/NVIDIA/spark-rapids/issues/11976)|[BUG] scala 2.13 rapids_integration test failed|
|[#11971](https://github.com/NVIDIA/spark-rapids/issues/11971)|[BUG] scala213 nightly build failed rapids-4-spark-tests_2.13 of spark400|
|[#11903](https://github.com/NVIDIA/spark-rapids/issues/11903)|[BUG] Unexpected large output batches due to implementation defects|
|[#11914](https://github.com/NVIDIA/spark-rapids/issues/11914)|[BUG] Nightly CI does not upload sources and Javadoc JARs as the release script does|
|[#11896](https://github.com/NVIDIA/spark-rapids/issues/11896)|[BUG] [BUILD] CI passes without checking for `operatorsScore.csv`, `supportedExprs.csv` update|
|[#11895](https://github.com/NVIDIA/spark-rapids/issues/11895)|[BUG] BasePythonRunner has a new parameter metrics in Spark 4.0|
|[#11107](https://github.com/NVIDIA/spark-rapids/issues/11107)|[BUG] Rework RapidsShuffleManager initialization for Apache Spark 4.0.0|
|[#11897](https://github.com/NVIDIA/spark-rapids/issues/11897)|[BUG] JsonScanRetrySuite is failing in the CI.|
|[#11885](https://github.com/NVIDIA/spark-rapids/issues/11885)|[BUG] data corruption with spill framework changes|
|[#11762](https://github.com/NVIDIA/spark-rapids/issues/11762)|[BUG] Non-nullable bools in a nullable struct fails|
|[#11526](https://github.com/NVIDIA/spark-rapids/issues/11526)|Fix Arithmetic tests on Databricks 14.3|
|[#11866](https://github.com/NVIDIA/spark-rapids/issues/11866)|[BUG]The CHANGELOG is generated based on the project's roadmap rather than the target branch.|
|[#11749](https://github.com/NVIDIA/spark-rapids/issues/11749)|[BUG] Include Databricks 14.3 shim into the dist jar|
|[#11822](https://github.com/NVIDIA/spark-rapids/issues/11822)|[BUG] [Spark 4] Type mismatch Exceptions from DFUDFShims.scala with Spark-4.0.0 expressions.Expression|
|[#11760](https://github.com/NVIDIA/spark-rapids/issues/11760)|[BUG] isTimestamp leaks a Scalar|
|[#11796](https://github.com/NVIDIA/spark-rapids/issues/11796)|[BUG] populate-daily-cache action masks errors|
|[#10901](https://github.com/NVIDIA/spark-rapids/issues/10901)|from_json throws exception when the json's structure only partially matches the provided schema|
|[#11736](https://github.com/NVIDIA/spark-rapids/issues/11736)|[BUG] Orc writes don't fully support Booleans with nulls|

### PRs
|||
|:---|:---|
|[#12294](https://github.com/NVIDIA/spark-rapids/pull/12294)|Update changelog for the v25.02 release [skip ci]|
|[#12289](https://github.com/NVIDIA/spark-rapids/pull/12289)|Update dependency version JNI, private to 25.02.1|
|[#12282](https://github.com/NVIDIA/spark-rapids/pull/12282)|[DOC] update the download page for 2502.1 hot release [skip ci]|
|[#12280](https://github.com/NVIDIA/spark-rapids/pull/12280)|Disable hybrid cases for spark35X [skip ci]|
|[#12259](https://github.com/NVIDIA/spark-rapids/pull/12259)|Add Spark 3.5.5 shim|
|[#12252](https://github.com/NVIDIA/spark-rapids/pull/12252)|Update dependency version private to 25.02.1-SNAPSHOT|
|[#12253](https://github.com/NVIDIA/spark-rapids/pull/12253)|Accelerate nightly build [skip ci]|
|[#12239](https://github.com/NVIDIA/spark-rapids/pull/12239)|Mirror the Cloudera Maven repository|
|[#12189](https://github.com/NVIDIA/spark-rapids/pull/12189)|Update version to 25.02.1-SNAPSHOT|
|[#12204](https://github.com/NVIDIA/spark-rapids/pull/12204)|[DOC] address ghpage pr comments [skip ci]|
|[#12212](https://github.com/NVIDIA/spark-rapids/pull/12212)|Fix deadlock in BounceBufferPool|
|[#12205](https://github.com/NVIDIA/spark-rapids/pull/12205)|Workaround: tmply disable the HostAllocSuite to unblock CI [skip ci]|
|[#12188](https://github.com/NVIDIA/spark-rapids/pull/12188)|Update dependency version JNI to 25.02.1-SNAPSHOT|
|[#12157](https://github.com/NVIDIA/spark-rapids/pull/12157)|Add hybrid sha1 to mvn verify cache key [skip ci]|
|[#12154](https://github.com/NVIDIA/spark-rapids/pull/12154)|Update latest changelog [skip ci]|
|[#12129](https://github.com/NVIDIA/spark-rapids/pull/12129)|Update dependency version JNI, private, hybrid to 25.02.0 [skip ci]|
|[#12102](https://github.com/NVIDIA/spark-rapids/pull/12102)|[DOC] update the download page for 2502 release [skip ci]|
|[#12112](https://github.com/NVIDIA/spark-rapids/pull/12112)|HybridParquetScan: Fix velox runtime error in hybrid scan when filter timestamp|
|[#12092](https://github.com/NVIDIA/spark-rapids/pull/12092)|Fix an assertion error in the sized hash join|
|[#12114](https://github.com/NVIDIA/spark-rapids/pull/12114)|Fix HybridParquetScan over select(1)|
|[#12109](https://github.com/NVIDIA/spark-rapids/pull/12109)|revert ucx 1.18 upgrade|
|[#12103](https://github.com/NVIDIA/spark-rapids/pull/12103)|Revert "Enable event log for qualification & profiling tools testing …|
|[#12058](https://github.com/NVIDIA/spark-rapids/pull/12058)|upgrade jucx to 1.18|
|[#12077](https://github.com/NVIDIA/spark-rapids/pull/12077)|Fix the issue of ExtraPlugins loading multiple times|
|[#12080](https://github.com/NVIDIA/spark-rapids/pull/12080)|Quick fix for hybrid tests without git information.|
|[#12068](https://github.com/NVIDIA/spark-rapids/pull/12068)|Do not build Spark-4.0.0-SNAPSHOT [skip ci]|
|[#12064](https://github.com/NVIDIA/spark-rapids/pull/12064)|Run mvn with the project's pom.xml in hybrid_execution.sh|
|[#12060](https://github.com/NVIDIA/spark-rapids/pull/12060)|Relax decimal metadata checks for mismatched precision/scale|
|[#12054](https://github.com/NVIDIA/spark-rapids/pull/12054)|Update the version of the rapids-hybrid-execution dependency.|
|[#11970](https://github.com/NVIDIA/spark-rapids/pull/11970)|Explicitly set Delta table props to accommodate for different defaults|
|[#12044](https://github.com/NVIDIA/spark-rapids/pull/12044)|Set CI=true for complete failure reason in summary|
|[#12050](https://github.com/NVIDIA/spark-rapids/pull/12050)|Fixed `FileSourceScanExec` and `BatchScanExec` inadvertently falling to the CPU in non-utc orc tests |
|[#12000](https://github.com/NVIDIA/spark-rapids/pull/12000)|HybridParquetScan: Refine filter push down to avoid double evaluation|
|[#12037](https://github.com/NVIDIA/spark-rapids/pull/12037)|Removed the assumption if a plan is Columnar it probably is on the GPU|
|[#11991](https://github.com/NVIDIA/spark-rapids/pull/11991)|Grab the GPU Semaphore when reading cached batch data with the GPU|
|[#11880](https://github.com/NVIDIA/spark-rapids/pull/11880)|Perform handle spill IO outside of locked section in SpillFramework|
|[#11997](https://github.com/NVIDIA/spark-rapids/pull/11997)|Configure 14.3 support at runtime|
|[#11977](https://github.com/NVIDIA/spark-rapids/pull/11977)|Use bounce buffer pools in the Spill Framework|
|[#11912](https://github.com/NVIDIA/spark-rapids/pull/11912)|Ensure Java Compatibility Check for Regex Patterns|
|[#11984](https://github.com/NVIDIA/spark-rapids/pull/11984)|Include the size information when printing a SCB|
|[#11889](https://github.com/NVIDIA/spark-rapids/pull/11889)|Change order of initialization so pinned pool is available for spill framework buffers|
|[#11956](https://github.com/NVIDIA/spark-rapids/pull/11956)|Enable tests in RapidsParquetSchemaPruningSuite|
|[#11981](https://github.com/NVIDIA/spark-rapids/pull/11981)|Protect the batch read by a retry block in agg|
|[#11967](https://github.com/NVIDIA/spark-rapids/pull/11967)|Add support for `org.apache.spark.sql.catalyst.expressions.Bin`|
|[#11982](https://github.com/NVIDIA/spark-rapids/pull/11982)|Use common add-to-project action [skip ci]|
|[#11978](https://github.com/NVIDIA/spark-rapids/pull/11978)|Try to fix Scala 2.13 nightly failure: can not find version-def.sh|
|[#11973](https://github.com/NVIDIA/spark-rapids/pull/11973)|Minor change: Make Hybrid version a separate config like priviate repo|
|[#11969](https://github.com/NVIDIA/spark-rapids/pull/11969)|Support `raise_error()` on  14.3, Spark 4.|
|[#11972](https://github.com/NVIDIA/spark-rapids/pull/11972)|Update MockTaskContext to support new functions added in Spark-4.0|
|[#11906](https://github.com/NVIDIA/spark-rapids/pull/11906)|Enable Hybrid test cases in premerge/nightly CIs|
|[#11720](https://github.com/NVIDIA/spark-rapids/pull/11720)|Introduce hybrid (CPU) scan for Parquet read|
|[#11911](https://github.com/NVIDIA/spark-rapids/pull/11911)|Avoid concatentating multiple host buffers when reading Parquet|
|[#11960](https://github.com/NVIDIA/spark-rapids/pull/11960)|Remove jlowe as committer since he retired|
|[#11958](https://github.com/NVIDIA/spark-rapids/pull/11958)|Update to use vulnerability-scan runner [skip ci]|
|[#11955](https://github.com/NVIDIA/spark-rapids/pull/11955)|Add Spark 3.5.4 shim|
|[#11959](https://github.com/NVIDIA/spark-rapids/pull/11959)|Remove inactive user from github workflow[skip ci]|
|[#11952](https://github.com/NVIDIA/spark-rapids/pull/11952)|Fix auto merge conflict 11948 [skip ci]|
|[#11908](https://github.com/NVIDIA/spark-rapids/pull/11908)|Fix two potential OOM issues in GPU aggregate.|
|[#11936](https://github.com/NVIDIA/spark-rapids/pull/11936)|Add throttle time metrics for async write|
|[#11929](https://github.com/NVIDIA/spark-rapids/pull/11929)|make maxCpuBatchSize in GpuPartitioning configurable|
|[#11939](https://github.com/NVIDIA/spark-rapids/pull/11939)|[DOC] update release note to add spark 353 support [skip ci]|
|[#11920](https://github.com/NVIDIA/spark-rapids/pull/11920)|Remove Alluxio support|
|[#11938](https://github.com/NVIDIA/spark-rapids/pull/11938)|Update codeowners file to use team [skip ci]|
|[#11915](https://github.com/NVIDIA/spark-rapids/pull/11915)|Deploy the sources and Javadoc JARs in the nightly CICD [skip ci]|
|[#11917](https://github.com/NVIDIA/spark-rapids/pull/11917)|Fix issue with CustomerShuffleReaderExec metadata copy|
|[#11910](https://github.com/NVIDIA/spark-rapids/pull/11910)|fix bug: enable if_modified_files check for all shims in github actions [skip ci]|
|[#11909](https://github.com/NVIDIA/spark-rapids/pull/11909)|Update copyright year in NOTICE [skip ci]|
|[#11907](https://github.com/NVIDIA/spark-rapids/pull/11907)|Fix generated doc for xxhash64 for Spark 400|
|[#11905](https://github.com/NVIDIA/spark-rapids/pull/11905)|Fix the build error for Spark 400|
|[#11904](https://github.com/NVIDIA/spark-rapids/pull/11904)|Eagerly initialize RapidsShuffleManager for SPARK-45762|
|[#11865](https://github.com/NVIDIA/spark-rapids/pull/11865)|Async write support for ORC|
|[#11816](https://github.com/NVIDIA/spark-rapids/pull/11816)|address some comments for 11792|
|[#11789](https://github.com/NVIDIA/spark-rapids/pull/11789)|Improve the retry support for nondeterministic expressions|
|[#11898](https://github.com/NVIDIA/spark-rapids/pull/11898)|Add missing json reader options for JsonScanRetrySuite|
|[#11859](https://github.com/NVIDIA/spark-rapids/pull/11859)|Xxhash64 supports nested types|
|[#11890](https://github.com/NVIDIA/spark-rapids/pull/11890)|Update operatorsScore,supportedExprs for TruncDate, TruncTimestamp|
|[#11886](https://github.com/NVIDIA/spark-rapids/pull/11886)|Support group-limit optimization for `ROW_NUMBER`|
|[#11887](https://github.com/NVIDIA/spark-rapids/pull/11887)|Make sure that the chunked packer bounce buffer is realease after the synchronize|
|[#11894](https://github.com/NVIDIA/spark-rapids/pull/11894)|Fix bug: add timeout for cache deps steps [skip ci]|
|[#11810](https://github.com/NVIDIA/spark-rapids/pull/11810)|Use faster multi-contains in `rlike` regex rewrite|
|[#11882](https://github.com/NVIDIA/spark-rapids/pull/11882)|Add metrics GpuPartitioning.CopyToHostTime|
|[#11864](https://github.com/NVIDIA/spark-rapids/pull/11864)|Add support for dumping write data to try and reproduce error cases|
|[#11781](https://github.com/NVIDIA/spark-rapids/pull/11781)|Fix non-nullable under nullable struct write|
|[#11877](https://github.com/NVIDIA/spark-rapids/pull/11877)|Fix auto merge conflict 11873 [skip ci]|
|[#11833](https://github.com/NVIDIA/spark-rapids/pull/11833)|Support `trunc` and `date_trunc` SQL function|
|[#11660](https://github.com/NVIDIA/spark-rapids/pull/11660)|Add `HiveHash` support for nested types|
|[#11855](https://github.com/NVIDIA/spark-rapids/pull/11855)|Add integration test for parquet async writer|
|[#11747](https://github.com/NVIDIA/spark-rapids/pull/11747)|Spill framework refactor for better performance and extensibility|
|[#11870](https://github.com/NVIDIA/spark-rapids/pull/11870)|Workaround: Exclude cudf_log.txt in RAT check|
|[#11867](https://github.com/NVIDIA/spark-rapids/pull/11867)|Generate the CHANGELOG based on the PR's target branch [skip ci]|
|[#11821](https://github.com/NVIDIA/spark-rapids/pull/11821)|add a few more stage level metrics|
|[#11856](https://github.com/NVIDIA/spark-rapids/pull/11856)|Document Hive text write serialization format checks|
|[#11805](https://github.com/NVIDIA/spark-rapids/pull/11805)|Enable some integration tests for `from_json`|
|[#11840](https://github.com/NVIDIA/spark-rapids/pull/11840)|Support running Databricks CI_PART2 integration tests with JARs built by CI_PART1|
|[#11847](https://github.com/NVIDIA/spark-rapids/pull/11847)|Some small improvements|
|[#11811](https://github.com/NVIDIA/spark-rapids/pull/11811)|Fix bug: populate cache deps [skip ci]|
|[#11817](https://github.com/NVIDIA/spark-rapids/pull/11817)|Optimize Databricks Jenkins scripts [skip ci]|
|[#11829](https://github.com/NVIDIA/spark-rapids/pull/11829)|Some minor improvements identified during benchmark|
|[#11827](https://github.com/NVIDIA/spark-rapids/pull/11827)|Deal with Spark changes for column<->expression conversions|
|[#11826](https://github.com/NVIDIA/spark-rapids/pull/11826)|Balance the pre-merge CI job's time for the ci_1 and ci_2 tests|
|[#11784](https://github.com/NVIDIA/spark-rapids/pull/11784)|Add support for kudo write metrics|
|[#11783](https://github.com/NVIDIA/spark-rapids/pull/11783)|Fix the task count check in TrafficController|
|[#11813](https://github.com/NVIDIA/spark-rapids/pull/11813)|Support some escape chars when rewriting regexp_replace to stringReplace|
|[#11819](https://github.com/NVIDIA/spark-rapids/pull/11819)|Add the 'test_type' parameter for Databricks script|
|[#11786](https://github.com/NVIDIA/spark-rapids/pull/11786)|Enable license header check|
|[#11791](https://github.com/NVIDIA/spark-rapids/pull/11791)|Incorporate checksum of internal dependencies in the GH cache key [skip ci]|
|[#11788](https://github.com/NVIDIA/spark-rapids/pull/11788)|Support running Databricks CI_PART2 integration tests with JARs built by CI_PART1|
|[#11778](https://github.com/NVIDIA/spark-rapids/pull/11778)|Remove unnecessary toBeReturned field from serialized batch iterators|
|[#11785](https://github.com/NVIDIA/spark-rapids/pull/11785)|Update advanced configs introduced by private repo [skip ci]|
|[#11772](https://github.com/NVIDIA/spark-rapids/pull/11772)|Update rapids JNI and private dependency to 25.02.0-SNAPSHOT|
|[#11756](https://github.com/NVIDIA/spark-rapids/pull/11756)|remove excluded release shim and TODO|

