# Change log
Generated on 2025-12-08

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
|[#13940](https://github.com/NVIDIA/spark-rapids/issues/13940)|[BUG] [Iceberg] After update MoR run select collect raise column not match|
|[#13854](https://github.com/NVIDIA/spark-rapids/issues/13854)|[BUG] run_other_join_modes_tests Integration tests: 1121 broadcast join failures and timeout the CI|
|[#13885](https://github.com/NVIDIA/spark-rapids/issues/13885)|[BUG] New added iceberg cases causes nightly integration tests timeout after 12 hours|
|[#13788](https://github.com/NVIDIA/spark-rapids/issues/13788)|[BUG] Scala test 'AsyncCpuTask task priority' failed UT|
|[#13798](https://github.com/NVIDIA/spark-rapids/issues/13798)|[BUG] GpuLoreSuite test failure: GpuInsertIntoHiveTable LoRE replay produces no data (Spark 3.5.7)|
|[#13848](https://github.com/NVIDIA/spark-rapids/issues/13848)|[BUG] test_delta_merge_sql_liquid_clustering fails with Delta 4|
|[#13860](https://github.com/NVIDIA/spark-rapids/issues/13860)|[Test] ERROR: Could not find a version that satisfies the requirement pyspark-client==3.5.6  and 3.5.7|
|[#13852](https://github.com/NVIDIA/spark-rapids/issues/13852)|[BUG] Iceberg merge tests failing after Delta clustered table update support|
|[#13861](https://github.com/NVIDIA/spark-rapids/issues/13861)|[BUG]src.main.python.join_test.test_sortmerge_join_ridealong  and test_sortmerge_join failures|
|[#13718](https://github.com/NVIDIA/spark-rapids/issues/13718)|[BUG] Spark Connect smoke test uses a  pip package `pyspark[connect]` with jars|
|[#13731](https://github.com/NVIDIA/spark-rapids/issues/13731)|[BUG] A special `select` query with the `where exists` expression brings error when using the GPU engines|
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
|[#13502](https://github.com/NVIDIA/spark-rapids/pull/13502)|Accelarate databricks-premerge with DBFS cache|
|[#13475](https://github.com/NVIDIA/spark-rapids/pull/13475)|Update dependency version JNI, private, hybrid to 25.12.0-SNAPSHOT|

## Release 25.10

### Features
|||
|:---|:---|
|[#12821](https://github.com/NVIDIA/spark-rapids/issues/12821)|[FEA] Zero Conf - set spark.rapids.memory.gpu.allocSize based on memory type|
|[#13503](https://github.com/NVIDIA/spark-rapids/issues/13503)|[FEA] Add integration test case to verify that falling back to cpu when insert into partitioned iceberg table with unsupported partition.|
|[#13400](https://github.com/NVIDIA/spark-rapids/issues/13400)|[FEA] Add gpu iceberg rolling file writer|
|[#13380](https://github.com/NVIDIA/spark-rapids/issues/13380)|[FEA] Add iceberg append data integration tests.|
|[#12575](https://github.com/NVIDIA/spark-rapids/issues/12575)|[FEA][DV]  Support Parquet+DV Scan using PERFILE GpuParquetReader|
|[#13418](https://github.com/NVIDIA/spark-rapids/issues/13418)|[FEA] Support Spark 4.0.1|
|[#13378](https://github.com/NVIDIA/spark-rapids/issues/13378)|[FEA] Add support for iceberg data writer factory.|
|[#13440](https://github.com/NVIDIA/spark-rapids/issues/13440)|[FEA] Use `RapidsFileIO` interface from `spark-rapids-jni`|
|[#13376](https://github.com/NVIDIA/spark-rapids/issues/13376)|[FEA] Add support for iceberg spark write|
|[#13394](https://github.com/NVIDIA/spark-rapids/issues/13394)|[FEA] Accelerate optimizing clustered tables|
|[#13377](https://github.com/NVIDIA/spark-rapids/issues/13377)|[FEA] Add support for iceberg partitioner|
|[#12553](https://github.com/NVIDIA/spark-rapids/issues/12553)|[FEA][Liquid] Accelerate writes to Clustered Tables|
|[#12882](https://github.com/NVIDIA/spark-rapids/issues/12882)|[FEA] It would be nice if we support Asia/Shanghai timezone for orc file scan|
|[#13379](https://github.com/NVIDIA/spark-rapids/issues/13379)|[FEA] Add support for iceberg parquet file writer.|
|[#13403](https://github.com/NVIDIA/spark-rapids/issues/13403)|[FEA] Add DV read support without support for combining files|
|[#12982](https://github.com/NVIDIA/spark-rapids/issues/12982)|[FEA] Support uuid|
|[#12552](https://github.com/NVIDIA/spark-rapids/issues/12552)|[FEA] [Liquid] Accelerate reads from Clustered Tables|
|[#13301](https://github.com/NVIDIA/spark-rapids/issues/13301)|[FEA] Setup integration tests against rest catalog for iceberg.|
|[#11193](https://github.com/NVIDIA/spark-rapids/issues/11193)|[FEA] Support "Fail on error" for `parse_url`|
|[#13012](https://github.com/NVIDIA/spark-rapids/issues/13012)|[FEA] Optimize command support for Delta IO|
|[#7008](https://github.com/NVIDIA/spark-rapids/issues/7008)|[FEA] Validate ORC deflate (zlib) compression|

### Performance
|||
|:---|:---|
|[#13311](https://github.com/NVIDIA/spark-rapids/issues/13311)|[FEA] Explore alternative way to calculate M2 for stddev* and variance*|
|[#13321](https://github.com/NVIDIA/spark-rapids/issues/13321)|Run NDS-H (TPC-H) SF100 locally on Delta 3.3.x|
|[#12471](https://github.com/NVIDIA/spark-rapids/issues/12471)|[FEA] Zero Config - Figure out a good default for CPU memory limits|

### Bugs Fixed
|||
|:---|:---|
|[#13478](https://github.com/NVIDIA/spark-rapids/issues/13478)|[BUG] databricks test to download from archive.apache.org consume too much time|
|[#13628](https://github.com/NVIDIA/spark-rapids/issues/13628)|[BUG] spark.rapids.memory.host.offHeapLimit doesn't play well with spark.memory.offHeap.size in Databricks|
|[#13578](https://github.com/NVIDIA/spark-rapids/issues/13578)|[BUG] Query failure in InMemoryTableScan with AQE enabled for aggregation|
|[#13554](https://github.com/NVIDIA/spark-rapids/issues/13554)|[BUG] iceberg-rest-catalog CI failed: Missing "org.apache.iceberg.rest.RESTCatalog"|
|[#13312](https://github.com/NVIDIA/spark-rapids/issues/13312)|[BUG] zip_with test failures|
|[#13513](https://github.com/NVIDIA/spark-rapids/issues/13513)|[BUG] Data mismatch when running a query with a filter against Delta table with DV|
|[#13562](https://github.com/NVIDIA/spark-rapids/issues/13562)|[BUG] cuda13 spark-rapids fails executors when profiler is enabled|
|[#12796](https://github.com/NVIDIA/spark-rapids/issues/12796)|[BUG] Delta CDF tests potentially use an incorrect read option|
|[#13519](https://github.com/NVIDIA/spark-rapids/issues/13519)|[BUG] NDS run failed java.lang.NoClassDefFoundError: org/apache/spark/sql/delta/DeltaParquetFileFormat|
|[#13553](https://github.com/NVIDIA/spark-rapids/issues/13553)|[BUG] iceberg_append_test failures due to IllegalArgumentException in pre_release|
|[#13542](https://github.com/NVIDIA/spark-rapids/issues/13542)|[BUG] new op time collection has huge overhead when op fallback happens|
|[#13535](https://github.com/NVIDIA/spark-rapids/issues/13535)|[BUG] iceberg_append_test failed Part of the plan is not columnar class org.apache.spark.sql.execution.SortExec|
|[#13531](https://github.com/NVIDIA/spark-rapids/issues/13531)|[BUG] IncrementMetric expression causes ColumnarToRow fallback|
|[#13514](https://github.com/NVIDIA/spark-rapids/issues/13514)|[BUG] MetricsEventLogValidationSuite failed: operator time metrics are less|
|[#13505](https://github.com/NVIDIA/spark-rapids/issues/13505)|[BUG] iceberg_append_test case failed: BasicColumnarWriteJobStatsTracker has no safety check for Active SparkContext|
|[#13445](https://github.com/NVIDIA/spark-rapids/issues/13445)|[BUG] com.nvidia.spark.rapids.jni.CpuRetryOOM: CPU OutOfMemory in performance test|
|[#13449](https://github.com/NVIDIA/spark-rapids/issues/13449)|[BUG] Device memory allocation failure when running the delete command for Delta tables with deletion vector enabled|
|[#13358](https://github.com/NVIDIA/spark-rapids/issues/13358)|[BUG] Failed to run IT on dataproc serverless with spark.executor.memoryOverhead|
|[#13477](https://github.com/NVIDIA/spark-rapids/issues/13477)|[BUG] MetricsEventLogValidationSuite failed Total operator time exceed total executor run time intermittently|
|[#13465](https://github.com/NVIDIA/spark-rapids/issues/13465)|[BUG] MetricsEventLogValidationSuite failed in spark400|
|[#13458](https://github.com/NVIDIA/spark-rapids/issues/13458)|[BUG] [TEST] Optimization for unit test: Reduce TimeZonePerfSuite test time|
|[#13457](https://github.com/NVIDIA/spark-rapids/issues/13457)|[BUG] delta_lake_liquid_clustering_test.test_delta_rtas_sql_liquid_clustering failed Part of the plan is not columnar class|
|[#12883](https://github.com/NVIDIA/spark-rapids/issues/12883)|[BUG] HostAllocSuite failed split should not happen immediately after fallback on memory contention failed|
|[#13455](https://github.com/NVIDIA/spark-rapids/issues/13455)|[BUG] CI integration test images fails build: Secondary flag is not valid for non-boolean flag|
|[#13443](https://github.com/NVIDIA/spark-rapids/issues/13443)|[BUG] CI failed: could not initialize class com.databricks.sql.transaction.tahoe.DeltaValidation$|
|[#13360](https://github.com/NVIDIA/spark-rapids/issues/13360)|[BUG] Column Support mismatch error with collect_list + JOIN when AQE is enabled|
|[#13396](https://github.com/NVIDIA/spark-rapids/issues/13396)|[BUG] markdown link check failed dead link loan-performance-data.html|
|[#13416](https://github.com/NVIDIA/spark-rapids/issues/13416)|[BUG] All IT join test and NDS test failed due to cudaErrorIllegalAddress an illegal memory access was encountered|
|[#13198](https://github.com/NVIDIA/spark-rapids/issues/13198)|[BUG] all tasks are bigger than the total memory limit FAILED|
|[#13370](https://github.com/NVIDIA/spark-rapids/issues/13370)|[BUG] liquid clustering should be triggered after enabled for testing|
|[#13361](https://github.com/NVIDIA/spark-rapids/issues/13361)|[BUG] Array index out of bounds when running iceberg query|
|[#10988](https://github.com/NVIDIA/spark-rapids/issues/10988)|[AUDIT][SPARK-48019] Fix incorrect behavior in ColumnVector/ColumnarArray with dictionary and nulls|
|[#13224](https://github.com/NVIDIA/spark-rapids/issues/13224)|[BUG] slightly deep expression planning/processing is slow|
|[#11921](https://github.com/NVIDIA/spark-rapids/issues/11921)|[BUG][AUDIT][SPARK-50258][SQL] Fix output column order changed issue after AQE optimization|
|[#13348](https://github.com/NVIDIA/spark-rapids/issues/13348)|[BUG] init_cudf_udf.sh failed: Script exit status is non-zero for databricks runtime since 25.10|
|[#13353](https://github.com/NVIDIA/spark-rapids/issues/13353)|[BUG] Regression with RapidsFileIO change|
|[#13326](https://github.com/NVIDIA/spark-rapids/issues/13326)|[BUG] hive_write_test.py::test_optimized_hive_ctas_configs_orc failure|
|[#13342](https://github.com/NVIDIA/spark-rapids/issues/13342)|[BUG] Build failure due to deprecation of some methods in ParseURI class from spark-rapids-jni|
|[#13337](https://github.com/NVIDIA/spark-rapids/issues/13337)|[BUG] `ParseURI` method deprecations causing pre-merge CI jobs failures|
|[#11106](https://github.com/NVIDIA/spark-rapids/issues/11106)|[BUG] Revisit integration tests with @disable_ansi_mode label|
|[#13319](https://github.com/NVIDIA/spark-rapids/issues/13319)|[BUG] some nvtx ranges pop out of order or have the wrong lifecycle|
|[#13138](https://github.com/NVIDIA/spark-rapids/issues/13138)|[FEA] Use iceberg FileIO to access parquet files.|
|[#13331](https://github.com/NVIDIA/spark-rapids/issues/13331)|[BUG] test_delta_optimize_fallback_on_clustered_table failed does not support truncate in batch mode|
|[#13270](https://github.com/NVIDIA/spark-rapids/issues/13270)|[BUG] Iceberg integration failed under spark connect|
|[#13145](https://github.com/NVIDIA/spark-rapids/issues/13145)|[BUG] NDS queries fail with sufficiently low offHeap limits in kudo deserializer|

### PRs
|||
|:---|:---|
|[#13831](https://github.com/NVIDIA/spark-rapids/pull/13831)|Support GpuMapFromEntries to prevent fallback 25.10|
|[#13782](https://github.com/NVIDIA/spark-rapids/pull/13782)|Ensure that task threads are the only ones registered as dedicated|
|[#13804](https://github.com/NVIDIA/spark-rapids/pull/13804)|[25.10] Add missing handling for the 0 input partition count in computing partition count in GpuOptimizeWriteExchangeExec|
|[#13631](https://github.com/NVIDIA/spark-rapids/pull/13631)|Update changelog for the v25.10 release [skip ci]|
|[#13627](https://github.com/NVIDIA/spark-rapids/pull/13627)|disable offHeapLimit by default|
|[#13588](https://github.com/NVIDIA/spark-rapids/pull/13588)|Update changelog for the v25.10 release [skip ci]|
|[#13593](https://github.com/NVIDIA/spark-rapids/pull/13593)|[DOC] update the download doc for 2510 release [skip ci]|
|[#13587](https://github.com/NVIDIA/spark-rapids/pull/13587)|Update dependency version JNI, private, hybrid to 25.10.0|
|[#13581](https://github.com/NVIDIA/spark-rapids/pull/13581)|Avoid host-vector access under TableCache by replacing CPU ColumnarToRow with GPU variant|
|[#13580](https://github.com/NVIDIA/spark-rapids/pull/13580)|Remove nested quotes and double quotes [skip ci]|
|[#13569](https://github.com/NVIDIA/spark-rapids/pull/13569)|Disable predicate pushdown filters in MULTITHREADED Delta Scan when reading Deletion Vectors|
|[#13563](https://github.com/NVIDIA/spark-rapids/pull/13563)|Fallback gracefully if libprofiler cannot be loaded|
|[#13532](https://github.com/NVIDIA/spark-rapids/pull/13532)|Fix read option name for change data feed for Delta|
|[#13545](https://github.com/NVIDIA/spark-rapids/pull/13545)|Add iceberg tests of insert into with values|
|[#13558](https://github.com/NVIDIA/spark-rapids/pull/13558)|Avoid throwing ClassNotFoundExceptions when checking isSupportedFormat with Delta Lake versions < 2.0.1|
|[#13541](https://github.com/NVIDIA/spark-rapids/pull/13541)|fix overhead of new op time collection upon c2r|
|[#13539](https://github.com/NVIDIA/spark-rapids/pull/13539)|Add env header for iceberg test variable [skip ci]|
|[#13537](https://github.com/NVIDIA/spark-rapids/pull/13537)|Fix iceberg integration test test_insert_into_partitioned_table_unsupported_partition_fallback [skip ci]|
|[#13335](https://github.com/NVIDIA/spark-rapids/pull/13335)|Add a GpuOverride for IncrementMetric |
|[#12967](https://github.com/NVIDIA/spark-rapids/pull/12967)|limit gpu alloc on integrated cards|
|[#13497](https://github.com/NVIDIA/spark-rapids/pull/13497)|Switch rounding calls to using new API|
|[#13515](https://github.com/NVIDIA/spark-rapids/pull/13515)|Fallback to cpu when inserting into partitioned iceberg table with unsupported partition.|
|[#13517](https://github.com/NVIDIA/spark-rapids/pull/13517)|more relaxed margin for MetricsEventLogValidationSuite|
|[#13516](https://github.com/NVIDIA/spark-rapids/pull/13516)|Fix wrong usage of GpuWriteJobStatsTracker in iceberg write|
|[#13499](https://github.com/NVIDIA/spark-rapids/pull/13499)|Refine column size estimation based on previous batch for Host2Gpu|
|[#13491](https://github.com/NVIDIA/spark-rapids/pull/13491)|Add Deletion Vector Read Support to Multithreaded Parquet Reader|
|[#13476](https://github.com/NVIDIA/spark-rapids/pull/13476)|Add GpuRollingFileWriter for iceberg.|
|[#13490](https://github.com/NVIDIA/spark-rapids/pull/13490)|Fix a Memory leak introduced as part of adding Deletion Vector support |
|[#13460](https://github.com/NVIDIA/spark-rapids/pull/13460)|Add support for Iceberg insert integration tests.|
|[#13492](https://github.com/NVIDIA/spark-rapids/pull/13492)|Update archive.apache usage in dockerfile [skip ci]|
|[#13439](https://github.com/NVIDIA/spark-rapids/pull/13439)|Add Partial support for Deletion Vector for PERFILE Reader Type|
|[#13481](https://github.com/NVIDIA/spark-rapids/pull/13481)|allow some margin for maxExpectedOperatorTime|
|[#13474](https://github.com/NVIDIA/spark-rapids/pull/13474)|Revert "insert coalesce after join" [skip ci]|
|[#13480](https://github.com/NVIDIA/spark-rapids/pull/13480)|avoid using Noop as default excludeMetric|
|[#13466](https://github.com/NVIDIA/spark-rapids/pull/13466)|fix evenlog by default zstd compression in spark 400|
|[#13459](https://github.com/NVIDIA/spark-rapids/pull/13459)|Optimization for unit test: Reduce TimeZonePerfSuite test time|
|[#13429](https://github.com/NVIDIA/spark-rapids/pull/13429)|refactor optime collection for correctness|
|[#13461](https://github.com/NVIDIA/spark-rapids/pull/13461)|Add a missing allow-non-gpu tag for CreateTableExec for test_delta_rtas_sql_liquid_clustering|
|[#13447](https://github.com/NVIDIA/spark-rapids/pull/13447)|Add Spark 4.0.1 shim|
|[#13444](https://github.com/NVIDIA/spark-rapids/pull/13444)|insert coalesce after join because join can cause row reductions|
|[#13456](https://github.com/NVIDIA/spark-rapids/pull/13456)|Remove legacy spacy pkg [skip ci]|
|[#13434](https://github.com/NVIDIA/spark-rapids/pull/13434)|Support InMemoryTableScan with AQE for Spark 3.5.2+|
|[#13369](https://github.com/NVIDIA/spark-rapids/pull/13369)|[DOC] Update memory debugging docs with scalar support [skip ci]|
|[#13441](https://github.com/NVIDIA/spark-rapids/pull/13441)|Substitue RapidsFileIO interface from spark-rapids-jni for RapidsFileIO in spark-rapids|
|[#13446](https://github.com/NVIDIA/spark-rapids/pull/13446)|Add support for iceberg GpuSparkWrite|
|[#13414](https://github.com/NVIDIA/spark-rapids/pull/13414)|Add support for the Delta optimize table command for clustered tables|
|[#13415](https://github.com/NVIDIA/spark-rapids/pull/13415)|Add gpu iceberg partitioner|
|[#13438](https://github.com/NVIDIA/spark-rapids/pull/13438)|Add rule to override StaticInvoke for iceberg|
|[#13417](https://github.com/NVIDIA/spark-rapids/pull/13417)|Add write support for Delta clustered tables|
|[#13367](https://github.com/NVIDIA/spark-rapids/pull/13367)|Allows AQE to fall back to the CPU for non-reused broadcast|
|[#13246](https://github.com/NVIDIA/spark-rapids/pull/13246)|Fix docstring typo in collect_data_or_exception [skip ci]|
|[#13052](https://github.com/NVIDIA/spark-rapids/pull/13052)|Orc supports non-UTC timezone when reader/writer timezones are the same|
|[#13409](https://github.com/NVIDIA/spark-rapids/pull/13409)|[DOC] update the download doc to add download plugin from maven [skip ci]|
|[#13407](https://github.com/NVIDIA/spark-rapids/pull/13407)|Add iceberg gpu parquet file appender and file writer factory|
|[#13366](https://github.com/NVIDIA/spark-rapids/pull/13366)|Fix the flaky test in `TrafficControllerSuite`|
|[#13359](https://github.com/NVIDIA/spark-rapids/pull/13359)|limit concurrent gpu task by a config|
|[#13392](https://github.com/NVIDIA/spark-rapids/pull/13392)|Add GpuAppendDataExec as prerequisite to support iceberg append|
|[#13364](https://github.com/NVIDIA/spark-rapids/pull/13364)|fix wrong timing to sleep(yield) in HostAllocSuite|
|[#13397](https://github.com/NVIDIA/spark-rapids/pull/13397)|Add support for iceberg bucket expression of int and long types.|
|[#12960](https://github.com/NVIDIA/spark-rapids/pull/12960)|Use async profiler to create per stage flame graph|
|[#13373](https://github.com/NVIDIA/spark-rapids/pull/13373)|Add an executor cache class|
|[#13371](https://github.com/NVIDIA/spark-rapids/pull/13371)|Optimize table to trigger liquid clustering before test|
|[#13372](https://github.com/NVIDIA/spark-rapids/pull/13372)|Fix index out of bound bug in iceberg parquet reader.|
|[#13289](https://github.com/NVIDIA/spark-rapids/pull/13289)|Filter out timezones not supported by Python|
|[#13308](https://github.com/NVIDIA/spark-rapids/pull/13308)|Add UUID function support|
|[#13225](https://github.com/NVIDIA/spark-rapids/pull/13225)|Fix performance issue with nested add|
|[#13318](https://github.com/NVIDIA/spark-rapids/pull/13318)|Add a missing numbers import in PySpark RAPIDS daemons|
|[#13329](https://github.com/NVIDIA/spark-rapids/pull/13329)|Add rest catalog integration tests for iceberg|
|[#13327](https://github.com/NVIDIA/spark-rapids/pull/13327)|Add read tests for the delta clustered table|
|[#13349](https://github.com/NVIDIA/spark-rapids/pull/13349)|Make the iceberg bug easier to produce.|
|[#13356](https://github.com/NVIDIA/spark-rapids/pull/13356)|CUDF Conda package has dropped cuda11 python packages from 25.10 [skip ci]|
|[#13354](https://github.com/NVIDIA/spark-rapids/pull/13354)|This fixes a big regression due to serialization of the hadoop configuration|
|[#13352](https://github.com/NVIDIA/spark-rapids/pull/13352)|Add ANSI mode support for parse_url function|
|[#13328](https://github.com/NVIDIA/spark-rapids/pull/13328)|Fix test_optimized_hive_ctas_configs_orc for non_utc timezones|
|[#13346](https://github.com/NVIDIA/spark-rapids/pull/13346)|Merge branch-25.08 to branch-25.10 [skip ci]|
|[#13338](https://github.com/NVIDIA/spark-rapids/pull/13338)|Suppress deprecation warnings for `ParseURI`|
|[#13317](https://github.com/NVIDIA/spark-rapids/pull/13317)|nvtx fixes in shuffle manager|
|[#13177](https://github.com/NVIDIA/spark-rapids/pull/13177)|enable more logging for BUFN_PLUS related issues|
|[#13252](https://github.com/NVIDIA/spark-rapids/pull/13252)|Add Rapids file io layer.|
|[#13333](https://github.com/NVIDIA/spark-rapids/pull/13333)|Fix test_delta_optimize_fallback_on_clustered_table test failure with Spark 3.5.6|
|[#13250](https://github.com/NVIDIA/spark-rapids/pull/13250)|add nvtx, fix row count, add data size metric|
|[#13299](https://github.com/NVIDIA/spark-rapids/pull/13299)|LORE: Add support to dump parquet with exact column names|
|[#13330](https://github.com/NVIDIA/spark-rapids/pull/13330)|Enable shellcheck: Fix nits [skip ci]|
|[#13323](https://github.com/NVIDIA/spark-rapids/pull/13323)|Update stringrepr for collation for StringGen|
|[#13309](https://github.com/NVIDIA/spark-rapids/pull/13309)|Enable shellcheck|
|[#13313](https://github.com/NVIDIA/spark-rapids/pull/13313)|Add support for optimize table command for Detla IO 3.3|
|[#13320](https://github.com/NVIDIA/spark-rapids/pull/13320)|Fix the checklists in the PR template to reduce confusion [skip test]|
|[#13316](https://github.com/NVIDIA/spark-rapids/pull/13316)|Fixed bug with arithmetic overflow on some map_zip_with tests|
|[#13322](https://github.com/NVIDIA/spark-rapids/pull/13322)|Fix testing.md to use fixed relative paths [skip ci]|
|[#13053](https://github.com/NVIDIA/spark-rapids/pull/13053)|Allowlist zlib for Orc writes|
|[#13220](https://github.com/NVIDIA/spark-rapids/pull/13220)|Add test cases for Collate, CharType and VarcharType|
|[#13295](https://github.com/NVIDIA/spark-rapids/pull/13295)|Avoid thread safety issues on input iterator in AsymmetricJoinSizer|
|[#13087](https://github.com/NVIDIA/spark-rapids/pull/13087)|Add Support for map_zip_with|
|[#13303](https://github.com/NVIDIA/spark-rapids/pull/13303)|wrap shuffle read's underlying input stream with timing logic|
|[#13288](https://github.com/NVIDIA/spark-rapids/pull/13288)|Replace 'get' with `getOrElse` to avoid None.get exception in GpuBatchScanExec|
|[#13287](https://github.com/NVIDIA/spark-rapids/pull/13287)|Revert "enable shellcheck" [skip ci]|
|[#13273](https://github.com/NVIDIA/spark-rapids/pull/13273)|enable offheap limits by default|
|[#13277](https://github.com/NVIDIA/spark-rapids/pull/13277)|Fix auto merge conflict 13276 [skip ci]|
|[#13122](https://github.com/NVIDIA/spark-rapids/pull/13122)|enable shellcheck|
|[#13262](https://github.com/NVIDIA/spark-rapids/pull/13262)|Run test_decimal_precision_over_max for all Spark versions|
|[#13255](https://github.com/NVIDIA/spark-rapids/pull/13255)|concat metrics should not contain iter.hasNext|
|[#13241](https://github.com/NVIDIA/spark-rapids/pull/13241)|Fix ThrottlingExecutor not closed bug for PR 12674|
|[#12674](https://github.com/NVIDIA/spark-rapids/pull/12674)|async threads for shuffle read|
|[#13229](https://github.com/NVIDIA/spark-rapids/pull/13229)|Update dependency version JNI, private, hybrid to 25.10.0-SNAPSHOT|

## Older Releases
Changelog of older releases can be found at [docs/archives](/docs/archives)
