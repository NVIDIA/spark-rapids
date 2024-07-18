# Change log
Generated on 2024-07-18

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
|[#11211](https://github.com/NVIDIA/spark-rapids/pull/11211)|Use fixed seed for test_from_json_struct_decimal|
|[#11203](https://github.com/NVIDIA/spark-rapids/pull/11203)|Update version to 24.06.1-SNAPSHOT|
|[#11205](https://github.com/NVIDIA/spark-rapids/pull/11205)|Update docs for 24.06.1 release [skip ci]|
|[#11056](https://github.com/NVIDIA/spark-rapids/pull/11056)|Update latest changelog [skip ci]|
|[#11052](https://github.com/NVIDIA/spark-rapids/pull/11052)|Add spark343 shim for scala2.13 dist jar|
|[#10981](https://github.com/NVIDIA/spark-rapids/pull/10981)|Update latest changelog [skip ci]|
|[#10984](https://github.com/NVIDIA/spark-rapids/pull/10984)|[DOC] Update docs for 24.06.0 release [skip ci]|
|[#10974](https://github.com/NVIDIA/spark-rapids/pull/10974)|Update rapids JNI and private dependency to 24.06.0|
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
|[#10846](https://github.com/NVIDIA/spark-rapids/pull/10846)|Update latest changelog [skip ci]|
|[#10836](https://github.com/NVIDIA/spark-rapids/pull/10836)|Catch exceptions when trying to examine Iceberg scan for metadata queries|
|[#10824](https://github.com/NVIDIA/spark-rapids/pull/10824)|Support zstd for GPU shuffle compression|
|[#10828](https://github.com/NVIDIA/spark-rapids/pull/10828)|Added DateTimeUtilsShims [Databricks]|
|[#10829](https://github.com/NVIDIA/spark-rapids/pull/10829)|Fix `Inheritance Shadowing` to add support for Spark 4.0.0|
|[#10811](https://github.com/NVIDIA/spark-rapids/pull/10811)|Fix NPE in GpuParseUrl for null keys.|
|[#10723](https://github.com/NVIDIA/spark-rapids/pull/10723)|Implement chunked ORC reader|
|[#10715](https://github.com/NVIDIA/spark-rapids/pull/10715)|Rewrite some rlike expression to StartsWith/Contains|
|[#10820](https://github.com/NVIDIA/spark-rapids/pull/10820)|workaround #10801 temporally|
|[#10812](https://github.com/NVIDIA/spark-rapids/pull/10812)|Replace ThreadPoolExecutor creation with ThreadUtils API|
|[#10816](https://github.com/NVIDIA/spark-rapids/pull/10816)|Fix a test error for DB13.3|
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
