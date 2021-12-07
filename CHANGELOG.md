# Change log
Generated on 2021-12-07

## Release 21.12

### Features
|||
|:---|:---|
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

## Release 0.5

### Features
|||
|:---|:---|
|[#938](https://github.com/NVIDIA/spark-rapids/issues/938)|[FEA] Have hashed shuffle match spark|
|[#1604](https://github.com/NVIDIA/spark-rapids/issues/1604)|[FEA] Support casting structs to strings |
|[#1920](https://github.com/NVIDIA/spark-rapids/issues/1920)|[FEA] Support murmur3 hashing of structs|
|[#2018](https://github.com/NVIDIA/spark-rapids/issues/2018)|[FEA] A way for user to find out the plugin version and cudf version in REPL|
|[#77](https://github.com/NVIDIA/spark-rapids/issues/77)|[FEA] Support ArrayContains|
|[#1721](https://github.com/NVIDIA/spark-rapids/issues/1721)|[FEA] build cudf jars with NVTX enabled|
|[#1782](https://github.com/NVIDIA/spark-rapids/issues/1782)|[FEA] Shim layers to support spark versions|
|[#1625](https://github.com/NVIDIA/spark-rapids/issues/1625)|[FEA] Support Decimal Casts to String and String to Decimal|
|[#166](https://github.com/NVIDIA/spark-rapids/issues/166)|[FEA] Support get_json_object|
|[#1698](https://github.com/NVIDIA/spark-rapids/issues/1698)|[FEA] Support casting structs to string|
|[#1912](https://github.com/NVIDIA/spark-rapids/issues/1912)|[FEA] Let `Scalar Pandas UDF ` support array of struct type.|
|[#1136](https://github.com/NVIDIA/spark-rapids/issues/1136)|[FEA] Audit: Script to list commits between different Spark versions/tags|
|[#1921](https://github.com/NVIDIA/spark-rapids/issues/1921)|[FEA] cudf version check should be lenient on later patch version|
|[#19](https://github.com/NVIDIA/spark-rapids/issues/19)|[FEA] Out of core sorts|

### Performance
|||
|:---|:---|
|[#2090](https://github.com/NVIDIA/spark-rapids/issues/2090)|[FEA] Make row count estimates available to the cost-based optimizer|
|[#1341](https://github.com/NVIDIA/spark-rapids/issues/1341)|Optimize unnecessary columnar->row->columnar transitions with AQE|
|[#1558](https://github.com/NVIDIA/spark-rapids/issues/1558)|[FEA] Initialize UCX early|
|[#1633](https://github.com/NVIDIA/spark-rapids/issues/1633)|[FEA] Implement a cost-based optimizer|
|[#1727](https://github.com/NVIDIA/spark-rapids/issues/1727)|[FEA] Put RangePartitioner data path on the GPU|

### Bugs Fixed
|||
|:---|:---|
|[#2279](https://github.com/NVIDIA/spark-rapids/issues/2279)|[BUG] Hash Partitioning can fail for very small batches|
|[#2314](https://github.com/NVIDIA/spark-rapids/issues/2314)|[BUG] v0.5.0 pre-release pytests join_test.py::test_hash_join_array FAILED on SPARK-EGX Yarn Cluster|
|[#2317](https://github.com/NVIDIA/spark-rapids/issues/2317)|[BUG] GpuColumnarToRowIterator can stop after receiving an empty batch|
|[#2244](https://github.com/NVIDIA/spark-rapids/issues/2244)|[BUG] Executors hanging when running NDS benchmarks|
|[#2278](https://github.com/NVIDIA/spark-rapids/issues/2278)|[BUG] FullOuter join can produce too many results|
|[#2220](https://github.com/NVIDIA/spark-rapids/issues/2220)|[BUG] csv_test.py::test_csv_fallback FAILED on the EMR Cluster|
|[#2225](https://github.com/NVIDIA/spark-rapids/issues/2225)|[BUG] GpuSort fails on tables containing arrays.|
|[#2232](https://github.com/NVIDIA/spark-rapids/issues/2232)|[BUG] hash_aggregate_test.py::test_hash_grpby_pivot FAILED on the Databricks Cluster|
|[#2231](https://github.com/NVIDIA/spark-rapids/issues/2231)|[BUG]string_test.py::test_re_replace FAILED on the Dataproc Cluster|
|[#2042](https://github.com/NVIDIA/spark-rapids/issues/2042)|[BUG] NDS q14a fails with "GpuColumnarToRow does not implement doExecuteBroadcast"|
|[#2203](https://github.com/NVIDIA/spark-rapids/issues/2203)|[BUG] Spark nightly cache tests fail with -- master flag|
|[#2230](https://github.com/NVIDIA/spark-rapids/issues/2230)|[BUG] qa_nightly_select_test.py::test_select FAILED on the Dataproc Cluster|
|[#1711](https://github.com/NVIDIA/spark-rapids/issues/1711)|[BUG] find a way to stop allocating from RMM on the shuffle-client thread|
|[#2109](https://github.com/NVIDIA/spark-rapids/issues/2109)|[BUG] Fix high priority violations detected by code analysis tools|
|[#2217](https://github.com/NVIDIA/spark-rapids/issues/2217)|[BUG] qa_nightly_select_test failure in test_select |
|[#2127](https://github.com/NVIDIA/spark-rapids/issues/2127)|[BUG] Parsing with two-digit year should fall back to CPU|
|[#2078](https://github.com/NVIDIA/spark-rapids/issues/2078)|[BUG] java.lang.ArithmeticException: divide by zero when spark.sql.ansi.enabled=true|
|[#2048](https://github.com/NVIDIA/spark-rapids/issues/2048)|[BUG] split function+ repartition result in "ai.rapids.cudf.CudaException: device-side assert triggered"|
|[#2036](https://github.com/NVIDIA/spark-rapids/issues/2036)|[BUG] Stackoverflow when writing wide parquet files.|
|[#1973](https://github.com/NVIDIA/spark-rapids/issues/1973)|[BUG] generate_expr_test FAILED on Dataproc Cluster|
|[#2079](https://github.com/NVIDIA/spark-rapids/issues/2079)|[BUG] koalas.sql fails with java.lang.ArrayIndexOutOfBoundsException|
|[#217](https://github.com/NVIDIA/spark-rapids/issues/217)|[BUG] CudaUtil should be removed|
|[#1550](https://github.com/NVIDIA/spark-rapids/issues/1550)|[BUG] The ORC output data of a query is not readable|
|[#2074](https://github.com/NVIDIA/spark-rapids/issues/2074)|[BUG] Intermittent NPE in RapidsBufferCatalog when running test suite|
|[#2027](https://github.com/NVIDIA/spark-rapids/issues/2027)|[BUG] udf_cudf_test.py integration tests fail |
|[#1899](https://github.com/NVIDIA/spark-rapids/issues/1899)|[BUG] Some queries fail when cost-based optimizations are enabled|
|[#1914](https://github.com/NVIDIA/spark-rapids/issues/1914)|[BUG] Add in float, double, timestamp, and date support to murmur3|
|[#2014](https://github.com/NVIDIA/spark-rapids/issues/2014)|[BUG] earlyStart option added in 0.5 can cause errors when starting UCX|
|[#1984](https://github.com/NVIDIA/spark-rapids/issues/1984)|[BUG] NDS q58 Decimal scale (59) cannot be greater than precision (38).|
|[#2001](https://github.com/NVIDIA/spark-rapids/issues/2001)|[BUG] RapidsShuffleManager didn't pass `dirs` to `getBlockData` from a wrapped `ShuffleBlockResolver`|
|[#1797](https://github.com/NVIDIA/spark-rapids/issues/1797)|[BUG] occasional crashes in CI|
|[#1861](https://github.com/NVIDIA/spark-rapids/issues/1861)|Encountered column data outside the range of input buffer|
|[#1905](https://github.com/NVIDIA/spark-rapids/issues/1905)|[BUG] Large concat task time in GpuShuffleCoalesce with pinned memory pool|
|[#1638](https://github.com/NVIDIA/spark-rapids/issues/1638)|[BUG] Tests `test_window_aggs_for_rows_collect_list` fails when there are null values in columns.|
|[#1864](https://github.com/NVIDIA/spark-rapids/issues/1864)|[BUG]HostColumnarToGPU inefficient when only doing count()|
|[#1862](https://github.com/NVIDIA/spark-rapids/issues/1862)|[BUG] spark 3.2.0-snapshot integration test failed due to conf change|
|[#1844](https://github.com/NVIDIA/spark-rapids/issues/1844)|[BUG] branch-0.5 nightly IT FAILED on the The mortgage ETL test "Could not read footer for file: file:/xxx/xxx.snappy.parquet"|
|[#1627](https://github.com/NVIDIA/spark-rapids/issues/1627)|[BUG] GDS exception when restoring spilled buffer|
|[#1802](https://github.com/NVIDIA/spark-rapids/issues/1802)|[BUG] Many decimal integration test failures for 0.5|

### PRs
|||
|:---|:---|
|[#2326](https://github.com/NVIDIA/spark-rapids/pull/2326)|Update changelog for 0.5.0 release|
|[#2316](https://github.com/NVIDIA/spark-rapids/pull/2316)|Update doc to note that single quoted json strings are not ok|
|[#2319](https://github.com/NVIDIA/spark-rapids/pull/2319)|Disable hash partitioning on arrays|
|[#2318](https://github.com/NVIDIA/spark-rapids/pull/2318)|Fix ColumnarToRowIterator handling of empty batches|
|[#2304](https://github.com/NVIDIA/spark-rapids/pull/2304)|Update CHANGELOG.md|
|[#2301](https://github.com/NVIDIA/spark-rapids/pull/2301)|Update doc to reflect nanosleep problem with 460.32.03|
|[#2298](https://github.com/NVIDIA/spark-rapids/pull/2298)|Update changelog for v0.5.0 release [skip ci]|
|[#2293](https://github.com/NVIDIA/spark-rapids/pull/2293)|update cudf version to 0.19.2|
|[#2289](https://github.com/NVIDIA/spark-rapids/pull/2289)|Update docs to warn against 450.80.02 driver with 10.x toolkit|
|[#2285](https://github.com/NVIDIA/spark-rapids/pull/2285)|Require single batch for full outer join streaming|
|[#2281](https://github.com/NVIDIA/spark-rapids/pull/2281)|Remove download section for unreleased 0.4.2|
|[#2264](https://github.com/NVIDIA/spark-rapids/pull/2264)|Add spark312 and spark320 versions of cache serializer|
|[#2254](https://github.com/NVIDIA/spark-rapids/pull/2254)|updated gcp docs with custom dataproc image instructions|
|[#2247](https://github.com/NVIDIA/spark-rapids/pull/2247)|Allow specifying a superclass for non-GPU execs|
|[#2235](https://github.com/NVIDIA/spark-rapids/pull/2235)|Fix distributed cache to read requested schema |
|[#2261](https://github.com/NVIDIA/spark-rapids/pull/2261)|Make CBO row count test more robust|
|[#2237](https://github.com/NVIDIA/spark-rapids/pull/2237)|update cudf version to 0.19.1|
|[#2240](https://github.com/NVIDIA/spark-rapids/pull/2240)|Get the correct 'PIPESTATUS' in bash [skip ci]|
|[#2242](https://github.com/NVIDIA/spark-rapids/pull/2242)|Add shuffle doc section on the periodicGC configuration|
|[#2251](https://github.com/NVIDIA/spark-rapids/pull/2251)|Fix issue when out of core sorting nested data types|
|[#2204](https://github.com/NVIDIA/spark-rapids/pull/2204)|Run nightly tests for ParquetCachedBatchSerializer|
|[#2245](https://github.com/NVIDIA/spark-rapids/pull/2245)|Fix pivot bug for decimalType|
|[#2093](https://github.com/NVIDIA/spark-rapids/pull/2093)|Initial implementation of row count estimates in cost-based optimizer|
|[#2188](https://github.com/NVIDIA/spark-rapids/pull/2188)|Support GPU broadcast exchange reuse to feed CPU BHJ when AQE is enabled|
|[#2227](https://github.com/NVIDIA/spark-rapids/pull/2227)|ParquetCachedBatchSerializer broadcast AllConfs instead of SQLConf to fix distributed mode|
|[#2223](https://github.com/NVIDIA/spark-rapids/pull/2223)|Adds subquery aggregate tests from SPARK-31620|
|[#2222](https://github.com/NVIDIA/spark-rapids/pull/2222)|Remove groupId already specified in parent pom|
|[#2209](https://github.com/NVIDIA/spark-rapids/pull/2209)|Fixed a few issues with out of core sort|
|[#2218](https://github.com/NVIDIA/spark-rapids/pull/2218)|Fix incorrect RegExpReplace children handling on Spark 3.1+|
|[#2207](https://github.com/NVIDIA/spark-rapids/pull/2207)|fix batch size default values in the tuning guide|
|[#2208](https://github.com/NVIDIA/spark-rapids/pull/2208)|Revert "add nightly cache tests (#2083)"|
|[#2206](https://github.com/NVIDIA/spark-rapids/pull/2206)|Fix shim301db build|
|[#2192](https://github.com/NVIDIA/spark-rapids/pull/2192)|Fix index-based access to the head elements|
|[#2210](https://github.com/NVIDIA/spark-rapids/pull/2210)|Avoid redundant collection conversions|
|[#2190](https://github.com/NVIDIA/spark-rapids/pull/2190)|JNI fixes for StringWordCount native UDF example|
|[#2086](https://github.com/NVIDIA/spark-rapids/pull/2086)|Updating documentation for data format support|
|[#2172](https://github.com/NVIDIA/spark-rapids/pull/2172)|Remove easy unused symbols|
|[#2089](https://github.com/NVIDIA/spark-rapids/pull/2089)|Update PandasUDF doc|
|[#2195](https://github.com/NVIDIA/spark-rapids/pull/2195)|fix cudf 0.19.0 download link [skip ci]|
|[#2175](https://github.com/NVIDIA/spark-rapids/pull/2175)|Branch 0.5 doc update|
|[#2168](https://github.com/NVIDIA/spark-rapids/pull/2168)|Simplify GpuExpressions w/ withResourceIfAllowed|
|[#2055](https://github.com/NVIDIA/spark-rapids/pull/2055)|Support PivotFirst|
|[#2183](https://github.com/NVIDIA/spark-rapids/pull/2183)|GpuParquetScan#readBufferToTable remove dead code|
|[#2129](https://github.com/NVIDIA/spark-rapids/pull/2129)|Fall back to CPU when parsing two-digit years|
|[#2083](https://github.com/NVIDIA/spark-rapids/pull/2083)|add nightly cache tests|
|[#2151](https://github.com/NVIDIA/spark-rapids/pull/2151)|add corresponding close call for HostMemoryOutputStream|
|[#2169](https://github.com/NVIDIA/spark-rapids/pull/2169)|Work around bug in Spark for integration test|
|[#2130](https://github.com/NVIDIA/spark-rapids/pull/2130)|Fix divide-by-zero in GpuAverage with ansi mode|
|[#2149](https://github.com/NVIDIA/spark-rapids/pull/2149)|Auto generate the supported types for the file formats|
|[#2072](https://github.com/NVIDIA/spark-rapids/pull/2072)|Disable CSV parsing by default and update tests to better show what is left|
|[#2157](https://github.com/NVIDIA/spark-rapids/pull/2157)|fix merge conflict for 0.4.2 [skip ci]|
|[#2144](https://github.com/NVIDIA/spark-rapids/pull/2144)|Allow array and struct types to pass thru when doing join|
|[#2145](https://github.com/NVIDIA/spark-rapids/pull/2145)|Avoid GPU shuffle for round-robin of unsortable types|
|[#2021](https://github.com/NVIDIA/spark-rapids/pull/2021)|Add in support for murmur3 hashing of structs|
|[#2128](https://github.com/NVIDIA/spark-rapids/pull/2128)|Add in Partition type check support|
|[#2116](https://github.com/NVIDIA/spark-rapids/pull/2116)|Add dynamic Spark configuration for Databricks|
|[#2132](https://github.com/NVIDIA/spark-rapids/pull/2132)|Log plugin and cudf versions on startup|
|[#2135](https://github.com/NVIDIA/spark-rapids/pull/2135)|Disable Spark 3.2 shim by default|
|[#2125](https://github.com/NVIDIA/spark-rapids/pull/2125)|enable auto-merge from 0.5 to 0.6 [skip ci]|
|[#2120](https://github.com/NVIDIA/spark-rapids/pull/2120)|Materialize Stream before serialization|
|[#2119](https://github.com/NVIDIA/spark-rapids/pull/2119)|Add more comprehensive documentation on supported date formats|
|[#1717](https://github.com/NVIDIA/spark-rapids/pull/1717)|Decimal32 support|
|[#2114](https://github.com/NVIDIA/spark-rapids/pull/2114)|Modified the Download page for 0.4.1 and updated doc to point to K8s guide|
|[#2106](https://github.com/NVIDIA/spark-rapids/pull/2106)|Fix some buffer leaks|
|[#2097](https://github.com/NVIDIA/spark-rapids/pull/2097)|fix the bound row project empty issue in row frame|
|[#2099](https://github.com/NVIDIA/spark-rapids/pull/2099)|Remove verbose log prints to make the build/test log clean|
|[#2105](https://github.com/NVIDIA/spark-rapids/pull/2105)|Cleanup prior Spark sessions in tests consistently|
|[#2104](https://github.com/NVIDIA/spark-rapids/pull/2104)| Clone apache spark source code to parse the git commit IDs|
|[#2095](https://github.com/NVIDIA/spark-rapids/pull/2095)|fix refcount when materializing device buffer from GDS|
|[#2100](https://github.com/NVIDIA/spark-rapids/pull/2100)|[BUG] add wget for fetching conda [skip ci]|
|[#2096](https://github.com/NVIDIA/spark-rapids/pull/2096)|Adjust images for integration tests|
|[#2094](https://github.com/NVIDIA/spark-rapids/pull/2094)|Changed name of parquet files for Mortgage ETL Integration test|
|[#2035](https://github.com/NVIDIA/spark-rapids/pull/2035)|Accelerate data transfer for map Pandas UDF plan|
|[#2050](https://github.com/NVIDIA/spark-rapids/pull/2050)|stream shuffle buffers from GDS to UCX|
|[#2084](https://github.com/NVIDIA/spark-rapids/pull/2084)|Enable ORC write by default|
|[#2088](https://github.com/NVIDIA/spark-rapids/pull/2088)|Upgrade ScalaTest plugin to respect JAVA_HOME|
|[#1932](https://github.com/NVIDIA/spark-rapids/pull/1932)|Create a getting started on K8s page|
|[#2080](https://github.com/NVIDIA/spark-rapids/pull/2080)|Improve error message after failed RMM shutdown|
|[#2064](https://github.com/NVIDIA/spark-rapids/pull/2064)|Optimize unnecessary columnar->row->columnar transitions with AQE|
|[#2025](https://github.com/NVIDIA/spark-rapids/pull/2025)|Update the doc for pandas udf on databricks|
|[#2059](https://github.com/NVIDIA/spark-rapids/pull/2059)|Add the flag 'TEST_TYPE' to avoid integration tests silently skipping some test cases|
|[#2075](https://github.com/NVIDIA/spark-rapids/pull/2075)|Remove debug println from CBO test|
|[#2046](https://github.com/NVIDIA/spark-rapids/pull/2046)|support casting Decimal to String|
|[#1812](https://github.com/NVIDIA/spark-rapids/pull/1812)|allow spilled buffers to be unspilled|
|[#2061](https://github.com/NVIDIA/spark-rapids/pull/2061)|Run the pandas udf using cudf on Databricks|
|[#1893](https://github.com/NVIDIA/spark-rapids/pull/1893)|Plug-in support for get_json_object|
|[#2044](https://github.com/NVIDIA/spark-rapids/pull/2044)|Use partition for GPU hash partitioning|
|[#1954](https://github.com/NVIDIA/spark-rapids/pull/1954)|Fix CBO bug where incompatible plans were produced with AQE on|
|[#2049](https://github.com/NVIDIA/spark-rapids/pull/2049)|Remove incompatable int overflow checking|
|[#2056](https://github.com/NVIDIA/spark-rapids/pull/2056)|Remove Spark 3.2 from premerge and nightly CI run|
|[#1814](https://github.com/NVIDIA/spark-rapids/pull/1814)|Struct to string casting functionality|
|[#2037](https://github.com/NVIDIA/spark-rapids/pull/2037)|Fix warnings from use of deprecated cudf methods|
|[#2033](https://github.com/NVIDIA/spark-rapids/pull/2033)|Bump up pre-merge OS from ubuntu 16 to ubuntu 18 [skip ci]|
|[#1883](https://github.com/NVIDIA/spark-rapids/pull/1883)|Enable sort for single-level nesting struct columns on GPU|
|[#2016](https://github.com/NVIDIA/spark-rapids/pull/2016)|Refactor logic for parallel testing|
|[#2022](https://github.com/NVIDIA/spark-rapids/pull/2022)|Update order by to not load native libraries when sorting|
|[#2017](https://github.com/NVIDIA/spark-rapids/pull/2017)|Add in murmur3 support for float, double, date and timestamp|
|[#1981](https://github.com/NVIDIA/spark-rapids/pull/1981)|Fix GpuSize|
|[#1999](https://github.com/NVIDIA/spark-rapids/pull/1999)|support casting string to decimal|
|[#2006](https://github.com/NVIDIA/spark-rapids/pull/2006)|Enable windowed `collect_list` by default|
|[#2000](https://github.com/NVIDIA/spark-rapids/pull/2000)|Use Spark's HybridRowQueue to avoid MemoryConsumer API shim|
|[#2015](https://github.com/NVIDIA/spark-rapids/pull/2015)|Fix bug where rkey buffer is getting advanced after the first handshake|
|[#2007](https://github.com/NVIDIA/spark-rapids/pull/2007)|Fix unknown column name error when filtering ORC file with no names|
|[#2005](https://github.com/NVIDIA/spark-rapids/pull/2005)|Update to new is_before_spark_311 function name|
|[#1944](https://github.com/NVIDIA/spark-rapids/pull/1944)|Support running scalar pandas UDF with array type.|
|[#1991](https://github.com/NVIDIA/spark-rapids/pull/1991)|Fixes creation of invalid DecimalType in GpuDivide.tagExprForGpu|
|[#1958](https://github.com/NVIDIA/spark-rapids/pull/1958)|Support legacy behavior of parameterless count |
|[#1919](https://github.com/NVIDIA/spark-rapids/pull/1919)|Add support for Structs for UnionExec|
|[#2002](https://github.com/NVIDIA/spark-rapids/pull/2002)|Pass dirs to getBlockData for a wrapped shuffle resolver|
|[#1983](https://github.com/NVIDIA/spark-rapids/pull/1983)|document building against different CUDA Toolkit versions|
|[#1994](https://github.com/NVIDIA/spark-rapids/pull/1994)|Merge 0.4 to 0.5 [skip ci]|
|[#1982](https://github.com/NVIDIA/spark-rapids/pull/1982)|Update ORC pushdown filter building to latest Spark logic|
|[#1978](https://github.com/NVIDIA/spark-rapids/pull/1978)|Add audit script to list commits from Spark|
|[#1976](https://github.com/NVIDIA/spark-rapids/pull/1976)|Temp fix for parquet write changes|
|[#1970](https://github.com/NVIDIA/spark-rapids/pull/1970)|add maven profiles for supported CUDA versions|
|[#1951](https://github.com/NVIDIA/spark-rapids/pull/1951)|Branch 0.5 doc remove numpartitions|
|[#1967](https://github.com/NVIDIA/spark-rapids/pull/1967)|Update FAQ for Dataset API and format supported versions|
|[#1972](https://github.com/NVIDIA/spark-rapids/pull/1972)|support GpuSize|
|[#1966](https://github.com/NVIDIA/spark-rapids/pull/1966)|add xml report for codecov|
|[#1955](https://github.com/NVIDIA/spark-rapids/pull/1955)|Fix typo in Arrow optimization config|
|[#1956](https://github.com/NVIDIA/spark-rapids/pull/1956)|Fix NPE in plugin shutdown|
|[#1930](https://github.com/NVIDIA/spark-rapids/pull/1930)|Relax cudf version check for patch-level versions|
|[#1787](https://github.com/NVIDIA/spark-rapids/pull/1787)|support distributed file path in cloud environment|
|[#1961](https://github.com/NVIDIA/spark-rapids/pull/1961)|change premege GPU_TYPE from secret to global env [skip ci]|
|[#1957](https://github.com/NVIDIA/spark-rapids/pull/1957)|Update Spark 3.1.2 shim for float upcast behavior|
|[#1889](https://github.com/NVIDIA/spark-rapids/pull/1889)|Decimal DIV changes |
|[#1947](https://github.com/NVIDIA/spark-rapids/pull/1947)|Move doc of Pandas UDF to additional-functionality|
|[#1938](https://github.com/NVIDIA/spark-rapids/pull/1938)|Add spark.executor.resource.gpu.amount=1 to YARN and K8s docs|
|[#1937](https://github.com/NVIDIA/spark-rapids/pull/1937)|Fix merge conflict with branch-0.4|
|[#1878](https://github.com/NVIDIA/spark-rapids/pull/1878)|spillable cache for GpuCartesianRDD|
|[#1843](https://github.com/NVIDIA/spark-rapids/pull/1843)|Refactor GpuGenerateExec and Explode|
|[#1933](https://github.com/NVIDIA/spark-rapids/pull/1933)|Split DB scripts to make them common for the build and IT pipeline|
|[#1935](https://github.com/NVIDIA/spark-rapids/pull/1935)|Update Alias SQL quoting and float-to-timestamp casting to match Spark 3.2|
|[#1926](https://github.com/NVIDIA/spark-rapids/pull/1926)|Consolidate RAT settings in parent pom|
|[#1918](https://github.com/NVIDIA/spark-rapids/pull/1918)|Minor code cleanup in dateTImeExpressions|
|[#1906](https://github.com/NVIDIA/spark-rapids/pull/1906)|Remove get call on timeZoneId|
|[#1908](https://github.com/NVIDIA/spark-rapids/pull/1908)|Remove the Scala version of Mortgage ETL tests from nightly test|
|[#1894](https://github.com/NVIDIA/spark-rapids/pull/1894)|Modified Download Page to re-order the items and change the format of download links|
|[#1909](https://github.com/NVIDIA/spark-rapids/pull/1909)|Avoid pinned memory for shuffle host buffers|
|[#1891](https://github.com/NVIDIA/spark-rapids/pull/1891)|Connect UCX endpoints early during app startup|
|[#1877](https://github.com/NVIDIA/spark-rapids/pull/1877)|remove docker build in pre-merge [skip ci]|
|[#1830](https://github.com/NVIDIA/spark-rapids/pull/1830)|Enable the tests for collect over window.|
|[#1882](https://github.com/NVIDIA/spark-rapids/pull/1882)|GpuArrowColumnarBatchBuilder retains the references of ArrowBuf until HostToGpuCoalesceIterator put them into device|
|[#1868](https://github.com/NVIDIA/spark-rapids/pull/1868)|Increase row limit when doing count() for HostColumnarToGpu |
|[#1855](https://github.com/NVIDIA/spark-rapids/pull/1855)|Expose row count statistics in GpuShuffleExchangeExec|
|[#1875](https://github.com/NVIDIA/spark-rapids/pull/1875)|Fix merge conflict with branch-0.4|
|[#1841](https://github.com/NVIDIA/spark-rapids/pull/1841)|Add in support for DateAddInterval|
|[#1869](https://github.com/NVIDIA/spark-rapids/pull/1869)|Fix tests for Spark 3.2.0 shim|
|[#1858](https://github.com/NVIDIA/spark-rapids/pull/1858)|fix shuffle manager doc on ucx library path|
|[#1836](https://github.com/NVIDIA/spark-rapids/pull/1836)|Add shim for Spark 3.1.2|
|[#1852](https://github.com/NVIDIA/spark-rapids/pull/1852)|Fix Part Suite Tests|
|[#1616](https://github.com/NVIDIA/spark-rapids/pull/1616)|Cost-based optimizer|
|[#1834](https://github.com/NVIDIA/spark-rapids/pull/1834)|Add shim for Spark 3.0.3|
|[#1839](https://github.com/NVIDIA/spark-rapids/pull/1839)|Refactor join code to reduce duplicated code|
|[#1848](https://github.com/NVIDIA/spark-rapids/pull/1848)|Fix merge conflict with branch-0.4|
|[#1796](https://github.com/NVIDIA/spark-rapids/pull/1796)|Have most of range partitioning run on the GPU|
|[#1845](https://github.com/NVIDIA/spark-rapids/pull/1845)|Fix fails on the mortgage ETL test|
|[#1829](https://github.com/NVIDIA/spark-rapids/pull/1829)|Cleanup unused Jenkins files and scripts|
|[#1704](https://github.com/NVIDIA/spark-rapids/pull/1704)|Create a shim for Spark 3.2.0 development|
|[#1838](https://github.com/NVIDIA/spark-rapids/pull/1838)|Make databricks build.sh more convenient for dev|
|[#1835](https://github.com/NVIDIA/spark-rapids/pull/1835)|Fix merge conflict with branch-0.4|
|[#1808](https://github.com/NVIDIA/spark-rapids/pull/1808)|Update mortgage tests to support reading multiple dataset formats|
|[#1822](https://github.com/NVIDIA/spark-rapids/pull/1822)|Fix conflict 0.4 to 0.5|
|[#1807](https://github.com/NVIDIA/spark-rapids/pull/1807)|Fix merge conflict between branch-0.4 and branch-0.5|
|[#1788](https://github.com/NVIDIA/spark-rapids/pull/1788)|Spill metrics everywhere|
|[#1719](https://github.com/NVIDIA/spark-rapids/pull/1719)|Add in out of core sort|
|[#1728](https://github.com/NVIDIA/spark-rapids/pull/1728)|Skip RAPIDS accelerated Java UDF tests if UDF fails to load|
|[#1689](https://github.com/NVIDIA/spark-rapids/pull/1689)|Update docs for plugin 0.5.0-SNAPSHOT and cudf 0.19-SNAPSHOT|
|[#1682](https://github.com/NVIDIA/spark-rapids/pull/1682)|init CI/CD dependencies branch-0.5|

## Release 0.4.1

### Bugs Fixed
|||
|:---|:---|
|[#1985](https://github.com/NVIDIA/spark-rapids/issues/1985)|[BUG] broadcast exchange can fail on 0.4|

### PRs
|||
|:---|:---|
|[#1995](https://github.com/NVIDIA/spark-rapids/pull/1995)|update changelog 0.4.1 [skip ci]|
|[#1990](https://github.com/NVIDIA/spark-rapids/pull/1990)|Prepare for v0.4.1 release|
|[#1988](https://github.com/NVIDIA/spark-rapids/pull/1988)|broadcast exchange can fail when job group set|

## Release 0.4

### Features
|||
|:---|:---|
|[#1773](https://github.com/NVIDIA/spark-rapids/issues/1773)|[FEA] Spark 3.0.2 release support|
|[#80](https://github.com/NVIDIA/spark-rapids/issues/80)|[FEA] Support the struct SQL function|
|[#76](https://github.com/NVIDIA/spark-rapids/issues/76)|[FEA] Support CreateArray|
|[#1635](https://github.com/NVIDIA/spark-rapids/issues/1635)|[FEA] RAPIDS accelerated Java UDF|
|[#1333](https://github.com/NVIDIA/spark-rapids/issues/1333)|[FEA] Support window operations on Decimal|
|[#1419](https://github.com/NVIDIA/spark-rapids/issues/1419)|[FEA] Support GPU accelerated UDF alternative for higher order function "aggregate" over window|
|[#1580](https://github.com/NVIDIA/spark-rapids/issues/1580)|[FEA] Support Decimal for ParquetCachedBatchSerializer|
|[#1600](https://github.com/NVIDIA/spark-rapids/issues/1600)|[FEA] Support ScalarSubquery|
|[#1072](https://github.com/NVIDIA/spark-rapids/issues/1072)|[FEA] Support for a custom DataSource V2 which supplies Arrow data|
|[#906](https://github.com/NVIDIA/spark-rapids/issues/906)|[FEA] Clarify query explanation to directly state what will run on GPU|
|[#1335](https://github.com/NVIDIA/spark-rapids/issues/1335)|[FEA] Support CollectLimitExec for decimal|
|[#1485](https://github.com/NVIDIA/spark-rapids/issues/1485)|[FEA] Decimal Support for Parquet Write|
|[#1329](https://github.com/NVIDIA/spark-rapids/issues/1329)|[FEA] Decimal support for multiply int div, add, subtract and null safe equals|
|[#1351](https://github.com/NVIDIA/spark-rapids/issues/1351)|[FEA] Execute UDFs that provide a RAPIDS execution path|
|[#1330](https://github.com/NVIDIA/spark-rapids/issues/1330)|[FEA] Support Decimal Casts|
|[#1353](https://github.com/NVIDIA/spark-rapids/issues/1353)|[FEA] Example of RAPIDS UDF using custom GPU code|
|[#1487](https://github.com/NVIDIA/spark-rapids/issues/1487)|[FEA] Change spark 3.1.0 to 3.1.1|
|[#1334](https://github.com/NVIDIA/spark-rapids/issues/1334)|[FEA] Add support for count aggregate on decimal|
|[#1325](https://github.com/NVIDIA/spark-rapids/issues/1325)|[FEA] Add in join support for decimal|
|[#1326](https://github.com/NVIDIA/spark-rapids/issues/1326)|[FEA] Add in Broadcast support for decimal values|
|[#37](https://github.com/NVIDIA/spark-rapids/issues/37)|[FEA] round and bround SQL functions|
|[#78](https://github.com/NVIDIA/spark-rapids/issues/78)|[FEA] Support CreateNamedStruct function|
|[#1331](https://github.com/NVIDIA/spark-rapids/issues/1331)|[FEA] UnionExec and ExpandExec support for decimal|
|[#1332](https://github.com/NVIDIA/spark-rapids/issues/1332)|[FEA] Support CaseWhen, Coalesce and IfElse for decimal|
|[#937](https://github.com/NVIDIA/spark-rapids/issues/937)|[FEA] have murmur3 hash function that matches exactly with spark|
|[#1324](https://github.com/NVIDIA/spark-rapids/issues/1324)|[FEA] Support Parquet Read of Decimal FIXED_LENGTH_BYTE_ARRAY|
|[#1428](https://github.com/NVIDIA/spark-rapids/issues/1428)|[FEA] Add support for unary decimal operations abs, floor, ceil, unary - and unary +|
|[#1375](https://github.com/NVIDIA/spark-rapids/issues/1375)|[FEA] Add log statement for what the concurrentGpuTasks tasks is set to on executor startup|
|[#1352](https://github.com/NVIDIA/spark-rapids/issues/1352)|[FEA] Example of RAPIDS UDF using cudf Java APIs|
|[#1328](https://github.com/NVIDIA/spark-rapids/issues/1328)|[FEA] Support sorting and shuffle of decimal|
|[#1316](https://github.com/NVIDIA/spark-rapids/issues/1316)|[FEA] Support simple DECIMAL aggregates|

### Performance
|||
|:---|:---|
|[#1435](https://github.com/NVIDIA/spark-rapids/issues/1435)|[FEA]Improve the file reading by using local file caching|
|[#1738](https://github.com/NVIDIA/spark-rapids/issues/1738)|[FEA] Reduce regex usage in CAST string to date/timestamp|
|[#987](https://github.com/NVIDIA/spark-rapids/issues/987)|[FEA] Optimize CAST from string to temporal types by using cuDF is_timestamp function|
|[#1594](https://github.com/NVIDIA/spark-rapids/issues/1594)|[FEA] RAPIDS accelerated ScalaUDF|
|[#103](https://github.com/NVIDIA/spark-rapids/issues/103)|[FEA] GPU version of TakeOrderedAndProject|
|[#1024](https://github.com/NVIDIA/spark-rapids/issues/1024)|Cleanup RAPIDS transport calls to `receive`|
|[#1366](https://github.com/NVIDIA/spark-rapids/issues/1366)|Seeing performance differences of multi-threaded/coalesce/perfile Parquet reader type for a single file|
|[#1200](https://github.com/NVIDIA/spark-rapids/issues/1200)|[FEA] Accelerate the scan speed for coalescing parquet reader when reading files from multiple partitioned folders|

### Bugs Fixed
|||
|:---|:---|
|[#1885](https://github.com/NVIDIA/spark-rapids/issues/1885)|[BUG] natural join on string key results in a data frame with spurious NULLs|
|[#1785](https://github.com/NVIDIA/spark-rapids/issues/1785)|[BUG] Rapids pytest integration tests FAILED on Yarn cluster with unrecognized arguments: `--std_input_path=src/test/resources/`|
|[#999](https://github.com/NVIDIA/spark-rapids/issues/999)|[BUG] test_multi_types_window_aggs_for_rows_lead_lag fails against Spark 3.1.0|
|[#1818](https://github.com/NVIDIA/spark-rapids/issues/1818)|[BUG] unmoored doc comment warnings in GpuCast|
|[#1817](https://github.com/NVIDIA/spark-rapids/issues/1817)|[BUG] Developer build with local modifications fails during verify phase|
|[#1644](https://github.com/NVIDIA/spark-rapids/issues/1644)|[BUG] test_window_aggregate_udf_array_from_python fails on databricks|
|[#1771](https://github.com/NVIDIA/spark-rapids/issues/1771)|[BUG] Databricks AWS CI/CD failing to create cluster|
|[#1157](https://github.com/NVIDIA/spark-rapids/issues/1157)|[BUG] Fix regression supporting to_date on GPU with Spark 3.1.0|
|[#716](https://github.com/NVIDIA/spark-rapids/issues/716)|[BUG] Cast String to TimeStamp issues|
|[#1117](https://github.com/NVIDIA/spark-rapids/issues/1117)|[BUG] CAST string to date returns wrong values for dates with out-of-range values|
|[#1670](https://github.com/NVIDIA/spark-rapids/issues/1670)|[BUG] Some TPC-DS queries fail with AQE when decimal types enabled|
|[#1730](https://github.com/NVIDIA/spark-rapids/issues/1730)|[BUG] Range Partitioning can crash when processing is in the order-by|
|[#1726](https://github.com/NVIDIA/spark-rapids/issues/1726)|[BUG] java url decode test failing on databricks, emr, and dataproc|
|[#1651](https://github.com/NVIDIA/spark-rapids/issues/1651)|[BUG] GDS exception when writing shuffle file|
|[#1702](https://github.com/NVIDIA/spark-rapids/issues/1702)|[BUG] check all tests marked xfail for Spark 3.1.1|
|[#575](https://github.com/NVIDIA/spark-rapids/issues/575)|[BUG] Spark 3.1 FAILED join_test.py::test_broadcast_join_mixed[FullOuter][IGNORE_ORDER] failed|
|[#577](https://github.com/NVIDIA/spark-rapids/issues/577)|[BUG] Spark 3.1 log arithmetic functions fail|
|[#1541](https://github.com/NVIDIA/spark-rapids/issues/1541)|[BUG] Tests fail in integration in distributed mode after allowing nested types through in sort and shuffle|
|[#1626](https://github.com/NVIDIA/spark-rapids/issues/1626)|[BUG] TPC-DS-like query 77 at scale=3TB fails with maxResultSize exceeded error|
|[#1576](https://github.com/NVIDIA/spark-rapids/issues/1576)|[BUG] loading SPARK-32639 example parquet file triggers a JVM crash |
|[#1643](https://github.com/NVIDIA/spark-rapids/issues/1643)|[BUG] TPC-DS-Like q10, q35, and q69 - slow or hanging at leftSemiJoin|
|[#1650](https://github.com/NVIDIA/spark-rapids/issues/1650)|[BUG] BenchmarkRunner does not include query name in JSON summary filename when running multiple queries|
|[#1654](https://github.com/NVIDIA/spark-rapids/issues/1654)|[BUG] TPC-DS-like query 59 at scale=3TB with AQE fails with join mismatch|
|[#1274](https://github.com/NVIDIA/spark-rapids/issues/1274)|[BUG] OutOfMemoryError - Maximum pool size exceeded while running 24 day criteo ETL Transform stage|
|[#1497](https://github.com/NVIDIA/spark-rapids/issues/1497)|[BUG] Spark-rapids v0.3.0 pytest integration tests with UCX on FAILED on Yarn cluster|
|[#1534](https://github.com/NVIDIA/spark-rapids/issues/1534)|[BUG] Spark 3.1.1 test failure in writing due to removal of InMemoryFileIndex.shouldFilterOut|
|[#1155](https://github.com/NVIDIA/spark-rapids/issues/1155)|[BUG] on shutdown don't print `Socket closed` exception when shutting down UCX.scala|
|[#1510](https://github.com/NVIDIA/spark-rapids/issues/1510)|[BUG] IllegalArgumentException during shuffle|
|[#1513](https://github.com/NVIDIA/spark-rapids/issues/1513)|[BUG] executor not fully initialized may get calls from Spark, in the process setting the `catalog` incorrectly|
|[#1466](https://github.com/NVIDIA/spark-rapids/issues/1466)|[BUG] Databricks build must run before the rapids nightly|
|[#1456](https://github.com/NVIDIA/spark-rapids/issues/1456)|[BUG] Databricks 0.4 parquet integration tests fail|
|[#1400](https://github.com/NVIDIA/spark-rapids/issues/1400)|[BUG] Regressions in spark-shell usage of benchmark utilities|
|[#1119](https://github.com/NVIDIA/spark-rapids/issues/1119)|[BUG] inner join fails with Column size cannot be negative|
|[#1079](https://github.com/NVIDIA/spark-rapids/issues/1079)|[BUG]The Scala UDF function cannot invoke the UDF compiler when it's passed to "explode"|
|[#1298](https://github.com/NVIDIA/spark-rapids/issues/1298)|TPCxBB query16 failed at UnsupportedOperationException: org.apache.parquet.column.values.dictionary.PlainValuesDictionary$PlainIntegerDictionary|
|[#1271](https://github.com/NVIDIA/spark-rapids/issues/1271)|[BUG] CastOpSuite and AnsiCastOpSuite failing with ArithmeticException on Spark 3.1|
|[#84](https://github.com/NVIDIA/spark-rapids/issues/84)|[BUG] sort does not match spark for -0.0 and 0.0|
|[#578](https://github.com/NVIDIA/spark-rapids/issues/578)|[BUG] Spark 3.1 qa_nightly_select_test.py Full join test failures|
|[#586](https://github.com/NVIDIA/spark-rapids/issues/586)|[BUG] Spark3.1 tpch failures|
|[#837](https://github.com/NVIDIA/spark-rapids/issues/837)|[BUG] Distinct count of floating point values differs with regular spark|
|[#953](https://github.com/NVIDIA/spark-rapids/issues/953)|[BUG] 3.1.0 pos_explode tests are failing|
|[#127](https://github.com/NVIDIA/spark-rapids/issues/127)|[BUG] String CSV parsing does not respect nullValues|
|[#1203](https://github.com/NVIDIA/spark-rapids/issues/1203)|[BUG] tpcds query 51 fails with join error on Spark 3.1.0|
|[#750](https://github.com/NVIDIA/spark-rapids/issues/750)|[BUG] udf_cudf_test::test_with_column fails with IPC error |
|[#1348](https://github.com/NVIDIA/spark-rapids/issues/1348)|[BUG] Host columnar decimal conversions are failing|
|[#1270](https://github.com/NVIDIA/spark-rapids/issues/1270)|[BUG] Benchmark runner fails to produce report if benchmark fails due to an invalid query plan|
|[#1179](https://github.com/NVIDIA/spark-rapids/issues/1179)|[BUG] SerializeConcatHostBuffersDeserializeBatch may have thread issues|
|[#1115](https://github.com/NVIDIA/spark-rapids/issues/1115)|[BUG] Unchecked type warning in SparkQueryCompareTestSuite|

### PRs
|||
|:---|:---|
|[#1963](https://github.com/NVIDIA/spark-rapids/pull/1963)|Update changelog 0.4 [skip ci]|
|[#1960](https://github.com/NVIDIA/spark-rapids/pull/1960)|Replace sonatype staging link with maven central link|
|[#1945](https://github.com/NVIDIA/spark-rapids/pull/1945)|Update changelog 0.4 [skip ci]|
|[#1910](https://github.com/NVIDIA/spark-rapids/pull/1910)|Make hash partitioning match CPU|
|[#1927](https://github.com/NVIDIA/spark-rapids/pull/1927)|Change cuDF dependency to 0.18.1|
|[#1934](https://github.com/NVIDIA/spark-rapids/pull/1934)|Update documentation to use cudf version 0.18.1|
|[#1871](https://github.com/NVIDIA/spark-rapids/pull/1871)|Disable coalesce batch spilling to avoid cudf contiguous_split bug|
|[#1849](https://github.com/NVIDIA/spark-rapids/pull/1849)|Update changelog for 0.4|
|[#1744](https://github.com/NVIDIA/spark-rapids/pull/1744)|Fix NullPointerException on null partition insert|
|[#1842](https://github.com/NVIDIA/spark-rapids/pull/1842)|Update to note support for 3.0.2|
|[#1832](https://github.com/NVIDIA/spark-rapids/pull/1832)|Spark 3.1.1 shim no longer a snapshot shim|
|[#1831](https://github.com/NVIDIA/spark-rapids/pull/1831)|Spark 3.0.2 shim no longer a snapshot shim|
|[#1826](https://github.com/NVIDIA/spark-rapids/pull/1826)|Remove benchmarks|
|[#1828](https://github.com/NVIDIA/spark-rapids/pull/1828)|Update cudf dependency to 0.18|
|[#1813](https://github.com/NVIDIA/spark-rapids/pull/1813)|Fix LEAD/LAG failures in Spark 3.1.1|
|[#1819](https://github.com/NVIDIA/spark-rapids/pull/1819)|Fix scaladoc warning in GpuCast|
|[#1820](https://github.com/NVIDIA/spark-rapids/pull/1820)|[BUG] make modified check pre-merge only|
|[#1780](https://github.com/NVIDIA/spark-rapids/pull/1780)|Remove SNAPSHOT from test and integration_test READMEs|
|[#1809](https://github.com/NVIDIA/spark-rapids/pull/1809)|check if modified files after update_config/supported|
|[#1804](https://github.com/NVIDIA/spark-rapids/pull/1804)|Update UCX documentation for RX_QUEUE_LEN and Docker|
|[#1810](https://github.com/NVIDIA/spark-rapids/pull/1810)|Pandas UDF: Sort the data before computing the sum.|
|[#1751](https://github.com/NVIDIA/spark-rapids/pull/1751)|Exclude foldable expressions from GPU if constant folding is disabled|
|[#1798](https://github.com/NVIDIA/spark-rapids/pull/1798)|Add documentation about explain not on GPU when AQE is on|
|[#1766](https://github.com/NVIDIA/spark-rapids/pull/1766)|Branch 0.4 release docs|
|[#1794](https://github.com/NVIDIA/spark-rapids/pull/1794)|Build python output schema from udf expressions|
|[#1783](https://github.com/NVIDIA/spark-rapids/pull/1783)|Fix the collect_list over window tests failures on db|
|[#1781](https://github.com/NVIDIA/spark-rapids/pull/1781)|Better float/double cases for casting tests|
|[#1790](https://github.com/NVIDIA/spark-rapids/pull/1790)|Record row counts in benchmark runs that call collect|
|[#1779](https://github.com/NVIDIA/spark-rapids/pull/1779)|Add support of DateType and TimestampType for GetTimestamp expression|
|[#1768](https://github.com/NVIDIA/spark-rapids/pull/1768)|Updating getting started Databricks docs|
|[#1742](https://github.com/NVIDIA/spark-rapids/pull/1742)|Fix regression supporting to_date with Spark-3.1|
|[#1775](https://github.com/NVIDIA/spark-rapids/pull/1775)|Fix ambiguous ordering for some tests|
|[#1760](https://github.com/NVIDIA/spark-rapids/pull/1760)|Update GpuDataSourceScanExec and GpuBroadcastExchangeExec to fix audit issues|
|[#1750](https://github.com/NVIDIA/spark-rapids/pull/1750)|Detect task failures in benchmarks|
|[#1767](https://github.com/NVIDIA/spark-rapids/pull/1767)|Consistent Spark version for test and production|
|[#1741](https://github.com/NVIDIA/spark-rapids/pull/1741)|Reduce regex use in CAST|
|[#1756](https://github.com/NVIDIA/spark-rapids/pull/1756)|Skip RAPIDS accelerated Java UDF tests if UDF fails to load|
|[#1716](https://github.com/NVIDIA/spark-rapids/pull/1716)|Update RapidsShuffleManager documentation for branch 0.4|
|[#1740](https://github.com/NVIDIA/spark-rapids/pull/1740)|Disable ORC writes until bug can be fixed|
|[#1747](https://github.com/NVIDIA/spark-rapids/pull/1747)|Fix resource leaks in unit tests|
|[#1725](https://github.com/NVIDIA/spark-rapids/pull/1725)|Branch 0.4 FAQ reorg|
|[#1718](https://github.com/NVIDIA/spark-rapids/pull/1718)|CAST string to temporal type now calls isTimestamp|
|[#1734](https://github.com/NVIDIA/spark-rapids/pull/1734)|Disable range partitioning if computation is needed|
|[#1723](https://github.com/NVIDIA/spark-rapids/pull/1723)|Removed StructTypes support for ParquetCachedBatchSerializer as cudf doesn't support it yet|
|[#1714](https://github.com/NVIDIA/spark-rapids/pull/1714)|Add support for RAPIDS accelerated Java UDFs|
|[#1713](https://github.com/NVIDIA/spark-rapids/pull/1713)|Call GpuDeviceManager.shutdown when the executor plugin is shutting down|
|[#1596](https://github.com/NVIDIA/spark-rapids/pull/1596)|Added in Decimal support to ParquetCachedBatchSerializer|
|[#1706](https://github.com/NVIDIA/spark-rapids/pull/1706)|cleanup unused is_before_spark_310|
|[#1685](https://github.com/NVIDIA/spark-rapids/pull/1685)|Fix CustomShuffleReader replacement when decimal types enabled|
|[#1699](https://github.com/NVIDIA/spark-rapids/pull/1699)|Add docs about Spark 3.1 in standalone modes not needing extra class path|
|[#1701](https://github.com/NVIDIA/spark-rapids/pull/1701)|remove xfail for orc test_input_meta for spark 3.1.0|
|[#1703](https://github.com/NVIDIA/spark-rapids/pull/1703)|Remove xfail for spark 3.1.0 test_broadcast_join_mixed FullOuter|
|[#1676](https://github.com/NVIDIA/spark-rapids/pull/1676)|BenchmarkRunner option to generate query plan diagrams in DOT format|
|[#1695](https://github.com/NVIDIA/spark-rapids/pull/1695)|support alternate jar paths|
|[#1694](https://github.com/NVIDIA/spark-rapids/pull/1694)|increase mem and limit parallelism for pre-merge|
|[#1691](https://github.com/NVIDIA/spark-rapids/pull/1691)|add validate_execs_in_gpu_plan to pytest.ini|
|[#1692](https://github.com/NVIDIA/spark-rapids/pull/1692)|Add the integration test resources to the test tarball|
|[#1677](https://github.com/NVIDIA/spark-rapids/pull/1677)|When PTDS is enabled, print warning if the allocator is not ARENA|
|[#1683](https://github.com/NVIDIA/spark-rapids/pull/1683)|update changelog to verify autotmerge 0.5 setup [skip ci]|
|[#1673](https://github.com/NVIDIA/spark-rapids/pull/1673)|support auto-merge for branch 0.5 [skip ci]|
|[#1681](https://github.com/NVIDIA/spark-rapids/pull/1681)|Xfail the collect_list tests for databricks|
|[#1678](https://github.com/NVIDIA/spark-rapids/pull/1678)|Fix array/struct checks in Sort and HashAggregate and sorting tests in distributed mode|
|[#1671](https://github.com/NVIDIA/spark-rapids/pull/1671)|Allow metrics to be configurable by level|
|[#1675](https://github.com/NVIDIA/spark-rapids/pull/1675)|add run_pyspark_from_build.sh to the pytest distribution tarball|
|[#1548](https://github.com/NVIDIA/spark-rapids/pull/1548)|Support executing collect_list on GPU with windowing.|
|[#1593](https://github.com/NVIDIA/spark-rapids/pull/1593)|Avoid unnecessary Table instances after contiguous split|
|[#1592](https://github.com/NVIDIA/spark-rapids/pull/1592)|Add in support for Decimal divide|
|[#1668](https://github.com/NVIDIA/spark-rapids/pull/1668)|Implement way for python integration tests to validate Exec is in GPU plan|
|[#1669](https://github.com/NVIDIA/spark-rapids/pull/1669)|Add FAQ entries for executor-per-GPU questions|
|[#1661](https://github.com/NVIDIA/spark-rapids/pull/1661)|Enable Parquet test for file containing map struct key|
|[#1664](https://github.com/NVIDIA/spark-rapids/pull/1664)|Filter nulls for left semi and left anti join to work around cudf|
|[#1665](https://github.com/NVIDIA/spark-rapids/pull/1665)|Add better automated tests for Arrow columnar copy in HostColumnarToGpu|
|[#1614](https://github.com/NVIDIA/spark-rapids/pull/1614)|add alluxio getting start document|
|[#1639](https://github.com/NVIDIA/spark-rapids/pull/1639)|support GpuScalarSubquery|
|[#1656](https://github.com/NVIDIA/spark-rapids/pull/1656)|Move UDF to Catalyst Expressions to its own document|
|[#1663](https://github.com/NVIDIA/spark-rapids/pull/1663)|BenchmarkRunner - Include query name in JSON summary filename|
|[#1655](https://github.com/NVIDIA/spark-rapids/pull/1655)|Fix extraneous shuffles added by AQE|
|[#1652](https://github.com/NVIDIA/spark-rapids/pull/1652)|Fix typo in arrow optimized config name - spark.rapids.arrowCopyOptimizationEnabled|
|[#1645](https://github.com/NVIDIA/spark-rapids/pull/1645)|Run Databricks IT with python-xdist parallel, includes test fixes and xfail|
|[#1649](https://github.com/NVIDIA/spark-rapids/pull/1649)|Move building from source docs to contributing guide|
|[#1637](https://github.com/NVIDIA/spark-rapids/pull/1637)|Fail DivModLike on zero divisor in ANSI mode|
|[#1646](https://github.com/NVIDIA/spark-rapids/pull/1646)|Update links in rapids-udfs.md after moving to subfolder|
|[#1641](https://github.com/NVIDIA/spark-rapids/pull/1641)|Xfail struct and array order by tests on Dataproc|
|[#1565](https://github.com/NVIDIA/spark-rapids/pull/1565)|Add GPU accelerated array_contains operator|
|[#1617](https://github.com/NVIDIA/spark-rapids/pull/1617)|Enable nightly test checks for Apache Spark|
|[#1636](https://github.com/NVIDIA/spark-rapids/pull/1636)|RAPIDS accelerated Spark Scala UDF support|
|[#1634](https://github.com/NVIDIA/spark-rapids/pull/1634)|Fix databricks build since Arrow code added|
|[#1599](https://github.com/NVIDIA/spark-rapids/pull/1599)|Add division by zero tests for Spark 3.1 behavior|
|[#1619](https://github.com/NVIDIA/spark-rapids/pull/1619)|Update GpuFileSourceScanExec to be in sync with DataSourceScanExec|
|[#1631](https://github.com/NVIDIA/spark-rapids/pull/1631)|Explicitly add maven-jar-plugin version to improve incremental build time.|
|[#1624](https://github.com/NVIDIA/spark-rapids/pull/1624)|Update explain format to show what will and will not run on the GPU|
|[#1622](https://github.com/NVIDIA/spark-rapids/pull/1622)|Support faster copy for a custom DataSource V2 which supplies Arrow data|
|[#1621](https://github.com/NVIDIA/spark-rapids/pull/1621)|Additional functionality docs|
|[#1618](https://github.com/NVIDIA/spark-rapids/pull/1618)|update blossom-ci for security updates [skip ci]|
|[#1562](https://github.com/NVIDIA/spark-rapids/pull/1562)|add alluxio support|
|[#1597](https://github.com/NVIDIA/spark-rapids/pull/1597)|Documentation for Parquet serializer|
|[#1611](https://github.com/NVIDIA/spark-rapids/pull/1611)|Add in flag for integration tests to not skip required tests|
|[#1609](https://github.com/NVIDIA/spark-rapids/pull/1609)|Disable float round/bround by default|
|[#1615](https://github.com/NVIDIA/spark-rapids/pull/1615)|Add in window support for average|
|[#1610](https://github.com/NVIDIA/spark-rapids/pull/1610)|Limit length of spark app name in BenchmarkRunner|
|[#1579](https://github.com/NVIDIA/spark-rapids/pull/1579)|Support TakeOrderedAndProject|
|[#1581](https://github.com/NVIDIA/spark-rapids/pull/1581)|Support Decimal type for CollectLimitExec|
|[#1591](https://github.com/NVIDIA/spark-rapids/pull/1591)|Add support for running multiple queries in BenchmarkRunner|
|[#1595](https://github.com/NVIDIA/spark-rapids/pull/1595)|Fix Github documentation issue template|
|[#1577](https://github.com/NVIDIA/spark-rapids/pull/1577)|rename directory from spark310 to spark311|
|[#1578](https://github.com/NVIDIA/spark-rapids/pull/1578)|Test to track RAPIDS-side issues re SPARK-32639|
|[#1583](https://github.com/NVIDIA/spark-rapids/pull/1583)|fix request-action issue [skip ci]|
|[#1555](https://github.com/NVIDIA/spark-rapids/pull/1555)|Enable ANSI mode for CAST string to timestamp|
|[#1531](https://github.com/NVIDIA/spark-rapids/pull/1531)|Decimal Support for writing Parquet|
|[#1545](https://github.com/NVIDIA/spark-rapids/pull/1545)|Support comparing ORC data|
|[#1570](https://github.com/NVIDIA/spark-rapids/pull/1570)|Branch 0.4 doc cleanup|
|[#1569](https://github.com/NVIDIA/spark-rapids/pull/1569)|Add shim method shouldIgnorePath|
|[#1564](https://github.com/NVIDIA/spark-rapids/pull/1564)|Add in support for Decimal Multiply and DIV|
|[#1561](https://github.com/NVIDIA/spark-rapids/pull/1561)|Decimal support for add and subtract|
|[#1560](https://github.com/NVIDIA/spark-rapids/pull/1560)|support sum in window aggregation for decimal|
|[#1546](https://github.com/NVIDIA/spark-rapids/pull/1546)|Cleanup shutdown logging for UCX shuffle|
|[#1551](https://github.com/NVIDIA/spark-rapids/pull/1551)|RAPIDS-accelerated Hive UDFs support all types|
|[#1543](https://github.com/NVIDIA/spark-rapids/pull/1543)|Shuffle/transport enabled by default|
|[#1552](https://github.com/NVIDIA/spark-rapids/pull/1552)|Disable blackduck signature check|
|[#1540](https://github.com/NVIDIA/spark-rapids/pull/1540)|Handle ShuffleManager api calls when plugin is not fully initialized|
|[#1547](https://github.com/NVIDIA/spark-rapids/pull/1547)|Cleanup shuffle transport receive calls|
|[#1512](https://github.com/NVIDIA/spark-rapids/pull/1512)|Support window operations on Decimal|
|[#1532](https://github.com/NVIDIA/spark-rapids/pull/1532)|Support casting from decimal to decimal|
|[#1542](https://github.com/NVIDIA/spark-rapids/pull/1542)|Change the number of partitions to zero when a range is empty|
|[#1506](https://github.com/NVIDIA/spark-rapids/pull/1506)|Add --use-decimals flag to TPC-DS ConvertFiles|
|[#1511](https://github.com/NVIDIA/spark-rapids/pull/1511)|Remove unused Jenkinsfiles [skip ci]|
|[#1505](https://github.com/NVIDIA/spark-rapids/pull/1505)|Add least, greatest and eqNullSafe support for DecimalType|
|[#1484](https://github.com/NVIDIA/spark-rapids/pull/1484)|add doc for nsight systems bundled with cuda toolkit|
|[#1478](https://github.com/NVIDIA/spark-rapids/pull/1478)|Documentation for RAPIDS-accelerated Hive UDFs|
|[#1477](https://github.com/NVIDIA/spark-rapids/pull/1477)|Allow structs and arrays to pass through for Shuffle and Sort |
|[#1489](https://github.com/NVIDIA/spark-rapids/pull/1489)|Adds in some support for the array sql function|
|[#1438](https://github.com/NVIDIA/spark-rapids/pull/1438)|Cast from numeric types to decimal type|
|[#1493](https://github.com/NVIDIA/spark-rapids/pull/1493)|Moved ParquetRecordMaterializer to the shim package to follow convention|
|[#1495](https://github.com/NVIDIA/spark-rapids/pull/1495)|Fix merge conflict, merge branch 0.3 to branch 0.4 [skip ci]|
|[#1472](https://github.com/NVIDIA/spark-rapids/pull/1472)|Add an example RAPIDS-accelerated Hive UDF using native code|
|[#1488](https://github.com/NVIDIA/spark-rapids/pull/1488)|Rename Spark 3.1.0 shim to Spark 3.1.1 to match community|
|[#1474](https://github.com/NVIDIA/spark-rapids/pull/1474)|Fix link|
|[#1476](https://github.com/NVIDIA/spark-rapids/pull/1476)|DecimalType support for Aggregate Count|
|[#1475](https://github.com/NVIDIA/spark-rapids/pull/1475)| Join support for DecimalType|
|[#1244](https://github.com/NVIDIA/spark-rapids/pull/1244)|Support round and bround SQL functions |
|[#1458](https://github.com/NVIDIA/spark-rapids/pull/1458)|Add in support for struct and named_struct|
|[#1465](https://github.com/NVIDIA/spark-rapids/pull/1465)|DecimalType support for UnionExec and ExpandExec|
|[#1450](https://github.com/NVIDIA/spark-rapids/pull/1450)|Add dynamic configs for the spark-rapids IT pipelines|
|[#1207](https://github.com/NVIDIA/spark-rapids/pull/1207)|Spark SQL hash function using murmur3|
|[#1457](https://github.com/NVIDIA/spark-rapids/pull/1457)|Support reading decimal columns from parquet files on Databricks|
|[#1455](https://github.com/NVIDIA/spark-rapids/pull/1455)|Upgrade Scala Maven Plugin to 4.3.0|
|[#1453](https://github.com/NVIDIA/spark-rapids/pull/1453)|DecimalType support for IfElse and Coalesce|
|[#1452](https://github.com/NVIDIA/spark-rapids/pull/1452)|Support DecimalType for CaseWhen|
|[#1444](https://github.com/NVIDIA/spark-rapids/pull/1444)|Improve UX when running benchmarks from Spark shell|
|[#1294](https://github.com/NVIDIA/spark-rapids/pull/1294)|Support reading decimal columns from parquet files|
|[#1153](https://github.com/NVIDIA/spark-rapids/pull/1153)|Scala UDF will compile children expressions in Project|
|[#1416](https://github.com/NVIDIA/spark-rapids/pull/1416)|Optimize mvn dependency download scripts|
|[#1430](https://github.com/NVIDIA/spark-rapids/pull/1430)|Add project for testing code that requires Spark 3.1.0 or later|
|[#1425](https://github.com/NVIDIA/spark-rapids/pull/1425)|Add in Decimal support for abs, floor, ceil, unary - and unary +|
|[#1427](https://github.com/NVIDIA/spark-rapids/pull/1427)|Revert "Make the multi-threaded parquet reader the default"|
|[#1420](https://github.com/NVIDIA/spark-rapids/pull/1420)|Add udf jar to nightly integration tests|
|[#1422](https://github.com/NVIDIA/spark-rapids/pull/1422)|Log the number of concurrent gpu tasks allowed on Executor startup|
|[#1401](https://github.com/NVIDIA/spark-rapids/pull/1401)|Accelerate the coalescing parquet reader when reading files from multiple partitioned folders|
|[#1413](https://github.com/NVIDIA/spark-rapids/pull/1413)|Add config for cast float to integral types|
|[#1313](https://github.com/NVIDIA/spark-rapids/pull/1313)|Support spilling to disk directly via cuFile/GDS|
|[#1411](https://github.com/NVIDIA/spark-rapids/pull/1411)|Add udf-examples jar to databricks build|
|[#1412](https://github.com/NVIDIA/spark-rapids/pull/1412)|Fix a lot of tests marked with xfail for Spark 3.1.0 that no longer fail|
|[#1414](https://github.com/NVIDIA/spark-rapids/pull/1414)|Build merged code of HEAD and BASE branch for pre-merge [skip ci]|
|[#1409](https://github.com/NVIDIA/spark-rapids/pull/1409)|Add option to use decimals in tpc-ds csv to parquet conversion|
|[#1410](https://github.com/NVIDIA/spark-rapids/pull/1410)|Add Decimal support for In, InSet, AtLeastNNonNulls, GetArrayItem, GetStructField, and GenerateExec|
|[#1408](https://github.com/NVIDIA/spark-rapids/pull/1408)|Support RAPIDS-accelerated HiveGenericUDF|
|[#1407](https://github.com/NVIDIA/spark-rapids/pull/1407)|Update docs and tests for null CSV support|
|[#1393](https://github.com/NVIDIA/spark-rapids/pull/1393)|Support RAPIDS-accelerated HiveSimpleUDF|
|[#1392](https://github.com/NVIDIA/spark-rapids/pull/1392)|Turn on hash partitioning for decimal support|
|[#1402](https://github.com/NVIDIA/spark-rapids/pull/1402)|Better GPU Cast type checks|
|[#1404](https://github.com/NVIDIA/spark-rapids/pull/1404)|Fix branch 0.4 merge conflict|
|[#1323](https://github.com/NVIDIA/spark-rapids/pull/1323)|More advanced type checking and documentation|
|[#1391](https://github.com/NVIDIA/spark-rapids/pull/1391)|Remove extra null join filtering because cudf is fast for this now.|
|[#1395](https://github.com/NVIDIA/spark-rapids/pull/1395)|Fix branch-0.3 -> branch-0.4 automerge|
|[#1382](https://github.com/NVIDIA/spark-rapids/pull/1382)|Handle "MM[/-]dd" and "dd[/-]MM" datetime formats in UnixTimeExprMeta|
|[#1390](https://github.com/NVIDIA/spark-rapids/pull/1390)|Accelerated columnar to row/row to columnar for decimal|
|[#1380](https://github.com/NVIDIA/spark-rapids/pull/1380)|Adds in basic support for decimal sort, sum, and some shuffle|
|[#1367](https://github.com/NVIDIA/spark-rapids/pull/1367)|Reuse gpu expression conversion rules when checking sort order|
|[#1349](https://github.com/NVIDIA/spark-rapids/pull/1349)|Add canonicalization tests|
|[#1368](https://github.com/NVIDIA/spark-rapids/pull/1368)|Move to cudf 0.18-SNAPSHOT|
|[#1361](https://github.com/NVIDIA/spark-rapids/pull/1361)|Use the correct precision when reading spark columnar data.|
|[#1273](https://github.com/NVIDIA/spark-rapids/pull/1273)|Update docs and scripts to 0.4.0-SNAPSHOT|
|[#1321](https://github.com/NVIDIA/spark-rapids/pull/1321)|Refactor to stop inheriting from HashJoin|
|[#1311](https://github.com/NVIDIA/spark-rapids/pull/1311)|ParquetCachedBatchSerializer code cleanup|
|[#1303](https://github.com/NVIDIA/spark-rapids/pull/1303)|Add explicit outputOrdering for BHJ and SHJ in spark310 shim|
|[#1299](https://github.com/NVIDIA/spark-rapids/pull/1299)|Benchmark runner improved error handling|

## Release 0.3

### Features
|||
|:---|:---|
|[#1002](https://github.com/NVIDIA/spark-rapids/issues/1002)|[FEA] RapidsHostColumnVectorCore should verify cudf data with respect to the expected spark type |
|[#444](https://github.com/NVIDIA/spark-rapids/issues/444)|[FEA] Plugable Cache|
|[#1158](https://github.com/NVIDIA/spark-rapids/issues/1158)|[FEA] Better documentation on type support|
|[#57](https://github.com/NVIDIA/spark-rapids/issues/57)|[FEA] Support INT96 for parquet reads and writes|
|[#1003](https://github.com/NVIDIA/spark-rapids/issues/1003)|[FEA] Reduce overlap between RapidsHostColumnVector and RapidsHostColumnVectorCore|
|[#913](https://github.com/NVIDIA/spark-rapids/issues/913)|[FEA] In Pluggable Cache Support CalendarInterval while creating CachedBatches|
|[#1092](https://github.com/NVIDIA/spark-rapids/issues/1092)|[FEA] In Pluggable Cache handle nested types having CalendarIntervalType and NullType|
|[#670](https://github.com/NVIDIA/spark-rapids/issues/670)|[FEA] Support NullType|
|[#50](https://github.com/NVIDIA/spark-rapids/issues/50)|[FEA] support `spark.sql.legacy.timeParserPolicy`|
|[#1144](https://github.com/NVIDIA/spark-rapids/issues/1144)|[FEA] Remove Databricks 3.0.0 shim layer|
|[#1096](https://github.com/NVIDIA/spark-rapids/issues/1096)|[FEA] Implement parquet CreateDataSourceTableAsSelectCommand|
|[#688](https://github.com/NVIDIA/spark-rapids/issues/688)|[FEA] udf compiler should be auto-appended to `spark.sql.extensions`|
|[#502](https://github.com/NVIDIA/spark-rapids/issues/502)|[FEA] Support Databricks 7.3 LTS Runtime|
|[#764](https://github.com/NVIDIA/spark-rapids/issues/764)|[FEA] Sanity checks for cudf jar mismatch|
|[#1018](https://github.com/NVIDIA/spark-rapids/issues/1018)|[FEA] Log details related to GPU memory fragmentation on GPU OOM|
|[#619](https://github.com/NVIDIA/spark-rapids/issues/619)|[FEA] log whether libcudf and libcudfjni were built for PTDS|
|[#905](https://github.com/NVIDIA/spark-rapids/issues/905)|[FEA] create AWS EMR 3.0.1 shim|
|[#838](https://github.com/NVIDIA/spark-rapids/issues/838)|[FEA] Support window count for a column|
|[#864](https://github.com/NVIDIA/spark-rapids/issues/864)|[FEA] config option to enable RMM arena memory resource|
|[#430](https://github.com/NVIDIA/spark-rapids/issues/430)|[FEA] Audit: Parquet Writer support for TIMESTAMP_MILLIS|
|[#818](https://github.com/NVIDIA/spark-rapids/issues/818)|[FEA] Create shim layer for AWS EMR |
|[#608](https://github.com/NVIDIA/spark-rapids/issues/608)|[FEA] Parquet small file optimization improve handle merge schema|

### Performance
|||
|:---|:---|
|[#446](https://github.com/NVIDIA/spark-rapids/issues/446)|[FEA] Test jucx in 1.9.x branch|
|[#1038](https://github.com/NVIDIA/spark-rapids/issues/1038)|[FEA] Accelerate the data transfer for plan `WindowInPandasExec`|
|[#533](https://github.com/NVIDIA/spark-rapids/issues/533)|[FEA] Improve PTDS performance|
|[#849](https://github.com/NVIDIA/spark-rapids/issues/849)|[FEA] Have GpuColumnarBatchSerializer return GpuColumnVectorFromBuffer instances|
|[#784](https://github.com/NVIDIA/spark-rapids/issues/784)|[FEA] Allow Host Spilling to be more dynamic|
|[#627](https://github.com/NVIDIA/spark-rapids/issues/627)|[FEA] Further parquet reading small file improvements|
|[#5](https://github.com/NVIDIA/spark-rapids/issues/5)|[FEA] Support Adaptive Execution|

### Bugs Fixed
|||
|:---|:---|
|[#1423](https://github.com/NVIDIA/spark-rapids/issues/1423)|[BUG] Mortgage ETL sample failed with spark.sql.adaptive enabled on AWS EMR 6.2 |
|[#1369](https://github.com/NVIDIA/spark-rapids/issues/1369)|[BUG] TPC-DS Query Failing on EMR 6.2 with AQE|
|[#1344](https://github.com/NVIDIA/spark-rapids/issues/1344)|[BUG] Spark-rapids Pytests failed on On Databricks cluster spark standalone mode|
|[#1279](https://github.com/NVIDIA/spark-rapids/issues/1279)|[BUG] TPC-DS query 2 failing with NPE|
|[#1280](https://github.com/NVIDIA/spark-rapids/issues/1280)|[BUG] TPC-DS query 93 failing with UnsupportedOperationException|
|[#1308](https://github.com/NVIDIA/spark-rapids/issues/1308)|[BUG] TPC-DS query 14a runs much slower on 0.3|
|[#1284](https://github.com/NVIDIA/spark-rapids/issues/1284)|[BUG] TPC-DS query 77 at scale=1TB fails with maxResultSize exceeded error|
|[#1061](https://github.com/NVIDIA/spark-rapids/issues/1061)|[BUG] orc_test.py is failing|
|[#1197](https://github.com/NVIDIA/spark-rapids/issues/1197)|[BUG] java.lang.NullPointerException when exporting delta table|
|[#685](https://github.com/NVIDIA/spark-rapids/issues/685)|[BUG] In ParqueCachedBatchSerializer, serializing parquet buffers might blow up in certain cases|
|[#1269](https://github.com/NVIDIA/spark-rapids/issues/1269)|[BUG] GpuSubstring is not expected to be a part of a SortOrder|
|[#1246](https://github.com/NVIDIA/spark-rapids/issues/1246)|[BUG] Many TPC-DS benchmarks fail when writing to Parquet|
|[#961](https://github.com/NVIDIA/spark-rapids/issues/961)|[BUG] ORC predicate pushdown should work with case-insensitive analysis|
|[#962](https://github.com/NVIDIA/spark-rapids/issues/962)|[BUG] Loading columns from an ORC file without column names returns no data|
|[#1245](https://github.com/NVIDIA/spark-rapids/issues/1245)|[BUG] Code adding buffers to the spillable store should synchronize|
|[#570](https://github.com/NVIDIA/spark-rapids/issues/570)|[BUG] Continue debugging OOM after ensuring device store is empty|
|[#972](https://github.com/NVIDIA/spark-rapids/issues/972)|[BUG] total time metric is redundant with scan time|
|[#1039](https://github.com/NVIDIA/spark-rapids/issues/1039)|[BUG] UNBOUNDED window ranges on null timestamp columns produces incorrect results.|
|[#1195](https://github.com/NVIDIA/spark-rapids/issues/1195)|[BUG] AcceleratedColumnarToRowIterator queue empty|
|[#1177](https://github.com/NVIDIA/spark-rapids/issues/1177)|[BUG] leaks possible in the rapids shuffle if batches are received after the task completes|
|[#1216](https://github.com/NVIDIA/spark-rapids/issues/1216)|[BUG] Failure to recognize ORC file format when loaded via Hive|
|[#898](https://github.com/NVIDIA/spark-rapids/issues/898)|[BUG] count reductions are failing on databricks because lack for Complete support|
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
|[#746](https://github.com/NVIDIA/spark-rapids/issues/746)|[BUG] cudf_udf_test.py is flakey|
|[#811](https://github.com/NVIDIA/spark-rapids/issues/811)|[BUG] 0.3 nightly is timing out |
|[#574](https://github.com/NVIDIA/spark-rapids/issues/574)|[BUG] Fix GpuTimeSub for Spark 3.1.0|

### PRs
|||
|:---|:---|
|[#1496](https://github.com/NVIDIA/spark-rapids/pull/1496)|Update changelog for v0.3.0 release [skip ci]|
|[#1473](https://github.com/NVIDIA/spark-rapids/pull/1473)|Update documentation for 0.3 release|
|[#1371](https://github.com/NVIDIA/spark-rapids/pull/1371)|Start Guide for RAPIDS on AWS EMR 6.2|
|[#1446](https://github.com/NVIDIA/spark-rapids/pull/1446)|Update changelog for 0.3.0 release [skip ci]|
|[#1439](https://github.com/NVIDIA/spark-rapids/pull/1439)|when AQE enabled we fail to fix up exchanges properly and EMR|
|[#1433](https://github.com/NVIDIA/spark-rapids/pull/1433)|fix pandas 1.2 compatible issue|
|[#1424](https://github.com/NVIDIA/spark-rapids/pull/1424)|Make the multi-threaded parquet reader the default since coalescing doesn't handle partitioned files well|
|[#1389](https://github.com/NVIDIA/spark-rapids/pull/1389)|Update project version to 0.3.0|
|[#1387](https://github.com/NVIDIA/spark-rapids/pull/1387)|Update cudf version to 0.17|
|[#1370](https://github.com/NVIDIA/spark-rapids/pull/1370)|[REVIEW] init changelog 0.3 [skip ci]|
|[#1376](https://github.com/NVIDIA/spark-rapids/pull/1376)|MetaUtils.getBatchFromMeta should return batches with GpuColumnVectorFromBuffer|
|[#1358](https://github.com/NVIDIA/spark-rapids/pull/1358)|auto-merge: instant merge after creation [skip ci]|
|[#1359](https://github.com/NVIDIA/spark-rapids/pull/1359)|Use SortOrder from shims.|
|[#1343](https://github.com/NVIDIA/spark-rapids/pull/1343)|Do not run UDFs when the partition is empty.|
|[#1342](https://github.com/NVIDIA/spark-rapids/pull/1342)|Fix and edit docs for standalone mode|
|[#1350](https://github.com/NVIDIA/spark-rapids/pull/1350)|fix GpuRangePartitioning canonicalization|
|[#1281](https://github.com/NVIDIA/spark-rapids/pull/1281)|Documentation added for testing|
|[#1336](https://github.com/NVIDIA/spark-rapids/pull/1336)|Fix missing post-shuffle coalesce with AQE|
|[#1318](https://github.com/NVIDIA/spark-rapids/pull/1318)|Fix copying GpuFileSourceScanExec node|
|[#1337](https://github.com/NVIDIA/spark-rapids/pull/1337)|Use UTC instead of GMT|
|[#1307](https://github.com/NVIDIA/spark-rapids/pull/1307)|Fallback to cpu when reading Delta log files for stats|
|[#1310](https://github.com/NVIDIA/spark-rapids/pull/1310)|Fix canonicalization of GpuFileSourceScanExec, GpuShuffleCoalesceExec|
|[#1302](https://github.com/NVIDIA/spark-rapids/pull/1302)|Add GpuSubstring handling to SortOrder canonicalization|
|[#1265](https://github.com/NVIDIA/spark-rapids/pull/1265)|Chunking input before writing a ParquetCachedBatch|
|[#1278](https://github.com/NVIDIA/spark-rapids/pull/1278)|Add a config to disable decimal types by default|
|[#1272](https://github.com/NVIDIA/spark-rapids/pull/1272)|Add Alias to shims|
|[#1268](https://github.com/NVIDIA/spark-rapids/pull/1268)|Adds in support docs for 0.3 release|
|[#1235](https://github.com/NVIDIA/spark-rapids/pull/1235)|Trigger reading and handling control data.|
|[#1266](https://github.com/NVIDIA/spark-rapids/pull/1266)|Updating Databricks getting started for 0.3 release|
|[#1291](https://github.com/NVIDIA/spark-rapids/pull/1291)|Increase pre-merge resource requests [skip ci]|
|[#1275](https://github.com/NVIDIA/spark-rapids/pull/1275)|Temporarily disable more CAST tests for Spark 3.1.0|
|[#1264](https://github.com/NVIDIA/spark-rapids/pull/1264)|Fix race condition in batch creation|
|[#1260](https://github.com/NVIDIA/spark-rapids/pull/1260)|Update UCX license info in NOTIFY-binary for 1.9 and RAPIDS plugin copyright dates|
|[#1247](https://github.com/NVIDIA/spark-rapids/pull/1247)|Ensure column names are valid when writing benchmark query results to file|
|[#1240](https://github.com/NVIDIA/spark-rapids/pull/1240)|Fix loading from ORC file with no column names|
|[#1242](https://github.com/NVIDIA/spark-rapids/pull/1242)|Remove compatibility documentation about unsupported INT96|
|[#1192](https://github.com/NVIDIA/spark-rapids/pull/1192)|[REVIEW]  Support GpuFilter and GpuCoalesceBatches for decimal data|
|[#1170](https://github.com/NVIDIA/spark-rapids/pull/1170)|Add nested type support to MetaUtils|
|[#1194](https://github.com/NVIDIA/spark-rapids/pull/1194)|Drop redundant total time metric from scan|
|[#1248](https://github.com/NVIDIA/spark-rapids/pull/1248)|At BatchedTableCompressor.finish synchronize to allow for "right-size…|
|[#1169](https://github.com/NVIDIA/spark-rapids/pull/1169)|Use CUDF's "UNBOUNDED" window boundaries for time-range queries.|
|[#1204](https://github.com/NVIDIA/spark-rapids/pull/1204)|Avoid empty batches on columnar to row conversion|
|[#1133](https://github.com/NVIDIA/spark-rapids/pull/1133)|Refactor batch coalesce to be based solely on batch data size|
|[#1237](https://github.com/NVIDIA/spark-rapids/pull/1237)|In transport, limit pending transfer requests to fit within a bounce|
|[#1232](https://github.com/NVIDIA/spark-rapids/pull/1232)|Move SortOrder creation to shims|
|[#1068](https://github.com/NVIDIA/spark-rapids/pull/1068)|Write int96 to parquet|
|[#1193](https://github.com/NVIDIA/spark-rapids/pull/1193)|Verify shuffle of decimal columns|
|[#1180](https://github.com/NVIDIA/spark-rapids/pull/1180)|Remove batches if they are received after the iterator detects that t…|
|[#1173](https://github.com/NVIDIA/spark-rapids/pull/1173)|Support relational operators for decimal type|
|[#1220](https://github.com/NVIDIA/spark-rapids/pull/1220)|Support replacing ORC format when Hive is configured|
|[#1219](https://github.com/NVIDIA/spark-rapids/pull/1219)|Upgrade to jucx 1.9.0|
|[#1081](https://github.com/NVIDIA/spark-rapids/pull/1081)|Add option to upload benchmark summary JSON file|
|[#1217](https://github.com/NVIDIA/spark-rapids/pull/1217)|Aggregate reductions in Complete mode should use updateExpressions|
|[#1218](https://github.com/NVIDIA/spark-rapids/pull/1218)|Remove obsolete HiveStringType usage|
|[#1214](https://github.com/NVIDIA/spark-rapids/pull/1214)|changelog update 2020-11-30. Trigger automerge check [skip ci]|
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
|[#1150](https://github.com/NVIDIA/spark-rapids/pull/1150)|udf spec|
|[#1188](https://github.com/NVIDIA/spark-rapids/pull/1188)|Add in tests for parquet nested pruning support|
|[#1189](https://github.com/NVIDIA/spark-rapids/pull/1189)|Enable NullType for First and Last in 3.0.1+|
|[#1181](https://github.com/NVIDIA/spark-rapids/pull/1181)|Fix resource leaks in unit tests|
|[#1186](https://github.com/NVIDIA/spark-rapids/pull/1186)|Fix compilation and scaladoc warnings|
|[#1187](https://github.com/NVIDIA/spark-rapids/pull/1187)|Updated documentation for distinct count compatibility|
|[#1182](https://github.com/NVIDIA/spark-rapids/pull/1182)|Close buffer catalog on device manager shutdown|
|[#1137](https://github.com/NVIDIA/spark-rapids/pull/1137)|Let GpuWindowInPandas declare ArrayType supported.|
|[#1176](https://github.com/NVIDIA/spark-rapids/pull/1176)|Add in support for null type|
|[#1174](https://github.com/NVIDIA/spark-rapids/pull/1174)|Fix race condition in SerializeConcatHostBuffersDeserializeBatch|
|[#1175](https://github.com/NVIDIA/spark-rapids/pull/1175)|Fix leaks seen in shuffle tests|
|[#1138](https://github.com/NVIDIA/spark-rapids/pull/1138)|[REVIEW] Support decimal type for GpuProjectExec|
|[#1162](https://github.com/NVIDIA/spark-rapids/pull/1162)|Set job descriptions in benchmark runner|
|[#1172](https://github.com/NVIDIA/spark-rapids/pull/1172)|Revert "Fix race condition (#1165)"|
|[#1060](https://github.com/NVIDIA/spark-rapids/pull/1060)|Show partition metrics for custom shuffler reader|
|[#1152](https://github.com/NVIDIA/spark-rapids/pull/1152)|Add spark301db shim layer for WindowInPandas.|
|[#1167](https://github.com/NVIDIA/spark-rapids/pull/1167)|Nulls out the dataframe if --gc-between-runs is set|
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
|[#807](https://github.com/NVIDIA/spark-rapids/pull/807)|Deploy integration-tests javadoc and sources|
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
