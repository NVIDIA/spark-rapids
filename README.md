# Caerus Spark UDF Compiler: Modified from "RAPIDS Accelerator For Apache Spark"

Currently Spark doesn't support any UDF pushdown with the exception of JDBC/database datascource case, and Spark UDF is run in a black box on compute-sdie, and Spark Catalyst, Spark SQL Optimizer, can't optimize UDF.

[RAPIDS Accelerator For Apache Spark](https://github.com/NVIDIA/spark-rapids) is a Nvidia open source project to provide a set of plugins for Apache Spark that leverage GPUs to accelerate processing via the RAPIDS libraries and UCX. 

Among these plugins, ["udf-compiler"](https://github.com/NVIDIA/spark-rapids/tree/branch-21.10/udf-compiler) is a UDF compiler extension (via Spark rule injection) to translate UDFs bytecode to Spark Catalyst expressions.

The "udf-compiler" is similar to the [Spark SQL Macros](https://github.com/hbutani/spark-sql-macros) project we previously investigate, they all attempt to translate Spark UDFs into native Spark Catalyst expressions, which will be optimized by the Spark Catalysts for code generation/serialization, so that the UDFs can be pushed down as the best as we can to the data sources (thus to storage). The task time of such solutions is [2-3 times faster than the native Spark UDFs](https://github.com/hbutani/spark-sql-macros)

Under the hood, the "udf-compiler" uses bytecode analyzer to translate, while the Macros use Scala metaprogramming mechanism to translate. The bytecode translation is easier to debug.

Compare to Spark SQL Macros project we previously investigated, "udf-compiler" has the following advantages:
- It is a fully automated solution that can translate spark UDFs without the need to change existing Spark application code
- It doesn't have the restriction on UDF registration: 
  - The Macros solution doesn't support UDF pushdwon if UDF is defined as a variable, such UDF definition (without register call) is often used in dataframe APIs  
  - The Macros solution needs all functions are defined in the UDM function body 

The feature set of the "udf-compiler" solution is still less than the Macros solution, but the "udf-compiler" is still being actively developed, the feature gaps might be filled in the future. 

The feature gap examples of the "udf-compiler" solution are listed as follows:
- It doesn't support tuple, map and collections
- It has less DateTime support than the Macros solution: monthsBetween, getDayInYear, getDayOfWeek etc.
- It doesn't support complex UDfs like recursive UDFs

The full supported features comparison can be found in the following documents:
- [udf-compiler](https://github.com/NVIDIA/spark-rapids/blob/branch-21.10/docs/additional-functionality/udf-to-catalyst-expressions.md)
- [Spark_SQL_Macro_examples](https://github.com/hbutani/spark-sql-macros/wiki/Spark_SQL_Macro_examples)

One of the issues of the "udf-compiler" is that it has dependency on GPU setting, it requires user to install many cuda related drivers to the system, and it might have runtime issues when system doesn't have the GPU hardware. This will limit the usage of "udf-compiler", especially for our UDF data source/storage pushdown (Near Data Processing) use cases. In order to address this issue, certain modifications are made to remove GPU dependency. Users can follow instructions below to deploy and use udf-compiler in NDP use cases.


## Getting Started
The steps below show how to use the UDF compiler:
### Step 1: Get the latest Caerus UDF Compiler code and build udf project
```
> git clone
> git pull
> cd $CAERUS_UDF_COMPILER_HOME/
> root@ubuntu1804:/home/ubuntu/openinfralabs/caerus-spark-udf-compiler-from-rapids# mvn --projects udf-compiler --also-make -DskipTests=true clean package 
root@ubuntu1804:/home/ubuntu/openinfralabs/caerus-spark-udf-compiler-from-rapids# ls -la udf-compiler/target/
...
-rw-r--r--  1 root root 139592 Aug 24 08:50 rapids-4-spark-udf_2.12-21.10.0-SNAPSHOT.jar
...
root@ubuntu1804:/home/ubuntu/openinfralabs/caerus-spark-udf-compiler-from-rapids#
```
### Step 2: Use UDF Compiler Jar
- spark-submit: see caerus-udf repo for a detail example. See more detail UDF pushdown example in [caerus-udf](https://github.com/open-infrastructure-labs/caerus-udf/tree/master/examples/spark-udf)
```
 spark-submit 
  --driver-library-path path-to-udf-compiler-jar\rapids-4-spark-udf_2.12-21.10.0-SNAPSHOT.jar \
  --class MyClass path-to-app-jar\main-application.jar
```
- spark-shell (for debugging/testing)
```
 spark-shell
  --driver-library-path path-to-udf-compiler-jar\rapids-4-spark-udf_2.12-21.10.0-SNAPSHOT.jar \
  --config "spark.sql.extensions"="com.nvidia.spark.udf.Plugin"
```
- ide (e.g. Intellij)
For easy debugging, a main application scala class can be put under udf-compiler/src/main/scala/Main.Scala, inside main class, it will have spark.sql.extensions and a UDF definition, after compile and run (debugging), the physical plan of a sql that calling udf should show different than it was not using UDF compiler (see below result example).

## Result Example
```
root@ubuntu1804:/home/ubuntu/openinfralabs/caerus-spark-udf-compiler-from-rapids# spark-shell --driver-class-path  udf-compiler/target/rapids-4-spark-udf_2.12-21.10.0-SNAPSHOT.jar --conf "spark.sql.extensions"="com.nvidia.spark.udf.Plugin"
SLF4J: Class path contains multiple SLF4J bindings.
...
Spark context Web UI available at http://10.124.62.103:4040
Spark context available as 'sc' (master = local[*], app id = local-1629810836057).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.1.1
      /_/
         
Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_292)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

scala>  val schema = new StructType().add("name", StringType, true).add("age", IntegerType, true)
schema: org.apache.spark.sql.types.StructType = StructType(StructField(name,StringType,true), StructField(age,IntegerType,true))

scala>  val df_with_schema = spark.read.schema(schema).json("file:///data/source/people.json")
df_with_schema: org.apache.spark.sql.DataFrame = [name: string, age: int]

scala>     df_with_schema.createOrReplaceTempView("people_with_schema")

scala>     spark.udf.register("intUDF", (i: Int) => {
     |       val j = 2
     |       i + j
     |     })
res1: org.apache.spark.sql.expressions.UserDefinedFunction = SparkUserDefinedFunction($Lambda$2424/1272194712@5fa7cb3,IntegerType,List(Some(class[value[0]: int])),Some(class[value[0]: int]),Some(intUDF),false,true)

scala>  val udfResult = spark.sql("SELECT * FROM people_with_schema WHERE intUDF(age) > 15")
udfResult: org.apache.spark.sql.DataFrame = [name: string, age: int]

scala> udfResult.explain(true)
21/08/24 09:15:42 INFO FileSourceStrategy: Pushed Filters: IsNotNull(age)
21/08/24 09:15:42 INFO FileSourceStrategy: Post-Scan Filters: isnotnull(age#1),((age#1 + 2) > 15)
21/08/24 09:15:42 INFO FileSourceStrategy: Output Data Schema: struct<name: string, age: int>
== Parsed Logical Plan ==
'Project [*]
+- 'Filter ('intUDF('age) > 15)
   +- 'UnresolvedRelation [people_with_schema], [], false

== Analyzed Logical Plan ==
name: string, age: int
Project [name#0, age#1]
+- Filter ((age#1 + 2) > 15)
   +- SubqueryAlias people_with_schema
      +- Relation[name#0,age#1] json

== Optimized Logical Plan ==
Filter (isnotnull(age#1) AND ((age#1 + 2) > 15))
+- Relation[name#0,age#1] json

== Physical Plan ==
*(1) Filter (isnotnull(age#1) AND ((age#1 + 2) > 15))
+- FileScan json [name#0,age#1] Batched: false, DataFilters: [isnotnull(age#1), ((age#1 + 2) > 15)], Format: JSON, Location: InMemoryFileIndex[file:/data/source/people.json], PartitionFilters: [], 
PushedFilters: [IsNotNull(age)], 
ReadSchema: struct<name:string,age:int>


scala> 

```



