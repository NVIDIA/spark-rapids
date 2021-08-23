# Caerus Spark UDF Compiler: Modified from "RAPIDS Accelerator For Apache Spark"

Currently Spark doesn't support any UDF pushdown with the exception of JDBC/database datascource case, and Spark UDF is run in a black box on compute-sdie, and Spark Catalyst, Spark SQL Optimizer, can't optimize UDF.

[RAPIDS Accelerator For Apache Spark](https://github.com/NVIDIA/spark-rapids) is a Nvidia open source project to provide a set of plugins for Apache Spark that leverage GPUs to accelerate processing via the RAPIDS libraries and UCX. 

Among these plugins, ["udf-compiler"](https://github.com/NVIDIA/spark-rapids/tree/branch-21.10/udf-compiler) is a UDF compiler extension (via Spark rule injection) to translate UDFs bytecode to Spark Catalyst expressions.

The "udf-compiler" is similar to the [Spark SQL Macros](https://github.com/hbutani/spark-sql-macros) project we previously investigate, they all attempt to translate Spark UDFs into native Spark Catalyst expressions, which will be optimized by the Spark Catalysts for code generation/serialization, so that the UDFs can be pushed down as the best as we can to the data sources (thus to storage). The task time of such solutions is [2-3 times faster than the native Spark UDFs] (https://github.com/hbutani/spark-sql-macros)

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


## Get Started

[TODO]

## References
[TODO]


