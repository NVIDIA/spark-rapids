UDF Compiler
============

How to run tests
----------------

From `rapids-plugin-4-spark` root directory, use this command to run the `OpcodeSuite`:

```
mvn test -DwildcardSuites=com.nvidia.spark.OpcodeSuite
```

How to run spark shell
----------------------

To run the spark-shell, you need a `SPARK_HOME`, the `cudf-0.15-cuda10-1.jar`, and the jars produced in the plugin. The cudf jar will be downloaded when mvn test (or package) is run into the ~/.m2 directory. It's easy to get the jar from this directory, and place somewhere accessible. In the case below, the cudf jar is assumed to be in a directory `$JARS`:

```
export SPARK_HOME=[your spark distribution directory]
export JARS=[path to cudf 0.15 jar]

$SPARK_HOME/bin/spark-shell \
--jars $JARS/cudf-0.15-cuda10-1.jar,udf-compiler/target/rapids-4-spark-udf_2.12-0.2.0-SNAPSHOT.jar,sql-plugin/target/rapids-4-spark-sql_2.12-0.2.0-SNAPSHOT.jar \
--conf spark.sql.extensions="com.nvidia.spark.SQLPlugin,com.nvidia.spark.udf.Plugin"
```
