# API validation script for Rapids Plugin

API validation script checks the compatibility of community Spark Execs and GPU Execs in the Rapids Plugin for Spark.  
For example: HashAggregateExec with GpuHashAggregateExec. The script prints Execs where validation fails.  
Validation fails when:
1) The number of parameters differ between community Spark Execs and Gpu Execs.
2) Parameters to the exec have a type mismatch.
3) Order of parameters differ between Spark Exec abd Gpu Exec.

# Dependencies

It requires cudf, rapids-4-spark and spark jars.

# Running the script

```
cd api_validation
mvn scala:run
```

# Output
Output is saved in `api-validation-result.log` in api_validation/target directory.  

Sample Output

```
************************************************************************************************
Types differ for below parameters in this Exec
SparkExec  - [org.apache.spark.sql.execution.aggregate.HashAggregateExec]
GpuExec - [ai.rapids.spark.GpuHashAggregateExec]
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Seq[org.apache.spark.sql.catalyst.expressions.Attribute]          | Seq[ai.rapids.spark.GpuAttributeReference]

************************************************************************************************
Types differ for below parameters in this Exec
SparkExec  - [org.apache.spark.sql.execution.GenerateExec]
GpuExec - [ai.rapids.spark.GpuGenerateExec]
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
org.apache.spark.sql.catalyst.expressions.Generator               | Boolean
Seq[org.apache.spark.sql.catalyst.expressions.Attribute]          | Seq[ai.rapids.spark.GpuExpression]
Boolean                                                           | Seq[org.apache.spark.sql.catalyst.expressions.Attribute]

************************************************************************************************
Types differ for below parameters in this Exec
SparkExec  - [org.apache.spark.sql.execution.ExpandExec]
GpuExec - [ai.rapids.spark.GpuExpandExec]
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Seq[org.apache.spark.sql.catalyst.expressions.Attribute]          | Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]

************************************************************************************************
Types differ for below parameters in this Exec
SparkExec  - [org.apache.spark.sql.execution.joins.SortMergeJoinExec]
GpuExec - [ai.rapids.spark.GpuShuffledHashJoinExec]
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Option[org.apache.spark.sql.catalyst.expressions.Expression]      | org.apache.spark.sql.execution.joins.BuildSide
org.apache.spark.sql.execution.SparkPlan                          | Option[ai.rapids.spark.GpuExpression]
Boolean                                                           | org.apache.spark.sql.execution.SparkPlan



************************************************************************************************
Parameter lengths don't match between Execs
SparkExec - [org.apache.spark.sql.execution.CollectLimitExec]
GpuExec - [ai.rapids.spark.GpuCollectLimitExec]
Spark code has 2 parameters where as plugin code has 3 parameters
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Int                                                               | Int
org.apache.spark.sql.execution.SparkPlan                          | ai.rapids.spark.GpuPartitioning
                                                                  | org.apache.spark.sql.execution.SparkPlan

************************************************************************************************
Parameter lengths don't match between Execs
SparkExec - [org.apache.spark.sql.execution.SortExec]
GpuExec - [ai.rapids.spark.GpuSortExec]
Spark code has 4 parameters where as plugin code has 5 parameters
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Seq[org.apache.spark.sql.catalyst.expressions.SortOrder]          | Seq[ai.rapids.spark.GpuSortOrder]
Boolean                                                           | Boolean
org.apache.spark.sql.execution.SparkPlan                          | org.apache.spark.sql.execution.SparkPlan
Int                                                               | ai.rapids.spark.CoalesceGoal
                                                                  | Int

************************************************************************************************
Parameter lengths don't match between Execs
SparkExec - [org.apache.spark.sql.execution.window.WindowExec]
GpuExec - [ai.rapids.spark.GpuWindowExec]
Spark code has 4 parameters where as plugin code has 2 parameters
Spark parameters                                                    Plugin parameters
------------------------------------------------------------------------------------------------
Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]    | Seq[ai.rapids.spark.GpuExpression]
Seq[org.apache.spark.sql.catalyst.expressions.Expression]         | org.apache.spark.sql.execution.SparkPlan
Seq[org.apache.spark.sql.catalyst.expressions.SortOrder]          | 
org.apache.spark.sql.execution.SparkPlan 
```
