---
layout: page
title: Developer Overview
nav_order: 10
has_children: true
permalink: /developer-overview/
---
# RAPIDS Plugin for Apache Spark Developer Overview
This document provides a developer overview of the project and covers the
following topics:
* [Spark SQL and Query Plans](#spark-sql-and-query-plans)
  * [Catalyst Query Plans](#catalyst-query-plans)
  * [How Spark Executes the Physical Plan](#how-spark-executes-the-physical-plan)
* [How the Plugin Works](#how-the-rapids-plugin-works)
  * [Plugin Replacement Rules](#plugin-replacement-rules)
* [Guidelines for Replacing Catalyst Executors and Expressions](#guidelines-for-replacing-catalyst-executors-and-expressions)
  * [Setting Up the Class](#setting-up-the-class)
  * [Expressions](#expressions)
  * [The GPU Semaphore](#the-gpu-semaphore)
* [Debugging Tips](#debugging-tips)
* [Profiling Tips](#profiling-tips)

## Spark SQL and Query Plans

Apache Spark provides a module for working with structured data called
[Spark SQL](https://spark.apache.org/sql).  Spark takes SQL queries, or the
equivalent in the
[DataFrame API](https://spark.apache.org/docs/latest/sql-programming-guide.html),
and creates an unoptimized logical plan to execute the query.  That plan is
then optimized by
[Catalyst](https://databricks.com/glossary/catalyst-optimizer),
a query optimizer built into Apache Spark. Catalyst optimizes the logical plan
in a series of phases and eventually forms a physical plan that is used to
execute the query on the Spark cluster.  Executing a SQL `EXPLAIN` statement
or using the `.explain` method on a DataFrame will show how Catalyst has
planned to execute the query.

### Catalyst Query Plans
Catalyst Query plans consist of a directed, acyclic graph of executor nodes.
Each node has an output schema and zero or more child nodes that provide
input.  The tree of executor nodes is rooted at the node that will produce the
final output of the query.  The leaves of the tree are the executors that will
load the initial data for the query (e.g.: table scans, etc.).

For example, the following shows the Spark explanation of a query plan.  Note
how the tree is rooted at the `Sort` node which is the last step in the query.
The leaves of the tree are `BatchScan` operations on files.
```
== Physical Plan ==
*(7) Sort [o_orderpriority#5 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(o_orderpriority#5 ASC NULLS FIRST, 200), true, [id=#446]
   +- *(6) HashAggregate(keys=[o_orderpriority#5], functions=[count(1)])
      +- Exchange hashpartitioning(o_orderpriority#5, 200), true, [id=#442]
         +- *(5) HashAggregate(keys=[o_orderpriority#5], functions=[partial_count(1)])
            +- *(5) Project [o_orderpriority#5]
               +- SortMergeJoin [o_orderkey#0L], [l_orderkey#18L], LeftSemi
                  :- *(2) Sort [o_orderkey#0L ASC NULLS FIRST], false, 0
                  :  +- Exchange hashpartitioning(o_orderkey#0L, 200), true, [id=#424]
                  :     +- *(1) Project [o_orderkey#0L, o_orderpriority#5]
                  :        +- *(1) Filter ((isnotnull(o_orderdate#4) AND (o_orderdate#4 >= 8582)) AND (o_orderdate#4 < 8674))
                  :           +- *(1) ColumnarToRow
                  :              +- BatchScan[o_orderkey#0L, o_orderdate#4, o_orderpriority#5] ParquetScan Location: InMemoryFileIndex[file:/home/example/parquet/orders.tbl], ReadSchema: struct<o_orderkey:bigint,o_orderdate:date,o_orderpriority:string>
                  +- *(4) Sort [l_orderkey#18L ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(l_orderkey#18L, 200), true, [id=#433]
                        +- *(3) Project [l_orderkey#18L]
                           +- *(3) Filter (((l_commitdate#29 < l_receiptdate#30) AND isnotnull(l_commitdate#29)) AND isnotnull(l_receiptdate#30))
                              +- *(3) ColumnarToRow
                                 +- BatchScan[l_orderkey#18L, l_commitdate#29, l_receiptdate#30] ParquetScan Location: InMemoryFileIndex[file:/home/example/parquet/lineitem.tbl], ReadSchema: struct<l_orderkey:bigint,l_commitdate:date,l_receiptdate:date>
```

### How Spark Executes the Physical Plan
Each node in the tree pulls inputs from child nodes via iterators of rows and
produces output via an iterator of rows.  Therefore executing the plan
consists of pulling rows from the output iterator of the root node.  That in
turn will need to pull values from the input nodes, chaining all the way down
the tree until eventually the iterator of the leaf nodes is pulled and causes
the reading of rows from the raw input data.

## How the RAPIDS Plugin Works
The plugin leverages two main features in Spark.  The first is a
plugin interface in [Catalyst](https://databricks.com/glossary/catalyst-optimizer)
that allows the optimizer to be extended.  The plugin is a Catalyst extension
that analyzes the physical plan and replaces executor and expression nodes with
GPU versions when those operations can be performed on the GPU.  The other
feature is [columnar processing](https://issues.apache.org/jira/browse/SPARK-27396)
which allows extensions to operate on Spark SQL data in a `ColumnarBatch` form.
Processing columnar data is much more GPU friendly than row-by-row processing.

For example, the same query plan shown above becomes the following plan after
being processed by the RAPIDS plugin:
```
*(5) Sort [o_orderpriority#5 ASC NULLS FIRST], true, 0
+- Exchange rangepartitioning(o_orderpriority#5 ASC NULLS FIRST, 200), true, [id=#611]
   +- *(4) HashAggregate(keys=[o_orderpriority#5], functions=[count(1)])
      +- Exchange hashpartitioning(o_orderpriority#5, 200), true, [id=#607]
         +- *(3) HashAggregate(keys=[o_orderpriority#5], functions=[partial_count(1)])
            +- *(3) GpuColumnarToRow false
               +- !GpuProject [o_orderpriority#5]
                  +- GpuRowToColumnar TargetSize(1000000)
                     +- SortMergeJoin [o_orderkey#0L], [l_orderkey#18L], LeftSemi
                        :- *(1) GpuColumnarToRow false
                        :  +- !GpuSort [o_orderkey#0L ASC NULLS FIRST], false, 0
                        :     +- GpuCoalesceBatches com.nvidia.spark.rapids.PreferSingleBatch$@40dcd875
                        :        +- !GpuColumnarExchange gpuhashpartitioning(o_orderkey#0L, 200), true, [id=#543]
                        :           +- !GpuProject [o_orderkey#0L, o_orderpriority#5]
                        :              +- GpuCoalesceBatches TargetSize(1000000)
                        :                 +- !GpuFilter ((gpuisnotnull(o_orderdate#4) AND (o_orderdate#4 >= 8582)) AND (o_orderdate#4 < 8674))
                        :                    +- GpuBatchScan[o_orderkey#0L, o_orderdate#4, o_orderpriority#5] GpuParquetScan Location: InMemoryFileIndex[file:/home/example/parquet/orders.tbl], ReadSchema: struct<o_orderkey:bigint,o_orderdate:date,o_orderpriority:string>
                        +- *(2) GpuColumnarToRow false
                           +- !GpuSort [l_orderkey#18L ASC NULLS FIRST], false, 0
                              +- GpuCoalesceBatches com.nvidia.spark.rapids.PreferSingleBatch$@40dcd875
                                 +- !GpuColumnarExchange gpuhashpartitioning(l_orderkey#18L, 200), true, [id=#551]
                                    +- !GpuProject [l_orderkey#18L]
                                       +- GpuCoalesceBatches TargetSize(1000000)
                                          +- !GpuFilter (((l_commitdate#29 < l_receiptdate#30) AND gpuisnotnull(l_commitdate#29)) AND gpuisnotnull(l_receiptdate#30))
                                             +- GpuBatchScan[l_orderkey#18L, l_commitdate#29, l_receiptdate#30] GpuParquetScan Location: InMemoryFileIndex[file:/home/example/parquet/lineitem.tbl], ReadSchema: struct<l_orderkey:bigint,l_commitdate:date,l_receiptdate:date>
```

Notice how most of the nodes in the original plan have been replaced with GPU
versions.  In the cases where nodes were not replaced, the plugin inserts data
format conversion nodes, like `GpuColumnarToRow` and `GpuRowToColumnar` to
convert between columnar processing for nodes that will execute on the GPU and
row processing for nodes that will execute on the CPU.

### Plugin Replacement Rules
The plugin uses a set of rules to update the query plan.  The physical plan is
walked, node by node, looking up rules based on the type of node (e.g.: scan,
executor, expression, etc.), and applying the rule that matches.  See the
`ColumnarOverrideRules` and `GpuOverrides` classes for more details.

## Guidelines for Replacing Catalyst Executors and Expressions
Most development work in the plugin involves translating various Catalyst
executor and expression nodes into new nodes that execute on the GPU.  This
section provides tips on how to construct a new Catalyst node class that will
execute on the GPU.

### Setting Up the Class
Catalyst requires that all query plan nodes are case classes.  Since the GPU
versions of nodes often shares significant functionality with the original CPU
version, it is tempting to derive directly from the CPU class.  **Do NOT
derive from the case class!**.  Case class derivation, while currently allowed
in Scala, is not guaranteed to continue working in the future.  In addition it
can cause subtle problems when these derived nodes appear in the query plan
because the derived case class compares as equal to the parent case class when
using the parent's comparator method.

The proper way to setup the class is to create a new case class that derives
from the same parent class of the CPU node case class.  Any methods overridden
by the CPU node case class should be examined closely to see if the same
overrides need to appear in the GPU version.

### Adding Configuration Properties
All plugin configuration properties should be cataloged in the `RapidsConf`
class.  Every property needs well-written documentation, and this documentation
is used to automatically generate the
[plugin configuration documentation](../configs.md).

#### Generating the Configuration Documentation
The plugin configuration documentation can be generated by executing the
`RapidsConf.help` method.  An easy way to do this is to use the Spark shell
REPL then copy-n-paste the resulting output.  For example:
```
scala> import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.RapidsConf

scala> RapidsConf.help(true)
# Rapids Plugin 4 Spark Configuration
The following is the list of options that `rapids-plugin-4-spark` supports.

On startup use: `--conf [conf key]=[conf value]`. For example:

[...]
```

### Expressions
For nodes expecting GPU columnar data as input and
producing GPU columnar data as output, the child node(s) passed to the case
class constructor should have the `Expression` type.  This is a little
odd because they should all be instances of `GpuExpression` except for
`AttributeReference` and `SortOrder`. This is needed because `AttributeReference`
is weaved into a lot of the magic that is built into Spark expressions.
`SortOrder` is similar as Spark itself will insert `SortOrder` instances into
the plan automatically in many cases.  These are both `Unevaluable` expressions
so they should never be run columnar or otherwise.  These `Expressions` should be
bound using `GpuBindReferences` which will make sure that all `AttributeReference`
instances are replaced with `GpuBoundReference` implementations and everything is
on the GPU. So after calling `GpuBindReferences.bindReferences` you should be able
to cast the result to `GpuExpression` unless you know you have a SortOrder in there,
which should be rare.

### The GPU Semaphore
Typically, Spark runs a task per CPU core, but there are often many more CPU
cores than GPUs.  This can lead to situations where Spark wants to run more
concurrent tasks than can reasonably fit on a GPU.  The plugin works around
this problem with the `GpuSemaphore` object.  This object acts as a traffic
cop, limiting the number of tasks concurrently operating on the GPU.

#### When and How to Use the Semaphore
The semaphore only needs to be used by nodes that are "transition" nodes,
i.e.: nodes that are transitioning the data to or from the GPU.  Most nodes
expect their input to already be on the GPU and produce output on the GPU, so
those nodes do not need to worry about using `GpuSemaphore`.  The general
rules for using the semaphore are:
* If the plan node has inputs not on the GPU but produces outputs on the GPU then the node must
acquire the semaphore by calling `GpuSemaphore.acquireIfNecessary`.
* If the plan node has inputs on the GPU but produces outputs not on the GPU
then the node must release the semaphore by calling
`GpuSemaphore.releaseIfNecessary`.

`GpuSemaphore` automatically installs a task completion listener when a task
acquires the semaphore for the first time.  This prevents task failures from
leaking references to the semaphore and possibly causing deadlocks.

#### Disabling the Semaphore
If there is ever a need to execute without the semaphore semantics, the
semaphore code can be disabled at runtime by setting the Java system property
`com.nvidia.spark.rapids.semaphore.enabled` to `false` before the `GpuSemaphore` class
is loaded.  Typically this would be set as one of the Spark executor Java
options, e.g.:
```
--conf spark.executor.extraJavaOptions=-Dcom.nvidia.spark.rapids.semaphore.enabled=false
```

## Debugging Tips
An easy way to debug the plugin is to run in
[Spark local mode](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls).
This runs the Spark driver and executor all in the same JVM process, making it
easy for breakpoints to catch everything.  You do not have to worry about
whether the code is executing on the driver or the executor, since they are
all part of the same process.

Once configured for local mode, a debugger agent can be added by specifying it
in the driver options, e.g.:
```
--conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
```

The Spark process will wait upon launch for a remote debugger to attach via
port 5005.

## Profiling Tips
[NVIDIA Nsight Systems](https://developer.nvidia.com/nsight-systems) makes
profiling easy.  In addition to showing where time is being spent in CUDA
runtime calls, GPU memory transfers, and GPU kernels, custom markers can be
added to the profile via NVTX ranges.  See
[the NVTX profiling guide](nvtx_profiling.md) for additional information on
setting up the build for profiling and adding NVTX ranges.

Note: Nsight Systems is installed as part of the CUDA Toolkit.
However, it is recommended that you download the latest release from the
[product page](https://developer.nvidia.com/nsight-systems) directly.

## Code Coverage

We use [jacoco](https://www.jacoco.org/jacoco/trunk/doc/) for code coverage because it lets
us gather code coverage for both Java and Scala.  It also lets us instrument shaded jars,
and use tests that are written in pyspark.  We have had to jump through some hoops to make
it work, which is partly why the tests are in the `test` and `integration_test` directories.

The regular [jacoco maven plugin](https://www.jacoco.org/jacoco/trunk/doc/maven.html), however
is not currently [able to support](https://github.com/jacoco/jacoco/issues/965) this type of
setup. So if you want to generate a coverage report you need to do it manually.  Coverage is
collected by default so first run the tests, and then generate the report, this should be run
from the root project directory.  It will print out the URL of the report at the end. 
 
```bash
mvn clean verify
./build/coverage-report
```
