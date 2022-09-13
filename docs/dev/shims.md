---
layout: page
title: Shim Development
nav_order: 4
parent: Developer Overview
---
# Shim Development

RAPIDS Accelerator For Apache Spark supports multiple feature version lines of
Apache Spark such as 3.1.x, 3.2.x, 3.3.0 and a number of vendor releases that contain
a mix of patches from different upstream releases. These artifacts are generally
incompatible between each other, at both source code level and even more often
at the binary level. The role of the Shim layer is to hide these issues from the
common code, maximize reuse, and minimize logic duplication.

This is achieved by using a ServiceProvider pattern. All Shims implement the same API,
the suitable Shim implementation is loaded after detecting the current Spark build version
attempting to instantiate our plugin. We use the
[ShimLoader](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin/src/main/scala/com/nvidia/spark/rapids/ShimLoader.scala)
class as a tight entry point for interacting with the host Spark runtime.

In the following we provide recipes for typical scenarios addressed by the Shim layer.

## Method signature discrepancies

It's among the easiest issues to resolve. We define a method in SparkShims
trait covering a superset of parameters from all versions and call it
```
SparkShimImpl.methodWithDiscrepancies(p_1, ..., p_n)
```
instead of referencing it directly. Shim implementations (SparkShimImpl) are in charge of dispatching it further
to correct version-dependent methods. Moreover, unlike in the below sections
conflicts between versions are easily avoided by using different package or class names
for conflicting Shim implementations.

## Base Classes/Traits Changes

### Compile-time issues
Upstream base classes we derive from might be incompatible in the sense that one version
requires us to implement/override the method `M` whereas the other prohibits it by marking
the base implementation `final`, E.g. `org.apache.spark.sql.catalyst.trees.TreeNode` changes
between Spark 3.1.x and Spark 3.2.x. So instead of deriving from such classes directly we
inject an intermediate trait e.g. `com.nvidia.spark.rapids.shims.ShimExpression` that
has a varying source code depending on the Spark version we compile against to overcome this
issue as you can see e.g., comparing TreeNode:
1. [ShimExpression For 3.1.x](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin/src/main/pre320-treenode/scala/com/nvidia/spark/rapids/shims/TreeNode.scala#L23)
2. [ShimExpression For 3.2.x](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin/src/main/post320-treenode/scala/com/nvidia/spark/rapids/shims/TreeNode.scala#L23)

This resolves compile-time problems, however, now we face the problem at run time.

### Run-time Issues

Plugin has to be able to deterministically load the right class files
for identically named classes depending on the detected
Spark runtime version. This is typically solved by using ASM-based relocation in the bytecode,
however it does not work easily with
[Scala packages](https://contributors.scala-lang.org/t/scala-signature-layout/3327/4)

So instead we resort to the idea of JDK's ParallelWorldClassloader in combination with the fact that
Spark runtime uses mutable classloaders we can alter after detecting the runtime version.
Using JarURLConnection URLs we create a Parallel World of the current version within the jar, e.g.:

Spark 3.0.2's URLs:
```
jar:file:/home/spark/rapids-4-spark_2.12-22.10.0.jar!/
jar:file:/home/spark/rapids-4-spark_2.12-22.10.0.jar!/spark3xx-common/
jar:file:/home/spark/rapids-4-spark_2.12-22.10.0.jar!/spark302/
```

Spark 3.2.0's URLs :
```
jar:file:/home/spark/rapids-4-spark_2.12-22.10.0.jar!/
jar:file:/home/spark/rapids-4-spark_2.12-22.10.0.jar!/spark3xx-common/
jar:file:/home/spark/rapids-4-spark_2.12-22.10.0.jar!/spark320/
```

### Late Inheritance in Public Classes

Most classes needed by the plugin can be disambiguated using Parallel World locations without
reservations except for documented classes that are exposed to the user that may be loaded before
the Plugin is even instantiated by the Spark runtime. The most important example of such a class
is a configurable ShuffleManager. `ShuffleManager` has also changed in a backwards incompatible
manner over the span of supported Spark versions.

The first issue with such a class, since it's loaded by Spark directly outside our control we
cannot have a single class name for our implementation that would work across versions. This is resolved,
by having the documented facade classes with a shim specifier in their package names.

The second issue that every parent class/trait in the inheritance graph is loaded using the classloader outside
Plugin's control. Therefore, all this bytecode must reside in the conventional jar location, and it must
be bitwise-identical across *all* shims. The only way to keep the source code for shared functionality unduplicated,
(i.e., in `sql-plugin/src/main/scala` as opposed to be duplicated in `sql-plugin/src/main/3*/scala` source code roots)
is to delay inheriting `ShuffleManager` until as late as possible, as close as possible to the facade class where we
have to split the source code anyway. Use traits as much as possible for flexibility.

### Late Initialization of Public Classes' Ancestors

The third issue may arise from the fact that the shared logic may transitively reference a class that
for one another reason resides in a Parallel World. Untangling this is tedious and may be unnecessary.
The following approach robustly prevents from running into issues related to that.

We know that at the time such a class is loaded by Spark it's not strictly needed if the Plugin
has not been loaded yet. More accurately, it may not be strictly needed until later when the first
query can be run when the Spark SQL session and its extensions are initialized. It improves the
user experience if the first query is not penalized beyond necessary though. By design, Plugin guarantees
that the classloader is
[set up at load time](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin/src/main/scala/com/nvidia/spark/SQLPlugin.scala#L29)
before the DriverPlugin and ExecutorPlugin instances are called the `init` method on.

By making a visible class merely a wrapper of the real implementation, extending `scala.Proxy` where `self` is a lazy
val, we prevent classes from Parallel Worlds to be loaded before they can be, and are actually required.
For examples see:

1. `abstract class ProxyRapidsShuffleInternalManagerBase`
2. `class ExclusiveModeGpuDiscoveryPlugin`

Note that we currently have to manually code up the delegation methods to the tune of:
```
  def method(x: SomeThing) = self.method(x)
```
This could be automatically generated with a simple tool processing the `scalap` output or Scala macros at
build/compile time. Pull requests are welcome.

## Exposing an internal class as a compile-time dependency

At some point you may find it necessary to expose an existing
`class/trait/object A` currently residing in a "hidden" parallel world
as a dependency for Maven modules/projects dependencies depending on the `dist`
module artifact `rapids-4-spark_2.12`.

This has two pre-requisites:
1. The .class file with the bytecode is bitwise-identical among the currently
supported Spark versions. To verify this you can inspect the dist jar and check
if the class file is under `spark3xx-common` jar entry. If this is not case the
code should be refactored until all discrepancies are shimmed away.
1. The transitive closure of the classes compile-time-referenced by `A` should
have the property above.

JDK ships the `jdeps` tool that can help analyze static dependencies of
a class. Unfortunately, it does not compute the transitive closure (recursive)
at the class granularity. Thus you need additional tools such as
`[Graphviz tool](https://graphviz.org/)` used here.

As an example, in Pull Request #6521 `com.nvidia.spark.rapids.GpuColumnVector`
is being externalized, and we need to figure out if it transitively loads
a class from a parallel world which will generate a NoClassDefFoundError at
run time.

To figure out the transitive closure of a class we first need to build
the `dist` module. While iterating on the PR, it should be sufficient
to build against the lowest and highest versions of the supported Spark version
range.

```Bash
 ./build/buildall --parallel=4  --profile=311,330 --module=dist
```

However, before submitting the PR execute the full build `--profile=noSnapshots`.

Then switch to the parallel-world build dir.
```
cd dist/target/parallel-world/
```

Execute `jdeps` against `spark3xx-common` and an exactly one parallel world such
as `spark330`
```Bash
$JAVA_HOME/bin/jdeps -v \
  -dotoutput /tmp/jdeps3 \
  -regex '(com|org)\..*\.rapids\..*' \
  spark3xx-common spark330
```

Looking at the output file `/tmp/jdeps3/spark3xx-common.dot`, unfortunately you
see that jdeps does not label the source class node but labels the targets
class node of an edge. Thus the graph is incorrect as it breaks paths if a node
has both incoming and outgoing edges.

```Bash
grep 'com.nvidia.spark.rapids.GpuFilterExec\$' spark3xx-common.dot
   "com.nvidia.spark.rapids.GpuFilterExec$"           -> "com.nvidia.spark.rapids.GpuFilterExec (spark330)";
   "com.nvidia.spark.rapids.GpuOverrides$$anon$204"   -> "com.nvidia.spark.rapids.GpuFilterExec$ (spark3xx-common)";
```

`spark3xx-common.dot` does not have `(spark330)`-labeled source nodes by construction.
and thus it is ok to retain this label for target nodes. Moreover, it is going
to be handy to determine when the transitive closure includes shimmed classes
that you are trying to prevent.

The following command lists all classes in the spark330 Shim reachable
(dist != inf) from GpuColumnVector.
```Bash
< spark3xx-common.dot sed 's/ (spark3xx-common)//' | \
  dijkstra -d -a com.nvidia.spark.rapids.GpuColumnVector | \
  grep -v -- '->' | grep -v dist=inf | grep spark330
        "org.apache.spark.sql.rapids.execution.TrampolineUtil$ (spark330)"      [dist=7.000];
        "com.nvidia.spark.rapids.GpuScalar$ (spark330)" [dist=2.000];
        "com.nvidia.spark.rapids.GpuCast (spark330)"    [dist=7.000];
        "com.nvidia.spark.rapids.HostColumnarToGpu (spark330)"  [dist=2.000];
        "com.nvidia.spark.rapids.GpuInSet (spark330)"   [dist=7.000];
        "org.apache.spark.sql.rapids.GpuAdd (spark330)" [dist=7.000];
        "org.apache.spark.sql.rapids.GpuAnd (spark330)" [dist=7.000];
        "org.apache.spark.sql.rapids.GpuBitwiseAnd (spark330)"  [dist=7.000];
        "org.apache.spark.sql.rapids.GpuBitwiseOr (spark330)"   [dist=7.000];
        "org.apache.spark.sql.rapids.GpuBitwiseXor (spark330)"  [dist=7.000];
        "org.apache.spark.sql.rapids.GpuEqualNullSafe (spark330)"       [dist=7.000];
        "org.apache.spark.sql.rapids.GpuEqualTo (spark330)"     [dist=7.000];
        "org.apache.spark.sql.rapids.GpuGetStructField (spark330)"      [dist=7.000];
        "org.apache.spark.sql.rapids.GpuGreaterThan (spark330)" [dist=7.000];
        "org.apache.spark.sql.rapids.GpuGreaterThanOrEqual (spark330)"  [dist=7.000];
        "org.apache.spark.sql.rapids.GpuGreatest (spark330)"    [dist=7.000];
        "org.apache.spark.sql.rapids.GpuLeast (spark330)"       [dist=7.000];
        "org.apache.spark.sql.rapids.GpuLessThan (spark330)"    [dist=7.000];
        "org.apache.spark.sql.rapids.GpuLessThanOrEqual (spark330)"     [dist=7.000];
        "org.apache.spark.sql.rapids.GpuMultiply (spark330)"    [dist=7.000];
        "org.apache.spark.sql.rapids.GpuNot (spark330)" [dist=7.000];
        "org.apache.spark.sql.rapids.GpuOr (spark330)"  [dist=7.000];
        "org.apache.spark.rapids.shims.storage.ShimDiskBlockManager (spark330)" [dist=5.000];
```

Focus on the nodes with lowest distance to eliminate dependency on the shim.