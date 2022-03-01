---
layout: page
title: Shim Development
nav_order: 4
parent: Developer Overview
---
# Shim Development

RAPIDS Accelerator For Apache Spark supports multiple feature version lines of 
Apache Spark such as 3.1.x, 3.2.x, and a number of vendor releases that contain
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
1. [ShimExpression For 3.0.x and 3.1.x](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin/src/main/post320-treenode/scala/com/nvidia/spark/rapids/shims/v2/TreeNode.scala#L23)
2. [ShimExpression For 3.2.x](https://github.com/NVIDIA/spark-rapids/blob/main/sql-plugin/src/main/pre320-treenode/scala/com/nvidia/spark/rapids/shims/v2/TreeNode.scala#L23)

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
jar:file:/home/spark/rapids-4-spark_2.12-22.04.0.jar!/
jar:file:/home/spark/rapids-4-spark_2.12-22.04.0.jar!/spark3xx-common/
jar:file:/home/spark/rapids-4-spark_2.12-22.04.0.jar!/spark302/
```

Spark 3.2.0's URLs :    
```
jar:file:/home/spark/rapids-4-spark_2.12-22.04.0.jar!/
jar:file:/home/spark/rapids-4-spark_2.12-22.04.0.jar!/spark3xx-common/
jar:file:/home/spark/rapids-4-spark_2.12-22.04.0.jar!/spark320/
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
(i.e., in `sql-plugin/src/main/scala` as opposed to be duplicated either in `shims/spark3xx` submodules or over
`sql-plugin/src/main/3*/scala` source code roots) is to delay inheriting `ShuffleManager` until as late as possible, 
as close as possible to the facade class where we have to split the source code anyways. Use traits as much 
as possible for flexibility. 

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
