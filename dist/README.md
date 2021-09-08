---
layout: page
title: Testing
nav_order: 1
parent: Developer Overview
---
# RAPIDS Accelerator for Apache Spark Distribution Packaging

The distribution module creates a jar with support for the Spark versions you need combined into a single jar.

See the [CONTRIBUTING.md](../CONTRIBUTING.md) doc for details on building and profiles available to build an Uber jar.

Note that when you use the profiles to build an Uber jar there are currently some hardcoded service provider files that get put into place. One file for each of the
above profiles. Please note that you will need to update these if adding or removing support for a Spark version.

Files are: `com.nvidia.spark.rapids.SparkShimServiceProvider.sparkNonSnapshot`, `com.nvidia.spark.rapids.SparkShimServiceProvider.sparkSnapshot`, `com.nvidia.spark.rapids.SparkShimServiceProvider.sparkNonSnapshotDB`, and `com.nvidia.spark.rapids.SparkShimServiceProvider.sparkSnapshotDB`.

The new Uber jar is structured like:

1. Base common classes are user visible classes. For these we use Spark 3.0.1 versions
2. META-INF/services. This is a file that has to list all the shim versions supported by this jar. The files talked about above for each profile are put into place here for uber jars.
3. META-INF base files are from 3.0.1  - maven, LICENSE, NOTICE, etc
4. shaded dependencies for Spark 3.0.1 in case the base common classes needed them.
5. Spark specific directory for each version of Spark supported in the jar. ie spark301/, spark302/, spark311/, etc.

If you have to change the contents of the uber jar the following files control what goes into the base jar as classes that are not shaded.

1. `unshimmed-base-classes.txt` - this has classes that should go into the base jar with their normal package name (not shaded). This includes user visible classes (ie com/nvidia/spark/SQLPlugin). Uses Spark 3.0.1 built jar for any of the uber jars.
2. `unshimmed-base-extras.txt` - these are other files applied to the base version of Spark that stay in the base of the jar and not put into Spark version specific directories. Note we choose Spark 3.0.1 as the base version to use for base and unshimmed classes.
3. `unshimmed-extras.txt` - These are files that are put into the base of the jar and not into the Spark specific directory from all of the other Spark version jars.

