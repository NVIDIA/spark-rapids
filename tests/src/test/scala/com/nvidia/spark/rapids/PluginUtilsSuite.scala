package com.nvidia.spark.rapids

import org.scalatest.funsuite.AnyFunSuite

class PluginUtilsSuite extends AnyFunSuite {

  test("detectMultipleJar - multiple jars found") {
    val conf = new RapidsConf()
    val plugin = new Plugin()

    // Set up the classloader with multiple jar resources
    val classloader = new TestClassLoader()
    classloader.addResource("rapids-4-spark-1.0.jar")
    classloader.addResource("rapids-4-spark-2.0.jar")
    classloader.addResource("rapids-4-spark-3.0.jar")
    classloader.addResource("other.jar")
    classloader.addResource("subdir/rapids-4-spark-4.0.jar")
    classloader.addResource("subdir/rapids-4-spark-5.0.jar")
    classloader.addResource("subdir/other.jar")
    ShimLoader.setShimClassLoader(classloader)

    // Call the method under test
    plugin.detectMultipleJar("rapids-4-spark", "Rapids", conf)

    // Assert the expected log warning message
    val expectedMsg =
      """Multiple Rapids jars found in the classpath:
        |revison: UNKNOWN
        |	jar URL: jar:file:/home/haoyangl/spark-rapids/sql-plugin/target/scala-2.12/test-classes/!/rapids-4-spark-1.0.jar
        |	UNKNOWN
        |	jar URL: jar:file:/home/haoyangl/spark-rapids/sql-plugin/target/scala-2.12/test-classes/!/rapids-4-spark-2.0.jar
        |	UNKNOWN
        |	jar URL: jar:file:/home/haoyangl/spark-rapids/sql-plugin/target/scala-2.12/test-classes/!/rapids-4-spark-3.0.jar
        |	UNKNOWN
        |
        |Please make sure there is only one Rapids jar in the classpath.
        |If it is impossible to fix the classpath you can suppress the error by setting allow.multiple.jars to SAME_REVISION or ALWAYS.
        |""".stripMargin
    assert(plugin.logWarningMsg == expectedMsg)
  }

  // Add more test cases here...

}