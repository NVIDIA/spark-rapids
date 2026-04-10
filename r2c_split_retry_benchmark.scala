/**
 * R2C Split Retry Performance Benchmark for spark-shell.
 *
 * Tests the overhead of HostColumnarBatchWithRowRange split retry mechanism
 * under normal (no-OOM) conditions. Uses createDataFrame(rdd, schema) with
 * StringType columns to force GpuRowToColumnarExec (non-codegen path).
 *
 * Usage: paste into spark-shell launched with the command below, or :load this file.
 */
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val numRows = 100000000
val numWarmup = 5
val numRuns = 10

def benchmark(label: String, action: => Unit): Double = {
  println(s"\n=== $label ===")
  (1 to numWarmup).foreach { i => action; println(s"  Warmup $i done") }
  val times = (1 to numRuns).map { i =>
    val start = System.nanoTime()
    action
    val elapsed = (System.nanoTime() - start) / 1e6
    println(f"  Run $i: $elapsed%.0f ms")
    elapsed
  }
  val sorted = times.sorted
  val median = sorted(sorted.length / 2)
  val p25 = sorted(sorted.length / 4)
  val p75 = sorted(sorted.length * 3 / 4)
  println(f"\n  Median: $median%.0f ms  P25: $p25%.0f ms  P75: $p75%.0f ms")
  median
}

// ---- Test 1: String columns (forces non-codegen RowToColumnarIterator) ----
val stringSchema = StructType(Seq(StructField("id", IntegerType), StructField("str_col", StringType), StructField("int_col", IntegerType)))
val stringRdd = sc.parallelize(1 to numRows, 4).map(i => Row(i, s"str_$i", i % 1000))
val dfString: DataFrame = spark.createDataFrame(stringRdd, stringSchema)

println("\n=== Query Plan (String columns) ===")
dfString.agg(count("*"), sum("int_col")).explain(true)

val medianString = benchmark("String columns (RowToColumnarIterator path)", { dfString.agg(count("*"), sum("int_col")).collect() })

// ---- Test 2: Nested type columns ----
val nestedSchema = StructType(Seq(StructField("id", IntegerType), StructField("arr_col", ArrayType(IntegerType)), StructField("struct_col", StructType(Seq(StructField("x", IntegerType), StructField("y", StringType))))))
val nestedRdd = sc.parallelize(1 to numRows, 4).map(i => Row(i, Seq(i, i + 1), Row(i, s"s_$i")))
val dfNested: DataFrame = spark.createDataFrame(nestedRdd, nestedSchema)

val medianNested = benchmark("Nested type columns (array + struct)", { dfNested.agg(count("*"), sum("id")).collect() })

// ---- Test 3: Fixed-width only (codegen path, control) ----
val fixedSchema = StructType(Seq(StructField("id", IntegerType), StructField("a", IntegerType), StructField("b", IntegerType), StructField("c", DoubleType)))
val fixedRdd = sc.parallelize(1 to numRows, 4).map(i => Row(i, i % 1000, i % 100, i * 1.5))
val dfFixed: DataFrame = spark.createDataFrame(fixedRdd, fixedSchema)

println("\n=== Query Plan (Fixed-width columns) ===")
dfFixed.agg(count("*"), sum("a"), sum("c")).explain(true)

val medianFixed = benchmark("Fixed-width columns (codegen path, control)", { dfFixed.agg(count("*"), sum("a"), sum("c")).collect() })

println("\n=== Summary ===")
println(f"String columns (RowToColumnarIterator):  $medianString%.0f ms")
println(f"Nested columns (RowToColumnarIterator):  $medianNested%.0f ms")
println(f"Fixed-width columns (codegen, control):  $medianFixed%.0f ms")
println("\nCompare the first two numbers between baseline and this PR.")
println("The third number (codegen) should be unaffected.")
