
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer


// Function to run a single test iteration
def runSingleTest(spark: SparkSession, totalRows: Long, numTasks: Int, lookupTable: org.apache.spark.sql.DataFrame): Long = {
  import spark.implicits._
  
  val startTime = System.nanoTime()
  val result = spark.range(0, totalRows, 1, numTasks)
    .selectExpr(
      "id",
      "id % 1000 as lookup_key",
      "cast(rand() * 50 as int) as shuffle_key",
      "id * 1.5 as value1",
      "id * 2.3 as value2"
    )
    // Broadcast join - this reduces batch size dramatically
    .join(broadcast(lookupTable), $"lookup_key" === $"lookup_id", "inner")
    .select("id", "shuffle_key", "value1", "value2", "category", "multiplier")
    // Shuffle with many small batches
    .repartition($"shuffle_key")
    .groupBy($"shuffle_key", $"category")
    .agg(
      count("id").alias("row_count"),
      avg("value1").alias("avg_val1"),
      sum("value2").alias("sum_val2")
    )
    .collect()
  val endTime = System.nanoTime()
  endTime - startTime
}

// Function to create the specific scenario with broadcast join reducing batch sizes (3 runs each)
def createBroadcastJoinScenario(): Unit = {
  val spark = SparkSession.active
  import spark.implicits._
  
  println("=== Broadcast Join Scenario: Many Small Batches After Join ===")
  println("Running 3 iterations for each configuration...")
  
  // Configure for the scenario
  spark.conf.set("spark.rapids.sql.batchSizeBytes", "65536") // 64KB
  spark.conf.set("spark.sql.adaptive.enabled", "false")
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
  spark.conf.set("spark.sql.adaptive.broadcastTimeout", "300s")
  
  val numRowsPerBatch = 3000 // Approximately 8192 rows per 64KB batch
  val batchesPerTask = 100   // Each mapper task processes 100 batches
  val numTasks = 200          // 500 mapper tasks
  val totalRows = numRowsPerBatch * batchesPerTask * numTasks
  
  println(s"Creating scenario:")
  println(s"- Total rows: $totalRows")
  println(s"- Mapper tasks: $numTasks")
  println(s"- Batches per task: $batchesPerTask")
  println(s"- Target batch size: 64KB")
  println(s"- Expected batch size after join: ~6.4KB (90% reduction)")
  
  // Create small lookup table for broadcast join
  val lookupTable = spark.range(10)
    .selectExpr(
      "id as lookup_id",
      "cast(id % 100 as string) as category",
      "cast(rand() * 10 as double) as multiplier"
    )
  
  println("\nTesting WITHOUT coalesce before shuffle (ratio = 0.0)...")
  spark.conf.set("spark.rapids.shuffle.coalesceBeforeShuffleTargetSizeRatio", "0.0")
  
  val timesNoCoalesce = ArrayBuffer[Long]()
  for (i <- 1 to 3) {
    println(s"  Run $i/3...")
    val time = runSingleTest(spark, totalRows, numTasks, lookupTable)
    timesNoCoalesce += time
    println(f"    Time: ${time / 1000000.0}%.2f ms")
  }
  
  println("\nTesting WITH coalesce before shuffle (ratio = 0.6)...")
  spark.conf.set("spark.rapids.shuffle.coalesceBeforeShuffleTargetSizeRatio", "0.6")
  
  val timesWithCoalesce = ArrayBuffer[Long]()
  for (i <- 1 to 3) {
    println(s"  Run $i/3...")
    val time = runSingleTest(spark, totalRows, numTasks, lookupTable)
    timesWithCoalesce += time
    println(f"    Time: ${time / 1000000.0}%.2f ms")
  }
  
  // Calculate averages and standard deviations
  val avgTimeNoCoalesce = timesNoCoalesce.sum.toDouble / timesNoCoalesce.length
  val avgTimeWithCoalesce = timesWithCoalesce.sum.toDouble / timesWithCoalesce.length
  
  val stdDevNoCoalesce = math.sqrt(timesNoCoalesce.map(t => math.pow(t - avgTimeNoCoalesce, 2)).sum / timesNoCoalesce.length)
  val stdDevWithCoalesce = math.sqrt(timesWithCoalesce.map(t => math.pow(t - avgTimeWithCoalesce, 2)).sum / timesWithCoalesce.length)
  
  println(f"\n=== Broadcast Join Scenario Results (3 runs each) ===")
  println("Without coalesce:")
  timesNoCoalesce.zipWithIndex.foreach { case (time, idx) => 
    println(f"  Run ${idx+1}: ${time / 1000000.0}%.2f ms")
  }
  println(f"  Average: ${avgTimeNoCoalesce / 1000000.0}%.2f ms")
  println(f"  Std Dev: ${stdDevNoCoalesce / 1000000.0}%.2f ms")
  
  println("\nWith coalesce:")
  timesWithCoalesce.zipWithIndex.foreach { case (time, idx) => 
    println(f"  Run ${idx+1}: ${time / 1000000.0}%.2f ms")
  }
  println(f"  Average: ${avgTimeWithCoalesce / 1000000.0}%.2f ms")
  println(f"  Std Dev: ${stdDevWithCoalesce / 1000000.0}%.2f ms")
  
  val improvement = ((avgTimeNoCoalesce - avgTimeWithCoalesce) / avgTimeNoCoalesce) * 100
  println(f"\nPerformance change: ${improvement}%+.2f%% (based on averages)")
  
  if (improvement > 5) {
    println("SUCCESS: Coalescing significantly improved performance!")
    println("This scenario demonstrates the benefit of coalesce before shuffle")
    println("when broadcast joins create many small batches.")
  } else if (improvement > 0) {
    println("MINOR IMPROVEMENT: Coalescing helped but not dramatically")
  } else if (improvement < -5) {
    println("REGRESSION: Coalescing hurt performance in this scenario")
  } else {
    println("NEUTRAL: Minimal difference between coalesced and non-coalesced")
  }
}


createBroadcastJoinScenario()

sys.exit(0)
// scalastyle:on