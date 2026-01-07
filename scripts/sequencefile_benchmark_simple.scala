/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * SequenceFile Performance Benchmark - Simple Version
 * 
 * Copy and paste directly into spark-shell to run
 * 
 * Usage:
 * 
 * 1. GPU mode:
 *    spark-shell --jars /path/to/rapids-4-spark.jar \
 *      --conf spark.plugins=com.nvidia.spark.SQLPlugin \
 *      --conf spark.rapids.sql.enabled=true
 * 
 * 2. CPU mode:
 *    spark-shell
 * 
 * Then paste the code below to run
 */

// ==================== CONFIGURATION ====================
val DATA_PATH = "/tmp/seqfile_bench"
val NUM_FILES = 200
val RECORDS_PER_FILE = 50000
val VALUE_SIZE = 1024
val ITERATIONS = 5

// ==================== IMPORTS ====================
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import java.util.Random

// ==================== UTILITY FUNCTIONS ====================
def fmt(ns: Long): String = {
  val ms = ns / 1e6
  if (ms >= 1000) f"${ms/1000}%.2f s" else f"$ms%.1f ms"
}

def genBytes(n: Int, r: Random): Array[Byte] = {
  val b = new Array[Byte](n)
  r.nextBytes(b)
  b
}

// ==================== DATA GENERATION ====================
println("\n========== Generating Test Data ==========")

val conf = spark.sparkContext.hadoopConfiguration
val fs = FileSystem.get(conf)
val basePath = new Path(DATA_PATH)

if (fs.exists(basePath)) {
  println(s"Cleaning up old data: $DATA_PATH")
  fs.delete(basePath, true)
}
fs.mkdirs(basePath)

val rng = new Random(42)
var totalBytes = 0L
val genStart = System.nanoTime()

for (f <- 0 until NUM_FILES) {
  val fp = new Path(basePath, f"part_$f%04d.seq")
  val w = SequenceFile.createWriter(conf,
    SequenceFile.Writer.file(fp),
    SequenceFile.Writer.keyClass(classOf[BytesWritable]),
    SequenceFile.Writer.valueClass(classOf[BytesWritable]),
    SequenceFile.Writer.compression(CompressionType.NONE))
  
  try {
    for (i <- 0 until RECORDS_PER_FILE) {
      val k = Array[Byte](
        ((i >> 24) & 0xFF).toByte,
        ((i >> 16) & 0xFF).toByte,
        ((i >> 8) & 0xFF).toByte,
        (i & 0xFF).toByte)
      val v = genBytes(VALUE_SIZE + rng.nextInt(256) - 128, rng)
      w.append(new BytesWritable(k), new BytesWritable(v))
      totalBytes += k.length + v.length
    }
  } finally {
    w.close()
  }
  print(s"\rGenerating files: ${f+1}/$NUM_FILES")
}

println(s"\nDone! Total data size: ${totalBytes / 1024 / 1024} MB, Time: ${fmt(System.nanoTime() - genStart)}")
println(s"Total records: ${NUM_FILES.toLong * RECORDS_PER_FILE}")

// ==================== GPU DETECTION ====================
val hasGpu = try {
  Class.forName("com.nvidia.spark.SQLPlugin")
  spark.conf.getOption("spark.plugins").exists(_.contains("SQLPlugin"))
} catch { case _: Exception => false }

println(s"\nGPU Plugin: ${if (hasGpu) "ENABLED" else "NOT AVAILABLE"}")

// ==================== BENCHMARK FUNCTION ====================
def benchmark(name: String, iter: Int)(action: => Long): (Long, Long, Long) = {
  println(s"\n--- $name ---")
  
  // Warmup
  print("Warmup... ")
  System.gc(); Thread.sleep(300)
  action
  println("done")
  
  // Benchmark runs
  var total = 0L
  var minT = Long.MaxValue
  var maxT = Long.MinValue
  var cnt = 0L
  
  for (i <- 1 to iter) {
    System.gc(); Thread.sleep(200)
    val st = System.nanoTime()
    cnt = action
    val t = System.nanoTime() - st
    total += t
    minT = math.min(minT, t)
    maxT = math.max(maxT, t)
    println(s"  Run $i: ${fmt(t)} ($cnt records)")
  }
  
  val avg = total / iter
  println(s"  Avg: ${fmt(avg)}, Min: ${fmt(minT)}, Max: ${fmt(maxT)}")
  println(s"  Throughput: ${totalBytes / (avg / 1e9) / 1024 / 1024} MB/s")
  (avg, minT, maxT)
}

// ==================== RUN BENCHMARKS ====================
println("\n========== Starting Performance Tests ==========")

val results = scala.collection.mutable.Map[String, Long]()

// 1. RDD Scan (Baseline)
val (rddAvg, _, _) = benchmark("RDD Scan (SequenceFileAsBinaryInputFormat)", ITERATIONS) {
  spark.sparkContext.newAPIHadoopFile(
    DATA_PATH,
    classOf[SequenceFileAsBinaryInputFormat],
    classOf[BytesWritable],
    classOf[BytesWritable]
  ).count()
}
results("RDD Scan") = rddAvg

// 2. CPU FileFormat
spark.conf.set("spark.rapids.sql.enabled", "false")
val (cpuAvg, _, _) = benchmark("CPU FileFormat (SequenceFileBinary)", ITERATIONS) {
  spark.read.format("SequenceFileBinary").load(DATA_PATH).count()
}
results("CPU FileFormat") = cpuAvg

// 3. GPU FileFormat (if available)
if (hasGpu) {
  spark.conf.set("spark.rapids.sql.enabled", "true")
  
  // 3a. GPU PERFILE reader (single file per batch, no multi-threading)
  spark.conf.set("spark.rapids.sql.format.sequencefile.reader.type", "PERFILE")
  val (gpuPerFileAvg, _, _) = benchmark("GPU PERFILE", ITERATIONS) {
    spark.read.format("SequenceFileBinary").load(DATA_PATH).count()
  }
  results("GPU PERFILE") = gpuPerFileAvg
  
  // 3b. GPU COALESCING reader (sequential multi-file, good for local storage)
  spark.conf.set("spark.rapids.sql.format.sequencefile.reader.type", "COALESCING")
  val (gpuCoalesceAvg, _, _) = benchmark("GPU COALESCING", ITERATIONS) {
    spark.read.format("SequenceFileBinary").load(DATA_PATH).count()
  }
  results("GPU COALESCING") = gpuCoalesceAvg
  
  // 3c. GPU MULTITHREADED reader (parallel I/O, good for cloud storage)
  spark.conf.set("spark.rapids.sql.format.sequencefile.reader.type", "MULTITHREADED")
  val (gpuMultiAvg, _, _) = benchmark("GPU MULTITHREADED", ITERATIONS) {
    spark.read.format("SequenceFileBinary").load(DATA_PATH).count()
  }
  results("GPU MULTITHREADED") = gpuMultiAvg
  
  // 3d. GPU AUTO (default, picks best reader based on file location)
  spark.conf.set("spark.rapids.sql.format.sequencefile.reader.type", "AUTO")
  val (gpuAutoAvg, _, _) = benchmark("GPU AUTO", ITERATIONS) {
    spark.read.format("SequenceFileBinary").load(DATA_PATH).count()
  }
  results("GPU AUTO") = gpuAutoAvg
}

// ==================== COLUMN PRUNING TEST ====================
println("\n========== Column Pruning Test (value only) ==========")

spark.conf.set("spark.rapids.sql.enabled", "false")
val (cpuValueOnly, _, _) = benchmark("CPU (value only)", ITERATIONS) {
  spark.read.format("SequenceFileBinary").load(DATA_PATH).select("value").count()
}
results("CPU value-only") = cpuValueOnly

if (hasGpu) {
  spark.conf.set("spark.rapids.sql.enabled", "true")
  spark.conf.set("spark.rapids.sql.format.sequencefile.reader.type", "AUTO")
  val (gpuValueOnly, _, _) = benchmark("GPU (value only)", ITERATIONS) {
    spark.read.format("SequenceFileBinary").load(DATA_PATH).select("value").count()
  }
  results("GPU value-only") = gpuValueOnly
}

// ==================== MULTI-THREAD THREAD COUNT TEST ====================
if (hasGpu) {
  println("\n========== Multi-Thread Reader Thread Count Comparison ==========")
  spark.conf.set("spark.rapids.sql.enabled", "true")
  spark.conf.set("spark.rapids.sql.format.sequencefile.reader.type", "MULTITHREADED")
  
  for (numThreads <- Seq(2, 4, 8)) {
    spark.conf.set("spark.rapids.sql.multiThreadedRead.numThreads", numThreads.toString)
    val (threadAvg, _, _) = benchmark(s"GPU MT-$numThreads threads", ITERATIONS) {
      spark.read.format("SequenceFileBinary").load(DATA_PATH).count()
    }
    results(s"GPU MT-$numThreads threads") = threadAvg
  }
  
  // Reset to default
  spark.conf.set("spark.rapids.sql.multiThreadedRead.numThreads", "20")
  spark.conf.set("spark.rapids.sql.format.sequencefile.reader.type", "AUTO")
}

// ==================== RESULTS SUMMARY ====================
println("\n" + "=" * 70)
println("Results Summary (Baseline: RDD Scan)")
println("=" * 70)
println(f"${"Test Case"}%-25s | ${"Avg Time"}%15s | ${"Speedup vs RDD"}%15s")
println("-" * 70)

val rddBase = results.getOrElse("RDD Scan", 1L)
results.toSeq.sortBy(_._2).foreach { case (name, time) =>
  val speedup = rddBase.toDouble / time
  val speedupStr = if (name == "RDD Scan") "baseline" else f"${speedup}%.2fx"
  println(f"$name%-25s | ${fmt(time)}%15s | $speedupStr%15s")
}

println("-" * 70)

if (hasGpu) {
  val gpuPerFile = results.getOrElse("GPU PERFILE", rddBase)
  val gpuCoalesce = results.getOrElse("GPU COALESCING", rddBase)
  val gpuMulti = results.getOrElse("GPU MULTITHREADED", rddBase)
  val cpuTime = results.getOrElse("CPU FileFormat", rddBase)
  println(f"\nPerformance Summary:")
  println(f"  GPU PERFILE vs RDD:       ${rddBase.toDouble / gpuPerFile}%.2fx")
  println(f"  GPU COALESCING vs RDD:    ${rddBase.toDouble / gpuCoalesce}%.2fx")
  println(f"  GPU MULTITHREADED vs RDD: ${rddBase.toDouble / gpuMulti}%.2fx")
  println(f"  GPU MULTITHREADED vs PERFILE: ${gpuPerFile.toDouble / gpuMulti}%.2fx")
  println(f"  CPU vs RDD Scan:          ${rddBase.toDouble / cpuTime}%.2fx")
}

println(s"\nTest data path: $DATA_PATH")
println("Cleanup command: fs.delete(new Path(\"" + DATA_PATH + "\"), true)")
