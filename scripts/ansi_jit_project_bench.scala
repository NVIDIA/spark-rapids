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
 * Benchmark ANSI integral project expressions with CPU, GPU project, and GPU AST JIT project.
 *
 * Run with spark-shell, for example:
 *
 *   export CUDA129=/home/haoyangl/code/.conda/cudf-cuda12.9-envs/rapids
 *   export LD_LIBRARY_PATH=$CUDA129/lib:$CUDA129/targets/x86_64-linux/lib:$LD_LIBRARY_PATH
 *   export LIBCUDF_JIT_ENABLED=1
 *
 *   $SPARK_HOME/bin/spark-shell \
 *     --jars dist/target/rapids-4-spark_2.12-*-cuda12.jar \
 *     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
 *     --conf spark.executorEnv.LIBCUDF_JIT_ENABLED=1 \
 *     --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
 *     -i scripts/ansi_jit_project_bench.scala
 *
 * Useful knobs:
 *   BENCH_ROWS=100000000
 *   BENCH_PARTITIONS=16
 *   BENCH_EXPR_COUNTS=8
 *   BENCH_EXPR_DEPTHS=1,2,4,8,16,32
 *   BENCH_WARMUPS=1
 *   BENCH_ITERS=3
 *   BENCH_BASE_PATH=/tmp/ansi_jit_project_bench
 *   BENCH_REGENERATE_DATA=true
 *   BENCH_PRINT_PLAN=false
 *   BENCH_CONSUME_MODE=aggregate
 */

import java.io.PrintWriter
import java.nio.file.Paths
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AnsiJitProjectBench {
  val spark: SparkSession = SparkSession.active

  def env(name: String, defaultValue: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(defaultValue)

  def envBool(name: String, defaultValue: Boolean): Boolean =
    env(name, defaultValue.toString).toBoolean

  def parseInts(value: String): Seq[Int] =
    value.split(",").map(_.trim).filter(_.nonEmpty).map(_.toInt).toSeq

  val rows: Long = env("BENCH_ROWS", "100000000").toLong
  val partitions: Int = env("BENCH_PARTITIONS",
    math.max(spark.sparkContext.defaultParallelism, 1).toString).toInt
  val exprCounts: Seq[Int] = parseInts(env("BENCH_EXPR_COUNTS", "8"))
  val exprDepths: Seq[Int] = sys.env.get("BENCH_EXPR_DEPTHS").filter(_.nonEmpty)
    .orElse(sys.env.get("BENCH_EXPR_DEPTH").filter(_.nonEmpty))
    .map(parseInts)
    .getOrElse(Seq(1, 2, 4, 8, 16, 32))
  val warmups: Int = env("BENCH_WARMUPS", "1").toInt
  val iters: Int = env("BENCH_ITERS", "3").toInt
  val basePath: String =
    env("BENCH_BASE_PATH", s"/tmp/ansi_jit_project_bench_${sys.props("user.name")}")
  val regenerateData: Boolean = envBool("BENCH_REGENERATE_DATA", false)
  val printPlan: Boolean = envBool("BENCH_PRINT_PLAN", false)
  val consumeMode: String = env("BENCH_CONSUME_MODE", "aggregate").toLowerCase(Locale.ROOT)
  val dataPath: String = s"$basePath/parquet_rows_${rows}_parts_${partitions}"
  val resultCsvPath: String = s"$basePath/results_${System.currentTimeMillis()}.csv"

  case class Mode(name: String, confs: Map[String, String])

  val modes: Seq[Mode] = Seq(
    Mode("CPU", Map(
      "spark.rapids.sql.enabled" -> "false",
      "spark.rapids.sql.projectAstEnabled" -> "false",
      "spark.rapids.sql.projectAstAnsiArithmeticEnabled" -> "false")),
    Mode("GPU_PROJECT", Map(
      "spark.rapids.sql.enabled" -> "true",
      "spark.rapids.sql.projectAstEnabled" -> "false",
      "spark.rapids.sql.projectAstAnsiArithmeticEnabled" -> "false")),
    Mode("GPU_AST_JIT", Map(
      "spark.rapids.sql.enabled" -> "true",
      "spark.rapids.sql.projectAstEnabled" -> "true",
      "spark.rapids.sql.projectAstAnsiArithmeticEnabled" -> "true"))
  )

  val commonConfs: Map[String, String] = Map(
    "spark.sql.ansi.enabled" -> "true",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.rapids.sql.metrics.level" -> "DEBUG")

  case class RunResult(
      mode: String,
      exprCount: Int,
      exprDepth: Int,
      iteration: Int,
      warmup: Boolean,
      wallMs: Double,
      projectOpMs: Double,
      allGpuOpMs: Double,
      rowsSeen: Long,
      checksum: Long,
      projectNodes: String,
      planHead: String)

  case class Summary(
      mode: String,
      exprCount: Int,
      exprDepth: Int,
      avgWallMs: Double,
      minWallMs: Double,
      avgProjectOpMs: Double,
      avgAllGpuOpMs: Double,
      speedupVsCpu: Double,
      projectNodes: String)

  def getConf(key: String): Option[String] = Try(spark.conf.get(key)).toOption

  def withConfs[T](confs: Map[String, String])(body: => T): T = {
    val old = confs.keys.map(k => k -> getConf(k)).toMap
    confs.foreach { case (k, v) => spark.conf.set(k, v) }
    try {
      body
    } finally {
      old.foreach {
        case (k, Some(v)) => Try(spark.conf.set(k, v))
        case (k, None) => Try(spark.conf.unset(k))
      }
    }
  }

  def hadoopPathExists(path: String): Boolean = {
    val p = new Path(path)
    p.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(p)
  }

  def deletePath(path: String): Unit = {
    val p = new Path(path)
    p.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(p, true)
  }

  def maybeGenerateData(): Unit = {
    if (regenerateData && hadoopPathExists(dataPath)) {
      println(s"Deleting existing data: $dataPath")
      deletePath(dataPath)
    }
    if (hadoopPathExists(dataPath)) {
      println(s"Using existing parquet data: $dataPath")
      return
    }

    println(s"Generating parquet data: rows=$rows partitions=$partitions path=$dataPath")
    withConfs(commonConfs ++ Map("spark.rapids.sql.enabled" -> "false")) {
      spark.range(0, rows, 1, partitions)
        .select(
          (col("id") % lit(64)).cast(IntegerType).as("i0"),
          (((col("id") * lit(3)) + lit(7)) % lit(64)).cast(IntegerType).as("i1"),
          (((col("id") * lit(5)) + lit(11)) % lit(64)).cast(IntegerType).as("i2"),
          (((col("id") * lit(7)) + lit(13)) % lit(64)).cast(IntegerType).as("i3"),
          (col("id") % lit(1024)).cast(LongType).as("l0"),
          (((col("id") * lit(3)) + lit(7)) % lit(1024)).cast(LongType).as("l1"),
          (((col("id") * lit(5)) + lit(11)) % lit(1024)).cast(LongType).as("l2"),
          (((col("id") * lit(7)) + lit(13)) % lit(1024)).cast(LongType).as("l3"))
        .write
        .mode("overwrite")
        .parquet(dataPath)
    }
  }

  def typedLit(value: Long, dt: DataType): Column =
    lit(value).cast(dt)

  def makeExpr(idx: Int, depth: Int): Column = {
    val useLong = (idx & 1) == 1
    val dt = if (useLong) LongType else IntegerType
    val names = if (useLong) Array("l0", "l1", "l2", "l3") else Array("i0", "i1", "i2", "i3")

    var c: Column = col(names(0))
    var d = 0
    while (d < depth) {
      val k = ((idx + d) % 17) + 1
      c = d % 6 match {
        case 0 => c + col(names(1))
        case 1 => c - col(names(2))
        case 2 => abs(c - col(names(3)))
        case 3 => -c
        case 4 => c * typedLit(2, dt)
        case _ => c + typedLit(k, dt)
      }
      d += 1
    }
    c.as(s"p_$idx")
  }

  def makeQuery(exprCount: Int, exprDepth: Int): DataFrame = {
    val input = spark.read.parquet(dataPath)
    input.select((0 until exprCount).map(i => makeExpr(i, exprDepth)): _*)
  }

  def flattenPlan(plan: SparkPlan): Seq[SparkPlan] =
    plan +: plan.children.flatMap(flattenPlan)

  def metricValueByKeyOrName(plan: SparkPlan, keyPart: String, namePart: String): Long = {
    flattenPlan(plan).flatMap { node =>
      node.metrics.collect {
        case (key, metric) if key.contains(keyPart) ||
            metric.name.exists(_.toLowerCase.contains(namePart.toLowerCase)) =>
          metric.value
      }
    }.sum
  }

  def isProjectNode(plan: SparkPlan): Boolean =
    plan.nodeName.contains("Project") || plan.getClass.getSimpleName.contains("Project")

  def projectOpTimeNs(plan: SparkPlan): Long = {
    flattenPlan(plan)
      .filter(isProjectNode)
      .flatMap(_.metrics.get("opTimeLegacy").map(_.value))
      .sum
  }

  def projectNodeNames(plan: SparkPlan): String = {
    val names = flattenPlan(plan)
      .filter(isProjectNode)
      .map(p => s"${p.nodeName}:${p.getClass.getSimpleName}")
      .distinct
    if (names.isEmpty) "none" else names.mkString("+")
  }

  def longValue(v: Any): Long = v match {
    case null => 0L
    case n: java.lang.Number => n.longValue()
    case d: java.math.BigDecimal => d.longValue()
    case d: BigDecimal => d.longValue
    case other => throw new IllegalStateException(s"Unexpected aggregate value: $other")
  }

  def consumeViaAggregate(df: DataFrame): (Long, Long, SparkPlan) = {
    val aggregateColumns = count(lit(1)).as("__row_count") +:
      df.columns.map(c => sum(col(c).cast(LongType)).as(s"sum_$c"))
    val agg = df.agg(aggregateColumns.head, aggregateColumns.tail: _*)
    val row = agg.collect()(0)
    val rowsSeen = row.getLong(0)
    var checksum = 0L
    var i = 1
    while (i < row.length) {
      checksum ^= longValue(row.get(i))
      i += 1
    }
    (rowsSeen, checksum, agg.queryExecution.executedPlan)
  }

  def columnarPlan(plan: SparkPlan): Option[SparkPlan] = {
    if (plan.supportsColumnar) {
      Some(plan)
    } else if (plan.nodeName.contains("ColumnarToRow") && plan.children.size == 1 &&
        plan.children.head.supportsColumnar) {
      Some(plan.children.head)
    } else {
      None
    }
  }

  def consumeViaPlan(df: DataFrame, label: String): (Long, Long, SparkPlan) = {
    val queryExecution = df.queryExecution
    val plan = queryExecution.executedPlan
    val rowsSeen = SQLExecution.withNewExecutionId(queryExecution, Some(label)) {
      columnarPlan(plan) match {
        case Some(columnar) =>
          columnar.executeColumnar().mapPartitions { batches =>
            var rows = 0L
            while (batches.hasNext) {
              val batch = batches.next()
              try {
                rows += batch.numRows().toLong
              } finally {
                batch.close()
              }
            }
            Iterator.single(rows)
          }.collect().sum
        case None =>
          plan.execute().mapPartitions { rowsIter =>
            var rows = 0L
            while (rowsIter.hasNext) {
              rowsIter.next()
              rows += 1
            }
            Iterator.single(rows)
          }.collect().sum
      }
    }
    (rowsSeen, rowsSeen, plan)
  }

  def consume(df: DataFrame, label: String): (Long, Long, SparkPlan) = consumeMode match {
    case "plan" => consumeViaPlan(df, label)
    case "aggregate" | "agg" => consumeViaAggregate(df)
    case other => throw new IllegalArgumentException(
      s"Unknown BENCH_CONSUME_MODE=$other. Expected plan or aggregate.")
  }

  def runOne(
      mode: Mode,
      exprCount: Int,
      exprDepth: Int,
      iteration: Int,
      warmup: Boolean): RunResult = {
    val label = s"${mode.name}_exprs_${exprCount}_depth_${exprDepth}_" +
      s"${if (warmup) "warmup" else s"iter_$iteration"}"
    withConfs(commonConfs ++ mode.confs) {
      spark.sparkContext.setJobDescription(label)
      spark.sparkContext.setLocalProperty("spark.job.description", label)

      val df = makeQuery(exprCount, exprDepth)
      val startNs = System.nanoTime()
      val (rowsSeen, checksum, plan) = consume(df, label)
      val wallMs = (System.nanoTime() - startNs).toDouble / 1000000.0
      val projectMs = projectOpTimeNs(plan).toDouble / 1000000.0
      val allGpuMs = metricValueByKeyOrName(plan, "opTime", "op time").toDouble / 1000000.0
      val nodes = projectNodeNames(plan)
      val head = plan.treeString.split('\n').take(8).mkString(" | ")
      if (printPlan) {
        println(s"plan[$label]:")
        println(plan.treeString)
      }

      spark.sparkContext.setLocalProperty("spark.job.description", null)
      RunResult(mode.name, exprCount, exprDepth, iteration, warmup, wallMs, projectMs, allGpuMs,
        rowsSeen, checksum, nodes, head)
    }
  }

  def summarize(results: Seq[RunResult]): Seq[Summary] = {
    val measured = results.filterNot(_.warmup)
    val byKey = measured.groupBy(r => (r.mode, r.exprCount, r.exprDepth))
    val cpuByExpr = byKey.collect {
      case (("CPU", exprCount, exprDepth), rs) =>
        (exprCount, exprDepth) -> (rs.map(_.wallMs).sum / rs.size)
    }

    byKey.toSeq.map { case ((mode, exprCount, exprDepth), rs) =>
      val avgWall = rs.map(_.wallMs).sum / rs.size
      val minWall = rs.map(_.wallMs).min
      val avgProject = rs.map(_.projectOpMs).sum / rs.size
      val avgAllGpu = rs.map(_.allGpuOpMs).sum / rs.size
      val speedup = cpuByExpr.get((exprCount, exprDepth)).map(_ / avgWall).getOrElse(Double.NaN)
      val nodes = rs.map(_.projectNodes).distinct.mkString("/")
      Summary(mode, exprCount, exprDepth, avgWall, minWall, avgProject, avgAllGpu, speedup, nodes)
    }.sortBy(s => (s.exprCount, s.exprDepth, modes.indexWhere(_.name == s.mode)))
  }

  def printTable(summaries: Seq[Summary]): Unit = {
    println()
    println("=== ANSI JIT Project Benchmark Summary ===")
    println(f"${"exprs"}%8s  ${"depth"}%6s  ${"mode"}%-12s  ${"avg_wall_ms"}%12s  " +
      f"${"min_wall_ms"}%12s  ${"speedup"}%8s  ${"project_op_ms"}%14s  " +
      f"${"all_gpu_op_ms"}%14s  ${"project_nodes"}")
    summaries.foreach { s =>
      val speed = if (s.speedupVsCpu.isNaN) "n/a" else f"${s.speedupVsCpu}%.2f"
      println(f"${s.exprCount}%8d  ${s.exprDepth}%6d  ${s.mode}%-12s  " +
        f"${s.avgWallMs}%12.1f  ${s.minWallMs}%12.1f  ${speed}%8s  " +
        f"${s.avgProjectOpMs}%14.1f  ${s.avgAllGpuOpMs}%14.1f  ${s.projectNodes}")
    }
    println()
    println("Notes:")
    println("  project_op_ms is read from Spark SQL metrics on Project/GpuProject nodes.")
    println("  These are the same SQL metric accumulators written to the Spark event log.")
    println("  For GPU_AST_JIT, project op time includes AST compile and compute_column JIT work.")
    println("  With BENCH_CONSUME_MODE=aggregate, CPU and GPU run the same SQL query end to end.")
    println("  With BENCH_CONSUME_MODE=plan, wall time is a lower-level Project materialization probe.")
  }

  def writeCsv(results: Seq[RunResult], summaries: Seq[Summary]): Unit = {
    val parent = Paths.get(resultCsvPath).getParent.toFile
    parent.mkdirs()

    val out = new PrintWriter(resultCsvPath)
    try {
      out.println("kind,mode,expr_count,expr_depth,iteration,warmup,wall_ms,project_op_ms," +
        "all_gpu_op_ms,rows_seen,checksum,project_nodes")
      results.foreach { r =>
        out.println(Seq("run", r.mode, r.exprCount, r.exprDepth, r.iteration, r.warmup, r.wallMs,
          r.projectOpMs, r.allGpuOpMs, r.rowsSeen, r.checksum,
          r.projectNodes.replace(',', ';')).mkString(","))
      }
      summaries.foreach { s =>
        out.println(Seq("summary", s.mode, s.exprCount, s.exprDepth, "", "", s.avgWallMs,
          s.avgProjectOpMs, s.avgAllGpuOpMs, "", "", s.projectNodes.replace(',', ';')).mkString(","))
      }
    } finally {
      out.close()
    }
    println(s"CSV written to: $resultCsvPath")
  }

  def printEnvironment(): Unit = {
    println("=== ANSI JIT Project Benchmark Config ===")
    println(s"rows=$rows")
    println(s"partitions=$partitions")
    println(s"exprCounts=${exprCounts.mkString(",")}")
    println(s"exprDepths=${exprDepths.mkString(",")}")
    println(s"warmups=$warmups")
    println(s"iters=$iters")
    println(s"dataPath=$dataPath")
    println(s"appId=${spark.sparkContext.applicationId}")
    println(s"eventLog.enabled=${getConf("spark.eventLog.enabled").getOrElse("false")}")
    println(s"eventLog.dir=${getConf("spark.eventLog.dir").getOrElse("(unset)")}")
    println(s"printPlan=$printPlan")
    println(s"consumeMode=$consumeMode")
    println(s"LIBCUDF_JIT_ENABLED=${sys.env.getOrElse("LIBCUDF_JIT_ENABLED", "(unset)")}")
    println(s"LD_LIBRARY_PATH=${sys.env.getOrElse("LD_LIBRARY_PATH", "(unset)")}")
    if (sys.env.get("LIBCUDF_JIT_ENABLED") != Some("1")) {
      println("WARNING: LIBCUDF_JIT_ENABLED is not 1. GPU_AST_JIT is expected to fail.")
    }
  }

  def run(): Unit = {
    printEnvironment()
    maybeGenerateData()

    val results = ArrayBuffer.empty[RunResult]
    exprCounts.foreach { exprCount =>
      exprDepths.foreach { exprDepth =>
        modes.foreach { mode =>
          println()
          println(s"--- mode=${mode.name} exprCount=$exprCount exprDepth=$exprDepth ---")
          (0 until warmups).foreach { w =>
            val r = runOne(mode, exprCount, exprDepth, w, warmup = true)
            results += r
            println(f"warmup[$w] wall=${r.wallMs}%.1f ms projectOp=${r.projectOpMs}%.1f ms " +
              s"rows=${r.rowsSeen} nodes=${r.projectNodes}")
          }
          (0 until iters).foreach { i =>
            val r = runOne(mode, exprCount, exprDepth, i, warmup = false)
            results += r
            println(f"iter[$i] wall=${r.wallMs}%.1f ms projectOp=${r.projectOpMs}%.1f ms " +
              f"allGpuOp=${r.allGpuOpMs}%.1f ms rows=${r.rowsSeen} checksum=${r.checksum} " +
              s"nodes=${r.projectNodes}")
          }
        }
      }
    }

    val summaries = summarize(results.toSeq)
    printTable(summaries)
    writeCsv(results.toSeq, summaries)
  }
}

AnsiJitProjectBench.run()
