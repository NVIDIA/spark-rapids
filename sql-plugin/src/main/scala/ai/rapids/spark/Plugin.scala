/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.cudf.{Cuda, DType, Rmm, RmmAllocationMode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.sources.v2.reader.Scan
import org.apache.spark.sql.types._
import org.apache.spark.{ExecutorPlugin, SparkConf, SparkEnv}

trait GpuExec extends SparkPlan {
  override def supportsColumnar = true

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuExec]
  }

  override def hashCode(): Int = super.hashCode()
}

class CannotReplaceException(str: String) extends RuntimeException(str) {

}

case class GpuOverrides(session: SparkSession) extends Rule[SparkPlan] with Logging {
  var incompatEnabled: Boolean = false
  var inputExecEnabled: Boolean = true

  def areAllSupportedTypes(types: DataType*): Boolean = {
    types.forall(_ match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case DateType => true
      case TimestampType => false // true we really need to understand how the timezone works with this
      case StringType => true
      case _ => false
    })
  }

  def replaceIncompatUnaryExpressions(exp: UnaryExpression, child: GpuExpression): GpuExpression = exp match {
      // for all of the following the floating point results are not exact
    case atan: Atan => new GpuAtan(child)
    case cos: Cos => new GpuCos(child)
    case exp: Exp => new GpuExp(child)
    case log: Log => new GpuLog(child)
    case sin: Sin => new GpuSin(child)
    case tan: Tan => new GpuTan(child)
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceUnaryExpressions(exp: UnaryExpression, child: GpuExpression): GpuExpression = exp match {
    case cast: Cast if GpuCast.canCast(cast.child.dataType, cast.dataType) =>
      new GpuCast(child, cast.dataType, cast.timeZoneId)
    case min: UnaryMinus => new GpuUnaryMinus(child)
    case plus: UnaryPositive => new GpuUnaryPositive(child)
    case year: Year => new GpuYear(child)
    case month: Month => new GpuMonth(child)
    case day: DayOfMonth => new GpuDayOfMonth(child)
    case abs: Abs => new GpuAbs(child)
    case acos: Acos => new GpuAcos(child)
    case asin: Asin => new GpuAsin(child)
    case sqrt: Sqrt => new GpuSqrt(child)
    case floor: Floor => new GpuFloor(child)
    case ceil: Ceil => new GpuCeil(child)
    case not: Not => new GpuNot(child)
    case isNull: IsNull => new GpuIsNull(child)
    case isNotNull: IsNotNull => new GpuIsNotNull(child)
    case exp if incompatEnabled => replaceIncompatUnaryExpressions(exp, child)
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceIncompatBinaryExpressions(exp: BinaryExpression,
      left: GpuExpression, right:GpuExpression): GpuExpression = exp match {
    // floating point results are not always bit for bit exact
    case pow: Pow => new GpuPow(left, right)
    // divide by 0 results in null for spark but -Infinity for cudf
    case div: Divide => new GpuDivide(left, right)
    // divide by 0 results in null for spark but -1 for cudf
    case div: IntegralDivide => new GpuIntegralDivide(left, right)
    // divide by 0 results in null for spark, but not for cudf
    case rem: Remainder => new GpuRemainder(left, right)
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceBinaryExpressions(exp: BinaryExpression,
      left: GpuExpression, right:GpuExpression): GpuExpression = exp match {
    case add: Add => new GpuAdd(left, right)
    case sub: Subtract => new GpuSubtract(left, right)
    case mul: Multiply => new GpuMultiply(left, right)
    case and: And => new GpuAnd(left, right)
    case or: Or => new GpuOr(left, right)
    case eq: EqualTo => new GpuEqualTo(left, right)
    case gt: GreaterThan => new GpuGreaterThan(left, right)
    case geq: GreaterThanOrEqual => new GpuGreaterThanOrEqual(left, right)
    case lt: LessThan => new GpuLessThan(left, right)
    case leq: LessThanOrEqual => new GpuLessThanOrEqual (left, right)
    case exp if incompatEnabled => replaceIncompatBinaryExpressions(exp, left, right)
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceWithGpuExpression(exp: Expression): GpuExpression = {
    if (!areAllSupportedTypes(exp.dataType)) {
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} produces an unsupported type ${exp.dataType}")
    }
    exp match {
      case a: Alias =>
        new GpuAlias(replaceWithGpuExpression(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      case att: AttributeReference =>
        new GpuAttributeReference(att.name, att.dataType, att.nullable,
          att.metadata)(att.exprId, att.qualifier)
      case lit: Literal =>
        new GpuLiteral(lit.value, lit.dataType)
      case exp: UnaryExpression if areAllSupportedTypes(exp.child.dataType) =>
        replaceUnaryExpressions(exp, replaceWithGpuExpression(exp.child))
      case exp: BinaryExpression if (areAllSupportedTypes(exp.left.dataType, exp.right.dataType)) =>
        replaceBinaryExpressions(exp,
          replaceWithGpuExpression(exp.left), replaceWithGpuExpression(exp.right))
      case exp =>
        throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
    }
  }

  def replaceBatchScan(scan: Scan): Scan = scan match {
    case scan: CSVScan =>
      GpuCSVScan.assertCanSupport(scan)
      new GpuCSVScan(scan.sparkSession,
        scan.fileIndex,
        scan.dataSchema,
        scan.readDataSchema,
        scan.readPartitionSchema,
        scan.options)
    case scan: ParquetScan =>
      GpuParquetScan.assertCanSupport(scan)
      new GpuParquetScan(scan.sparkSession,
        scan.hadoopConf,
        scan.fileIndex,
        scan.dataSchema,
        scan.readDataSchema,
        scan.readPartitionSchema,
        scan.pushedFilters,
        scan.options)
    case scan: OrcScan =>
      GpuOrcScan.assertCanSupport(scan)
      new GpuOrcScan(scan.sparkSession,
        scan.hadoopConf,
        scan.fileIndex,
        scan.dataSchema,
        scan.readDataSchema,
        scan.readPartitionSchema,
        scan.options,
        scan.pushedFilters)
    case _ =>
      throw new CannotReplaceException(s"scan ${scan.getClass} ${scan} is not currently supported.")
  }

  def replaceWithGpuPartitioning(part: Partitioning): Partitioning = part match {
    case hp: HashPartitioning =>
      if (hp.expressions.map(_.dataType).contains(StringType)) {
        throw new CannotReplaceException("strings are not supported as the keys for hash partitioning.")
      }
      new GpuHashPartitioning(hp.expressions.map(replaceWithGpuExpression), hp.numPartitions)
    case _ =>
      throw new CannotReplaceException(s"${part.getClass} is not supported for partitioning")
  }

  def replaceWithGpuPlan(plan: SparkPlan): SparkPlan =
    try {
      if (!areAllSupportedTypes(plan.output.map(_.dataType) :_*)) {
        throw new CannotReplaceException("unsupported data types in its output")
      }
      if (!areAllSupportedTypes(plan.children.flatMap(_.output.map(_.dataType)) :_*)) {
        throw new CannotReplaceException("unsupported data types in its input")
      }
      plan match {
        case plan: ProjectExec =>
          new GpuProjectExec(plan.projectList.map(replaceWithGpuExpression),
            replaceWithGpuPlan(plan.child))
        case exec : BatchScanExec =>
          if (!inputExecEnabled) {
            throw new CannotReplaceException(s"GPU input parsing has been disabled, to enable it" +
              s" set ${Plugin.INPUT_EXECS_CONF} to true")
          }
          new GpuBatchScanExec(exec.output.map((exec) => replaceWithGpuExpression(exec).asInstanceOf[AttributeReference]),
            replaceBatchScan(exec.scan))
        case filter: FilterExec =>
          new GpuFilterExec(replaceWithGpuExpression(filter.condition),
            replaceWithGpuPlan(filter.child))
        case shuffle: ShuffleExchangeExec =>
          new GpuShuffleExchangeExec(replaceWithGpuPartitioning(shuffle.outputPartitioning),
            replaceWithGpuPlan(shuffle.child), shuffle.canChangeNumPartitions)
        case union: UnionExec =>
          new GpuUnionExec(union.children.map(replaceWithGpuPlan))
        case p =>
          logWarning(s"GPU Processing for ${p.getClass} is not currently supported.")
          p.withNewChildren(p.children.map(replaceWithGpuPlan))
      }
    } catch {
      case exp: CannotReplaceException =>
        logWarning(s"GPU processing for ${plan.getClass} is not currently supported" +
          s" because ${exp.getMessage}")
        plan.withNewChildren(plan.children.map(replaceWithGpuPlan))
    }

  override def apply(plan: SparkPlan) :SparkPlan = {
    if (Plugin.isSqlEnabled(session)) {
      incompatEnabled = Plugin.isIncompatEnabled(session)
      inputExecEnabled = Plugin.isInputExecEnabled(session)
      replaceWithGpuPlan(plan)
    } else {
      plan
    }
  }
}

case class GpuTransitionOverrides(session: SparkSession) extends Rule[SparkPlan] {
  def optimizeGpuPlanTransitions(plan: SparkPlan): SparkPlan = plan match {
    case HostColumnarToGpu(r2c: RowToColumnarExec) =>
      new GpuRowToColumnarExec(optimizeGpuPlanTransitions(r2c.child))
    case ColumnarToRowExec(bb: GpuBringBackToHost) =>
      new GpuColumnarToRowExec(optimizeGpuPlanTransitions(bb.child))
    case p =>
      p.withNewChildren(p.children.map(optimizeGpuPlanTransitions))
  }

  /**
   * Inserts a transition to be running on the CPU columnar
   */
  private def insertColumnarFromGpu(plan: SparkPlan): SparkPlan = {
    if (plan.supportsColumnar && plan.isInstanceOf[GpuExec]) {
      GpuBringBackToHost(insertColumnarToGpu(plan))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarFromGpu))
    }
  }

  /**
   * Inserts a transition to be running on the GPU from CPU columnar
   */
  private def insertColumnarToGpu(plan: SparkPlan): SparkPlan = {
    if (plan.supportsColumnar && !plan.isInstanceOf[GpuExec]) {
      HostColumnarToGpu(insertColumnarFromGpu(plan))
    } else {
      plan.withNewChildren(plan.children.map(insertColumnarToGpu))
    }
  }

  def assertIsOnTheGpu(exp: Expression): Unit = {
    if (!exp.isInstanceOf[GpuExpression]) {
      throw new IllegalArgumentException(s"The expression ${exp} is not columnar ${exp.getClass}")
    }
  }

  def assertIsOnTheGpu(plan: SparkPlan): Unit = {
    plan match {
      case lts: LocalTableScanExec =>
        if (!lts.expressions.forall(_.isInstanceOf[AttributeReference])) {
          throw new IllegalArgumentException("It looks like some operations were " +
            s"pushed down to LocalTableScanExec ${lts.expressions.mkString(",")}")
        }
      case _: GpuColumnarToRowExec => () // Ignored
      case _: ShuffleExchangeExec => () // Ignored for now
      case _: HashAggregateExec => () // Ignored for now
      case _ =>
        if (!plan.supportsColumnar) {
          throw new IllegalArgumentException(s"Part of the plan is not columnar ${plan.getClass}\n${plan}")
        }
        plan.expressions.foreach(assertIsOnTheGpu)
    }
    plan.children.foreach(assertIsOnTheGpu)
  }

  override def apply(plan: SparkPlan) :SparkPlan = {
    if (Plugin.isSqlEnabled(session)) {
      val tmp = insertColumnarFromGpu(plan)
      val ret = optimizeGpuPlanTransitions(tmp)
      if (Plugin.isTestEnabled(session)) {
        assertIsOnTheGpu(ret)
      }
      ret
    } else {
      plan
    }
  }
}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  val overrides = GpuOverrides(session)
  val overrideTransitions = GpuTransitionOverrides(session)

  override def preColumnarTransitions : Rule[SparkPlan] = overrides

  override def postColumnarTransitions: Rule[SparkPlan] = overrideTransitions
}

object Plugin {
  val SQL_ENABLED_CONF: String = "spark.rapids.sql.enabled"
  // Some operations are currently not 100% compatible with spark.  This will enable
  // those operations if a customer is willing to work around compatibility issues for
  // more processing on the GPU
  val INCOMPATIBLE_OPS_CONF: String = "spark.rapids.sql.incompatible_ops"
  val INPUT_EXECS_CONF: String = "spark.rapids.sql.input_parsing"
  val TEST_CONF: String = "spark.rapids.sql.testing"

  def isSqlEnabled(session: SparkSession): Boolean = session.sqlContext.
    getConf(SQL_ENABLED_CONF, "true").trim.toBoolean

  def isInputExecEnabled(session: SparkSession): Boolean = session.sqlContext.
    getConf(INPUT_EXECS_CONF, "true").trim.toBoolean

  def isIncompatEnabled(session: SparkSession): Boolean = session.sqlContext.
    getConf(INCOMPATIBLE_OPS_CONF, "false").trim.toBoolean

  def isTestEnabled(session: SparkSession): Boolean = session.sqlContext.
    getConf(TEST_CONF, "false").trim.toBoolean
}

/**
  * Extension point to enable GPU processing.
  *
  * To run on a GPU set spark.sql.extensions to ai.rapids.spark.Plugin
  */
class Plugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logWarning("Installing extensions to enable rapids GPU SQL support." +
      " To disable GPU support set `spark.rapids.sql.enabled` to false")
    extensions.injectColumnar((session) => ColumnarOverrideRules(session))
  }
}

object GpuResourceManager {
  val MEM_DEBUG_CONF: String = "spark.rapids.memory_debug"

  def isMemDebugEnabled(conf: SparkConf): Boolean =
    conf.getBoolean(MEM_DEBUG_CONF, false)
}

/**
 * Config to enable pooled GPU memory allocation which can improve performance.  This should be off
 * if you want to use operators that also use GPU memory like XGBoost or Tensorflow, as the pool
 * it allocates cannot be used by other tools.
 *
 * To enable this set spark.executor.plugins to ai.rapids.spark.GpuResourceManager
 */
class GpuResourceManager extends ExecutorPlugin with Logging {
  var loggingEnabled = false

  override def init(): Unit = synchronized {
    // We eventually will need a way to know which GPU to use/etc, but for now, we will just
    // go with the default GPU.
    if (!Rmm.isInitialized) {
      val env = SparkEnv.get
      val conf = env.conf
      loggingEnabled = GpuResourceManager.isMemDebugEnabled(conf)
      val info = Cuda.memGetInfo()
      val initialAllocation = info.free / 4
      logInfo(s"Initializing RMM ${initialAllocation / 1024 / 1024.0} MB")
      try {
        Rmm.initialize(RmmAllocationMode.POOL, loggingEnabled, initialAllocation)
      } catch {
        case e: Exception => logError("Could not initialize RMM", e)
      }
    }
  }

  override def shutdown(): Unit = {
    if (loggingEnabled) {
      logWarning(s"RMM LOG\n${Rmm.getLog}")
    }
  }
}
