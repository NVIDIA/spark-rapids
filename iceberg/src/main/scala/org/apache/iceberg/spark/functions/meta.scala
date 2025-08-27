package org.apache.iceberg.spark.functions

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke

class GpuStaticInvokeMeta(expr: StaticInvoke,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule) extends BaseExprMeta[StaticInvoke](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    if (expr.staticObject != classOf[BucketFunction.BucketInt] ||
        expr.staticObject != classOf[BucketFunction.BucketLong]) {
      willNotWorkOnGpu(s"only BucketFunction.BucketInt and BucketFunction.BucketLong are supported")
    }
  }

  override def convertToGpu(): Expression = {
    expr.staticObject match {
      case cls if cls == classOf[BucketFunction.BucketInt] =>
        GpuBucketExpression(expr.arguments.head, expr.arguments(1))
      case cls if cls == classOf[BucketFunction.BucketLong] =>
        GpuBucketExpression(expr.arguments.head, expr.arguments(1))
      case _ =>
        throw new IllegalStateException(
          s"only BucketFunction.BucketInt and BucketFunction.BucketLong are supported")
    }
  }
}

