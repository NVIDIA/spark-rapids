package org.apache.iceberg.spark.functions

import com.nvidia.spark.rapids.{DataFromReplacementRule, ExprMeta, GpuExpression, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke

class GpuStaticInvokeMeta(expr: StaticInvoke,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: DataFromReplacementRule) extends ExprMeta[StaticInvoke](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    if (expr.staticObject != classOf[BucketFunction.BucketInt] &&
        expr.staticObject != classOf[BucketFunction.BucketLong]) {
      willNotWorkOnGpu(s"only BucketFunction.BucketInt and BucketFunction.BucketLong are supported")
    }
  }

  override def convertToGpu(): GpuExpression = {
    val Seq(left, right) = childExprs.map(_.convertToGpu())
    GpuBucketExpression(left, right)
  }
}