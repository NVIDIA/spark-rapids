/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.python.{AggregateInPandasExec, ArrowEvalPythonExec, FlatMapGroupsInPandasExec, WindowInPandasExec}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

object ShimOverrides {

  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    GpuOverrides.expr[KnownNotNull](
      "Tag an expression as known to not be null",
      ExprChecks.unaryProjectInputMatchesOutput(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY + TypeSig.CALENDAR +
          TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT).nested(), TypeSig.all),
      (k, conf, p, r) => new UnaryExprMeta[KnownNotNull](k, conf, p, r) {
      }),
     GpuOverrides.expr[WeekDay](
      "Returns the day of the week (0 = Monday...6=Sunday)",
      ExprChecks.unaryProject(TypeSig.INT, TypeSig.INT,
        TypeSig.DATE, TypeSig.DATE),
      (a, conf, p, r) => new UnaryExprMeta[WeekDay](a, conf, p, r) {
      }),
    GpuOverrides.expr[ElementAt](
      "Returns element of array at given(1-based) index in value if column is array. " +
        "Returns value for the given key in value if column is map",
      ExprChecks.binaryProject(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
          TypeSig.DECIMAL_128 + TypeSig.MAP).nested(), TypeSig.all,
        ("array/map", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
          TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP) +
          TypeSig.MAP.nested(TypeSig.STRING)
            .withPsNote(TypeEnum.MAP ,"If it's map, only string is supported."),
          TypeSig.ARRAY.nested(TypeSig.all) + TypeSig.MAP.nested(TypeSig.all)),
        ("index/key", (TypeSig.lit(TypeEnum.INT) + TypeSig.lit(TypeEnum.STRING))
          .withPsNote(TypeEnum.INT, "ints are only supported as array indexes, " +
            "not as maps keys")
          .withPsNote(TypeEnum.STRING, "strings are only supported as map keys, " +
            "not array indexes"),
          TypeSig.all)),
      (in, conf, p, r) => new BinaryExprMeta[ElementAt](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          // To distinguish the supported nested type between Array and Map
          val checks = in.left.dataType match {
            case _: MapType =>
              // Match exactly with the checks for GetMapValue
              ExprChecks.binaryProject(TypeSig.STRING, TypeSig.all,
                ("map", TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.all)),
                ("key", TypeSig.lit(TypeEnum.STRING), TypeSig.all))
            case _: ArrayType =>
              // Match exactly with the checks for GetArrayItem
              ExprChecks.binaryProject(
                (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.NULL +
                  TypeSig.DECIMAL_128 + TypeSig.MAP).nested(),
                TypeSig.all,
                ("array", TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.ARRAY +
                  TypeSig.STRUCT + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.MAP),
                  TypeSig.ARRAY.nested(TypeSig.all)),
                ("ordinal", TypeSig.lit(TypeEnum.INT), TypeSig.INT))
            case _ => throw new IllegalStateException("Only Array or Map is supported as input.")
          }
          checks.tag(this)
        }
      }),
    GpuOverrides.expr[ArrayMin](
      "Returns the minimum value in the array",
      ExprChecks.unaryProject(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
        TypeSig.orderable,
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL)
            .withPsNote(TypeEnum.DOUBLE, GpuOverrides.nanAggPsNote)
            .withPsNote(TypeEnum.FLOAT, GpuOverrides.nanAggPsNote),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      (in, conf, p, r) => new UnaryExprMeta[ArrayMin](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          GpuOverrides.checkAndTagFloatNanAgg("Min", in.dataType, conf, this)
        }
      }),
    GpuOverrides.expr[ArrayMax](
      "Returns the maximum value in the array",
      ExprChecks.unaryProject(
        TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL,
        TypeSig.orderable,
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL)
            .withPsNote(TypeEnum.DOUBLE, GpuOverrides.nanAggPsNote)
            .withPsNote(TypeEnum.FLOAT, GpuOverrides.nanAggPsNote),
        TypeSig.ARRAY.nested(TypeSig.orderable)),
      (in, conf, p, r) => new UnaryExprMeta[ArrayMax](in, conf, p, r) {
        override def tagExprForGpu(): Unit = {
          GpuOverrides.checkAndTagFloatNanAgg("Max", in.dataType, conf, this)
        }

      }),
    GpuOverrides.expr[LambdaFunction](
      "Holds a higher order SQL function",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all,
        Seq(ParamCheck("function",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY +
              TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all)),
        Some(RepeatingParamCheck("arguments",
          (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY +
              TypeSig.STRUCT + TypeSig.MAP).nested(),
          TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[LambdaFunction](in, conf, p, r) {
      }),
    GpuOverrides.expr[NamedLambdaVariable](
      "A parameter to a higher order SQL function",
      ExprChecks.projectOnly(
        (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY +
            TypeSig.STRUCT + TypeSig.MAP).nested(),
        TypeSig.all),
      (in, conf, p, r) => new ExprMeta[NamedLambdaVariable](in, conf, p, r) {
      }),
    GpuOverrides.expr[ArrayTransform](
      "Transform elements in an array using the transform function. This is similar to a `map` " +
          "in functional programming",
      ExprChecks.projectOnly(TypeSig.ARRAY.nested(TypeSig.commonCudfTypes +
        TypeSig.DECIMAL_128 + TypeSig.NULL + TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
        TypeSig.ARRAY.nested(TypeSig.all),
        Seq(
          ParamCheck("argument",
            TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP),
            TypeSig.ARRAY.nested(TypeSig.all)),
          ParamCheck("function",
            (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
                TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(),
            TypeSig.all))),
      (in, conf, p, r) => new ExprMeta[ArrayTransform](in, conf, p, r) {
      }),
    GpuOverrides.expr[Concat](
      "List/String concatenate",
      ExprChecks.projectOnly((TypeSig.STRING + TypeSig.ARRAY).nested(
        TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
        (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.STRING + TypeSig.ARRAY).nested(
            TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128),
          (TypeSig.STRING + TypeSig.BINARY + TypeSig.ARRAY).nested(TypeSig.all)))),
      (a, conf, p, r) => new ComplexTypeMergingExprMeta[Concat](a, conf, p, r) {
      }),
    GpuOverrides.expr[PythonUDF](
      "UDF run in an external python process. Does not actually run on the GPU, but " +
          "the transfer of data to/from it can be accelerated",
      ExprChecks.fullAggAndProject(
        // Different types of Pandas UDF support different sets of output type. Please refer to
        //   https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py#L98
        // for more details.
        // It is impossible to specify the exact type signature for each Pandas UDF type in a single
        // expression 'PythonUDF'.
        // So use the 'unionOfPandasUdfOut' to cover all types for Spark. The type signature of
        // plugin is also an union of all the types of Pandas UDF.
        (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested() + TypeSig.STRUCT,
        TypeSig.unionOfPandasUdfOut,
        repeatingParamCheck = Some(RepeatingParamCheck(
          "param",
          (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ExprMeta[PythonUDF](a, conf, p, r) {
        override def replaceMessage: String = "not block GPU acceleration"
        override def noReplacementPossibleMessage(reasons: String): String =
          s"blocks running on GPU because $reasons"
        }),
    GpuOverrides.expr[ReplicateRows](
      "Given an input row replicates the row N times",
      ExprChecks.projectOnly(
        // The plan is optimized to run HashAggregate on the rows to be replicated.
        // HashAggregateExec doesn't support grouping by 128-bit decimal value yet.
        // Issue to track decimal 128 support: https://github.com/NVIDIA/spark-rapids/issues/4410
        TypeSig.ARRAY.nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT),
        TypeSig.ARRAY.nested(TypeSig.all),
        repeatingParamCheck = Some(RepeatingParamCheck("input",
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 +
              TypeSig.ARRAY + TypeSig.STRUCT).nested(),
          TypeSig.all))),
      (a, conf, p, r) => new ReplicateRowsExprMeta[ReplicateRows](a, conf, p, r) {
     })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap

  val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[ArrowEvalPythonExec](
      "The backend of the Scalar Pandas UDFs. Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY + TypeSig.STRUCT).nested(),
        TypeSig.all),
      (e, conf, p, r) =>
        new SparkPlanMeta[ArrowEvalPythonExec](e, conf, p, r) {
          val udfs: Seq[BaseExprMeta[PythonUDF]] =
            e.udfs.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          val resultAttrs: Seq[BaseExprMeta[Attribute]] =
            e.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
          override val childExprs: Seq[BaseExprMeta[_]] = udfs ++ resultAttrs
          override def replaceMessage: String = "partially run on GPU"
          override def noReplacementPossibleMessage(reasons: String): String =
            s"cannot run even partially on the GPU because $reasons"
      }),
    GpuOverrides.exec[FlatMapGroupsInPandasExec](
      "The backend for Flat Map Groups Pandas UDF, Accelerates the data transfer between the" +
        " Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (flatPy, conf, p, r) => new SparkPlanMeta[FlatMapGroupsInPandasExec](flatPy, conf, p, r) {
        override def replaceMessage: String = "partially run on GPU"
        override def noReplacementPossibleMessage(reasons: String): String =
          s"cannot run even partially on the GPU because $reasons"

        private val groupingAttrs: Seq[BaseExprMeta[Attribute]] =
          flatPy.groupingAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        private val udf: BaseExprMeta[PythonUDF] = GpuOverrides.wrapExpr(
          flatPy.func.asInstanceOf[PythonUDF], conf, Some(this))

        private val resultAttrs: Seq[BaseExprMeta[Attribute]] =
          flatPy.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

        override val childExprs: Seq[BaseExprMeta[_]] = groupingAttrs ++ resultAttrs :+ udf
      }),
    GpuOverrides.exec[WindowInPandasExec](
      "The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between" +
        " the Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled. For now it only supports row based window frame.",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.ARRAY).nested(TypeSig.commonCudfTypes),
        TypeSig.all),
      (winPy, conf, p, r) => new GpuWindowInPandasExecMetaBase(winPy, conf, p, r) {
        override val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
          winPy.windowExpression.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      }).disabledByDefault("it only supports row based frame for now"),
    GpuOverrides.exec[AggregateInPandasExec](
      "The backend for an Aggregation Pandas UDF, this accelerates the data transfer between" +
        " the Java process and the Python process. It also supports scheduling GPU resources" +
        " for the Python process when enabled.",
      ExecChecks(TypeSig.commonCudfTypes, TypeSig.all),
      (aggPy, conf, p, r) => new GpuAggregateInPandasExecMeta(aggPy, conf, p, r))
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap

}

abstract class GpuWindowInPandasExecMetaBase(
    winPandas: WindowInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[WindowInPandasExec](winPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]]

  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    winPandas.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    winPandas.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  // Same check with that in GpuWindowExecMeta
  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing" +
        " Pandas UDF; cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))

    // Early check for the frame type, only supporting RowFrame for now, which is different from
    // the node GpuWindowExec.
    windowExpressions
      .flatMap(meta => meta.wrapped.collect { case e: SpecifiedWindowFrame => e })
      .filter(swf => swf.frameType.equals(RangeFrame))
      .foreach(rf => willNotWorkOnGpu(because = s"Only support RowFrame for now," +
        s" but found ${rf.frameType}"))
  }
}


class GpuAggregateInPandasExecMeta(
    aggPandas: AggregateInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AggregateInPandasExec](aggPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  private val groupingNamedExprs: Seq[BaseExprMeta[NamedExpression]] =
    aggPandas.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val udfs: Seq[BaseExprMeta[PythonUDF]] =
    aggPandas.udfExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val resultNamedExprs: Seq[BaseExprMeta[NamedExpression]] =
    aggPandas.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = groupingNamedExprs ++ udfs ++ resultNamedExprs
}

