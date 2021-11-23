package org.apache.spark.sql.hive.execution

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

object HiveOverrides {
  lazy val hiveExec: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[HiveTableScanExec](
      "Reading data from files, often from Hive tables",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
        TypeSig.ARRAY + TypeSig.DECIMAL_128_FULL).nested(), TypeSig.all),
      (fsse, conf, p, r) => new SparkPlanMeta[HiveTableScanExec](fsse, conf, p, r) {
        // partition filters and data filters are not run on the GPU
        override val childExprs: Seq[ExprMeta[_]] = Seq.empty

        override def tagPlanForGpu(): Unit = {
          // should add more checks for not running on GPU
        }

        override def convertToGpu(): GpuExec = {

          val logicalRelation = new HiveMetastoreCatalogUtils(this.wrapped.sqlContext.sparkSession)
            .convert(this.wrapped.relation)

          val relation = logicalRelation.relation match {
            case fs: HadoopFsRelation =>
              HadoopFsRelation(
                fs.location,
                fs.partitionSchema,
                fs.dataSchema,
                fs.bucketSpec,
                GpuFileSourceScanExec.convertFileFormat(fs.fileFormat),
                fs.options)(this.wrapped.sqlContext.sparkSession)
            case _ => throw new RuntimeException("Wrong relation")
          }

          GpuFileSourceScanExec(
            relation,
            wrapped.output,
            wrapped.relation.schema,
            wrapped.partitionPruningPred,
            None,
            None,
            Seq(),
            None)(conf)
        }
      })
  ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap

}
