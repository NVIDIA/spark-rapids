package ai.rapids.spark

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning}
import org.apache.spark.sql.execution.{LimitExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

/**
 * Helper trait which defines methods that are shared by both
 * [[GpuLocalLimitExec]] and [[GpuGlobalLimitExec]].
 */
trait GpuBaseLimitExec extends LimitExec with GpuExec {
  override def output: Seq[Attribute] = child.output

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val crdd = child.executeColumnar()
    crdd.mapPartitions { cbIter =>
      var batch: ColumnarBatch = null // incoming batch
      var remainingLimit = limit
      val resultCVs = new ListBuffer[GpuColumnVector]
      try {
        while (remainingLimit > 0 && cbIter.hasNext) {
          batch = cbIter.next()
          // if batch > remainingLimit, then slice and add to the resultBatches, break
          if (batch.numRows() > remainingLimit) {
            val table = GpuColumnVector.from(batch)
            var exception: Throwable = null
            try {
              for (i <- 0 until table.getNumberOfColumns) {
                val slices = table.getColumn(i).slice(0, remainingLimit)
                assert(slices.length > 0)
                resultCVs.append(GpuColumnVector.from(slices(0)))
              }
              remainingLimit = 0
            } catch {
              case e: Throwable =>
                exception = e
                closeAndAddSuppressed(exception, batch)
                throw e
            } finally {
              // close the table
              closeAndAddSuppressed(exception, table)
            }
          } else {
            // else batch < remainingLimit, add to the resultBatch
            GpuColumnVector.extractColumns(batch).foreach(gpuVector => {
              gpuVector.incRefCount()
              resultCVs.append(gpuVector)
            })
            remainingLimit -= batch.numRows()
          }
          batch.close()
          batch = null
        }
        if (resultCVs.nonEmpty) {
          new Iterator[ColumnarBatch] {
            var batch = new ColumnarBatch(resultCVs.toArray, resultCVs(0).getRowCount.toInt)

            TaskContext.get().addTaskCompletionListener[Unit] { _ =>
              if (batch != null) {
                batch.close()
                batch = null
              }
            }
            override def hasNext: Boolean = batch != null

            override def next(): ColumnarBatch = {
              val out = batch
              batch = null
              out
            }
          }
        } else {
          Iterator.empty
        }
      } catch {
        case e: Throwable =>
          if (resultCVs.nonEmpty) {
            resultCVs.foreach(gpuVector => {
              closeAndAddSuppressed(e, gpuVector)
            })
          }
          throw e
      }
    }
  }

  private def closeAndAddSuppressed(e: Throwable,
                                    resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }
}

/**
 * Take the first `limit` elements of each child partition, but do not collect or shuffle them.
 */
case class GpuLocalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec {

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

/**
 * Take the first `limit` elements of the child's single output partition.
 */
case class GpuGlobalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec {

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}