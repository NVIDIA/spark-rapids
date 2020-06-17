/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.{mutable, SeqLike}
import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag

import org.apache.spark.sql.vectorized.ColumnarBatch

/**
  * RapidsPluginImplicits, adds implicit functions for ColumnarBatch, Seq, Seq[AutoCloseable],
  * and Array[AutoCloseable] that help make resource management easier within the project.
  */
object RapidsPluginImplicits {
  import scala.language.implicitConversions

  implicit class AutoCloseableColumn[A <: AutoCloseable](autoCloseable: AutoCloseable) {

    /**
     * safeClose: Is an implicit on AutoCloseable class that tries to close the resource, if an
     * Exception was thrown prior to this close, it adds the new exception to the suppressed
     * exceptions, otherwise just throws
     *
     * @param e Exception which we don't want to suppress
     */
    def safeClose(e: Throwable = null): Unit = {
      if (e != null) {
        try {
          autoCloseable.close()
        } catch {
          case suppressed: Throwable => e.addSuppressed(suppressed)
        }
      } else {
        autoCloseable.close()
      }
    }
  }

  implicit class AutoCloseableSeq[A <: AutoCloseable](val in: SeqLike[A, _]) {
    /**
      * safeClose: Is an implicit on a sequence of AutoCloseable classes that tries to close each
      * element of the sequence, even if prior close calls fail. In case of failure in any of the
      * close calls, an Exception is thrown containing the suppressed exceptions (getSuppressed),
      * if any.
      */
    def safeClose(): Unit = if (in != null) {
      var closeException: Throwable = null
      in.foreach { element =>
        if (element != null) {
          try {
            element.close()
          } catch {
            case e: Throwable if closeException == null => closeException = e
            case e: Throwable => closeException.addSuppressed(e)
          }
        }
      }
      if (closeException != null) {
        // an exception happened while we were trying to safely close
        // resources, throw the exception to alert the caller
        throw closeException
      }
    }
  }

  implicit class AutoCloseableArray[A <: AutoCloseable](val in: Array[A]) {
    def safeClose(): Unit = if (in != null) {
      in.toSeq.safeClose()
    }
  }

  class MapsSafely[A, Repr] {
    /**
      * safeMap: safeMap implementation that is leveraged by other type-specific implicits.
      *
      * safeMap has the added safety net that as you produce AutoCloseable values they are
      * tracked, and if an exception were to occur within the maps's body, it will make every
      * attempt to close each produced value.
      *
      * Note: safeMap will close in case of errors, without any knowledge of whether it should
      * or not.
      * Use safeMap only in these circumstances if `fn` increases the reference count,
      * producing an AutoCloseable, and nothing else is tracking these references:
      *    a) seq.safeMap(x => {...; x.incRefCount; x})
      *    b) seq.safeMap(x => GpuColumnVector.from(...))
      *
      * Usage of safeMap chained with other maps is a bit confusing:
      *
      * seq.map(GpuColumnVector.from).safeMap(couldThrow)
      *
      * Will close the column vectors produced from couldThrow up until the time where safeMap
      * throws.
      *
      * The correct pattern of usage in cases like this is:
      *
      *   val closeTheseLater = seq.safeMap(GpuColumnVector.from)
      *   closeTheseLater.safeMap{ x =>
      *     var success = false
      *     try {
      *       val res = couldThrow(x.incRefCount())
      *       success = true
      *       res // return a ref count of 2
      *     } finally {
      *       if (!success) {
      *         // in case of an error, we close x as part of normal error handling
      *         // the exception will be caught by the safeMap, and it will close all
      *         // AutoCloseables produced before x
      *         // - Sequence looks like: [2, 2, 2, ..., 2] + x, which has also has a refcount of 2
      *         x.close() // x now has a ref count of 1, the rest of the sequence has 2s
      *       }
      *     }
      *   } // safeMap cleaned, and now everything has 1s for ref counts (as they were before)
      *
      *   closeTheseLater.safeClose() // go from 1 to 0 in all things inside closeTheseLater
      *
      * @param in the Seq[A] to map on
      * @param fn a function that takes A, and produces B (a subclass of AutoCloseable)
      * @tparam A the type of the elements in Seq
      * @tparam B the type of the elements produced in the safeMap (should be subclasses of
      *                AutoCloseable)
      * @tparam Repr the type of the input collection (needed by builder)
      * @tparam That the type of the output collection (needed by builder)
      * @return a sequence of B, in the success case
      */
    protected def safeMap[B <: AutoCloseable, That](
        in: SeqLike[A, Repr],
        fn: A => B)
        (implicit bf: CanBuildFrom[Repr, B, That]): That = {
      def builder: mutable.Builder[B, That] = {
        val b = bf(in.asInstanceOf[Repr])
        b.sizeHint(in)
        b
      }
      val b = builder
      for (x <- in) {
        var success = false
        try {
          b += fn(x)
          success = true
        } finally {
          if (!success) {
            val res = b.result() // can be a SeqLike or an Array
            res match {
              // B is erased at this point, even if ClassTag is used
              // @ unchecked suppresses a warning that the type of B
              // was eliminated due to erasure. That said B is AutoCloseble
              // and SeqLike[AutoCloseable, _] is defined
              case b: SeqLike[B @ unchecked, _] => b.safeClose()
              case a: Array[AutoCloseable] => a.safeClose()
            }
          }
        }
      }
      b.result()
    }
  }

  implicit class AutoCloseableProducingSeq[A](val in: Seq[A]) extends MapsSafely[A, Seq[A]] {
    /**
      * safeMap: implicit map on a Seq[A] that produces Seq[B], where B is a subclass of
      * AutoCloseable.
      * See [[MapsSafely.safeMap]] for a more detailed explanation.
      *
      * @param fn a function that takes A, and produces B (a subclass of AutoCloseable)
      * @tparam A the type of the elements in Seq
      * @tparam B the type of the elements produced in the safeMap (should be subclasses of
      *             AutoCloseable)
      * @return a sequence of B, in the success case
      */
    def safeMap[B <: AutoCloseable](fn: A => B): Seq[B] = super.safeMap(in, fn)
  }

  implicit class AutoCloseableProducingArray[A](val in: Array[A]) extends MapsSafely[A, Array[A]] {
    /**
      * safeMap: implicit map on a Seq[A] that produces Seq[B], where B is a subclass of
      * AutoCloseable.
      * See [[MapsSafely.safeMap]] for a more detailed explanation.
      *
      * @param fn a function that takes A, and produces B (a subclass of AutoCloseable)
      * @tparam A the type of the elements in Seq
      * @tparam B the type of the elements produced in the safeMap (should be subclasses of
      *             AutoCloseable)
      * @return a sequence of B, in the success case
      */
    def safeMap[B <: AutoCloseable : ClassTag](fn: A => B): Array[B] = super.safeMap(in, fn)
  }

  implicit class AutoCloseableFromBatchColumns(val in: ColumnarBatch)
    extends MapsSafely[Int, Seq[Int]] {
    /**
      * safeMap: Is an implicit on ColumnarBatch, that lets you map over the columns
      * of a batch as if the batch was a Seq[GpuColumnVector], iff safeMap's body is producing
      * AutoCloseable (otherwise, it is not defined).
      *
      * See [[MapsSafely.safeMap]] for a more detailed explanation.
      *
      * @param fn a function that takes GpuColumnVector, and returns a subclass of AutoCloseable
      * @tparam B the type of the elements produced in the safeMap (should be subclasses of
      *           AutoCloseable)
      * @return a sequence of B, in the success case
      */
    def safeMap[B <: AutoCloseable](fn: GpuColumnVector => B): Seq[B] = {
      val colIds: Seq[Int] = 0 until in.numCols
      super.safeMap(colIds, (i: Int) => fn(in.column(i).asInstanceOf[GpuColumnVector]))
    }
  }
}
