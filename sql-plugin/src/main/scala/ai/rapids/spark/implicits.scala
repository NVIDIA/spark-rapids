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

import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.generic.CanBuildFrom
import scala.collection.{SeqLike, mutable}
import scala.collection.mutable.ArrayBuffer

/**
  * RapidsPluginImplicits, adds implicit functions for ColumnarBatch, Seq, and Seq[AutoCloseable]
  * that help make resource management easier within the project.
  */
object RapidsPluginImplicits {
  import scala.language.implicitConversions

  class MapsSafely {
    /**
      * safeMap: safeMap implementation that is leveraged by other specific implicits.
      *
      * safeMap has the added safety net that as you produce AutoCloseable values they are tracked, and if an
      * exception were to occur within the maps's body, it will make every attempt to close each produced value.
      *
      * Note: safeMap will close regardless of the reference count of the elements produced by fn.
      *
      * @param in - the Seq[A] to map on
      * @param fn - a function that takes A, and produces B (a subclass of AutoCloseable)
      * @tparam A - the type of the elements in Seq
      * @tparam B - the type of the elements produced in the safeMap (should be subclasses of AutoCloseable)
      * @return - a sequence of B, in the success case
      */
    def safeMap[A, B <: AutoCloseable](in: Seq[A], fn: A => B): Seq[B] = {
      var closeEm = new ArrayBuffer[B](in.size)
      in.map(i => {
        var success = false
        try { 
          val res = fn(i)
          println("done with function " + res + " " +fn)
          success = true
          closeEm += res
          res
        } catch {
          case e: Throwable => 
            println ("exception: " + e)
            throw e
        } finally {
          if (!success) {
            closeEm.safeClose
          }
        }
      })
    }
  }

  implicit class AutoCloseableProducingSeq[A, B <: AutoCloseable](val in: Seq[A]) extends MapsSafely {
    /**
      * safeMap: implicit map on a Seq[A] that produces Seq[B], where B is a subclass of AutoCloseable.
      * See [[MapsSafely.safeMap]] for a more detailed explanation.
      *
      * @param fn - a function that takes A, and produces B (a subclass of AutoCloseable)
      * @tparam A - the type of the elements in Seq
      * @tparam B - the type of the elements produced in the safeMap (should be subclasses of AutoCloseable)
      * @return - a sequence of B, in the success case
      */
    def safeMap[B <: AutoCloseable](fn: A => B) = super.safeMap(in, fn)
  }

  implicit class AutoCloseableProducingArray[A, B <: AutoCloseable](val in: Array[A]) extends MapsSafely {
    /**
      * safeMap: implicit map on a Seq[A] that produces Seq[B], where B is a subclass of AutoCloseable.
      * See [[MapsSafely.safeMap]] for a more detailed explanation.
      *
      * @param fn - a function that takes A, and produces B (a subclass of AutoCloseable)
      * @tparam A - the type of the elements in Seq
      * @tparam B - the type of the elements produced in the safeMap (should be subclasses of AutoCloseable)
      * @return - a sequence of B, in the success case
      */
    def safeMap[B <: AutoCloseable](fn: A => B) = super.safeMap(in, fn)
  }

  implicit class AutoCloseableProducingBatch(val in: ColumnarBatch) extends MapsSafely {
    /**
      * safeMap: Is an implicit on ColumnarBatch, that lets you map over the columns
      * of a batch as if the batch was a Seq[GpuColumnVector], iff safeMap's body is producing AutoCloseable
      * (otherwise, it is not defined).
      *
      * See [[MapsSafely.safeMap]] for a more detailed explanation.
      *
      * @param fn - a function that takes GpuColumnVector, and returns a subclass of AutoCloseable
      * @tparam B - the type of the elements produced in the safeMap (should be subclasses of AutoCloseable)
      * @return - a sequence of B, in the success case
      */
    def safeMap[B <: AutoCloseable](fn: GpuColumnVector => B): Seq[B] =
      super.safeMap[Int, B](0 until in.numCols,
        i => fn(in.column(i).asInstanceOf[GpuColumnVector])).toSeq
  }

  implicit class AutoCloseableArray[A <: AutoCloseable](val in: Array[A]) {
    def safeClose(): Unit = {
      in.safeClose()
    }
  }

  implicit class AutoCloseableSeq[A <: AutoCloseable](val in: Seq[A]) {
    /**
      * safeClose: Is an implicit on a sequence of AutoCloseable classes that tries to close each element
      * of the sequence, even if prior close calls fail. In case of failure in any of the close calls, an Exception
      * is thrown containing the suppressed exceptions (getSuppressed), if any.
      *
      * @return - Unit
      */
    def safeClose(): Unit = {
      var closeException: Throwable = null
      in.foreach(element => {
        if (element != null) {
          try {
            element.close
          } catch {
            case e: Throwable => {
              if (closeException == null) {
                closeException = e
              } else {
                closeException.addSuppressed(e)
              }
            }
          }
        }
      })
      if (closeException != null) {
        // an exception happened while we were trying to safely close
        // resources, throw the exception to alert the caller
        throw closeException
      }
    }
  }

}
