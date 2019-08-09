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

import ai.rapids.spark.RapidsPluginImplicits._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class ImplicitsTestSuite extends FlatSpec with Matchers {
  private class Test (i: Int, throwOnClose: Boolean) extends AutoCloseable {
    var closeAttempted: Boolean = false
    var refCount: Int = 0
    override def close(): Unit = {
      closeAttempted = true
      refCount = refCount - 1
      if (refCount < 0) {
        throw new Exception(s"close called to many times for $i")
      }
      if (throwOnClose) {
        throw new Exception(s"cannot close $i")
      }
    }
    def incRefCount(): Test = {
      refCount = refCount + 1
      this
    }
    def leaked(): Boolean = {
      refCount > 0
    }
  }

  it should "handle exceptions within safeMap body" in {
    val resources = (0 until 10).map(new Test(_, false))

    assertThrows[Throwable] {
      resources.zipWithIndex.safeMap { 
        case (res, i) =>
          println(s"mapping away $i")
          if (i > 5) {
            throw new Exception("bad! " + i)
          }
          res.incRefCount()
      }
    }
    assert(resources.forall(!_.leaked))
  }

  it should "handle exceptions while closing safeMap" in {
    var threw = false
    val resources = (0 until 10).map(new Test(_, true))
    try {
      resources.zipWithIndex.safeMap { case (res, i) => {
        println("howdy")
        if (i > 5) {
          throw new Exception("bad!")
        }
        res.incRefCount()
      }}
    } catch {
      case t: Throwable => {
        threw = true
        assert(t.getSuppressed().size == 5)
      }
    }
    assert(threw)
    assert(resources.forall(!_.leaked))
  }

  it should "handle an error in a safeMap from a ColumnarBatch" in {
    val resources = new ArrayBuffer[Test]()
    val batch = new ColumnarBatch((0 until 10).map { ix => {
      val col = GpuColumnVector.from(GpuScalar.from(ix, IntegerType), 5)
      resources += new Test(ix, false)
      col
    }}.toArray)

    var colIx = 0
    assertThrows[java.lang.Exception] {
      batch.safeMap(_ => {
        if (colIx > 5) {
          throw new Exception("this is going to close my cols")
        }
        val res = resources(colIx)
        colIx = colIx + 1
        res.incRefCount()
      })
    }
    batch.close
    assert(resources.forall(!_.leaked))
  }

  it should "handle an error while closing in a safeMap from a ColumnarBatch" in {
    val resources = new ArrayBuffer[Test]()
    val batch = new ColumnarBatch((0 until 10).map { ix => {
      val col = GpuColumnVector.from(GpuScalar.from(ix, IntegerType), 5)
      resources += new Test(ix, true)
      col
    }}.toArray)

    var threw = false
    var colIx = 0
    try {
      batch.safeMap(_ => {
        if (colIx > 5) {
          throw new Exception("this is going to close my cols")
        }
        val res = resources(colIx)
        colIx = colIx + 1
        res.incRefCount()
      })
    } catch {
      case t: Throwable => {
        threw = true
        assert(t.getSuppressed().size == 5)
      }
    }
    batch.close
    assert(threw)
    assert(resources.forall(!_.leaked))
  }

  it should "safeMap/safeClose handle the success case" in {
    val resources = (0 until 10).map(new Test(_, false))
    val extraReferences = resources.safeMap(_.incRefCount)
    extraReferences.safeClose
    assert(resources.forall(!_.leaked))
  }

  it should "handle the successful case from a ColumnarBatch" in {
    val resources = new ArrayBuffer[Test]()
    val batch = new ColumnarBatch((0 until 10).map { ix => {
      val col = GpuColumnVector.from(GpuScalar.from(ix, IntegerType), 5)
      resources += new Test(ix, false)
      col
    }}.toArray)

    var colIx = 0
    val result = batch.safeMap(_ => {
      val res = resources(colIx)
      colIx = colIx + 1
      res.incRefCount()
    })
    batch.close
    result.safeClose
    assert(resources.forall(!_.leaked))
  }
}

