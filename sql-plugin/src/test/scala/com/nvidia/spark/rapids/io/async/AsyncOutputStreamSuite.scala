/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.io.async

import java.io.{BufferedOutputStream, File, FileOutputStream, IOException, OutputStream}
import java.util.concurrent.Callable

import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class AsyncOutputStreamSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val bufLen = 128 * 1024
  private val buf: Array[Byte] = new Array[Byte](bufLen)
  private val maxBufCount = 10
  private val trafficController = new TrafficController(
    new HostMemoryThrottle(bufLen * maxBufCount))

  def openStream(): AsyncOutputStream = {
    new AsyncOutputStream(() => {
      val file = File.createTempFile("async-write-test", "tmp")
      new BufferedOutputStream(new FileOutputStream(file))
    }, trafficController)
  }

  test("open, write, and close") {
    val numBufs = 1000
    val stream = openStream()
    withResource(stream) { os =>
      for (_ <- 0 until numBufs) {
        os.write(buf)
      }
    }
    assertResult(bufLen * numBufs)(stream.metrics.numBytesScheduled)
    assertResult(bufLen * numBufs)(stream.metrics.numBytesWritten.get())
  }

  test("write after closed") {
    val os = openStream()
    os.close()
    assertThrows[IOException] {
      os.write(buf)
    }
  }

  test("flush after closed") {
    val os = openStream()
    os.close()
    assertThrows[IOException] {
      os.flush()
    }
  }

  class ThrowingOutputStream extends OutputStream {

    var failureCount = 0

    override def write(i: Int): Unit = {
      failureCount += 1
      throw new IOException(s"Failed ${failureCount} times")
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      failureCount += 1
      throw new IOException(s"Failed ${failureCount} times")
    }
  }

  def assertThrowsWithMsg[T](fn: Callable[T], clue: String,
      expectedMsgPrefix: String): Unit = {
    withClue(clue) {
      try {
        fn.call()
      } catch {
        case t: Throwable =>
          assertIOExceptionMsg(t, expectedMsgPrefix)
      }
    }
  }

  def assertIOExceptionMsg(t: Throwable, expectedMsgPrefix: String): Unit = {
    if (t.getClass.isAssignableFrom(classOf[IOException])) {
      if (!t.getMessage.contains(expectedMsgPrefix)) {
        fail(s"Unexpected exception message: ${t.getMessage}")
      }
    } else {
      if (t.getCause != null) {
        assertIOExceptionMsg(t.getCause, expectedMsgPrefix)
      } else {
        fail(s"Unexpected exception: $t")
      }
    }
  }

  test("write after error") {
    val os = new AsyncOutputStream(() => new ThrowingOutputStream, trafficController)

    // The first call to `write` should succeed
    os.write(buf)

    // Wait for the first write to fail
    while (os.lastError.get().isEmpty) {
      Thread.sleep(100)
    }

    // The second `write` call should fail with the exception thrown by the first write failure
    assertThrowsWithMsg(() => os.write(buf),
      "The second write should fail with the exception thrown by the first write failure",
      "Failed 1 times")

    // `close` throws the same exception
    assertThrowsWithMsg(() => os.close(),
      "The second write should fail with the exception thrown by the first write failure",
      "Failed 1 times")

    assertResult(bufLen)(os.metrics.numBytesScheduled)
    assertResult(0)(os.metrics.numBytesWritten.get())
    assert(os.lastError.get().get.isInstanceOf[IOException])
  }

  test("flush after error") {
    val os = new AsyncOutputStream(() => new ThrowingOutputStream, trafficController)

    // The first write should succeed
    os.write(buf)

    // The flush should fail with the exception thrown by the write failure
    assertThrowsWithMsg(() => os.flush(),
      "The flush should fail with the exception thrown by the write failure",
      "Failed 1 times")

    // `close` throws the same exception
    assertThrowsWithMsg(() => os.close(),
      "The flush should fail with the exception thrown by the write failure",
      "Failed 1 times")
  }

  test("close after error") {
    val os = new AsyncOutputStream(() => new ThrowingOutputStream, trafficController)

    os.write(buf)

    assertThrowsWithMsg(() => os.close(),
      "Close should fail with the exception thrown by the write failure",
      "Failed 1 times")
  }
}
