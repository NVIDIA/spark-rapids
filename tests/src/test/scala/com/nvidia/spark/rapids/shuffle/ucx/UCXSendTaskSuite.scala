/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle.ucx

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.openucx.jucx.{UcxCallback, UcxException}
import org.openucx.jucx.ucp.UcpRequest
import org.scalatest.funsuite.AnyFunSuite

/**
 * The UCX send task, through the helper that runs it: a task that throws must still end its
 * send exactly once, or the send's transaction never completes. A test cannot build a UCX, so
 * each task here stands in for the header allocation and JUCX's send.
 */
class UCXSendTaskSuite extends AnyFunSuite {

  /** A sender's callback that records how its send ended. */
  private class Ended extends UcxCallback {
    val calls = new ArrayBuffer[String]()
    override def onSuccess(request: UcpRequest): Unit = calls += "success"
    override def onError(ucsStatus: Int, errorMsg: String): Unit =
      calls += s"error $ucsStatus: $errorMsg"
  }

  test("a send task that throws before UCX calls back reports one error and rethrows") {
    val ended = new Ended
    // what allocating the send's header throws when direct memory runs out
    val oom = new OutOfMemoryError("Direct buffer memory")
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(ended)(_ => throw oom))
    assert(thrown eq oom)
    assertResult(1)(ended.calls.size)
    assert(ended.calls.head.startsWith(s"error ${UCX.SEND_TASK_FAILED}: "))
    assert(ended.calls.head.contains("Direct buffer memory"))
  }

  test("a send that UCX failed at once, before JUCX threw, is reported once") {
    // JUCX 1.22 calls the callback for an immediate error, then the exception reaches Java
    val ended = new Ended
    val failed = new UcxException("Connection reset by remote peer", -25)
    val thrown = intercept[UcxException](UCX.sendOrReport(ended) { completion =>
      completion.handOff()
      completion.onError(-25, "Connection reset by remote peer")
      throw failed
    })
    assert(thrown eq failed)
    assertResult(Seq("error -25: Connection reset by remote peer"))(ended.calls)
  }

  test("a send that succeeds, at once or later, reports one success") {
    val atOnce = new Ended
    UCX.sendOrReport(atOnce) { completion =>
      completion.handOff()
      completion.onSuccess(null)
    }
    assertResult(Seq("success"))(atOnce.calls)

    val later = new Ended
    var pending: UCX.SendCompletion = null
    UCX.sendOrReport(later) { completion =>
      completion.handOff()
      pending = completion
    }
    assert(later.calls.isEmpty)
    pending.onSuccess(null)
    assertResult(Seq("success"))(later.calls)
  }

  test("a JVM error after the hand-off is left to the send's own callback") {
    // JUCX can run out of heap after UCX took the send, which then still calls back, and may
    // read the send's buffers until it does
    val ended = new Ended
    var pending: UCX.SendCompletion = null
    val oom = new OutOfMemoryError("Java heap space")
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(ended) { completion =>
      completion.handOff()
      pending = completion
      throw oom
    })
    assert(thrown eq oom)
    assert(ended.calls.isEmpty)
    pending.onSuccess(null)
    assertResult(Seq("success"))(ended.calls)
  }

  test("a callback after the task's error was reported is dropped") {
    val ended = new Ended
    var pending: UCX.SendCompletion = null
    val failure = new IllegalStateException("the endpoint is closed in test")
    intercept[IllegalStateException](UCX.sendOrReport(ended) { completion =>
      completion.handOff()
      pending = completion
      throw failure
    })
    assertResult(1)(ended.calls.size)
    pending.onSuccess(null)
    pending.onError(-25, "a late error in test")
    assertResult(1)(ended.calls.size)
  }

  test("a send task hands JUCX its header's address, what it prepared and the completion") {
    val ended = new Ended
    var header: ByteBuffer = null
    var sent: (Long, String, Long) = null
    UCX.sendTask(0x1234L, ended, () => "params",
        allocate = size => {
          header = ByteBuffer.allocateDirect(size)
          header
        },
        addressOf = buffer => {
          assert(buffer eq header)
          42L
        }) { (address, prepared, completion) =>
      sent = (address, prepared, header.getLong(0))
      completion.onSuccess(null)
    }
    assertResult((42L, "params", 0x1234L))(sent)
    assertResult(Seq("success"))(ended.calls)
  }

  test("a send task whose header cannot be allocated reports it once") {
    val ended = new Ended
    val oom = new OutOfMemoryError("Direct buffer memory")
    var called = false
    val thrown = intercept[OutOfMemoryError](UCX.sendTask(1L, ended, () => (),
      allocate = _ => throw oom)((_, _, _) => called = true))
    assert(thrown eq oom)
    assert(!called)
    assertResult(1)(ended.calls.size)
  }

  test("a send task whose request parameters cannot be built reports it once") {
    val ended = new Ended
    val oom = new OutOfMemoryError("Java heap space")
    var called = false
    val thrown = intercept[OutOfMemoryError](
      UCX.sendTask(1L, ended, () => throw oom)((_, _, _) => called = true))
    assert(thrown eq oom)
    assert(!called)
    assertResult(1)(ended.calls.size)
  }

  test("a send task that cannot take its header's address reports it once") {
    // Taking the address boxes it, which can run out of heap before JUCX is called, so UCX
    // never has the send and nothing else would end it.
    val ended = new Ended
    val oom = new OutOfMemoryError("Java heap space")
    var called = false
    val thrown = intercept[OutOfMemoryError](UCX.sendTask(1L, ended, () => (),
      addressOf = _ => throw oom)((_, _, _) => called = true))
    assert(thrown eq oom)
    assert(!called)
    assertResult(1)(ended.calls.size)
  }

  test("a report that throws keeps the send task's error, with the report's as suppressed") {
    val reportFailure = new IllegalStateException("the sender's callback failed in test")
    val failing = new UcxCallback {
      override def onError(ucsStatus: Int, errorMsg: String): Unit = throw reportFailure
    }
    val oom = new OutOfMemoryError("Direct buffer memory")
    val thrown = intercept[OutOfMemoryError](UCX.sendOrReport(failing)(_ => throw oom))
    assert(thrown eq oom)
    assert(thrown.getSuppressed.exists(_ eq reportFailure))
  }
}
