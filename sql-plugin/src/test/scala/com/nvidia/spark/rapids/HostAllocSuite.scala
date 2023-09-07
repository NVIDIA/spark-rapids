/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.util.concurrent.{ExecutionException, Future, LinkedBlockingQueue, TimeoutException, TimeUnit}

import ai.rapids.cudf.{HostMemoryBuffer, HostMemoryReservation, PinnedMemoryPool}
import com.nvidia.spark.rapids.Arm.withResource
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.{Signaler, TimeLimits}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time._
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.TaskContext
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class HostAllocSuite extends AnyFunSuite with BeforeAndAfterEach with
    BeforeAndAfterAll with TimeLimits {


  def setMockContext(taskAttemptId: Long): Unit = {
    val context = mock[TaskContext]
    when(context.taskAttemptId()).thenReturn(taskAttemptId)
    TrampolineUtil.setTaskContext(context)
  }

  trait TaskThreadOp[T] {
    def doIt(): T
  }

  object TaskThread {
    private class TaskThreadDoneOp(private var wrapped: TaskThread) extends TaskThreadOp[Void]
        with Future[Void]{
      override def toString = "TASK DONE"

      override def doIt(): Void = null

      override def cancel(b: Boolean) = false

      override def isCancelled = false

      override def isDone: Boolean = !wrapped.isAlive

      override def get = throw new RuntimeException("FUTURE NEEDS A TIMEOUT. THIS IS A TEST!")

      override def get(l: Long, timeUnit: TimeUnit): Void = {
        wrapped.join(timeUnit.toMillis(l))
        null
      }
    }

    private class TaskThreadTrackingOp[T](private val wrapped: TaskThreadOp[T])
        extends TaskThreadOp[T] with Future[T] {
      private var done = false
      private var t: Option[Throwable] = None
      private var ret: T = null.asInstanceOf[T]

      override def toString: String = wrapped.toString

      override def doIt(): T = try {
        val tmp = wrapped.doIt()
        this.synchronized {
          ret = tmp
          return ret
        }
      } catch {
        case t: Throwable =>
          synchronized {
            this.t = Some(t)
          }
          null.asInstanceOf[T]
      } finally synchronized {
        done = true
        this.notifyAll()
      }

      override def cancel(b: Boolean) = false

      override def isCancelled = false

      override def isDone: Boolean = synchronized {
        done
      }

      override def get =
        throw new RuntimeException("This is a test you should always have timeouts...")

      override def get(l: Long, timeUnit: TimeUnit): T = synchronized {
        if (!done) {
          wait(timeUnit.toMillis(l))
          if (!done) throw new TimeoutException()
        }
        t.map( e => throw new ExecutionException(e))
        ret
      }
    }
  }

  class TaskThread(private val name: String, private val taskId: Long) extends Thread(name) {
    private val queue = new LinkedBlockingQueue[TaskThreadOp[_]]()
    private var inDoIt: Boolean = false

    def initialize(): Unit = {
      setDaemon(true)
      start()
      val waitForStart = doIt(new TaskThreadOp[Void]() {
        override def doIt(): Void = {
          setMockContext(taskId)
          null
        }

        override def toString: String = s"INIT TASK $name TASK $taskId"
      })
      waitForStart.get(1000, TimeUnit.MILLISECONDS)
    }

    def done: Future[Void] = {
      val op = new TaskThread.TaskThreadDoneOp(this)
      queue.offer(op)
      op
    }

    def waitForBlockedOnAlloc(): Unit = {
      val start = System.nanoTime()
      var (state, inDo) = synchronized {
        (getState, inDoIt)
      }
      while (!isBlockedState(state) && inDo) {
        val end = System.nanoTime()
        if (TimeUnit.SECONDS.toNanos(1) <= (end - start)) {
          throw new TimeoutException(s"$name in $state after ${end - start} ns")
        }
        Thread.sleep(10)
        synchronized {
          state = getState
          inDo = inDoIt
        }
      }
    }

    private def isBlockedState(state: Thread.State): Boolean = state match {
      case Thread.State.BLOCKED | Thread.State.WAITING | Thread.State.TIMED_WAITING => true
      case _ => false
    }

    def isBlocked: Boolean = synchronized {
      isBlockedState(getState) && inDoIt
    }

    def doIt[T](op: TaskThreadOp[T]): Future[T] = {
      if (!isAlive) throw new IllegalStateException("Thread is already done...")
      val tracking = new TaskThread.TaskThreadTrackingOp[T](op)
      queue.offer(tracking)
      tracking
    }

    override def run(): Unit = {
      try {
        while (true) {
          val op = queue.poll(1000, TimeUnit.MILLISECONDS)
          if (op.isInstanceOf[TaskThread.TaskThreadDoneOp]) return
          // null is returned from the queue on a timeout
          if (op != null) {
            synchronized {
              inDoIt = true
            }
            try {
              op.doIt()
            } finally {
              synchronized {
                inDoIt = false
              }
            }
          }
        }
      } catch {
        case t: Throwable =>
          System.err.println("THROWABLE CAUGHT IN " + name)
          t.printStackTrace(System.err)
      }
    }
  }

  class AllocOnAnotherThread(val thread: TaskThread,
      val size: Long,
      val preferPinned: Boolean = true) extends AutoCloseable {
    var b: Option[HostMemoryBuffer] = None
    val fb: Future[Void] = thread.doIt(new TaskThreadOp[Void] {
      override def doIt(): Void = {
        doAlloc()
        null
      }

      override def toString: String = "ALLOC(" + size + ")"
    })
    var fc: Option[Future[Void]] = None

    def waitForAlloc(): Unit = {
      fb.get(1000, TimeUnit.MILLISECONDS)
    }

    def assertAllocSize(expectedSize: Long): Unit = synchronized {
      assert(b.isDefined)
      assertResult(expectedSize)(b.get.getLength)
    }

    def freeOnThread(): Unit = {
      if (fc.isDefined) throw new IllegalStateException("free called multiple times")
      fc = Option(thread.doIt(new TaskThreadOp[Void]() {
        override def doIt(): Void = {
          close()
          null
        }

        override def toString: String = "FREE(" + size + ")"
      }))
    }

    def waitForFree(): Unit = {
      if (fc.isEmpty) freeOnThread()
      fc.get.get(1000, TimeUnit.MILLISECONDS)
    }

    def freeAndWait(): Unit = {
      waitForFree()
    }

    private def doAlloc(): Void = {
      val tmp = HostAlloc.alloc(size, preferPinned)
      synchronized {
        b = Option(tmp)
      }
      null
    }

    override def close(): Unit = synchronized {
      b.foreach(_.close())
      b = None
    }
  }

  class ReserveOnAnotherThread(val thread: TaskThread,
      val size: Long,
      val preferPinned: Boolean = true) extends AutoCloseable {
    var b: Option[HostMemoryReservation] = None
    val fb: Future[Void] = thread.doIt(new TaskThreadOp[Void] {
      override def doIt(): Void = {
        doReservation()
        null
      }

      override def toString: String = "RESERVE(" + size + ")"
    })
    var fc: Option[Future[Void]] = None

    def waitForReservation(): HostMemoryReservation = {
      fb.get(1000, TimeUnit.MILLISECONDS)
      getReservation()
    }

    def getReservation(): HostMemoryReservation = synchronized {
      b.getOrElse {
        throw new IllegalStateException("No reservation was found")
      }
    }

    def closeOnThread(): Unit = {
      if (fc.isDefined) throw new IllegalStateException("free called multiple times")
      fc = Option(thread.doIt(new TaskThreadOp[Void]() {
        override def doIt(): Void = {
          close()
          null
        }

        override def toString: String = "CLOSE(" + size + ")"
      }))
    }

    def waitForClose(): Unit = {
      if (fc.isEmpty) closeOnThread()
      fc.get.get(1000, TimeUnit.MILLISECONDS)
    }

    def closeAndWait(): Unit = {
      waitForClose()
    }

    private def doReservation(): Void = {
      val tmp = HostAlloc.reserve(size, preferPinned)
      synchronized {
        b = Option(tmp)
      }
      null
    }

    override def close(): Unit = synchronized {
      b.foreach(_.close())
      b = None
    }
  }

  object MyThreadSignaler extends Signaler {
    override def apply(testThread: Thread): Unit = {
      System.err.println("\n\n\t\tTEST THREAD APPEARS TO BE STUCK")
      Thread.getAllStackTraces.forEach {
        case (thread, trace) =>
          val name = if (thread.getId == testThread.getId) {
            s"TEST THREAD ${thread.getName}"
          } else {
            thread.getName
          }
          System.err.println(name + "\n\t" + trace.mkString("\n\t"))
      }
    }
  }

  implicit val signaler: Signaler = MyThreadSignaler

  override def beforeEach(): Unit = {
    PinnedMemoryPool.shutdown()
    HostAlloc.initialize(-1)
  }

  override def afterAll(): Unit = {
    PinnedMemoryPool.shutdown()
    // 1 GiB
    PinnedMemoryPool.initialize(1 * 1024 * 1024 * 1024)
    HostAlloc.initialize(-1)
  }

  test("simple pinned tryAlloc") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(0)

    failAfter(Span(10, Seconds)) {
      withResource(HostAlloc.tryAlloc(1024, preferPinned = false)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(1024)
      }

      withResource(HostAlloc.tryAlloc(4 * 1024)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(4 * 1024)
        withResource(HostAlloc.tryAlloc(1)) { got2 =>
          // We ran out of memory
          assert(got2.isEmpty)
        }
      }

      assertThrows[IllegalArgumentException] {
        withResource(HostAlloc.tryAlloc(4 * 1024 + 1)) { _ =>
        }
      }
    }
  }

  test("simple non-pinned tryAlloc") {
    PinnedMemoryPool.initialize(0)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      withResource(HostAlloc.tryAlloc(1024, preferPinned = false)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(1024)
      }

      withResource(HostAlloc.tryAlloc(4 * 1024)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(4 * 1024)
        withResource(HostAlloc.tryAlloc(1)) { got2 =>
          // We ran out of memory
          assert(got2.isEmpty)
        }
      }

      assertThrows[IllegalArgumentException] {
        withResource(HostAlloc.tryAlloc(4 * 1024 + 1)) { _ =>
        }
      }
    }
  }

  test("simple mixed tryAlloc") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      withResource(HostAlloc.tryAlloc(1024, preferPinned = false)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(1024)
      }

      withResource(HostAlloc.tryAlloc(4 * 1024)) { got =>
        assert(got.isDefined)
        val buffer = got.get
        assertResult(buffer.getLength)(4 * 1024)
        withResource(HostAlloc.tryAlloc(4 * 1024)) { got2 =>
          assert(got2.isDefined)
          val buffer2 = got2.get
          assertResult(buffer2.getLength)(4 * 1024)
          withResource(HostAlloc.tryAlloc(1)) { got3 =>
            // We ran out of memory
            assert(got3.isEmpty)
          }
        }
      }

      assertThrows[IllegalArgumentException] {
        withResource(HostAlloc.tryAlloc(4 * 1024 + 1)) { _ =>
        }
      }
    }
  }

  test("simple pinned blocking alloc") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(0)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 1024, preferPinned = false)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(1024)
          a.freeAndWait()
        }

        withResource(new AllocOnAnotherThread(thread1,4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)

          withResource(new AllocOnAnotherThread(thread2, 1)) { a2 =>
            // We ran out of memory, and this should have blocked
            thread2.waitForBlockedOnAlloc()

            a.freeOnThread()
            a2.waitForAlloc()
            a2.assertAllocSize(1)
            a2.freeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("simple non-pinned blocking alloc") {
    PinnedMemoryPool.initialize(0)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 1024, preferPinned = false)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(1024)
          a.freeAndWait()
        }

        withResource(new AllocOnAnotherThread(thread1, 4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)

          withResource(new AllocOnAnotherThread(thread2, 1)) { a2 =>
            // We ran out of memory, and this should have blocked
            thread2.waitForBlockedOnAlloc()

            a.freeOnThread()
            a2.waitForAlloc()
            a2.assertAllocSize(1)
            a2.freeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("simple mixed blocking alloc") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()
      val thread3 = new TaskThread("thread3", 3)
      thread3.initialize()

      try {
        withResource(new AllocOnAnotherThread(thread1, 4 * 1024)) { a =>
          a.waitForAlloc()
          a.assertAllocSize(4 * 1024)

          withResource(new AllocOnAnotherThread(thread2, 4 * 1024, false)) { a2 =>
            a2.waitForAlloc()
            a2.assertAllocSize(4 * 1024)

            withResource(new AllocOnAnotherThread(thread3, 1, false)) { a3 =>
              // We ran out of memory, and this should have blocked
              thread3.waitForBlockedOnAlloc()
              a.freeAndWait()
              // We should still be unblocked because pinned memory was freed, and even if we
              // don't really want pinned, it is memory, so lets go
              a3.waitForAlloc()
              a3.assertAllocSize(1)
              a3.waitForFree()

              a2.waitForFree()
            }
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("simple pinned reservation") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(0)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        withResource(new ReserveOnAnotherThread(thread1, 1024, preferPinned = false)) { a =>
          val ra = a.waitForReservation()
          // reservations should be non-blocking
          withResource(ra.allocate(1024)) { _ =>
            // The size matches what we expected
          }
          a.closeAndWait()
        }

        withResource(new ReserveOnAnotherThread(thread1, 4 * 1024)) { a =>
          val ra = a.waitForReservation()
          withResource(ra.allocate(1024)) { _ =>
            withResource(ra.allocate(1024)) { _ =>
              withResource(ra.allocate(1024)) { _ =>
                withResource(ra.allocate(1024)) { _ =>
                  assertThrows[OutOfMemoryError] {
                    withResource(ra.allocate(1)) { _ =>

                    }
                  }
                }
              }
            }
          }

          withResource(new ReserveOnAnotherThread(thread2, 1)) { a2 =>
            // We ran out of memory, and this should have blocked
            thread2.waitForBlockedOnAlloc()

            a.closeOnThread()
            val ra2 = a2.waitForReservation()
            withResource(ra2.allocate(1)) { _ =>
              // NOOP
            }
            a2.closeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }

  test("simple non-pinned reservation") {
    PinnedMemoryPool.initialize(0)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()

      try {
        withResource(new ReserveOnAnotherThread(thread1, 1024, preferPinned = false)) { a =>
          val ra = a.waitForReservation()
          // reservations should be non-blocking
          withResource(ra.allocate(1024)) { _ =>
            // The size matches what we expected
          }
          a.closeAndWait()
        }

        withResource(new ReserveOnAnotherThread(thread1, 4 * 1024)) { a =>
          val ra = a.waitForReservation()
          withResource(ra.allocate(1024)) { _ =>
            withResource(ra.allocate(1024)) { _ =>
              withResource(ra.allocate(1024)) { _ =>
                withResource(ra.allocate(1024)) { _ =>
                  assertThrows[OutOfMemoryError] {
                    withResource(ra.allocate(1)) { _ =>

                    }
                  }
                }
              }
            }
          }

          withResource(new ReserveOnAnotherThread(thread2, 1)) { a2 =>
            // We ran out of memory, and this should have blocked
            thread2.waitForBlockedOnAlloc()

            a.closeOnThread()
            val ra2 = a2.waitForReservation()
            withResource(ra2.allocate(1)) { _ =>
              // NOOP
            }
            a2.closeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
      }
    }
  }


  test("simple mixed reservation") {
    PinnedMemoryPool.initialize(4 * 1024)
    HostAlloc.initialize(4 * 1024)

    failAfter(Span(10, Seconds)) {
      val thread1 = new TaskThread("thread1", 1)
      thread1.initialize()
      val thread2 = new TaskThread("thread2", 2)
      thread2.initialize()
      val thread3 = new TaskThread("thread3", 3)
      thread3.initialize()

      try {
        withResource(new ReserveOnAnotherThread(thread1, 1024, preferPinned = false)) { a =>
          val ra = a.waitForReservation()
          // reservations should be non-blocking
          withResource(ra.allocate(1024)) { _ =>
            // The size matches what we expected
          }
          a.closeAndWait()
        }

        withResource(new ReserveOnAnotherThread(thread1, 4 * 1024)) { a =>
          val ra = a.waitForReservation()
          withResource(ra.allocate(1024)) { _ =>
            withResource(ra.allocate(1024)) { _ =>
              withResource(ra.allocate(1024)) { _ =>
                withResource(ra.allocate(1024)) { _ =>
                }
              }
            }
          }

          withResource(new ReserveOnAnotherThread(thread2, 4096)) { a2 =>
            val ra2 = a2.waitForReservation()
            withResource(ra2.allocate(1)) { _ =>
              // NOOP
            }

            withResource(new AllocOnAnotherThread(thread3, 1024)) { a3 =>
              // We ran out of memory, and this should have blocked
              thread2.waitForBlockedOnAlloc()

              a.closeOnThread()

              a3.waitForAlloc()
              a3.assertAllocSize(1024)
              a3.freeAndWait()
            }
            a2.closeAndWait()
          }
        }
      } finally {
        thread1.done.get(1, TimeUnit.SECONDS)
        thread2.done.get(1, TimeUnit.SECONDS)
        thread3.done.get(1, TimeUnit.SECONDS)
      }
    }
  }
}
