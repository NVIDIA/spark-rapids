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

import java.util.Comparator

import ai.rapids.cudf.{ColumnView, HostMemoryBuffer, HostMemoryReservation, MemoryBuffer, PinnedMemoryPool}
import com.nvidia.spark.rapids.HostAlloc.align

import org.apache.spark.TaskContext

private class HostAlloc(nonPinnedLimit: Long) {
  private var currentNonPinnedAllocated: Long = 0L
  private var currentNonPinnedReserved: Long = 0L
  private val pinnedLimit: Long = PinnedMemoryPool.getTotalPoolSizeBytes()
  // For now we are going to assume that we are the only ones calling into the pinned pool
  // That is not really true, but should be okay.
  private var currentPinnedAllocated: Long = 0L
  private val isUnlimited = nonPinnedLimit < 0
  private val isPinnedOnly = nonPinnedLimit == 0

  private val compareBlocks = new Comparator[BlockedAllocation] {
    override def compare(a: BlockedAllocation, b: BlockedAllocation): Int = {
      java.lang.Long.compare(a.taskId, b.taskId)
    }
  }

  /**
   * Host memory allocations that are still pending.
   */
  private val pendingAllowedQueue = new HashedPriorityQueue[BlockedAllocation](100, compareBlocks)

  /**
   * An allocation that has not been completed yet. It is blocked waiting for more resources.
   */
  private class BlockedAllocation(val amount: Long, val taskId: Long) {
    var shouldWake = false

    /**
     * Wait until we should retry the allocation because it might succeed. It is not
     * guaranteed though.
     * It is required that the parent lock is held before this is called.
     */
    def waitUntilPossiblyReady(): Unit = {
      shouldWake = false
      while (!shouldWake) {
        HostAlloc.this.wait(1000)
      }
    }

    /**
     * Wake up all threads that are blocked waiting for an allocation.
     */
    def wakeUpItMightBeWorthIt(): Unit = {
      shouldWake = true
      HostAlloc.this.notifyAll()
    }
  }

  /**
   * A callback class so we know when a non-pinned host buffer was released
   */
  private class OnCloseCallback(amount: Long) extends MemoryBuffer.EventHandler {
    override def onClosed(refCount: Int): Unit = {
      if (refCount == 0) {
        releaseNonPinned(amount)
      }
    }
  }

  /**
   * A callback so we know when a pinned host buffer was released.
   * @param amount
   */
  private class OnPinnedCloseCallback(amount: Long) extends MemoryBuffer.EventHandler {
    override def onClosed(refCount: Int): Unit = {
      if (refCount == 0) {
        releasePinned(amount)
      }
    }
  }

  /**
   * A wrapper around a pinned memory reservation so we can add in callbacks as needed.
   */
  private class WrappedPinnedReservation(val wrap: HostMemoryReservation)
    extends HostMemoryReservation {

    private def addEventHandlerAndUpdateMetrics(b: HostMemoryBuffer): HostMemoryBuffer =
      synchronized {
        val amount = b.getLength
        currentPinnedAllocated += amount
        // I need callbacks for the pinned
        HostAlloc.addEventHandler(b, new OnPinnedCloseCallback(amount))
        b
      }

    override def allocate(amount: Long, preferPinned: Boolean): HostMemoryBuffer =
      addEventHandlerAndUpdateMetrics(wrap.allocate(amount, preferPinned))

    override def allocate(amount: Long): HostMemoryBuffer =
      addEventHandlerAndUpdateMetrics(wrap.allocate(amount))

    override def close(): Unit = wrap.close()
  }

  /**
   * A non-pinned host memory reservation.
   */
  private class NonPinnedReservation(var reservedAmount: Long) extends HostMemoryReservation {
    override def allocate(amount: Long, preferPinned: Boolean): HostMemoryBuffer = {
      allocate(amount)
    }

    override def allocate(amount: Long): HostMemoryBuffer = synchronized {
      if (amount > reservedAmount) {
        throw new OutOfMemoryError("Could not allocate. Remaining memory reservation is " +
          s"too small $amount out of $reservedAmount")
      }
      val buff = allocNonPinnedFromReserved(amount)
      reservedAmount -= align(buff.getLength)
      buff
    }

    override def close(): Unit = synchronized {
      releaseNonPinnedReservation(reservedAmount)
      reservedAmount = 0
    }
  }

  /**
   * A reservation for the special mode when there are no host memory limits.
   */
  private object UnlimitedReservation extends HostMemoryReservation {
    override def allocate(amount: Long, preferPinned: Boolean): HostMemoryBuffer =
      HostAlloc.alloc(amount, preferPinned)

    override def allocate(amount: Long): HostMemoryBuffer =
      HostAlloc.alloc(amount)

    override def close(): Unit = {
      // NOOP
    }
  }

  /**
   * Wake up any blocked allocation that are still pending up to the amount that has been freed.
   * Note that this assume that there is no fragmentation that might prevent an allocation from
   * succeeding.
   * @param amountLeftToWakeInput the amount of memory that is available in bytes.
   * @return true if anything was woken up, else false.
   */
  private def wakeUpAsNeeded(amountLeftToWakeInput: Long): Boolean = synchronized {
    var amountLeftToWake = amountLeftToWakeInput
    var ret = false
    while (amountLeftToWake > 0 && !pendingAllowedQueue.isEmpty) {
      val peek = pendingAllowedQueue.peek()
      if (peek.amount <= amountLeftToWake) {
        val head = pendingAllowedQueue.poll()
        amountLeftToWake -= head.amount
        head.wakeUpItMightBeWorthIt()
        ret = true
      } else {
        return ret
      }
    }
    ret
  }

  private def wakeUpPinned(): Boolean = synchronized {
    val amountLeftToWake = pinnedLimit - currentPinnedAllocated
    wakeUpAsNeeded(amountLeftToWake)
  }

  private def wakeUpNonPinned(): Boolean = synchronized {
    val amountLeftToWake = nonPinnedLimit - (currentNonPinnedAllocated + currentNonPinnedReserved)
    wakeUpAsNeeded(amountLeftToWake)
  }

  private def releasePinned(amount: Long): Unit = synchronized {
    currentPinnedAllocated -= amount
    if (wakeUpPinned()) {
      wakeUpNonPinned()
    }
  }

  private def releaseNonPinned(amount: Long): Unit = synchronized {
    currentNonPinnedAllocated -= amount
    if (wakeUpNonPinned()) {
      wakeUpPinned()
    }
  }

  private def releaseNonPinnedReservation(reservedAmount: Long): Unit = synchronized {
    currentNonPinnedReserved -= reservedAmount
    if (wakeUpPinned()) {
      wakeUpNonPinned()
    }
  }

  private def tryReservePinned(amount: Long): Option[HostMemoryReservation] = {
    val ret = Option(PinnedMemoryPool.tryReserve(amount))
    ret.map { reservation =>
      new WrappedPinnedReservation(reservation)
    }
  }

  private def tryReserveNonPinned(amount: Long): Option[HostMemoryReservation] = {
    if (isUnlimited) {
      Some(UnlimitedReservation)
    } else {
      synchronized {
        if ((currentNonPinnedAllocated + currentNonPinnedReserved + amount) <= nonPinnedLimit) {
          currentNonPinnedReserved += amount
          Some(new NonPinnedReservation(amount))
        } else {
          None
        }
      }
    }
  }

  private def tryAllocPinned(amount: Long): Option[HostMemoryBuffer] = {
    val ret = Option(PinnedMemoryPool.tryAllocate(amount))
    ret.foreach { b =>
      synchronized {
        currentPinnedAllocated += amount
      }
      HostAlloc.addEventHandler(b, new OnPinnedCloseCallback(amount))
    }
    ret
  }

  private def allocNonPinnedFromReserved(amount: Long): HostMemoryBuffer = {
    val ret = if (isUnlimited) {
      HostMemoryBuffer.allocate(amount, false)
    } else {
      synchronized {
        currentNonPinnedReserved -= amount
        currentNonPinnedAllocated += amount
        HostMemoryBuffer.allocate(amount, false)
      }
    }
    if (ret == null) {
      throw new OutOfMemoryError(s"Internal Error: could not allocate non-pinned memory $amount")
    }

    HostAlloc.addEventHandler(ret, new OnCloseCallback(amount))
  }

  private def tryAllocNonPinned(amount: Long): Option[HostMemoryBuffer] = {
    val ret = if (isUnlimited) {
      Some(HostMemoryBuffer.allocate(amount, false))
    } else {
      synchronized {
        if ((currentNonPinnedAllocated + currentNonPinnedReserved + amount) <= nonPinnedLimit) {
          currentNonPinnedAllocated += amount
          Some(HostMemoryBuffer.allocate(amount, false))
        } else {
          None
        }
      }
    }
    ret.foreach { b =>
      HostAlloc.addEventHandler(b, new OnCloseCallback(amount))
    }
    ret
  }

  private def checkSize(amount: Long, tryPinned: Boolean): Unit = {
    val pinnedFailed = (isPinnedOnly || tryPinned) && (amount > pinnedLimit)
    val nonPinnedFailed = isPinnedOnly || (amount > nonPinnedLimit)
    if (pinnedFailed && nonPinnedFailed) {
      throw new IllegalArgumentException(s"The amount requested $amount is larger than the " +
      s"maximum pool size ${math.max(pinnedLimit, nonPinnedLimit)}")
    }
  }

  def tryAlloc(amount: Long, preferPinned: Boolean = true): Option[HostMemoryBuffer] = {
    checkSize(amount, preferPinned)
    val firstPass = if (preferPinned) {
      tryAllocPinned(amount)
    } else {
      tryAllocNonPinned(amount)
    }
    firstPass.orElse {
      if (preferPinned) {
        tryAllocNonPinned(amount)
      } else {
        tryAllocPinned(amount)
      }
    }
  }

  def alloc(amount: Long, preferPinned: Boolean = true): HostMemoryBuffer = synchronized {
    var ret: Option[HostMemoryBuffer] = None
    var blocked: BlockedAllocation = null
    do {
      ret = tryAlloc(amount, preferPinned)
      if (ret.isEmpty) {
        if (blocked == null) {
          blocked = new BlockedAllocation(amount, TaskContext.get().taskAttemptId())
        }
        pendingAllowedQueue.offer(blocked)
        blocked.waitUntilPossiblyReady()
      }
    } while(ret.isEmpty)
    ret.get
  }

  def reserve(amount: Long, preferPinned: Boolean): HostMemoryReservation = synchronized {
    var ret: Option[HostMemoryReservation] = None
    var blocked: BlockedAllocation = null
    do {
      checkSize(amount, preferPinned)
      val firstPass = if (preferPinned) {
        tryReservePinned(amount)
      } else {
        tryReserveNonPinned(amount)
      }
      ret = firstPass.orElse {
        if (preferPinned) {
          tryReserveNonPinned(amount)
        } else {
          tryReservePinned(amount)
        }
      }
      if (ret.isEmpty) {
        if (blocked == null) {
          blocked = new BlockedAllocation(amount, TaskContext.get().taskAttemptId())
        }
        pendingAllowedQueue.offer(blocked)
        blocked.waitUntilPossiblyReady()
      }
    } while (ret.isEmpty)
    ret.get
  }
}

/**
 * A new API for host memory allocation. This can be used to limit the amount of host memory.
 */
object HostAlloc {
  private val ALIGNMENT = ColumnView.hostPaddingSizeInBytes
  private def align(amount: Long): Long = {
    ((amount + ALIGNMENT - 1) / ALIGNMENT) * ALIGNMENT
  }

  private var singleton: HostAlloc = new HostAlloc(-1)

  private def getSingleton: HostAlloc = synchronized {
    singleton
  }

  def initialize(nonPinnedLimit: Long): Unit = synchronized {
    singleton = new HostAlloc(nonPinnedLimit)
  }

  def tryAlloc(amount: Long, preferPinned: Boolean = true): Option[HostMemoryBuffer] = {
    getSingleton.tryAlloc(amount, preferPinned)
  }

  def alloc(amount: Long, preferPinned: Boolean = true): HostMemoryBuffer = {
    getSingleton.alloc(amount, preferPinned)
  }

  def reserve(amount: Long, preferPinned: Boolean = true): HostMemoryReservation = {
    getSingleton.reserve(amount, preferPinned)
  }

  def addEventHandler(buff: HostMemoryBuffer,
                      handler: MemoryBuffer.EventHandler): HostMemoryBuffer = {
    buff.synchronized {
      val previous = Option(buff.getEventHandler)
      val handlerToSet = previous.map { p =>
        MultiEventHandler(p, handler)
      }.getOrElse {
        handler
      }
      buff.setEventHandler(handlerToSet)
      buff
    }
  }

  private def removeEventHandlerFrom(
      multiEventHandler: MultiEventHandler,
      handler: MemoryBuffer.EventHandler): MemoryBuffer.EventHandler = {
    if (multiEventHandler.a == handler) {
      multiEventHandler.b
    } else if (multiEventHandler.b == handler) {
      multiEventHandler.a
    } else multiEventHandler.a match {
      case oldA: MultiEventHandler =>
        // From how the MultiEventHandler is set up we know that b cannot be one
        val newA = removeEventHandlerFrom(oldA, handler)
        MultiEventHandler(newA, multiEventHandler.b)
      case _ =>
        multiEventHandler
    }
  }

  def removeEventHandler(buff: HostMemoryBuffer,
                         handler: MemoryBuffer.EventHandler): HostMemoryBuffer = {
    buff.synchronized {
      val newHandler = buff.getEventHandler match {
        case multi: MultiEventHandler =>
          removeEventHandlerFrom(multi, handler)
        case other =>
          if (other == handler) null else other
      }
      buff.setEventHandler(newHandler)
      buff
    }
  }

  private case class MultiEventHandler(a: MemoryBuffer.EventHandler,
                                       b: MemoryBuffer.EventHandler)
    extends MemoryBuffer.EventHandler {
    override def onClosed(i: Int): Unit = {
      var t: Option[Throwable] = None
      try {
        a.onClosed(i)
      } catch {
        case e: Throwable =>
          t = Some(e)
      }
      try {
        b.onClosed(i)
      } catch {
        case e: Throwable =>
          t match {
            case Some(previousError) =>
              previousError.addSuppressed(e)
            case None =>
              t = Some(e)
          }
      }
      t.foreach { error =>
        throw error
      }
    }
  }
}
