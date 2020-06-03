/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.rapids.spark

import java.nio.ByteBuffer
import java.util.{Random => JavaRandom}

import scala.reflect.ClassTag
import scala.util.Random
import scala.util.hashing.MurmurHash3

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

object SamplingUtils {

  /**
    * Reservoir sampling implementation that also returns the input size.
    *
    * @param input input size
    * @param k reservoir size
    * @param seed random seed
    * @return (samples, input size)
    */
  def reservoirSampleAndCount[T: ClassTag](
                                            input: Iterator[T],
                                            k: Int,
                                            seed: Long = Random.nextLong()) : (Array[T], Long) = {
    val reservoir = new Array[T](k)
    // Put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val copyRow = input.next().asInstanceOf[UnsafeRow].copy()
      reservoir(i) = copyRow.asInstanceOf[T]
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement.
    if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      (trimReservoir, i)
    } else {
      // If input size > k, continue the sampling process.
      var l = i.toLong
      val rand = new XORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next().asInstanceOf[UnsafeRow].copy()
        l += 1
        // There are k elements in the reservoir, and the l-th element has been
        // consumed. It should be chosen with probability k/l. The expression
        // below is a random long chosen uniformly from [0,l)
        val replacementIndex = (rand.nextDouble() * l).toLong
        if (replacementIndex < k) {
          reservoir(replacementIndex.toInt) = item.asInstanceOf[T]
        }
      }
      (reservoir, l)
    }
  }
}


/**
  * This class implements a XORShift random number generator algorithm
  * Source:
  * Marsaglia, G. (2003). Xorshift RNGs. Journal of Statistical Software, Vol. 8, Issue 14.
  * @see <a href="http://www.jstatsoft.org/v08/i14/paper">Paper</a>
  * This implementation is approximately 3.5 times faster than
  * {@link java.util.Random java.util.Random}, partly because of the algorithm, but also due
  * to renouncing thread safety. JDK's implementation uses an AtomicLong seed, this class
  * uses a regular Long. We can forgo thread safety since we use a new instance of the RNG
  * for each thread.
  */
private[spark] class XORShiftRandom(init: Long) extends JavaRandom(init) {

  def this() = this(System.nanoTime)

  private var seed = XORShiftRandom.hashSeed(init)

  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
  override protected def next(bits: Int): Int = {
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    seed = nextSeed
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long) {
    seed = XORShiftRandom.hashSeed(s)
  }
}

/** Contains benchmark method and main method to run benchmark of the RNG */
private[spark] object XORShiftRandom {
  /** Hash seeds to have 0/1 bits throughout. */
  private def hashSeed(seed: Long): Long = {
    val bytes = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(seed).array()
    val lowBits = MurmurHash3.bytesHash(bytes, MurmurHash3.arraySeed)
    val highBits = MurmurHash3.bytesHash(bytes, lowBits)
    (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
  }
}
