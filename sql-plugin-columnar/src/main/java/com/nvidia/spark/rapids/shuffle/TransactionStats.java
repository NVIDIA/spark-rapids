/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle;

import java.util.Objects;

/** Statistics for a shuffle transaction. */
public final class TransactionStats {
  private final double txTimeMs;
  private final long sendSize;
  private final long receiveSize;
  private final double sendThroughput;
  private final double recvThroughput;

  public TransactionStats(double txTimeMs, long sendSize, long receiveSize,
      double sendThroughput, double recvThroughput) {
    this.txTimeMs = txTimeMs;
    this.sendSize = sendSize;
    this.receiveSize = receiveSize;
    this.sendThroughput = sendThroughput;
    this.recvThroughput = recvThroughput;
  }

  public double txTimeMs() {
    return txTimeMs;
  }

  public long sendSize() {
    return sendSize;
  }

  public long receiveSize() {
    return receiveSize;
  }

  public double sendThroughput() {
    return sendThroughput;
  }

  public double recvThroughput() {
    return recvThroughput;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TransactionStats)) {
      return false;
    }
    TransactionStats other = (TransactionStats) obj;
    return Double.compare(txTimeMs, other.txTimeMs) == 0 &&
        sendSize == other.sendSize &&
        receiveSize == other.receiveSize &&
        Double.compare(sendThroughput, other.sendThroughput) == 0 &&
        Double.compare(recvThroughput, other.recvThroughput) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(txTimeMs, sendSize, receiveSize, sendThroughput, recvThroughput);
  }

  @Override
  public String toString() {
    return "TransactionStats(" + txTimeMs + "," + sendSize + "," + receiveSize + "," +
        sendThroughput + "," + recvThroughput + ")";
  }
}
