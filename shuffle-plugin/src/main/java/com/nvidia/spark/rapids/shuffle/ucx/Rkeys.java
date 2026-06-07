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

package com.nvidia.spark.rapids.shuffle.ucx;

import java.nio.ByteBuffer;
import java.util.Objects;

import scala.collection.Seq;

/** UCX remote keys registered for a peer. */
public final class Rkeys {
  private final Seq<ByteBuffer> rkeys;

  public Rkeys(Seq<ByteBuffer> rkeys) {
    this.rkeys = rkeys;
  }

  public Seq<ByteBuffer> rkeys() {
    return rkeys;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Rkeys)) {
      return false;
    }
    Rkeys other = (Rkeys) obj;
    return Objects.equals(rkeys, other.rkeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rkeys);
  }

  @Override
  public String toString() {
    return "Rkeys(" + rkeys + ")";
  }
}
