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

package com.nvidia.spark;

/**
 * An interface that can be used by Retry framework of RAPIDS Plugin to handle the GPU OOMs.
 *
 * GPU memory is a limited resource, so OOM can happen if too many tasks run in parallel.
 * Retry framework is introduced to improve the stability by retrying the work when it
 * meets OOMs. The overall process of Retry framework is similar as the below.
 *    ```
 *      Retryable retryable
 *      retryable.checkpoint()
 *      boolean hasOOM = false
 *      do {
 *        try {
 *          runWorkOnGpu(retryable) // May lead to GPU OOM
 *          hasOOM = false
 *        } catch (OOMError oom) {
 *          hasOOM = true
 *          tryToReleaseSomeGpuMemoryFromLowPriorityTasks()
 *          retryable.restore()
 *        }
 *      } while(hasOOM)
 *    ```
 * In a retry, "checkpoint" will be called first and only once, which is used to save the
 * state for later loops. When OOM happens, "restore" will be called to restore the
 * state that saved by "checkpoint". After that, it will try the same work again. And
 * the whole process runs on Spark executors.
 *
 * Retry framework expects the "runWorkOnGpu" always outputs the same result when running
 * it multiple times in a retry. So if "runWorkOnGpu" is non-deterministic, it can not be
 * used by Retry framework.
 * The "Retryable" is designed for this kind of cases. By implementing this interface,
 * "runWorkOnGpu" can become deterministic inside a retry process, making it usable for
 * Retry framework to improve the stability.
 */
public interface Retryable {
  /**
   * Save the state, so it can be restored in case of an OOM Retry.
   * This is called inside a Spark task context on executors.
   */
  void checkpoint();

  /**
   * Restore the state that was saved by calling to "checkpoint".
   * This is called inside a Spark task context on executors.
   */
  void restore();
}
