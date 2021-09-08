/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package org.apache.spark.ml.linalg

import com.nvidia.spark.ml.linalg.JniCUBLAS

/**
 * BLAS routines for MLlib's vectors and matrices.
 */
private[spark] object CUBLAS extends Serializable {

  @transient private var _jniCUBLAS: JniCUBLAS = _

  private[spark] def jniCUBLAS: JniCUBLAS = {
    if (_jniCUBLAS == null) {
      _jniCUBLAS = JniCUBLAS.getInstance
    }
    _jniCUBLAS
  }

  /**
   * Adds alpha * v * v.t to a matrix in-place. This is the same as BLAS's ?SPR.
   *
   * @param U the upper triangular part of the matrix packed in an array (column major)
   */
  def spr(v: DenseVector, U: Array[Double]): Unit = {
    jniCUBLAS.dspr(v.size, v.values, U)
  }

  /**
   * C := B.transpose * B, only used for PCA covariance matrix computation.
   *
   * @param B the matrix B that will be left multiplied by its transpose. Size of m x n.
   * @param C the resulting matrix C. Size of n x n.
   * @param deviceID the GPU index this function will run on.
   */
  def gemm(B: DenseMatrix, C: DenseMatrix, deviceID: Int): Unit = {
    val rows = B.numRows
    val cols = B.numCols
    require(B.isTransposed, "B is not transposed")
    require(C.numRows == cols, s"The rows of C don't match the columns of B. C: ${C.numRows}, B: $cols")
    require(C.numCols == cols, s"The columns of C don't match the columns of B. C: ${C.numCols}, B: $cols")
    // Since B is transposed, we treat it as A in JNI.
    jniCUBLAS.dgemm(rows, cols, B.values, C.values, deviceID)
  }

  /**
   * C := A * B, only used for PCA transform computation
   *
   * @param A the matrix A that will multiply matrix B. Size of m x n. The raw matrix.
   * @param B the matrix B that will be left multiplied by A. Size of n x k. The principal compunent matrix.
   * @param C the resulting matrix C. Size of m x k.
   * @param deviceID the GPU index this function will run on.
   */
  def gemm_b(A: DenseMatrix, B: DenseMatrix, C: DenseMatrix, deviceID: Int): Unit = {
    require(C.numRows == A.numRows, s"The rows of C don't match the columns of B. C: ${C.numRows}, B: ${A.numRows}")
    require(C.numCols == B.numCols, s"The columns of C don't match the columns of B. C: ${C.numCols}, B: ${B.numCols}")
    jniCUBLAS.dgemm_b(A.numRows, B.numCols, A.numCols, A.values, B.values, C.values, deviceID)
  }
}
