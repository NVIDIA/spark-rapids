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

#include <cublas_v2.h>
#include <cuda_runtime.h>
#include <jni.h>

extern "C" {

JNIEXPORT void JNICALL Java_com_nvidia_spark_ml_linalg_JniCUBLAS_dspr(JNIEnv* env, jclass, jint n, jdoubleArray x,
                                                                      jdoubleArray A) {
  jclass jlexception = env->FindClass("java/lang/Exception");
  auto size_A = env->GetArrayLength(A);

  double* dev_x;
  auto cuda_error = cudaMalloc((void**)&dev_x, n * sizeof(double));
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error allocating device memory for x");
  }

  double* dev_A;
  cuda_error = cudaMalloc((void**)&dev_A, size_A * sizeof(double));
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error allocating device memory for A");
  }

  auto* host_x = env->GetDoubleArrayElements(x, nullptr);
  cuda_error = cudaMemcpyAsync(dev_x, host_x, n * sizeof(double), cudaMemcpyDefault);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error copying x to device");
  }

  auto* host_A = env->GetDoubleArrayElements(A, nullptr);
  cuda_error = cudaMemcpyAsync(dev_A, host_A, size_A * sizeof(double), cudaMemcpyDefault);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error copying A to device");
  }

  cublasHandle_t handle;
  auto status = cublasCreate(&handle);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error creating cuBLAS handle");
  }

  double alpha = 1.0;
  status = cublasDspr(handle, CUBLAS_FILL_MODE_UPPER, n, &alpha, dev_x, 1, dev_A);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error calling cublasDspr");
  }

  cuda_error = cudaMemcpyAsync(host_A, dev_A, size_A * sizeof(double), cudaMemcpyDefault);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error copying A from device");
  }

  cuda_error = cudaFree(dev_x);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error freeing x from device");
  }

  cuda_error = cudaFree(dev_A);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error freeing A from device");
  }

  status = cublasDestroy(handle);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error destroying cuBLAS handle");
  }

  env->ReleaseDoubleArrayElements(x, host_x, JNI_ABORT);
  env->ReleaseDoubleArrayElements(A, host_A, 0);
}

JNIEXPORT void JNICALL Java_com_nvidia_spark_ml_linalg_JniCUBLAS_dgemm(JNIEnv* env, jclass, jint rows, jint cols,
                                                                       jdoubleArray A, jdoubleArray C, jint deviceID) {
  cudaSetDevice(deviceID);
  jclass jlexception = env->FindClass("java/lang/Exception");
  auto size_A = env->GetArrayLength(A);
  auto size_C = env->GetArrayLength(C);

  double* dev_A;
  auto cuda_error = cudaMalloc((void**)&dev_A, size_A * sizeof(double));
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error allocating device memory for A");
  }

  double* dev_C;
  cuda_error = cudaMalloc((void**)&dev_C, size_C * sizeof(double));
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error allocating device memory for C");
  }

  auto* host_A = env->GetDoubleArrayElements(A, nullptr);
  cuda_error = cudaMemcpyAsync(dev_A, host_A, size_A * sizeof(double), cudaMemcpyDefault);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error copying A to device");
  }

  cublasHandle_t handle;
  auto status = cublasCreate(&handle);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error creating cuBLAS handle");
  }

  double alpha = 1.0;
  double beta = 0.0;
  status = cublasDgemm(handle, CUBLAS_OP_N, CUBLAS_OP_T, cols, cols, rows, &alpha, dev_A, cols, dev_A, cols, &beta,
                       dev_C, cols);
  cudaSetDevice(deviceID);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error calling cublasDgemm");
  }

  auto* host_C = env->GetDoubleArrayElements(C, nullptr);
  cuda_error = cudaMemcpyAsync(host_C, dev_C, size_C * sizeof(double), cudaMemcpyDefault);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error copying C from device");
  }

  cuda_error = cudaFree(dev_A);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error freeing A from device");
  }

  cuda_error = cudaFree(dev_C);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error freeing C from device");
  }

  status = cublasDestroy(handle);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error destroying cuBLAS handle");
  }

  env->ReleaseDoubleArrayElements(A, host_A, JNI_ABORT);
  env->ReleaseDoubleArrayElements(C, host_C, 0);
}

JNIEXPORT void JNICALL Java_com_nvidia_spark_ml_linalg_JniCUBLAS_dgemm_1b(JNIEnv* env, jclass, jint rows_a, jint cols_b, jint cols_a,
                                                                       jdoubleArray A, jdoubleArray B, jdoubleArray C, jint deviceID) {

  cudaSetDevice(deviceID);
  jclass jlexception = env->FindClass("java/lang/Exception");
  auto size_A = env->GetArrayLength(A);
  auto size_B = env->GetArrayLength(B);
  auto size_C = env->GetArrayLength(C);

  double* dev_A;
  auto cuda_error = cudaMalloc((void**)&dev_A, size_A * sizeof(double));
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error allocating device memory for A");
  }

   double* dev_B;
   cuda_error = cudaMalloc((void**)&dev_B, size_B * sizeof(double));
   if (cuda_error != cudaSuccess) {
     env->ThrowNew(jlexception, "Error allocating device memory for A");
   }

  double* dev_C;
  cuda_error = cudaMalloc((void**)&dev_C, size_C * sizeof(double));
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error allocating device memory for C");
  }

  auto* host_A = env->GetDoubleArrayElements(A, nullptr);
  cuda_error = cudaMemcpyAsync(dev_A, host_A, size_A * sizeof(double), cudaMemcpyDefault);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error copying A to device");
  }

  auto* host_B = env->GetDoubleArrayElements(B, nullptr);
  cuda_error = cudaMemcpyAsync(dev_B, host_B, size_B * sizeof(double), cudaMemcpyDefault);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error copying A to device");
  }

  cublasHandle_t handle;
  auto status = cublasCreate(&handle);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error creating cuBLAS handle");
  }

  double alpha = 1.0;
  double beta = 0.0;
  status = cublasDgemm(handle, CUBLAS_OP_T, CUBLAS_OP_N, rows_a, cols_b, cols_a, &alpha, dev_A, cols_a, dev_B, cols_a, &beta,
                       dev_C, rows_a);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error calling cublasDgemm");
  }

  auto* host_C = env->GetDoubleArrayElements(C, nullptr);
  cuda_error = cudaMemcpyAsync(host_C, dev_C, size_C * sizeof(double), cudaMemcpyDefault);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error copying C from device");
  }

  cuda_error = cudaFree(dev_A);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error freeing A from device");
  }

  cuda_error = cudaFree(dev_C);
  if (cuda_error != cudaSuccess) {
    env->ThrowNew(jlexception, "Error freeing C from device");
  }

  status = cublasDestroy(handle);
  if (status != CUBLAS_STATUS_SUCCESS) {
    env->ThrowNew(jlexception, "Error destroying cuBLAS handle");
  }

  env->ReleaseDoubleArrayElements(A, host_A, JNI_ABORT);
  env->ReleaseDoubleArrayElements(C, host_C, 0);
}

}  // extern "C"
