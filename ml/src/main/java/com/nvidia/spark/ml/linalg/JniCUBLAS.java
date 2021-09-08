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

package com.nvidia.spark.ml.linalg;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;

public final class JniCUBLAS {
  private static final JniCUBLAS instance = new JniCUBLAS();

  private JniCUBLAS() {
    String osArch = System.getProperty("os.arch");
    if (osArch == null || osArch.isEmpty()) {
      throw new RuntimeException("Unable to load native implementation");
    }
    String osName = System.getProperty("os.name");
    if (osName == null || osName.isEmpty()) {
      throw new RuntimeException("Unable to load native implementation");
    }

    Path temp;
    try (InputStream resource = this.getClass().getClassLoader().getResourceAsStream(
        String.format("%s/%s/libcublas_jni.so", osArch, osName))) {
      assert resource != null;
      Files.copy(resource, temp = Files.createTempFile("libcublas_jni.so", "",
              PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-x---"))),
          StandardCopyOption.REPLACE_EXISTING);
      temp.toFile().deleteOnExit();
    } catch (IOException e) {
      throw new RuntimeException("Unable to load native implementation", e);
    }

    System.load(temp.toString());
  }

  public static JniCUBLAS getInstance() {
    return instance;
  }

  public native void dspr(int n, double[] x, double[] A);

  public native void dgemm(int m, int n, double[] A, double[] C, int deviceID);
  public native void dgemm_b(int m, int n, int k, double[] A, double[] B, double[] C, int deviceID);
}
