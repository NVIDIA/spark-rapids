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

package org.apache.iceberg.spark.source;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Package-local access to Iceberg Spark write classes.
 *
 * <p>Iceberg keeps SparkWrite and SparkPositionDeltaWrite package-private. Resolve those
 * classes from a conventional-root helper so runtime package access is checked in the same
 * class loader as Iceberg itself.
 */
public final class GpuSparkWriteAccess {
  private static final String SPARK_WRITE_CLASS =
      "org.apache.iceberg.spark.source.SparkWrite";
  private static final String SPARK_POSITION_DELTA_WRITE_CLASS =
      "org.apache.iceberg.spark.source.SparkPositionDeltaWrite";
  private static final String TASK_COMMIT_CLASS =
      "org.apache.iceberg.spark.source.SparkWrite$TaskCommit";
  private static final String DELTA_TASK_COMMIT_CLASS =
      "org.apache.iceberg.spark.source.SparkPositionDeltaWrite$DeltaTaskCommit";

  private GpuSparkWriteAccess() {
  }

  public static boolean supports(Class<? extends Write> cpuClass) {
    ClassLoader loader = cpuClass.getClassLoader();
    return isAssignableFrom(SPARK_WRITE_CLASS, cpuClass, loader)
        || isAssignableFrom(SPARK_POSITION_DELTA_WRITE_CLASS, cpuClass, loader);
  }

  public static String sparkWriteClassName() {
    return SPARK_WRITE_CLASS;
  }

  public static WriterCommitMessage taskCommit(Object files) {
    Object commit = newInstance(TASK_COMMIT_CLASS, bridgeClassLoader(), files);
    invoke(commit, "reportOutputMetrics");
    return (WriterCommitMessage) commit;
  }

  public static Object[] taskCommitFiles(WriterCommitMessage message) {
    return (Object[]) invoke(message, "files");
  }

  public static WriterCommitMessage deltaTaskCommit(Object result) {
    return (WriterCommitMessage) newInstance(DELTA_TASK_COMMIT_CLASS, bridgeClassLoader(), result);
  }

  private static ClassLoader bridgeClassLoader() {
    return GpuSparkWriteAccess.class.getClassLoader();
  }

  private static boolean isAssignableFrom(
      String className,
      Class<? extends Write> cpuClass,
      ClassLoader loader) {
    Class<?> baseClass = loadClass(className, loader);
    return baseClass != null && baseClass.isAssignableFrom(cpuClass);
  }

  private static Class<?> loadClass(String className, ClassLoader loader) {
    try {
      return Class.forName(className, false, loader);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  private static Object newInstance(String className, ClassLoader loader, Object arg) {
    Class<?> targetClass = requireClass(className, loader);
    Constructor<?> constructor = findConstructor(targetClass, arg.getClass());
    try {
      constructor.setAccessible(true);
      return constructor.newInstance(arg);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to instantiate " + className, e);
    }
  }

  private static Constructor<?> findConstructor(Class<?> targetClass, Class<?> argClass) {
    for (Constructor<?> constructor : targetClass.getDeclaredConstructors()) {
      Class<?>[] parameterTypes = constructor.getParameterTypes();
      if (parameterTypes.length == 1 && parameterTypes[0].isAssignableFrom(argClass)) {
        return constructor;
      }
    }
    throw new IllegalStateException("No matching constructor for " + targetClass.getName());
  }

  private static Object invoke(Object target, String methodName) {
    try {
      Method method = target.getClass().getDeclaredMethod(methodName);
      method.setAccessible(true);
      return method.invoke(target);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke " + methodName + " on " + target.getClass().getName(), e);
    }
  }

  private static Class<?> requireClass(String className, ClassLoader loader) {
    Class<?> targetClass = loadClass(className, loader);
    if (targetClass == null) {
      throw new IllegalStateException("Unable to load " + className);
    }
    return targetClass;
  }
}
