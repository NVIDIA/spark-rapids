/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.python

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

/**
 * A class to hold all the info. for the Python UDF arguments.
 *
 * @param flattenedArgs All the distinct arguments
 * @param flattenedTypes The data types of the distinct arguments
 * @param argOffsets The offsets of the original arguments in "flattenedArgs"
 * @param argNames The optional argument names
 */
class GpuPythonArguments(
    val flattenedArgs: Seq[Expression],
    val flattenedTypes: Seq[DataType],
    val argOffsets: Array[Array[Int]],
    val argNames: Option[Array[Array[Option[String]]]])

/** Gpu version of ArgumentMetadata */
class GpuArgumentMeta(val offset: Int, val name: Option[String])
