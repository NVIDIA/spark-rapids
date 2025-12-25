/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.protobuf

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors

/**
 * Minimal descriptor utilities for locating a message descriptor in a FileDescriptorSet.
 *
 * This is intentionally lightweight for the "simple types" from_protobuf patch: it supports
 * descriptor sets produced by `protoc --include_imports --descriptor_set_out=...`.
 */
object ProtobufDescriptorUtils {

  def buildMessageDescriptor(
      fileDescriptorSetBytes: Array[Byte],
      messageName: String): Descriptors.Descriptor = {
    val fds = DescriptorProtos.FileDescriptorSet.parseFrom(fileDescriptorSetBytes)
    val protos = fds.getFileList.asScala.toSeq
    val byName = protos.map(p => p.getName -> p).toMap
    val cache = mutable.HashMap.empty[String, Descriptors.FileDescriptor]

    def buildFileDescriptor(name: String): Descriptors.FileDescriptor = {
      cache.getOrElseUpdate(name, {
        val p = byName.getOrElse(name,
          throw new IllegalArgumentException(s"Missing FileDescriptorProto for '$name'"))
        val deps = p.getDependencyList.asScala.map(buildFileDescriptor _).toArray
        Descriptors.FileDescriptor.buildFrom(p, deps)
      })
    }

    val fileDescriptors = protos.map(p => buildFileDescriptor(p.getName))
    val candidates = fileDescriptors.iterator.flatMap(fd => findMessageDescriptors(fd, messageName))
      .toSeq

    candidates match {
      case Seq(d) => d
      case Seq() =>
        throw new IllegalArgumentException(
          s"Message '$messageName' not found in FileDescriptorSet")
      case many =>
        val names = many.map(_.getFullName).distinct.sorted
        throw new IllegalArgumentException(
          s"Message '$messageName' is ambiguous; matches: ${names.mkString(", ")}")
    }
  }

  private def findMessageDescriptors(
      fd: Descriptors.FileDescriptor,
      messageName: String): Iterator[Descriptors.Descriptor] = {
    def matches(d: Descriptors.Descriptor): Boolean = {
      d.getName == messageName ||
        d.getFullName == messageName ||
        d.getFullName.endsWith("." + messageName)
    }

    def walk(d: Descriptors.Descriptor): Iterator[Descriptors.Descriptor] = {
      val nested = d.getNestedTypes.asScala.iterator.flatMap(walk _)
      if (matches(d)) Iterator.single(d) ++ nested else nested
    }

    fd.getMessageTypes.asScala.iterator.flatMap(walk _)
  }
}
