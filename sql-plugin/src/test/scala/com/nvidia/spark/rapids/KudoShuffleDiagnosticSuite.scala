package com.nvidia.spark.rapids

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.nvidia.spark.rapids.jni.kudo.KudoSerializer
import org.scalatest.funsuite.AnyFunSuite

class KudoShuffleDiagnosticSuite extends AnyFunSuite {
  test("invalid second Kudo header reports its decompressed offset") {
    val output = new ByteArrayOutputStream()
    KudoSerializer.writeRowCountToStream(output, 1)
    new DataOutputStream(output).writeInt(0x78332E39)

    val input = new DataInputStream(new ByteArrayInputStream(output.toByteArray))
    val iterator = new KudoSerializedBatchIterator(input, NoopMetric)
    assert(iterator.peekNextBatchSize().contains(0L))
    val (_, batch) = iterator.next()
    batch.close()

    val error = intercept[IllegalStateException] {
      iterator.peekNextBatchSize()
    }
    assert(error.getMessage.contains("decompressedOffset=28"))
    assert(error.getMessage.contains("previousHeaderOffset=0"))
    assert(error.getMessage.contains("expectedNextHeaderOffset=28"))
    assert(error.getCause.getMessage.contains("found 2016620089"))
  }

  test("end of a Kudo stream is not reported as a corrupt header") {
    val output = new ByteArrayOutputStream()
    KudoSerializer.writeRowCountToStream(output, 1)

    val input = new DataInputStream(new ByteArrayInputStream(output.toByteArray))
    val iterator = new KudoSerializedBatchIterator(input, NoopMetric)
    val (_, batch) = iterator.next()
    batch.close()

    assert(iterator.peekNextBatchSize().isEmpty)
  }
}
