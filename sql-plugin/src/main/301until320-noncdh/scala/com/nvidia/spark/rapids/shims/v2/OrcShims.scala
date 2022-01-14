package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.orc.Reader

object OrcShims extends OrcShims301until320Base {

  // the ORC Reader in non CDH Spark is closeable
  def withReader[T <: AutoCloseable, V](r: T)(block: T => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  // the ORC Reader in non CDH Spark is closeable
  def closeReader(reader: Reader): Unit = {
    if (reader != null) {
      reader.close()
    }
  }
}
