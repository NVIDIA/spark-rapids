package com.nvidia.spark.rapids.shims.v2

import org.apache.orc.Reader

object OrcShims extends OrcShims301until320Base {

  // ORC Reader of the 311cdh Spark has no close method.
  // The resource is closed internally.
  def withReader[V](r: Reader)(block: Reader => V): V = {
    block(r)
  }

  // empty
  def closeReader(reader: Reader): Unit = {
  }

}
