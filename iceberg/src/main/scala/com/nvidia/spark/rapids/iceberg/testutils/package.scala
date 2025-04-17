package com.nvidia.spark.rapids.iceberg

import ai.rapids.cudf.HostColumnVectorCore
import org.apache.iceberg.common.DynMethods

package object testutils {
  private val HOST_COL_GET_ELEMENT: DynMethods.UnboundMethod = DynMethods
    .builder("getElement")
    .hiddenImpl(classOf[HostColumnVectorCore], classOf[Int])
    .buildChecked()

  def elementAt(hostColumnVector: HostColumnVectorCore, index: Int): AnyRef = {
    HOST_COL_GET_ELEMENT
      .invoke(hostColumnVector, java.lang.Integer.valueOf(index))
      .asInstanceOf[AnyRef]
  }
}
