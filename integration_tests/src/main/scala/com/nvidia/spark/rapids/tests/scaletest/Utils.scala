package com.nvidia.spark.rapids.tests.scaletest

import java.io.{PrintWriter, StringWriter}

object Utils {
  def stackTraceAsString(e: Throwable): String = {
    val sw = new StringWriter()
    val w = new PrintWriter(sw)
    e.printStackTrace(w)
    w.close()
    sw.toString
  }
}
