package com.udf

class IntegerMultiplyBy2UDF extends Function1[Integer, Integer] with Serializable {
  override def apply(value: Integer): Integer = {
    if (value == null) null else value * 2
  }
}
