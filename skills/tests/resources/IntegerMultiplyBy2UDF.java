package com.udf;

import org.apache.spark.sql.api.java.UDF1;

public class IntegerMultiplyBy2UDF implements UDF1<Integer, Integer> {
  @Override
  public Integer call(Integer value) {
    return value == null ? null : value * 2;
  }
}
