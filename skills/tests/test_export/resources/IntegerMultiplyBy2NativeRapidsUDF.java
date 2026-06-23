package com.udf;

import ai.rapids.cudf.ColumnVector;
import com.nvidia.spark.RapidsUDF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.api.java.UDF1;

public class IntegerMultiplyBy2NativeRapidsUDF extends UDF
        implements UDF1<Integer, Integer>, RapidsUDF {
    @Override
    public Integer call(Integer value) {
        return value == null ? null : value * 2;
    }

    @Override
    public ColumnVector evaluateColumnar(int numRows, ColumnVector... args) {
        NativeUDFLoader.ensureLoaded();
        return new ColumnVector(integerMultiplyBy2(args[0].getNativeView()));
    }

    private static native long integerMultiplyBy2(long inputView);
}
