// automatically generated by the FlatBuffers compiler, do not modify

package com.nvidia.spark.rapids.format;

public final class CodecType {
  private CodecType() { }
  /**
   * data simply copied, codec is only for testing
   */
  public static final byte COPY = -1;
  /**
   * no compression codec was used on the data
   */
  public static final byte UNCOMPRESSED = 0;
  /**
   * data compressed with the nvcomp LZ4 codec
   */
  public static final byte NVCOMP_LZ4 = 1;
  /**
   * data compressed with the nvcomp ZSTD codec
   */
  public static final byte NVCOMP_ZSTD = 2;

  public static final String[] names = { "COPY", "UNCOMPRESSED", "NVCOMP_LZ4", "NVCOMP_ZSTD", };

  public static String name(int e) { return names[e - COPY]; }
}

