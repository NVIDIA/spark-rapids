// automatically generated by the FlatBuffers compiler, do not modify

package com.nvidia.spark.rapids.format;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
/**
 * Metadata about cuDF Columns
 */
public final class ColumnMeta extends Table {
  public static ColumnMeta getRootAsColumnMeta(ByteBuffer _bb) { return getRootAsColumnMeta(_bb, new ColumnMeta()); }
  public static ColumnMeta getRootAsColumnMeta(ByteBuffer _bb, ColumnMeta obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public ColumnMeta __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * number of nulls in the column or -1 if unknown
   */
  public long nullCount() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public boolean mutateNullCount(long null_count) { int o = __offset(4); if (o != 0) { bb.putLong(o + bb_pos, null_count); return true; } else { return false; } }
  /**
   * number of rows in the column
   */
  public long rowCount() { int o = __offset(6); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public boolean mutateRowCount(long row_count) { int o = __offset(6); if (o != 0) { bb.putLong(o + bb_pos, row_count); return true; } else { return false; } }
  /**
   * information about the column data buffer
   */
  public SubBufferMeta data() { return data(new SubBufferMeta()); }
  public SubBufferMeta data(SubBufferMeta obj) { int o = __offset(8); return o != 0 ? obj.__assign(o + bb_pos, bb) : null; }
  /**
   * information about the column validity buffer
   */
  public SubBufferMeta validity() { return validity(new SubBufferMeta()); }
  public SubBufferMeta validity(SubBufferMeta obj) { int o = __offset(10); return o != 0 ? obj.__assign(o + bb_pos, bb) : null; }
  /**
   * information about the column offset buffer
   */
  public SubBufferMeta offsets() { return offsets(new SubBufferMeta()); }
  public SubBufferMeta offsets(SubBufferMeta obj) { int o = __offset(12); return o != 0 ? obj.__assign(o + bb_pos, bb) : null; }
  /**
   * child column metadata
   */
  public ColumnMeta children(int j) { return children(new ColumnMeta(), j); }
  public ColumnMeta children(ColumnMeta obj, int j) { int o = __offset(14); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int childrenLength() { int o = __offset(14); return o != 0 ? __vector_len(o) : 0; }
  /**
   * ordinal of DType enum
   */
  public int dtypeId() { int o = __offset(16); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public boolean mutateDtypeId(int dtype_id) { int o = __offset(16); if (o != 0) { bb.putInt(o + bb_pos, dtype_id); return true; } else { return false; } }
  /**
   * DType scale for decimal types
   */
  public int dtypeScale() { int o = __offset(18); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public boolean mutateDtypeScale(int dtype_scale) { int o = __offset(18); if (o != 0) { bb.putInt(o + bb_pos, dtype_scale); return true; } else { return false; } }

  public static void startColumnMeta(FlatBufferBuilder builder) { builder.startObject(8); }
  public static void addNullCount(FlatBufferBuilder builder, long nullCount) { builder.addLong(0, nullCount, 0L); }
  public static void addRowCount(FlatBufferBuilder builder, long rowCount) { builder.addLong(1, rowCount, 0L); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addStruct(2, dataOffset, 0); }
  public static void addValidity(FlatBufferBuilder builder, int validityOffset) { builder.addStruct(3, validityOffset, 0); }
  public static void addOffsets(FlatBufferBuilder builder, int offsetsOffset) { builder.addStruct(4, offsetsOffset, 0); }
  public static void addChildren(FlatBufferBuilder builder, int childrenOffset) { builder.addOffset(5, childrenOffset, 0); }
  public static int createChildrenVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startChildrenVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addDtypeId(FlatBufferBuilder builder, int dtypeId) { builder.addInt(6, dtypeId, 0); }
  public static void addDtypeScale(FlatBufferBuilder builder, int dtypeScale) { builder.addInt(7, dtypeScale, 0); }
  public static int endColumnMeta(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
}

