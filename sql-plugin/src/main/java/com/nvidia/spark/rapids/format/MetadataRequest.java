// automatically generated by the FlatBuffers compiler, do not modify

package com.nvidia.spark.rapids.format;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
/**
 * Flat buffer for Rapids UCX Shuffle Metadata Request.
 */
public final class MetadataRequest extends Table {
  public static MetadataRequest getRootAsMetadataRequest(ByteBuffer _bb) { return getRootAsMetadataRequest(_bb, new MetadataRequest()); }
  public static MetadataRequest getRootAsMetadataRequest(ByteBuffer _bb, MetadataRequest obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public MetadataRequest __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * array of shuffle block descriptors for which metadata is needed
   */
  public BlockIdMeta blockIds(int j) { return blockIds(new BlockIdMeta(), j); }
  public BlockIdMeta blockIds(BlockIdMeta obj, int j) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int blockIdsLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createMetadataRequest(FlatBufferBuilder builder,
      int block_idsOffset) {
    builder.startObject(1);
    MetadataRequest.addBlockIds(builder, block_idsOffset);
    return MetadataRequest.endMetadataRequest(builder);
  }

  public static void startMetadataRequest(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addBlockIds(FlatBufferBuilder builder, int blockIdsOffset) { builder.addOffset(0, blockIdsOffset, 0); }
  public static int createBlockIdsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startBlockIdsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endMetadataRequest(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishMetadataRequestBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedMetadataRequestBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }
}

