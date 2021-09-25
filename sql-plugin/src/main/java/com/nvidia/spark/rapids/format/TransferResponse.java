// automatically generated by the FlatBuffers compiler, do not modify

package com.nvidia.spark.rapids.format;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
/**
 * Flat buffer for Rapids UCX Shuffle Transfer Response.
 */
public final class TransferResponse extends Table {
  public static TransferResponse getRootAsTransferResponse(ByteBuffer _bb) { return getRootAsTransferResponse(_bb, new TransferResponse()); }
  public static TransferResponse getRootAsTransferResponse(ByteBuffer _bb, TransferResponse obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public TransferResponse __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * array of buffer responses, one for each requested
   */
  public BufferTransferResponse responses(int j) { return responses(new BufferTransferResponse(), j); }
  public BufferTransferResponse responses(BufferTransferResponse obj, int j) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int responsesLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createTransferResponse(FlatBufferBuilder builder,
      int responsesOffset) {
    builder.startObject(1);
    TransferResponse.addResponses(builder, responsesOffset);
    return TransferResponse.endTransferResponse(builder);
  }

  public static void startTransferResponse(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addResponses(FlatBufferBuilder builder, int responsesOffset) { builder.addOffset(0, responsesOffset, 0); }
  public static int createResponsesVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startResponsesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endTransferResponse(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishTransferResponseBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedTransferResponseBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }
}

