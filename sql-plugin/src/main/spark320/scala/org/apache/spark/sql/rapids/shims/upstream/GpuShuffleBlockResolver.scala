package org.apache.spark.sql.rapids.shims.upstream

import com.nvidia.spark.rapids.ShuffleBufferCatalog

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.sql.rapids.GpuShuffleBlockResolverBase
import org.apache.spark.storage.ShuffleBlockId

class GpuShuffleBlockResolver(resolver: IndexShuffleBlockResolver, catalog: ShuffleBufferCatalog)
    extends GpuShuffleBlockResolverBase(resolver, catalog) {
  override def getMergedBlockData(
      blockId: ShuffleBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    throw new UnsupportedOperationException("TODO after shim is done")
  }

  override def getMergedBlockMeta(
      blockId: ShuffleBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    throw new UnsupportedOperationException("TODO after shim is done")
  }
}
