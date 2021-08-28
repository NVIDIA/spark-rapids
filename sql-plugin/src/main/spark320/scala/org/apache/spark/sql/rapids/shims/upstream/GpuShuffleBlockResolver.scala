package org.apache.spark.sql.rapids.shims.upstream

import com.nvidia.spark.rapids.ShuffleBufferCatalog

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.sql.rapids.GpuShuffleBlockResolverBase
import org.apache.spark.storage.ShuffleMergedBlockId

class GpuShuffleBlockResolver(resolver: IndexShuffleBlockResolver, catalog: ShuffleBufferCatalog)
    extends GpuShuffleBlockResolverBase(resolver, catalog) {

  /**
 * Retrieve the data for the specified merged shuffle block as multiple chunks.
 */
  def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    throw new UnsupportedOperationException("TODO")
  }

  /**
   * Retrieve the meta data for the specified merged shuffle block.
   */
  def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    throw new UnsupportedOperationException("TODO")
  }
}
