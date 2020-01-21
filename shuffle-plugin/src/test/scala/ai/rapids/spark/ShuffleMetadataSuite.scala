package ai.rapids.spark

import ai.rapids.cudf.ColumnVector.BufferType
import ai.rapids.cudf.{ColumnVector, DType, Table}
import ai.rapids.spark.format.ColumnMeta
import org.scalatest.FunSuite

class ShuffleMetadataSuite extends FunSuite {

  test ("can create shuffle metadata for strings without nulls") {
    val cv = ColumnVector.fromStrings("hello", "strings", "foo")
    println(cv.getType)
    val table = new Table(cv)
    val meta = ShuffleMetadata.materializeResponse(ShuffleMetadata.getShuffleMetadataResponse(true, Seq(table).toArray, 0))

    assert(meta.tableMetaLength() == 1)
    assert(meta.tableMeta(0).columnMetasLength() == 1)
    val colMeta: ColumnMeta = meta.tableMeta(0).columnMetas(0)
    assert(DType.values()(colMeta.dType()) == DType.STRING)
    assert(colMeta.data().len() == cv.getDeviceBufferFor(BufferType.DATA).getLength)
    assert(colMeta.offsets().len() == cv.getDeviceBufferFor(BufferType.OFFSET).getLength)
    assert(colMeta.validity() == null)
  }

  test ("can create shuffle metadata for strings with nulls") {
    val cv = ColumnVector.fromStrings("hello", "strings", null, "foo")
    println(cv.getType)
    val table = new Table(cv)
    val meta = ShuffleMetadata.materializeResponse(ShuffleMetadata.getShuffleMetadataResponse(true, Seq(table).toArray, 0))

    assert(meta.tableMetaLength() == 1)
    assert(meta.tableMeta(0).columnMetasLength() == 1)
    val colMeta: ColumnMeta = meta.tableMeta(0).columnMetas(0)
    assert(DType.values()(colMeta.dType()) == DType.STRING)
    assert(colMeta.data().len() == cv.getDeviceBufferFor(BufferType.DATA).getLength)
    assert(colMeta.offsets().len() == cv.getDeviceBufferFor(BufferType.OFFSET).getLength)
    assert(colMeta.validity().len() == cv.getDeviceBufferFor(BufferType.VALIDITY).getLength)
  }
}
