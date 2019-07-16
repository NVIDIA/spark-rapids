/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.rapids.spark

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import ai.rapids.cudf.{ColumnVector, JCudfSerialization}

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext

/**
 * Serializer for serializing [[ColumnarBatch]]s during shuffle.
 * The batches will be stored in an internal format specific to rapids.
 */
class GpuColumnarBatchSerializer(
      numFields: Int,
      dataSize: SQLMetric = null) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance =
    new GpuColumnarBatchSerializerInstance(numFields, dataSize)
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class GpuColumnarBatchSerializerInstance(
    numFields: Int,
    dataSize: SQLMetric) extends SerializerInstance {

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val batch = value.asInstanceOf[ColumnarBatch]
      val numColumns = batch.numCols()
      val columns: Array[ColumnVector] = new Array(numColumns)
      var startRow = 0
      var numRows = batch.numRows()
      val firstCol = batch.column(0)
      if (firstCol.isInstanceOf[SlicedGpuColumnVector]) {
        // We don't have control over ColumnarBatch to put in the slice, so we have to do it
        // for each column.  In this case we are using the first column.
        startRow = firstCol.asInstanceOf[SlicedGpuColumnVector].getStart
        for (i <- 0 until numColumns) {
          columns(i) = batch.column(i).asInstanceOf[SlicedGpuColumnVector].getBase
        }
      } else {
        for (i <- 0 until numColumns) {
          columns(i) = batch.column(i).asInstanceOf[GpuColumnVector].getBase
        }
      }

      if (dataSize != null) {
        dataSize.add(JCudfSerialization.getSerializedSizeInBytes(columns, startRow, numRows))
      }
      JCudfSerialization.writeToStream(columns, dOut, startRow, numRows)
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      dOut.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        new Iterator[(Int, ColumnarBatch)] {
          var toBeClosed : ColumnarBatch = null
          var toBeReturned: Option[ColumnarBatch] = None

          private def closeCurrentBatch(): Unit = {
            if (toBeClosed != null) {
              toBeClosed.close
              toBeClosed = null
            }
          }

          TaskContext.get().addTaskCompletionListener[Unit]((tc: TaskContext) => {
            closeCurrentBatch()
            toBeReturned.foreach(_.close())
            toBeReturned = None
          })

          def tryReadNext(): Option[ColumnarBatch] = {
            try {
              val table = JCudfSerialization.readTableFrom(dIn)
              try {
                Some(GpuColumnVector.from(table))
              } finally {
                table.close()
              }
            } catch {
              case e: EOFException => None // Ignored
            }
          }

          override def hasNext: Boolean = {
            if (toBeReturned.isEmpty) {
              toBeReturned = tryReadNext()
            }

            toBeReturned.isDefined
          }

          override def next(): (Int, ColumnarBatch) = {
            if (toBeReturned.isEmpty) {
              toBeReturned = tryReadNext()
              if (toBeReturned.isEmpty) {
                throw new NoSuchElementException("Walked off of the end...")
              }
            }
            closeCurrentBatch()
            toBeClosed = toBeReturned.get
            toBeReturned = None
            (0, toBeClosed)
          }
        }
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T]()(implicit classType: ClassTag[T]): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T]()(implicit classType: ClassTag[T]): T = {
        val table = JCudfSerialization.readTableFrom(dIn)
        val cb = GpuColumnVector.from(table)
        table.close()
        cb.asInstanceOf[T]
      }

      override def readObject[T]()(implicit classType: ClassTag[T]): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
