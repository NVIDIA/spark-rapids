package com.nvidia.spark.rapids.fileio;

import ai.rapids.cudf.HostMemoryBuffer;

import java.io.IOException;
import java.util.List;

public interface RapidsInputFile {
    long getLength() throws IOException;
    SeekableInputStream open() throws IOException;
    /**
     * Reads data from the input file into the provided output buffer using vectored read.
     *
     * @param output the buffer to read data into
     * @param copyRanges a list of copy ranges specifying the input offsets, lengths, and output offsets
     * @throws IOException if an I/O error occurs during reading
     */
    default void readVectored(HostMemoryBuffer output, List<CopyRange> copyRanges) throws IOException {
        // By default we read the copy ranges one by one, but concrete implementations could override this
        // method to provide a more efficient implementation. For example, for s3 we could use
        // https://github.com/awslabs/analytics-accelerator-s3/blob/3f1a827c74898bf5ec2aa22c4610a6d2a511db47/input-stream/src/main/java/software/amazon/s3/analyticsaccelerator/S3SeekableInputStream.java#L229
        throw new UnsupportedOperationException(
                "readVectored is not implemented for " + this.getClass().getName());
    }

    /**
     * Reads the last length bytes of the input file into the provided output buffer.
     *
     * @param length the number of bytes to read from the tail
     * @param output the buffer to read data into
     * @throws IOException if an I/O error occurs during reading
     */
    default void readTail(long length, HostMemoryBuffer output) throws IOException {
        // Since we know the length of the file, and we could also open a SeekableInputStream, we
        // could implement this method by seeking to (file_length - length) of the file and reading
        // length bytes. Concrete implementations could override this method to provide a more
        // efficient implementation, for example, for s3 we could use
        // https://github.com/awslabs/analytics-accelerator-s3/blob/4f1a897ba84681809b4c82edff01ffbbbfe84ca4/input-stream/src/main/java/software/amazon/s3/analyticsaccelerator/SeekableInputStream.java#L65
        throw new UnsupportedOperationException(
                "readTail is not implemented for " + this.getClass().getName());
    }

    class CopyRange {
        private final long inputOffset;
        private final long length;
        private final long outputOffset;

      public CopyRange(long inputOffset, long length, long outputOffset) {
        this.inputOffset = inputOffset;
        this.length = length;
        this.outputOffset = outputOffset;
      }

        public long getInputOffset() {
            return inputOffset;
        }

        public long getLength() {
            return length;
        }

        public long getOutputOffset() {
            return outputOffset;
        }
    }
}
