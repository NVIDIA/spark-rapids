package com.nvidia.spark.rapids.fileio;

import ai.rapids.cudf.HostMemoryBuffer;
import ai.rapids.cudf.NvtxColor;
import ai.rapids.cudf.NvtxRange;
import com.nvidia.spark.rapids.GpuMetric;
import com.nvidia.spark.rapids.HostMemoryOutputStream;
import com.nvidia.spark.rapids.RapidsConf$;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

public interface ParquetIO extends RapidsFileIO {
    int FOOTER_LENGTH_SIZE = 4;
    byte[] PARQUET_MAGIC_ENCRYPTED = "PARE".getBytes(StandardCharsets.US_ASCII);

    Map<String, GpuMetric> metrics();

    default HostMemoryBuffer readParquetFooterBuffer(Path filePath) throws IOException {
        RapidsInputFile inputFile = open(filePath);
        // Much of this code came from the parquet_mr projects ParquetFileReader, and was modified
        // to match our needs
        long fileLen = inputFile.getLength();
        // MAGIC + data + footer + footerIndex + MAGIC
        if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) {
            throw new RuntimeException(filePath + " is not a Parquet file (too small " +
                    "length: " + fileLen);
        }
        long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;
        try (SeekableInputStream inputStream = inputFile.open()) {
            try (NvtxRange ignore = new NvtxRange("ReadFooterBytes", NvtxColor.YELLOW)) {
                inputStream.seek(footerLengthIndex);
                long footerLength = readIntLittleEndian(inputStream);
                byte[] magic = new byte[MAGIC.length];
                IOUtils.readFully(inputStream, magic);
                long footerIndex = footerLengthIndex - footerLength;
                verifyParquetMagic(filePath, magic);
                if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
                    throw new RuntimeException("corrupted file: the footer index is not within " +
                            "the file: " + footerIndex);
                }
                int hmbLength = Math.toIntExact(fileLen - footerIndex);
                HostMemoryBuffer outBuffer = null;
                try {
                    outBuffer = HostMemoryBuffer.allocate(hmbLength + MAGIC.length,
                            false);
                    HostMemoryOutputStream out = new HostMemoryOutputStream(outBuffer);
                    out.write(MAGIC);
                    inputStream.seek(footerIndex);
                    // read the footer til the end of the file
                    byte[] tmpBuffer = new byte[4096];
                    int bytesLeft = hmbLength;
                    while (bytesLeft > 0) {
                        int readLength = Math.min(bytesLeft, tmpBuffer.length);
                        IOUtils.readFully(inputStream, tmpBuffer, 0, readLength);
                        out.write(tmpBuffer, 0, readLength);
                        bytesLeft -= readLength;
                    }
                    return outBuffer;
                } catch (Throwable t) {
                    if (outBuffer != null) {
                        outBuffer.close();
                    }
                    throw t;
                }
            }
        }
    }

    class CopyRange {
        private final long offset;
        private final long length;
        private final long outputOffset;

        public CopyRange(long offset, long length, long outputOffset) {
            this.offset = offset;
            this.length = length;
            this.outputOffset = outputOffset;
        }

        public long getOffset() {
            return offset;
        }

        public long getLength() {
            return length;
        }

        public long getOutputOffset() {
            return outputOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            CopyRange copyRange = (CopyRange) o;
            return offset == copyRange.offset && length == copyRange.length && outputOffset == copyRange.outputOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, length, outputOffset);
        }
    }


    /**
     * Copy a list of slices from input file to output memory buffer.
     * @param output This function will not close the output stream. The caller should take care
     *               of it.
     * @param inputFile Input file path.
     * @param ranges List of ranges to copy.
     * @return Total bytes copied.
     */
    default long copyToHostMemoryBuffer(HostMemoryOutputStream output, Path inputFile,
                                        List<CopyRange> ranges, int copyBufferSize) throws IOException {
        try(SeekableInputStream inputStream = open(inputFile).open()) {
            byte[] copyBuffer = new byte[copyBufferSize];
            long totalBytesCopied = 0;
            for (CopyRange range : ranges) {
            }

            return totalBytesCopied;
        }
    }

    static void verifyParquetMagic(Path filePath, byte[] magic) {
        if (!Arrays.equals(MAGIC, magic)) {
            if (Arrays.equals(PARQUET_MAGIC_ENCRYPTED, magic)) {
                throw new RuntimeException("The GPU does not support reading encrypted Parquet " +
                        "files. To read encrypted or columnar encrypted files, disable the GPU Parquet " +
                        "reader via " + RapidsConf$.MODULE$.ENABLE_PARQUET().key());
            } else {
                throw new RuntimeException(filePath + " is not a Parquet file. " +
                        "Expected magic number at tail " + Arrays.toString(MAGIC) +
                        "but found " + Arrays.toString(magic));
            }
        }
    }
}
