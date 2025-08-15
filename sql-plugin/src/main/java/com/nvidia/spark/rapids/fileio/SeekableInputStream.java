package com.nvidia.spark.rapids.fileio;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@code SeekableInputStream} is an interface with the methods needed to read data from a file or
 * Hadoop data stream.
 *
 */
public abstract class SeekableInputStream extends InputStream {
    /**
     * Return the current position in the InputStream.
     *
     * @return current position in bytes from the start of the stream
     * @throws IOException If the underlying stream throws IOException
     */
    public abstract long getPos() throws IOException;

    /**
     * Seek to a new position in the InputStream.
     *
     * @param newPos the new position to seek to
     * @throws IOException If the underlying stream throws IOException
     */
    public abstract void seek(long newPos) throws IOException;
}
