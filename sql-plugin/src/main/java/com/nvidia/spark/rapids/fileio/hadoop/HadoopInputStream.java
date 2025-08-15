package com.nvidia.spark.rapids.fileio.hadoop;

import com.nvidia.spark.rapids.fileio.SeekableInputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class HadoopInputStream extends SeekableInputStream {
    private final FSDataInputStream in;
    private boolean closed;

    public HadoopInputStream(FSDataInputStream in) {
        requireNonNull(in, "in can't be null");
        this.in = in;
        this.closed = false;
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
        in.seek(newPos);
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        if (!closed)  {
            super.close();
            this.closed = true;
        }
    }
}
