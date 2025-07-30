package com.nvidia.spark.rapids.fileio;

import java.io.IOException;

public interface RapidsInputFile {
    long getLength() throws IOException;
    SeekableInputStream open() throws IOException;
}
