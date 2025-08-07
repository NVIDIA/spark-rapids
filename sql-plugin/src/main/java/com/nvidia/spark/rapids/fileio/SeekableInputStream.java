/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
