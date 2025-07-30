package com.nvidia.spark.rapids.fileio.hadoop;

import com.nvidia.spark.rapids.fileio.RapidsFileIO;
import com.nvidia.spark.rapids.fileio.RapidsInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation {@link RapidsFileIO} using the hadoop file system.
 * <br/>
 */
public class HadoopFileIO implements RapidsFileIO {
    private final SerializableConfiguration hadoopConf;

    public HadoopFileIO(Configuration hadoopConf) {
        Objects.requireNonNull(hadoopConf, "hadoopConf can't be null");
        this.hadoopConf = new SerializableConfiguration(hadoopConf);
    }

    @Override
    public RapidsInputFile open(String path) throws IOException {
        return this.open(new Path(path));
    }

    @Override
    public RapidsInputFile open(Path path) throws IOException {
        return HadoopInputFile.create(path, hadoopConf.value());
    }

    @Override
    public void close() throws IOException {
    }
}
