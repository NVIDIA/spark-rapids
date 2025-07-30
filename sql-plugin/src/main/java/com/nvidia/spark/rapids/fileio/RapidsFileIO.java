package com.nvidia.spark.rapids.fileio;

import com.nvidia.spark.rapids.GpuMetric;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * An io layer to access different underlying storages such as hdfs, aws s3.
 *<br/>
 *<br/>
 * This layer is heavily inspired by io abstractions layers like
 * <a href="https://github.com/apache/iceberg/blob/50d310aef17908f03f595d520cd751527483752a/api/src/main/java/org/apache/iceberg/io/FileIO.java#L36">iceberg's FileIO</a>,
 * <a href="https://github.com/apache/parquet-java/tree/master/parquet-common/src/main/java/org/apache/parquet/io">parquet io</a>.
 */
public interface RapidsFileIO extends Serializable, Closeable {
    RapidsInputFile open(String path) throws IOException;


    default RapidsInputFile open(Path path) throws IOException {
        return open(path.toString());
    }
}
