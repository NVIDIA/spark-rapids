This doc describes how to dump an Exec runtime meta and column batch data to a directory,
and replay the Exec with the dumped meta and data. When encountering a perf issue using this tool
can dump the runtime meta/data and then replay. It will spend less time when collecting NSYS and
NCU information when replaying the dumped runtime meta/data.

# Dump a Exec meta and a column batch data

## compile Spark-Rapids jar
Compile with option "-DallowConventionalDistJar"
e.g.: compile for Spark330
```
mvn clean install -DskipTests -pl dist -am -DallowConventionalDistJar=true -Dbuildver=330 
```
Note: Should specify `-DallowConventionalDistJar`, this config will make sure to generate a
conventional jar. If you do not specify this config, then replay will fail because of can not
find the class `org.apache.spark.sql.rapids.debug.ProjectExecReplayer`

## enable dump
This assumes that the RAPIDs Accelerator has already been enabled.   
This feature is disabled by default.   
Set the following configurations to enable this feature:

``` 
spark.conf.set("spark.rapids.sql.debug.replay.exec.types", "project")
```
Default `types` value is empty which means do not dump.
Define the Exec types for dumping, separated by comma, e.g.: `project`.
Note currently only support `project`, so it's no need to specify comma.

```
spark.conf.set("spark.rapids.sql.debug.replay.exec.dumpDir", "file:/tmp")
```
Specify the dump directory which can be a local path or a remote path. E.g.: 
`file:/tmp/my-debug-path`. Default value is `file:/tmp`.
This path should be a directory or someplace that we can create a directory to
store files in. Remote path is supported e.g.: `hdfs://url:9000/path/to/save`. When using
remote path, should specify corresponding configs to get access for the remote storage.

```
spark.conf.set("spark.rapids.sql.debug.replay.exec.threshold.timeMS", 1000)
```
Default value is 1000MS.   
Only dump the column batches when it's executing time exceeds threshold time.

```  
spark.conf.set("spark.rapids.sql.debug.replay.exec.batch.limit", 1)
```
This config defines the max dumping number of column batches.   
It's a limit for per Exec instance.   
Default value is 1.

## run a Spark job
This dumping only happens when GPU Exec is running, so should enable Spark-Rapids.
After the job is done, check the dump path will have files like:
```
/tmp/replay-exec:
  - execUUID_xxx_meta_GpuTieredProject_randomID_xxx.meta // this is serialized GPU Tiered Project case class  
  - execUUID_xxx_meta_cb_types_randomID_xxx.meta         // this is types for column batch
  - execUUID_xxx_cb_data_index_0_randomID_xxx_744305176.parquet // this is data for column batch
```
The `xxx` after `execUUID` is the UUID for a specific Project Exec, this is used to distinguish multiple
Project Execs.

# Replay saved Exec runtime meta and data

## Set environment variable
```
export PLUGIN_JAR=path_to_spark_rapids_jar
export SPARK_HOME=path_to_spark_home_330
```

## replay command

### Collect NSYS with replaying
```
nsys profile $SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.sql.rapids.debug.ProjectExecReplayer \
  --conf spark.rapids.sql.explain=ALL \
  --master local[*] \
  --jars ${PLUGIN_JAR} \
  ${PLUGIN_JAR} <path_to_saved_replay_dir> <UUID_of_project_exec>
```

<path_to_saved_replay_dir> is the replay directory.   
<UUID_of_project_exec> is the UUID for a specific Project Exec. Dumping may generate
multiple data for each project, here should specify replay which project.   
If there are multiple column batches for a project, it will only replay the first column batch.
