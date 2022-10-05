# RAPIDS Accelerator for Apache Spark Docker container for Databricks

## Usage

The easiest way to use the RAPIDS Accelerator for Spark on Databricks is use the pre-built Docker
container and Databricks Container Services.

Currently the Docker container supports the following Databricks runtime(s) via Databricks Container Services:
- [Databricks 10.4 LTS](https://docs.databricks.com/release-notes/runtime/10.4.html#system-environment)

See [Customize containers with Databricks Container Services](https://docs.databricks.com/clusters/custom-containers.html) for more information.

Create a Databricks cluster by going to Clusters, then clicking `+ Create Cluster`.  Ensure the
cluster meets the prerequisites above by configuring it as follows:

1. In the `Databricks runtime version` field, click `Standard` and select `Runtime: 10.4 LTS (Scala 2.12, Spark 3.2.1)` (do NOT use `Runtime: 10.4 LTS ML (GPU, Scala 2.12, Spark 3.2.1)` from the `ML` tab).

2. Ensure `Use Photon Acceleration` is disabled.

3. Ensure `Enable autoscaling` is disabled.

4. Choose the number of workers that matches the number of GPUs you want to use.

5. Select a worker type.  On AWS, use nodes with 1 GPU each such as `p3.2xlarge` or `g4dn.xlarge`.
   p2 nodes do not meet the architecture requirements (Pascal or higher) for the Spark worker
   (although they can be used for the driver node).  For Azure, choose GPU nodes such as
   Standard_NC6s_v3.  For GCP, choose N1 or A2 instance types with GPUs. 

6. Select a driver type. Generally, this can be set the same as the worker, but you can select a node that 
   does NOT include a GPU if you don't plan to do any GPU-related operations on the driver. On AWS, this 
   can be an `i3.xlarge` or larger.

7. Under the `Advanced options`, select the `Docker` tab.

8. Select `Use your own Docker container`.

9. In the `Docker Image URL` field, enter `navkumar82/rapids-4-spark-databricks:22.10.0`.

10. Leave `Authentication` set to `Default`.

11. Now select the `Init Scripts` tab.

12. In the `Destination` field, select `FILE`.

13. In the `Init script path` field, enter `file:/opt/spark-rapids/init.sh`

14. Click `Add`.

15. Add any other configs, such as SSH Key, Logging, or additional Spark configuration. The Docker container uses the configuration in `00-custom-spark-driver-defaults.conf` by default. When adding additional lines to `Spark config` in the UI, the configuration will override those defaults that are configured in the Docker container.

16. Start the cluster.

### Enabling Alluxio in the Databricks cluster using the Docker container

If you would like to enable the Alluxio cluster on your Databricks cluster, you will need to add the following configuration to your cluster.

1. Edit the desired cluster.

2. Under the `Advanced options`, select the `Spark` tab.

3. In the `Spark config` field, add the following lines. The second 2 are good starting points when using Alluxio but could be tuned
if needed.

```
spark.databricks.io.cached.enabled false
spark.rapids.alluxio.automount.enabled true
spark.rapids.sql.coalescing.reader.numFilterParallel 2
spark.rapids.sql.multiThreadedRead.numThreads 40
```

4. In the `Environment variables` field, add the line `ENABLE_ALLUXIO=1`.

5. Customize Alluxio configuration using the following configs if needed. These should be added in the `Environment variables` field if you wish to change them.

  - The default amount of disk space used for Alluxio on the Workers is 70%.  This can be adjusted using the configuration below. `ALLUXIO_STORAGE_PERCENT=70`

  - The default heap size used by the Alluxio Master process is 16GB, this may need to be changed depending on the size of the driver node. Make sure it has enough memory for the Master and the Spark driver processes.  `ALLUXIO_MASTER_HEAP=16g`

  - To copy the Alluxio Master and Worker logs off of local disk to be able to look at them after the cluster is shutdown you can configure this to some path accessible via rsync.  For instance, on Databricks this might be a path in /dbfs/.  `ALLUXIO_COPY_LOG_PATH=/dbfs/somedirectory/`

5. Click `Confirm` (if the cluster is currently stopped) or `Confirm and Restart` if the cluster is currently running.

6. Ensure the cluster is started by click `Start` if necessary.

To verify the alluxio cluster is working, you can use the Web Terminal:

1. Ensure the cluster is fully up and running. Then in the cluster UI, click the `Apps` tab.

2. Click `Launch Web Terminal`.

3. In the new tab that opens, you will get a terminal session.

4. Run the following command:

```bash
$ /opt/alluxio-2.8.0/bin/alluxio fsadmin report
```

5. You should see a line indicating the number of active workers, ensure this is equal to the configured number of workers you used for the cluster:

```
...
     Live Workers: 2
...
```
