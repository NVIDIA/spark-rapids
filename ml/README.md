# spark-cuml

RAPIDS ML - RAPIDS Machine Learning Library on Spark


## API change

We describe main API changes for gpu accelerated algorithms:

### 1. PCA
```scala
val pca = new org.apache.spark.ml.feature.PCA()
  .setInputCol("feature")
  .setOutputCol("feature_value_3d")
  .setK(3)
  .useGemm(true) // or false, switch to use original BLAS bsr or cuBLAS gemm to compute covariance matrix
  .meanCentering(true) // or false, switch to do mean centering before computing covariance matrix
  .fit(vectorDf)
```

## Build
The artifact jar for this submodule will not be included in the plugin jar of the whole spark-rapids project. User can build it directly in the _project root path_:
```
mvn -pl ml -am clean package
```
Then `rapids-4-spark-ml_2.12-21.10.0-SNAPSHOT.jar` will be generated under `ml/target` folder.

_Note_: This module contains both native and Java/Scala code. When compiling native code to get the essential library, `cmake`, `ninja`, and `gcc` are all required. 

## How to use

Add the artifact jar built from this module to the Spark, for example:
```bash
$SPARK_HOME/bin/spark-shell --master $SPARK_MASTER \
 --driver-memory 20G \
 --executor-memory 30G \
 --conf spark.driver.maxResultSize=8G \
 --jars target/rapids-4-spark-ml_2.12-21.10.0-SNAPSHOT.jar \
 --conf spark.task.resource.gpu.amount=0.08 \
 --conf spark.executor.resource.gpu.amount=1 \
 --conf spark.executor.resource.gpu.discoveryScript=./getGpusResources.sh \
 --files ${SPARK_HOME}/examples/src/main/scripts/getGpusResources.sh
```
