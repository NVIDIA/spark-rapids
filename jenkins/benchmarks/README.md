# Running benchmarks in CICD

This page provides instructions to manually run the benchmarks in Kubernetes based on the provided
Docker images.

The goal is to integrate this with CICD.

## Clone and build the plugin

```bash
git clone git@github.com:NVIDIA/spark-rapids.git
cd spark-rapids
mvn clean verify
```

## Set environment variables

```bash
export RAPIDS_PLUGIN_REPO=`pwd`
```

Also, set `DOCKER_REPO` to the docker repo URL where you will be pushing images e.g. `myrepo.com/nvidia`.

## Build the base Spark/CUDA base image

Run this command from the root of a Spark distribution. This example is for Spark 3.0.1.

```bash
docker build -t $DOCKER_REPO/spark:k8s-cicd-spark301 \
  -f $RAPIDS_PLUGIN_REPO/jenkins/benchmarks/Dockerfile.spark301 .
docker push $DOCKER_REPO/spark:k8s-cicd-spark301
```

## Build the benchmark Docker image

This docker image will be the container image deployed to k8s for the driver and executors.

```bash
docker build -t $DOCKER_REPO/spark:k8s-cicd-benchmark \
  --build-arg DOCKER_REPO=$DOCKER_REPO \
  -f jenkins/benchmarks/Dockerfile.benchmark .
docker push $DOCKER_REPO/spark:k8s-cicd-benchmark
```

## Run the benchmarks

```bash
export K8S_MASTER=https://your.k8s.master
export SPARK_HOME=/opt/spark

python integration_tests/src/main/benchmark.py \
      --template jenkins/benchmark/spark-submit-template.sh \
      --benchmark tpcds \
      --query q4 q5
      --input /path/to/input \
      --input-format parquet \
      --configs cpu-aqe-off gpu-aqe-off gpu-aqe-on
```



