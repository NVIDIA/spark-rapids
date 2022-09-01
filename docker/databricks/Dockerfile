# Copyright (c) 2022, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

############# 
# Combine all below Dockerfiles together:
# https://github.com/databricks/containers/blob/master/ubuntu/gpu/cuda-11/base/Dockerfile
# https://github.com/databricks/containers/blob/master/ubuntu/minimal/Dockerfile
# https://github.com/databricks/containers/blob/master/ubuntu/python/Dockerfile
# https://github.com/databricks/containers/blob/master/ubuntu/dbfsfuse/Dockerfile
# https://github.com/databricks/containers/blob/master/ubuntu/standard/Dockerfile
# https://github.com/databricks/containers/blob/master/experimental/ubuntu/ganglia/Dockerfile
#############
ARG CUDA_VERSION=11.3.1
FROM nvidia/cuda:${CUDA_VERSION}-cudnn8-runtime-ubuntu20.04 as databricks

ARG CUDA_MAJOR=11-3

#############
# Install all needed libs
#############
COPY requirements_10.4ML.txt /tmp/requirements.txt

RUN set -ex && \ 
    apt-get -y update && \
    apt-get -y upgrade && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get -y install python3.8 virtualenv python3-filelock libcairo2 cuda-cupti-${CUDA_MAJOR} \
               cuda-toolkit-${CUDA_MAJOR}-config-common cuda-toolkit-11-config-common cuda-toolkit-config-common \
               openjdk-8-jdk-headless iproute2 bash sudo coreutils procps wget gpg fuse openssh-server && \
    apt-get -y install cuda-cudart-dev-${CUDA_MAJOR} cuda-cupti-dev-${CUDA_MAJOR} cuda-driver-dev-${CUDA_MAJOR} \
               cuda-nvcc-${CUDA_MAJOR} cuda-thrust-${CUDA_MAJOR} cuda-toolkit-${CUDA_MAJOR}-config-common cuda-toolkit-11-config-common \
               cuda-toolkit-config-common python3.8-dev libpq-dev libcairo2-dev build-essential unattended-upgrades cmake ccache \
               openmpi-bin linux-headers-5.4.0-117 linux-headers-5.4.0-117-generic linux-headers-generic libopenmpi-dev unixodbc-dev \
               sysstat && \
    /var/lib/dpkg/info/ca-certificates-java.postinst configure && \
    # Initialize the default environment that Spark and notebooks will use
    virtualenv -p python3.8 --system-site-packages /databricks/python3 \
        && /databricks/python3/bin/pip install --no-cache-dir --upgrade pip \
        && /databricks/python3/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
        # Install Python libraries for Databricks environment
        && /databricks/python3/bin/pip cache purge && \
    mkdir -p /databricks/jars && \
    apt-get -y purge --autoremove software-properties-common cuda-cudart-dev-${CUDA_MAJOR} cuda-cupti-dev-${CUDA_MAJOR}
               cuda-driver-dev-${CUDA_MAJOR} cuda-nvcc-${CUDA_MAJOR} cuda-thrust-${CUDA_MAJOR} \
               python3.8-dev libpq-dev libcairo2-dev build-essential unattended-upgrades cmake ccache openmpi-bin \
               linux-headers-5.4.0-117 linux-headers-5.4.0-117-generic linux-headers-generic libopenmpi-dev unixodbc-dev \
               virtualenv python3-virtualenv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    mkdir -p /mnt/driver-daemon && \
    #############
    # Disable NVIDIA repos to prevent accidental upgrades.
    #############
    ln -s /databricks/jars /mnt/driver-daemon/jars && \
    cd /etc/apt/sources.list.d && \
    mv cuda.list cuda.list.disabled && \
    # Create user "ubuntu"
    useradd --create-home --shell /bin/bash --groups sudo ubuntu


#############
# Set all env variables
#############
ENV PYSPARK_PYTHON=/databricks/python3/bin/python3
ENV DATABRICKS_RUNTIME_VERSION=10.4
ENV LANG=C.UTF-8
ENV USER=ubuntu
ENV PATH=/usr/local/nvidia/bin:/databricks/python3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin

#############
# Spark RAPIDS configuration
#############
ARG DRIVER_CONF_FILE=00-custom-spark-driver-defaults.conf
ARG JAR_URL=https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.08.0/rapids-4-spark_2.12-22.08.0.jar
COPY ${DRIVER_CONF_FILE} /databricks/driver/conf/00-custom-spark-driver-defaults.conf

WORKDIR /databricks/jars
RUN wget $JAR_URL

WORKDIR /databricks

#############
# Setup Ganglia
#############
FROM databricks as databricks-ganglia

WORKDIR /databricks
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -q -y --force-yes --fix-missing --ignore-missing \
        ganglia-monitor \
        ganglia-webfrontend \
        ganglia-monitor-python \
        python3-pip \
        wget \
        rsync \
        cron \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
# Upgrade Ganglia to 3.7.2 to patch XSS bug, see CJ-15250
# Upgrade Ganglia to 3.7.4 and use private forked repo to patch several security bugs, see CJ-20114
# SC-17279: We run gmetad as user ganglia, so change the owner from nobody to ganglia for the rrd directory
RUN cd /tmp \
  && export GANGLIA_WEB=ganglia-web-3.7.4-db-4 \
  && wget https://s3-us-west-2.amazonaws.com/databricks-build-files/$GANGLIA_WEB.tar.gz \
  && tar xvzf $GANGLIA_WEB.tar.gz \
  && cd $GANGLIA_WEB \
  && make install \
  && chown ganglia:ganglia /var/lib/ganglia/rrds
# Install Phantom.JS
RUN cd /tmp \
  && export PHANTOM_JS="phantomjs-2.1.1-linux-x86_64" \
  && wget https://s3-us-west-2.amazonaws.com/databricks-build-files/$PHANTOM_JS.tar.bz2 \
  && tar xvjf $PHANTOM_JS.tar.bz2 \
  && mv $PHANTOM_JS /usr/local/share \
  && ln -sf /usr/local/share/$PHANTOM_JS/bin/phantomjs /usr/local/bin
# Apache2 config. The `sites-enabled` config files are loaded into the container
# later.
RUN rm /etc/apache2/sites-enabled/* && a2enmod proxy && a2enmod proxy_http
RUN mkdir -p /etc/monit/conf.d
ADD ganglia/ganglia-monitor-not-active /etc/monit/conf.d
ADD ganglia/gmetad-not-active /etc/monit/conf.d
ADD ganglia/spark-slave-not-active /etc/monit/conf.d
RUN echo $'\n\
check process spark-slave with pidfile /tmp/spark-root-org.apache.spark.deploy.worker.Worker-1.pid\n\
      start program = "/databricks/spark/scripts/restart-workers"\n\
      stop program = "/databricks/spark/scripts/kill_worker.sh"\n\
' > /etc/monit/conf.d/spark-slave-not-active
# add Ganglia configuration file indicating the DocumentRoot - Databricks checks this to enable Ganglia upon cluster startup
RUN mkdir -p /etc/apache2/sites-enabled
ADD ganglia/ganglia.conf /etc/apache2/sites-enabled
RUN chmod 775 /etc/apache2/sites-enabled/ganglia.conf
ADD ganglia/gconf/* /etc/ganglia/
RUN mkdir -p /databricks/spark/scripts/ganglia/
RUN mkdir -p /databricks/spark/scripts/
ADD ganglia/start_spark_slave.sh /databricks/spark/scripts/start_spark_slave.sh
# add local monit shell script in the right location
RUN mkdir -p /etc/init.d
ADD ganglia/monit /etc/init.d
RUN chmod 775 /etc/init.d/monit

FROM databricks-ganglia as databricks-alluxio

#############
# Setup Alluxio
#############
ARG ALLUXIO_VERSION=2.8.0
ARG ALLUXIO_HOME="/opt/alluxio-${ALLUXIO_VERSION}"
ARG ALLUXIO_TAR_FILE="alluxio-${ALLUXIO_VERSION}-bin.tar.gz"
ARG ALLUXIO_DOWNLOAD_URL="https://downloads.alluxio.io/downloads/files/${ALLUXIO_VERSION}/${ALLUXIO_TAR_FILE}"

RUN wget -O /tmp/$ALLUXIO_TAR_FILE ${ALLUXIO_DOWNLOAD_URL} \
    && tar zxf /tmp/${ALLUXIO_TAR_FILE} -C /opt/ \
    && rm -f /tmp/${ALLUXIO_TAR_FILE} \
    && cp ${ALLUXIO_HOME}/client/alluxio-${ALLUXIO_VERSION}-client.jar /databricks/jars/

#############
# Allow ubuntu user to sudo without password
#############
RUN echo "ubuntu ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/ubuntu \
    && chmod 555 /etc/sudoers.d/ubuntu
