FROM databricksruntime/standard:latest

RUN apt-get update \
  && apt-get install -y openssh-server \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /databricks

# set environment variables which Databricks shell script (in tarball) uses upon startup to create crontab for Ganglia to capture metrics
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -q -y --force-yes --fix-missing --ignore-missing \
        ganglia-monitor \
        ganglia-webfrontend \
        ganglia-monitor-python \
        python3-pip \
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

ADD ganglia-monitor-not-active /etc/monit/conf.d
ADD gmetad-not-active /etc/monit/conf.d
ADD spark-slave-not-active /etc/monit/conf.d

RUN echo $'\n\
check process spark-slave with pidfile /tmp/spark-root-org.apache.spark.deploy.worker.Worker-1.pid\n\
      start program = "/databricks/spark/scripts/restart-workers"\n\
      stop program = "/databricks/spark/scripts/kill_worker.sh"\n\
' > /etc/monit/conf.d/spark-slave-not-active


# add Ganglia configuration file indicating the DocumentRoot - Databricks checks this to enable Ganglia upon cluster startup
RUN mkdir -p /etc/apache2/sites-enabled
ADD ganglia.conf /etc/apache2/sites-enabled
RUN chmod 775 /etc/apache2/sites-enabled/ganglia.conf

ADD gconf/* /etc/ganglia/
RUN mkdir -p /databricks/spark/scripts/ganglia/
RUN mkdir -p /databricks/spark/scripts/
ADD start_spark_slave.sh /databricks/spark/scripts/start_spark_slave.sh

# add local monit shell script in the right location
RUN mkdir -p /etc/init.d
ADD monit /etc/init.d
RUN chmod 775 /etc/init.d/monit
