FROM phusion/baseimage:0.11

WORKDIR /root

RUN useradd --create-home --shell /bin/bash hadoop

RUN apt-get update && apt-get install -y wget runit openjdk-8-jdk-headless net-tools iputils-ping

# install hadoop 3.1.1
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-3.1.1/hadoop-3.1.1.tar.gz && \
    tar -xzvf hadoop-3.1.1.tar.gz && \
    mv hadoop-3.1.1 /usr/local/hadoop && \
    rm hadoop-3.1.1.tar.gz

# set environment variable
ENV JAVA_HOME="/usr"
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin

RUN mkdir -p /home/hadoop/spark && \
    mkdir -p /home/hadoop/hdfs/namenode && \
    mkdir -p /home/hadoop/hdfs/datanode && \
    chown -R hadoop:hadoop /home/hadoop && \
    mkdir $HADOOP_HOME/logs && \
    chown hadoop:hadoop $HADOOP_HOME/logs && \
    rm -f $HADOOP_HOME/etc/hadoop/workers && \
    touch $HADOOP_HOME/etc/hadoop/workers

COPY config/* /tmp/
RUN mv /tmp/hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml && \
    mv /tmp/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml && \
    mv /tmp/yarn-site.xml.tmpl $HADOOP_HOME/etc/hadoop/yarn-site.xml.tmpl

#setup runit
RUN mkdir -p /etc/service/dfs /etc/service/resourcemanager /etc/service/nodemanager
COPY config/dfs.service /etc/service/dfs/run
COPY config/resourcemanager.service /etc/service/resourcemanager/run
COPY config/nodemanager.service /etc/service/nodemanager/run
RUN chmod +x /etc/service/*/run
