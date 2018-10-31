FROM airdock/oracle-jdk:jdk-8u112

WORKDIR /root

RUN useradd --create-home --shell /bin/bash hadoop

# install openssh-server and wget
RUN apt-get update && apt-get install -y openssh-server wget

# install hadoop 2.7.7
RUN wget http://apache.rediris.es/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz && \
    tar -xzvf hadoop-2.7.7.tar.gz && \
    mv hadoop-2.7.7 /usr/local/hadoop && \
    rm hadoop-2.7.7.tar.gz

# set environment variable
ENV HADOOP_HOME=/usr/local/hadoop 
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin 

# generate shh keys
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

RUN mkdir -p ~/hdfs/namenode && \ 
    mkdir -p ~/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs

COPY config/* /tmp/
RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hadoop-env.sh /root/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml && \ 
    mv /tmp/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml && \
    mv /tmp/slaves $HADOOP_HOME/etc/hadoop/slaves && \
    mv /tmp/start-hadoop.sh ~/start-hadoop.sh 

RUN chmod +x /root/start-hadoop.sh && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh && \
    echo "AcceptEnv JAVA_HOME" >> /etc/ssh/sshd_config


# format namenode
RUN /usr/local/hadoop/bin/hdfs namenode -format

CMD [ "sh", "/root/start-hadoop.sh"]
