FROM apache/spark:4.0.2-scala2.13-java17-python3-ubuntu

WORKDIR /app

ARG HADOOP_AWS_VERSION=3.4.0
# Modern AWS SDK v2 series
ARG AWS_SDK_V2_VERSION=2.23.19
# Target Delta version matching Spark 4.0 (Scala 2.13)
ARG DELTA_VERSION=4.0.0

USER root

RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y openssh-server python3-pip && \
    echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    apt-get clean

COPY requirements.txt /app/
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P "${SPARK_HOME}/jars/" && \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar -P "${SPARK_HOME}/jars/" && \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWS_SDK_V2_VERSION}/url-connection-client-${AWS_SDK_V2_VERSION}.jar -P "${SPARK_HOME}/jars/" && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar -P "${SPARK_HOME}/jars/" && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar -P "${SPARK_HOME}/jars/" && \
    pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME=/opt/java/openjdk
ENV PYTHONPATH=/opt/spark

CMD service ssh start && tail -f /dev/null