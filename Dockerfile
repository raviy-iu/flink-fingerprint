FROM flink:1.18-scala_2.12-java11

# Install Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies (with extended timeout for large packages)
RUN pip3 install --no-cache-dir --timeout=300 --retries=3 \
    apache-flink==1.18.0 \
    kafka-python \
    pandas \
    numpy

# Kafka connector
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

# PyFlink config
ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3

WORKDIR /opt/flink
