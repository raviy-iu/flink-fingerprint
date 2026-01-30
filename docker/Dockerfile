FROM flink:1.18-scala_2.12-java11

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and dependencies
RUN pip3 install --no-cache-dir \
    apache-flink==1.18.0 \
    kafka-python==2.0.2 \
    pandas==2.0.3 \
    numpy==1.24.4

# Download Kafka connector JARs for Flink
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar

# Set Python environment
ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3
ENV PYTHONPATH=/opt/flink/jobs
ENV PYTHONUNBUFFERED=1

# Create directories for job files and output
RUN mkdir -p /opt/flink/jobs /opt/flink/output

# Copy job submission script
COPY submit_job.py /opt/flink/submit_job.py

WORKDIR /opt/flink
