FROM apache/airflow:3.1.0

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root

# ✅ Install Java for PySpark (ARM-safe)
RUN apt-get update && \
       apt-get install -y openjdk-17-jdk-headless wget && \
       apt-get clean && \
       rm -rf /var/lib/apt/lists/*

# ✅ Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ✅ Download PostgreSQL JDBC driver (latest stable)
RUN mkdir -p /opt/spark/jars && \
       wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -O /opt/spark/jars/postgresql.jar

# ✅ Optional: If your Spark script references SPARK_HOME
ENV SPARK_CLASSPATH=/opt/spark/jars/postgresql.jar

# ✅ Copy project data
RUN mkdir -p /opt/airflow/data
COPY data/ /opt/airflow/data/
RUN chown -R airflow:root /opt/airflow/data

USER airflow
