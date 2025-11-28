FROM apache/airflow:2.9.0

USER root

# 1. Instalacja OpenJDK (wymagane przez Sparka) i narzędzi
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 2. Ustawienie JAVA_HOME (Uniwersalne dla Debian/Ubuntu w obrazie Airflow)
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# 3. Instalacja bibliotek Python (Provider Sparka i PySpark)
# Robimy to podczas budowania obrazu, żeby nie tracić czasu przy każdym starcie kontenera
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark