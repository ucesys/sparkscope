FROM docker.io/bitnami/spark:3.2.4

USER root

RUN apt update && apt install -y wget curl iputils-ping

RUN useradd -r -u 1001 appuser

USER appuser

RUN mkdir /tmp/csv-metrics && mkdir /tmp/spark-events

COPY ./lib/ /tmp/jars