FROM debian:12-slim

RUN apt update && apt install wget -y && apt install default-jre -y

RUN wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz && \
  tar xvfz kafka_2.13-3.8.0.tgz && \
  mv kafka_2.13-3.8.0 /opt/kafka && \
  rm kafka_2.13-3.8.0.tgz

RUN apt install jq -y
RUN wget https://github.com/tsenart/vegeta/releases/download/v12.12.0/vegeta_12.12.0_linux_amd64.tar.gz && \
  tar xvfz vegeta_12.12.0_linux_amd64.tar.gz && \
  mv vegeta /usr/local/bin/ && \
  rm vegeta_12.12.0_linux_amd64.tar.gz

RUN mkdir -p /opt/app
COPY ./config.yaml /opt/app/config.yaml
COPY ./main /opt/app/krp
