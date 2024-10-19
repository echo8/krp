FROM debian:12-slim

ARG config_path=./local/config.yaml

RUN mkdir -p /opt/app
COPY $config_path /opt/app/config.yaml
COPY ./src/main /opt/app/krp
