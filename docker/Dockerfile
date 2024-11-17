FROM debian:12-slim

ARG config_path=./local/config.yaml
ARG binary_path=./bin/krp

RUN mkdir -p /opt/app
COPY $config_path /opt/app/config.yaml
COPY $binary_path /opt/app/krp

ENTRYPOINT ["/opt/app/krp"]
CMD ["-config", "/opt/app/config.yaml"]
EXPOSE 8080
