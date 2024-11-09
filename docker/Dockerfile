FROM debian:12-slim

ARG config_path=./local/config.yaml

RUN mkdir -p /opt/app
COPY $config_path /opt/app/config.yaml
COPY ./bin/krp /opt/app/krp

ENTRYPOINT ["/opt/app/krp"]
CMD ["-config", "/opt/app/config.yaml"]
EXPOSE 8080
