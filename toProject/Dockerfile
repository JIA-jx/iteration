FROM alpine:3.20

ENV TZ=Asia/Shanghai

RUN apk add --no-cache tzdata ca-certificates curl \
    && cp /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo "${TZ}" > /etc/timezone

WORKDIR /Initial

RUN mkdir -p /msproject/project-project/logs/debug \
    && mkdir -p /msproject/project-project/logs/info \
    && mkdir -p /msproject/project-project/logs/error

COPY ./target/project-project .
COPY ./config/bootstrap.yaml .
COPY ./config/config-docker.yaml .
RUN mkdir -p config && mv bootstrap.yaml config/bootstrap.yaml && mv config-docker.yaml config/config.yaml

EXPOSE 8081 8882 9091

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:9091/metrics || exit 1

ENTRYPOINT ["./project-project"]
