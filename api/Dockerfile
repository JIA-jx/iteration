FROM alpine:3.20

ENV TZ=Asia/Shanghai

RUN apk add --no-cache tzdata ca-certificates curl \
    && cp /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo "${TZ}" > /etc/timezone

WORKDIR /Initial

RUN mkdir -p /iteration/api/logs/debug \
    && mkdir -p /iteration/api/logs/info \
    && mkdir -p /iteration/api/logs/error

COPY ./target/project-api .
COPY ./config/config-docker.yaml .
RUN mkdir -p config && mv config-docker.yaml config/config.yaml

EXPOSE 80 9090 8082

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:9090/metrics || exit 1

ENTRYPOINT ["./project-api"]
