FROM alpine:3.20

ENV TZ=Asia/Shanghai

RUN apk add --no-cache tzdata ca-certificates curl \
    && cp /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo "${TZ}" > /etc/timezone

WORKDIR /Initial
COPY ./target/project-user .
COPY ./config/config-docker.yaml .
RUN mkdir -p config && mv config-docker.yaml config/config.yaml

EXPOSE 8080 8081 8881 8083

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8083/metrics || exit 1

ENTRYPOINT ["./project-user"]
