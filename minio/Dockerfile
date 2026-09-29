FROM golang:1.24-alpine AS build

ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z

ENV CGO_ENABLED=0 \
    GOBIN=/out

RUN apk add --no-cache ca-certificates git
RUN go install github.com/minio/minio@${MINIO_VERSION}
RUN go install github.com/minio/mc@${MC_VERSION}

FROM alpine:3.22

RUN apk add --no-cache ca-certificates
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /out/mc /usr/local/bin/mc

ENV MC_CONFIG_DIR=/tmp/.mc

VOLUME ["/data"]
EXPOSE 9000 9001

ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
