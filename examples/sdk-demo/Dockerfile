# Dockerfile for the sdk-demo Weibo job. Mirrors Dockerfile.runner: a static
# CGO-free binary on distroless/nonroot. `go mod download` fetches the
# released Weibo SDK (github.com/ASHUTOSH-SWAIN-GIT/weibo) declared in go.mod.
#
#   docker build -t weibo-sdk-demo:1.0 .

FROM golang:1.26 AS build
WORKDIR /src

# GOSUMDB=off avoids the checksum-DB round-trip for the freshly published
# module; go.sum (committed) still verifies every download locally.
ENV GOSUMDB=off

# Cache modules first (proxy,direct handles the fresh SDK tag).
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/job .
# Pre-create mount points so fresh volumes inherit nonroot ownership.
RUN mkdir -p /out/data /out/savepoints

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/job /usr/local/bin/job
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/savepoints /savepoints
ENV DATA_DIR=/data \
    SAVEPOINT_DIR=/savepoints \
    PORT=8080
VOLUME ["/data", "/savepoints"]
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/job"]
