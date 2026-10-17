# One runtime image for realmucketd and rmkctl.
# Build from the repo root:  docker build -f docker/runtime.Dockerfile -t realmucket:dev .
# Requires realmucket-dev:latest (docker build -f docker/dev.Dockerfile -t realmucket-dev .).
FROM realmucket-dev:latest AS build
WORKDIR /src
COPY . .
RUN cmake -S . -B /tmp/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DRMK_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX=/opt/rmk \
    && cmake --build /tmp/build \
    && cmake --install /tmp/build

FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates libgrpc++1.51t64 libprotobuf32t64 libssl3t64 librocksdb8.9 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin rmk \
    && mkdir -p /var/lib/rmk/data && chown -R rmk:rmk /var/lib/rmk
COPY --from=build /opt/rmk/bin /opt/rmk/bin
ENV PATH=/opt/rmk/bin:$PATH
USER 10001:10001
WORKDIR /var/lib/rmk
EXPOSE 7000 9100
# The command picks the binary: realmucketd | rmkctl
CMD ["realmucketd"]
