# Build and test environment. Matches the production runtime (Linux) so that
# FoundationDB, gRPC and the k8s images are all exercised exactly as deployed.
FROM ubuntu:24.04

ARG FDB_VERSION=7.3.69
ARG TARGETARCH

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates curl pkg-config \
        libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
        libssl-dev libcurl4-openssl-dev nlohmann-json3-dev libgtest-dev python3 \
    && rm -rf /var/lib/apt/lists/*

# FoundationDB client library and headers, pinned and checksum-verified.
RUN set -eux; \
    case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
        arm64) FDB_ARCH=aarch64 ;; \
        amd64) FDB_ARCH=amd64 ;; \
        *) echo "unsupported arch"; exit 1 ;; \
    esac; \
    base="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}"; \
    deb="foundationdb-clients_${FDB_VERSION}-1_${FDB_ARCH}.deb"; \
    curl -fsSLO "${base}/${deb}"; \
    expected="$(curl -fsSL "${base}/${deb}.sha256" | cut -c1-64)"; \
    echo "${expected}  ${deb}" | sha256sum -c -; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "./${deb}"; \
    rm "${deb}"; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
