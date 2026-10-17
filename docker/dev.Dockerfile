# Build and test environment. Matches the production runtime (Linux) so that
# RocksDB and gRPC are exercised exactly as deployed.
FROM ubuntu:24.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates curl pkg-config \
        libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
        libssl-dev nlohmann-json3-dev libgtest-dev python3 librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
