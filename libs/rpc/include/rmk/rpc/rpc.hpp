#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "rmk/core/cluster_store.hpp"
#include "rmk/core/env.hpp"
#include "rmk/core/error.hpp"
#include "rmk/core/extent_io.hpp"
#include "rmk/core/config_store.hpp"
#include "rmk/core/meta_store.hpp"
#include "rmk/core/types.hpp"

namespace rmk::rpc {

// ----- Errors on the wire (docs/IMPLEMENTATION.md §8).
grpc::Status to_status(const Error& error);
Error from_status(const grpc::Status& status);

// ----- Transport security. ADR 0011: TLS everywhere; plaintext only when explicitly allowed.
struct TlsConfig {
    bool insecure = false;   // RMK_INSECURE=true
    std::string cert_file;   // RMK_TLS_CERT: this process's certificate (servers)
    std::string key_file;    // RMK_TLS_KEY
    std::string ca_file;     // RMK_TLS_CA: trust root for outgoing internal calls
};
Result<TlsConfig> load_tls(const EnvLookup& env);  // invalid_argument if neither TLS nor insecure
std::shared_ptr<grpc::ServerCredentials> server_credentials(const TlsConfig& tls);

// Shared channels to Data Nodes, one per address.
class Channels {
public:
    explicit Channels(TlsConfig tls);
    std::shared_ptr<grpc::Channel> get(const std::string& address);

private:
    TlsConfig tls_;
    std::mutex mu_;
    std::vector<std::pair<std::string, std::shared_ptr<grpc::Channel>>> channels_;
};

// ----- Stores (docs/IMPLEMENTATION.md §7): FoundationDB when RMK_FDB_CLUSTER_FILE is set,
// otherwise fresh in-memory stores.
struct Stores {
    std::shared_ptr<MetaStore> meta;
    std::shared_ptr<ConfigStore> config;
    std::shared_ptr<ClusterStore> cluster;
};
Result<Stores> open_stores(const EnvLookup& env, std::chrono::seconds pending_upload_retention);

// ----- Data path (docs/IMPLEMENTATION.md §6).

struct WriterOptions {
    std::string writer_id;                 // unique per process
    std::chrono::seconds lease{30};
    std::size_t writers_per_policy = 4;
    int attempts = 3;
};

// Places bytes durably on every replica of an open Chunk it holds the writer lease for.
// Thread-safe: concurrent writes go to different Chunks of the pool or wait their turn.
class ChunkWriter final : public ExtentWriter {
public:
    ChunkWriter(std::shared_ptr<ClusterStore> cluster, std::shared_ptr<Channels> channels,
                WriterOptions options);
    ~ChunkWriter() override;  // stops renewing and releases its leases (recording each Chunk's size)

    Result<Extent> write(const StoragePolicy& policy, std::span<const std::byte> bytes) override;

    ChunkWriter(const ChunkWriter&) = delete;
    ChunkWriter& operator=(const ChunkWriter&) = delete;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// Reads Extents from replicas or erasure-coded shards, verifying Extent.crc32c.
class ChunkReader final : public ExtentReader {
public:
    ChunkReader(std::shared_ptr<ClusterStore> cluster, std::shared_ptr<Channels> channels);
    ~ChunkReader() override;

    Result<std::vector<std::byte>> read(const Extent& extent) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace rmk::rpc
