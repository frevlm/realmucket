#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "rmk/crypto/crypto.hpp"
#include "rmk/core/env.hpp"
#include "rmk/rpc/rpc.hpp"

namespace rmk::controller {

struct Options {
    std::string id;  // RMK_CONTROLLER_ID
    std::chrono::seconds interval{5};
    std::chrono::seconds lead_ttl{15};
    std::chrono::seconds suspect_after{30};
    std::chrono::seconds repair_delay{900};  // ADR 0009
    std::uint64_t chunk_capacity = std::uint64_t{1} << 30;
    std::uint32_t open_chunks_per_policy = 4;
    std::uint32_t compaction_garbage_percent = 50;  // ADR 0016
    std::uint64_t compaction_bytes_per_second = std::uint64_t{50} << 20;
    std::chrono::seconds compaction_grace{600};  // since sealed_at (§6 safety rule 2)
    std::uint32_t rewrap_batch = 1000;
    std::chrono::seconds writer_lease{30};
};

// docs/IMPLEMENTATION.md §6 "Background" and §7.
Result<Options> load_options(const EnvLookup& env);

struct Dependencies {
    rpc::Stores stores;
    std::shared_ptr<crypto::KeyProvider> keys;
    std::shared_ptr<rpc::Channels> channels;
};

// What one pass did; tests and metrics read it.
struct Report {
    bool leader = false;
    std::uint32_t chunks_created = 0;
    std::uint32_t chunks_sealed = 0;
    std::uint32_t chunks_repaired = 0;
    std::uint32_t chunks_encoded = 0;
    std::uint32_t chunks_compacted = 0;
    std::uint32_t chunks_deleted = 0;
    std::uint32_t uploads_reclaimed = 0;
    std::uint32_t garbage_entries = 0;
    std::uint32_t versions_rewrapped = 0;
    std::uint32_t nodes_removed = 0;
};

class Controller {
public:
    static Result<std::unique_ptr<Controller>> create(Options options, Dependencies deps);
    ~Controller();  // stops run()

    // One pass of every duty, as of `now`. Tests drive this directly.
    Result<Report> run_once(Clock::time_point now);
    // run_once every interval until destroyed.
    void start();

private:
    Controller() = default;
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace rmk::controller
