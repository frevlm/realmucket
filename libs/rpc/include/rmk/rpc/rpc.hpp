#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <grpcpp/grpcpp.h>

#include "rmk/core/env.hpp"
#include "rmk/core/error.hpp"
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

// The two stores the Gateway keeps its state in.
struct Stores {
    std::shared_ptr<MetaStore> meta;
    std::shared_ptr<ConfigStore> config;
};

}  // namespace rmk::rpc
