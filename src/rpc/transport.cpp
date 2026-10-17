#include <fstream>
#include <sstream>
#include <string>
#include <utility>

#include "rmk/rpc.hpp"

namespace rmk::rpc {
namespace {

// Room for a 1 MiB data message plus framing, and for any unary reply.
constexpr int kMaxMessageBytes = 32 << 20;

Result<std::string> read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in)
        return fail(Errc::invalid_argument, "cannot read " + path);
    std::ostringstream out;
    out << in.rdbuf();
    if (in.bad())
        return fail(Errc::invalid_argument, "cannot read " + path);
    return out.str();
}

}  // namespace

Result<TlsConfig> load_tls(const EnvLookup& env) {
    TlsConfig tls;
    if (auto s = env_bool(env, "RMK_INSECURE", tls.insecure); !s)
        return std::unexpected(s.error());
    if (auto s = env_string(env, "RMK_TLS_CERT", false, tls.cert_file); !s)
        return std::unexpected(s.error());
    if (auto s = env_string(env, "RMK_TLS_KEY", false, tls.key_file); !s)
        return std::unexpected(s.error());
    if (auto s = env_string(env, "RMK_TLS_CA", false, tls.ca_file); !s)
        return std::unexpected(s.error());
    if (tls.insecure)
        return tls;

    if (tls.cert_file.empty() != tls.key_file.empty())
        return fail(Errc::invalid_argument, "RMK_TLS_CERT and RMK_TLS_KEY must be set together");
    if (tls.cert_file.empty() && tls.ca_file.empty())
        return fail(Errc::invalid_argument, "TLS is not configured: set RMK_TLS_CERT/RMK_TLS_KEY/RMK_TLS_CA "
                                            "or RMK_INSECURE=true");
    for (const std::string* path : {&tls.cert_file, &tls.key_file, &tls.ca_file}) {
        if (path->empty())
            continue;
        if (auto content = read_file(*path); !content)
            return std::unexpected(content.error());
    }
    return tls;
}

std::shared_ptr<grpc::ServerCredentials> server_credentials(const TlsConfig& tls) {
    if (tls.insecure)
        return grpc::InsecureServerCredentials();
    auto cert = read_file(tls.cert_file);
    auto key = read_file(tls.key_file);
    if (!cert || !key)
        return nullptr;
    // Internal callers present certificates signed by RMK_TLS_CA (§6 safety rule 4). They are
    // requested and verified here; services that must only serve components (the Data Node)
    // additionally refuse peers without one. External clients of the Gateway present none.
    grpc::SslServerCredentialsOptions options(
        tls.ca_file.empty() ? GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE : GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY);
    options.pem_key_cert_pairs.push_back({*key, *cert});
    if (!tls.ca_file.empty()) {
        auto ca = read_file(tls.ca_file);
        if (!ca)
            return nullptr;
        options.pem_root_certs = *ca;
    }
    return grpc::SslServerCredentials(options);
}

Channels::Channels(TlsConfig tls) : tls_(std::move(tls)) {}

std::shared_ptr<grpc::Channel> Channels::get(const std::string& address) {
    std::lock_guard lock(mu_);
    for (const auto& [addr, channel] : channels_)
        if (addr == address)
            return channel;

    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (tls_.insecure) {
        creds = grpc::InsecureChannelCredentials();
    } else {
        grpc::SslCredentialsOptions options;
        if (!tls_.ca_file.empty())
            options.pem_root_certs = read_file(tls_.ca_file).value_or("");
        // Our certificate doubles as the client identity Data Nodes verify.
        if (!tls_.cert_file.empty() && !tls_.key_file.empty()) {
            options.pem_cert_chain = read_file(tls_.cert_file).value_or("");
            options.pem_private_key = read_file(tls_.key_file).value_or("");
        }
        creds = grpc::SslCredentials(options);
    }
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(kMaxMessageBytes);
    args.SetMaxSendMessageSize(kMaxMessageBytes);
    auto channel = grpc::CreateCustomChannel(address, creds, args);
    channels_.emplace_back(address, channel);
    return channel;
}

}  // namespace rmk::rpc
