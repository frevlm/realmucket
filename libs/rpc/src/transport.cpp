#include <fstream>
#include <sstream>
#include <string>
#include <utility>

#include "rmk/rpc/rpc.hpp"

namespace rmk::rpc {
namespace {

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
    // With RMK_TLS_CA set, a client certificate is requested and verified when one is presented.
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

}  // namespace rmk::rpc
