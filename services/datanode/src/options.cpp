#include <cctype>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>

#include "rmk/datanode/datanode.hpp"
#include "rmk/http/http.hpp"

namespace rmk::datanode {
namespace {

constexpr const char* kServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount";
constexpr std::string_view kZoneLabel = "\"topology.kubernetes.io/zone\"";

Result<std::string> read_text(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in)
        return fail(Errc::unavailable, "cannot read " + path);
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

// The value of the zone label in a k8s Node object, or empty when the label is absent.
// Label keys are unique JSON object keys, so finding the quoted key followed by ':' is
// enough; an escaped occurrence inside some other string never ends in a bare quote.
Result<std::string> zone_from_node_json(std::string_view json) {
    for (std::size_t at = json.find(kZoneLabel); at != std::string_view::npos;
         at = json.find(kZoneLabel, at + 1)) {
        std::size_t p = at + kZoneLabel.size();
        while (p < json.size() && std::isspace(static_cast<unsigned char>(json[p])))
            ++p;
        if (p >= json.size() || json[p] != ':')
            continue;
        ++p;
        while (p < json.size() && std::isspace(static_cast<unsigned char>(json[p])))
            ++p;
        if (p >= json.size() || json[p] != '"')
            return fail(Errc::invalid_argument, "zone label is not a string");
        std::string value;
        for (++p; p < json.size() && json[p] != '"'; ++p) {
            if (json[p] == '\\') {
                if (++p >= json.size())
                    break;
                if (json[p] == 'u')  // not expected in a label value
                    return fail(Errc::invalid_argument, "unsupported escape in zone label");
            }
            value.push_back(json[p]);
        }
        if (p >= json.size())
            return fail(Errc::invalid_argument, "unterminated zone label");
        return value;
    }
    return std::string();
}

Result<std::string> lookup_zone(const std::string& k8s_node) {
    const std::string dir = kServiceAccountDir;
    auto token = read_text(dir + "/token");
    if (!token)
        return std::unexpected(token.error());
    while (!token->empty() && std::isspace(static_cast<unsigned char>(token->back())))
        token->pop_back();
    http::GetOptions options;
    options.bearer_token = *token;
    options.ca_file = dir + "/ca.crt";
    auto body = http::get("https://kubernetes.default.svc/api/v1/nodes/" + k8s_node, options);
    if (!body)
        return std::unexpected(body.error());
    return zone_from_node_json(*body);
}

}  // namespace

Result<Options> load_options(const EnvLookup& env) {
    Options options;
    std::string data_dir;
    bool zone_lookup = false;
    for (auto [name, out] : {std::pair{"RMK_NODE_ID", &options.identity.id},
                             std::pair{"RMK_ADVERTISE_ADDRESS", &options.identity.address},
                             std::pair{"RMK_DATA_DIR", &data_dir}})
        if (auto s = env_required(env, name, *out); !s)
            return std::unexpected(s.error());
    if (auto s = env_string(env, "RMK_ZONE", true, options.identity.zone); !s)
        return std::unexpected(s.error());
    if (auto s = env_string(env, "RMK_K8S_NODE", true, options.identity.k8s_node); !s)
        return std::unexpected(s.error());
    if (auto s = env_u64(env, "RMK_CAPACITY_BYTES", 0, std::numeric_limits<std::uint64_t>::max(),
                         options.identity.capacity_bytes);
        !s)
        return std::unexpected(s.error());
    if (auto s = env_bool(env, "RMK_K8S_ZONE_LOOKUP", zone_lookup); !s)
        return std::unexpected(s.error());
    options.data_dir = data_dir;

    if (zone_lookup) {
        if (options.identity.k8s_node.empty())
            return fail(Errc::invalid_argument, "RMK_K8S_ZONE_LOOKUP=true needs RMK_K8S_NODE");
        auto zone = lookup_zone(options.identity.k8s_node);
        if (!zone)
            return fail(zone.error().code, "zone lookup for node " + options.identity.k8s_node + ": " +
                                               zone.error().message);
        options.identity.zone = *zone;
    }
    return options;
}

}  // namespace rmk::datanode
