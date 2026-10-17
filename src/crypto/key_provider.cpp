#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <system_error>

#include "rmk/crypto.hpp"

namespace rmk::crypto {
namespace {

std::string trim(std::string s) {
    auto space = [](unsigned char c) { return std::isspace(c) != 0; };
    s.erase(s.begin(), std::find_if_not(s.begin(), s.end(), space));
    s.erase(std::find_if_not(s.rbegin(), s.rend(), space).base(), s.end());
    return s;
}

bool parse_id(const std::string& s, std::uint32_t& out) {
    if (s.empty() || s.size() > 10 || (s.size() > 1 && s[0] == '0') ||
        !std::ranges::all_of(s, [](unsigned char c) { return c >= '0' && c <= '9'; }))
        return false;
    unsigned long long v = std::stoull(s);
    if (v > UINT32_MAX) return false;
    out = static_cast<std::uint32_t>(v);
    return true;
}

int hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

Result<std::string> read_file(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return fail(Errc::invalid_argument, "cannot read " + path.string());
    std::ostringstream ss;
    ss << in.rdbuf();
    return trim(ss.str());
}

Result<Key> parse_key(const std::string& hex, const std::filesystem::path& path) {
    if (hex.size() != 2 * kKeySize) return fail(Errc::invalid_argument, "master key file must hold 64 hex characters: " + path.string());
    Key key;
    for (std::size_t i = 0; i < kKeySize; ++i) {
        int hi = hex_value(hex[2 * i]);
        int lo = hex_value(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return fail(Errc::invalid_argument, "master key file is not hex: " + path.string());
        key[i] = static_cast<std::byte>(hi << 4 | lo);
    }
    return key;
}

}  // namespace

Result<DirectoryKeyProvider> DirectoryKeyProvider::load(const std::filesystem::path& dir) {
    std::error_code ec;
    std::filesystem::directory_iterator it(dir, ec);
    if (ec) return fail(Errc::invalid_argument, "cannot read key directory " + dir.string() + ": " + ec.message());

    DirectoryKeyProvider out;
    bool have_active = false;
    for (const auto& entry : it) {
        const std::string name = entry.path().filename().string();
        if (name.starts_with('.')) continue;

        auto text = read_file(entry.path());
        if (!text) return std::unexpected(text.error());
        if (name == "active") {
            if (!parse_id(*text, out.active_)) return fail(Errc::invalid_argument, "malformed active key id");
            have_active = true;
            continue;
        }
        std::uint32_t id = 0;
        if (!parse_id(name, id)) return fail(Errc::invalid_argument, "unexpected file in key directory: " + name);
        auto key = parse_key(*text, entry.path());
        if (!key) return std::unexpected(key.error());
        out.keys_[id] = *key;
    }
    if (!have_active) return fail(Errc::invalid_argument, "key directory has no \"active\" file");
    if (!out.keys_.contains(out.active_))
        return fail(Errc::invalid_argument, "active master key " + std::to_string(out.active_) + " has no key file");
    return out;
}

Result<Key> DirectoryKeyProvider::master_key(std::uint32_t id) const {
    auto it = keys_.find(id);
    if (it == keys_.end()) return fail(Errc::not_found, "unknown master key " + std::to_string(id));
    return it->second;
}

}  // namespace rmk::crypto
