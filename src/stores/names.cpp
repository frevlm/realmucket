#include "rmk/names.hpp"

#include <cstdint>
#include <string>

namespace rmk {
namespace {

constexpr std::size_t kMaxNameLength = 63;
constexpr int kMaxUnits = 20;

bool is_alnum(char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); }

Status validate_name(std::string_view name, const char* what) {
    if (name.empty() || name.size() > kMaxNameLength)
        return fail(Errc::invalid_argument, std::string(what) + " name must be 1-63 characters");
    if (!is_alnum(name.front()))
        return fail(Errc::invalid_argument,
                    std::string(what) + " name must start with a letter or digit");
    for (char c : name)
        if (!is_alnum(c) && c != '.' && c != '_' && c != '-')
            return fail(Errc::invalid_argument,
                        std::string(what) + " name may only contain [a-z0-9._-]");
    return {};
}

bool valid_utf8(std::string_view s) {
    std::size_t i = 0;
    while (i < s.size()) {
        auto b0 = static_cast<std::uint8_t>(s[i]);
        std::size_t extra;
        std::uint32_t code;
        std::uint32_t min;
        if (b0 < 0x80) {
            ++i;
            continue;
        } else if ((b0 & 0xE0) == 0xC0) {
            extra = 1, code = b0 & 0x1F, min = 0x80;
        } else if ((b0 & 0xF0) == 0xE0) {
            extra = 2, code = b0 & 0x0F, min = 0x800;
        } else if ((b0 & 0xF8) == 0xF0) {
            extra = 3, code = b0 & 0x07, min = 0x10000;
        } else {
            return false;
        }
        if (i + extra >= s.size()) return false;
        for (std::size_t k = 1; k <= extra; ++k) {
            auto b = static_cast<std::uint8_t>(s[i + k]);
            if ((b & 0xC0) != 0x80) return false;
            code = (code << 6) | (b & 0x3F);
        }
        if (code < min || code > 0x10FFFF || (code >= 0xD800 && code <= 0xDFFF)) return false;
        i += extra + 1;
    }
    return true;
}

}  // namespace

Status validate_tenant_name(std::string_view name) { return validate_name(name, "tenant"); }

Status validate_bucket_name(std::string_view name) { return validate_name(name, "bucket"); }

Status validate_object_key(std::string_view key) {
    if (key.empty() || key.size() > kMaxObjectKeyBytes)
        return fail(Errc::invalid_argument, "object key must be 1-1024 bytes");
    if (key.find('\0') != std::string_view::npos)
        return fail(Errc::invalid_argument, "object key must not contain NUL");
    if (!valid_utf8(key)) return fail(Errc::invalid_argument, "object key must be valid UTF-8");
    return {};
}

Status validate_list_limit(std::size_t limit) {
    if (limit < 1 || limit > kMaxListLimit)
        return fail(Errc::invalid_argument, "list limit must be 1-1000");
    return {};
}

Status validate_bucket_config(const BucketConfig& config) {
    const auto& p = config.storage_policy;
    if (p.data_units < 1)
        return fail(Errc::invalid_argument, "storage policy needs at least one data unit");
    if (p.data_units + p.parity_units > kMaxUnits)
        return fail(Errc::invalid_argument, "storage policy data + parity units must be <= 20");
    return {};
}

}  // namespace rmk
