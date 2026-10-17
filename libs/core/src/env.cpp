#include "rmk/core/env.hpp"

#include <charconv>
#include <cstdlib>
#include <string_view>

namespace rmk {

EnvLookup process_env() {
    return [](const char* name) { return std::getenv(name); };
}

Status env_u64(const EnvLookup& env, const char* name, std::uint64_t min, std::uint64_t max,
               std::uint64_t& out) {
    const char* raw = env(name);
    if (raw == nullptr)
        return {};
    std::string_view text{raw};
    std::uint64_t value = 0;
    auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || ec != std::errc{} || end != text.data() + text.size())
        return fail(Errc::invalid_argument, std::string(name) + "='" + raw + "' is not an unsigned integer");
    if (value < min || value > max)
        return fail(Errc::invalid_argument, std::string(name) + " must be between " + std::to_string(min) +
                                                " and " + std::to_string(max));
    out = value;
    return {};
}

Status env_seconds(const EnvLookup& env, const char* name, std::chrono::seconds min,
                   std::chrono::seconds& out) {
    auto seconds = static_cast<std::uint64_t>(out.count());
    if (auto s = env_u64(env, name, static_cast<std::uint64_t>(min.count()), UINT32_MAX, seconds); !s)
        return s;
    out = std::chrono::seconds(seconds);
    return {};
}

Status env_string(const EnvLookup& env, const char* name, bool allow_empty, std::string& out) {
    const char* raw = env(name);
    if (raw == nullptr)
        return {};
    if (!allow_empty && *raw == '\0')
        return fail(Errc::invalid_argument, std::string(name) + " must not be empty");
    out = raw;
    return {};
}

Status env_bool(const EnvLookup& env, const char* name, bool& out) {
    const char* raw = env(name);
    if (raw == nullptr)
        return {};
    std::string_view text{raw};
    if (text != "true" && text != "false")
        return fail(Errc::invalid_argument, std::string(name) + " must be 'true' or 'false'");
    out = text == "true";
    return {};
}

Status env_required(const EnvLookup& env, const char* name, std::string& out) {
    if (env(name) == nullptr)
        return fail(Errc::invalid_argument, std::string(name) + " is required");
    return env_string(env, name, false, out);
}

}  // namespace rmk
