#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

#include "rmk/error.hpp"

namespace rmk {

using EnvLookup = std::function<const char*(const char*)>;
EnvLookup process_env();

// Each reader leaves `out` untouched when the variable is unset, and rejects malformed or
// out-of-range values (ADR 0006: never fall back silently).
Status env_u64(const EnvLookup& env, const char* name, std::uint64_t min, std::uint64_t max,
               std::uint64_t& out);
Status env_seconds(const EnvLookup& env, const char* name, std::chrono::seconds min,
                   std::chrono::seconds& out);
Status env_string(const EnvLookup& env, const char* name, bool allow_empty, std::string& out);
Status env_bool(const EnvLookup& env, const char* name, bool& out);  // "true" / "false"
// Unset is an error.
Status env_required(const EnvLookup& env, const char* name, std::string& out);

}  // namespace rmk
