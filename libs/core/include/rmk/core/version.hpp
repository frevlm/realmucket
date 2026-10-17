#pragma once

#include <string_view>

#include "rmk/core/error.hpp"

namespace rmk {

// Semantic versions "MAJOR.MINOR.PATCH" (ADR 0015). Returns <0, 0, >0; invalid_argument if malformed.
Result<int> compare_versions(std::string_view a, std::string_view b);
inline constexpr std::string_view kSdkVersion = "1.0.0";

}  // namespace rmk
