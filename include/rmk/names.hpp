#pragma once

#include <cstddef>
#include <string_view>

#include "rmk/error.hpp"
#include "rmk/types.hpp"

namespace rmk {

inline constexpr std::size_t kMaxObjectKeyBytes = 1024;
inline constexpr std::size_t kMaxListLimit = 1000;

// 1-63 chars of [a-z0-9._-], starting with a letter or digit.
Status validate_tenant_name(std::string_view name);
Status validate_bucket_name(std::string_view name);
// 1-1024 bytes of valid UTF-8 without NUL.
Status validate_object_key(std::string_view key);
// 1-1000.
Status validate_list_limit(std::size_t limit);
// 1 <= data_units, data_units + parity_units <= 20.
Status validate_bucket_config(const BucketConfig& config);

}  // namespace rmk
