#include "rmk/core/version.hpp"

#include <array>
#include <cstdint>
#include <optional>

namespace rmk {

Result<int> compare_versions(std::string_view a, std::string_view b) {
    auto parse = [](std::string_view text) -> std::optional<std::array<std::uint64_t, 3>> {
        std::array<std::uint64_t, 3> out{};
        std::size_t field = 0;
        std::size_t digits = 0;
        for (char c : text) {
            if (c == '.') {
                if (digits == 0 || ++field > 2) return std::nullopt;
                digits = 0;
            } else if (c >= '0' && c <= '9') {
                if (++digits > 18) return std::nullopt;
                out[field] = out[field] * 10 + static_cast<std::uint64_t>(c - '0');
            } else {
                return std::nullopt;
            }
        }
        if (digits == 0 || field != 2) return std::nullopt;
        return out;
    };
    auto va = parse(a);
    auto vb = parse(b);
    if (!va || !vb) return fail(Errc::invalid_argument, "malformed version");
    if (*va == *vb) return 0;
    return *va < *vb ? -1 : 1;
}

}  // namespace rmk
