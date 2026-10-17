#include <gtest/gtest.h>

#include "rmk/core/version.hpp"

namespace rmk {

TEST(CompareVersions, OrdersByMajorMinorPatch) {
    EXPECT_EQ(*compare_versions("1.2.3", "1.2.3"), 0);
    EXPECT_LT(*compare_versions("1.2.3", "1.10.0"), 0);
    EXPECT_GT(*compare_versions("2.0.0", "1.99.99"), 0);
}

TEST(CompareVersions, RejectsMalformedText) {
    EXPECT_FALSE(compare_versions("1.2", "1.2.3"));
    EXPECT_FALSE(compare_versions("1.2.x", "1.2.3"));
    EXPECT_FALSE(compare_versions("", "1.2.3"));
}

}  // namespace rmk
