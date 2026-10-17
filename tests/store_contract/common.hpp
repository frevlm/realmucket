#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>

#include "rmk/core/config_store.hpp"
#include "rmk/core/meta_store.hpp"

namespace rmk::contract {

// A factory returns fresh, empty stores on every call. Returning a StoreSet with a null `meta`
// means the backend is unavailable and every contract test is skipped.
struct StoreSet {
    std::shared_ptr<rmk::MetaStore> meta;
    std::shared_ptr<rmk::ConfigStore> config;
};

using StoreFactory = std::function<StoreSet(std::chrono::seconds pending_upload_retention)>;

inline const Clock::time_point kT0 = Clock::time_point{} + std::chrono::hours(24 * 365 * 40);

inline Clock::time_point at(long seconds) { return kT0 + std::chrono::seconds(seconds); }

template <class R>
::testing::AssertionResult Ok(const R& result) {
    if (result.has_value()) return ::testing::AssertionSuccess();
    return ::testing::AssertionFailure()
           << "expected success, got " << errc_name(result.error().code) << ": "
           << result.error().message;
}

template <class R>
::testing::AssertionResult Fails(const R& result, Errc code) {
    if (result.has_value()) return ::testing::AssertionFailure() << "expected " << errc_name(code) << ", got success";
    if (result.error().code == code) return ::testing::AssertionSuccess();
    return ::testing::AssertionFailure() << "expected " << errc_name(code) << ", got "
                                         << errc_name(result.error().code) << ": "
                                         << result.error().message;
}

}  // namespace rmk::contract

#define RMK_ASSERT_OK(expr) ASSERT_TRUE(::rmk::contract::Ok(expr))
#define RMK_EXPECT_OK(expr) EXPECT_TRUE(::rmk::contract::Ok(expr))
#define RMK_EXPECT_ERR(expr, code) EXPECT_TRUE(::rmk::contract::Fails((expr), ::rmk::Errc::code))
