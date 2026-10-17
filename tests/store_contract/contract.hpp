#pragma once

// Contract suite for MetaStore and ConfigStore. Include this header from exactly
// one translation unit per test executable and instantiate it:
//
//   RMK_INSTANTIATE_STORE_CONTRACT(Memory, [](std::chrono::seconds retention) {
//       return rmk::contract::StoreSet{...};
//   });
//
// A factory returns fresh, empty stores on every call. Returning a StoreSet with a null `meta`
// skips every test (backend unavailable).

#include "store_contract/common.hpp"
#include "store_contract/config_contract.hpp"
#include "store_contract/meta_basic_contract.hpp"
#include "store_contract/meta_maintenance_contract.hpp"
#include "store_contract/meta_objects_contract.hpp"

#define RMK_INSTANTIATE_STORE_CONTRACT(Name, factory_expression)                                  \
    INSTANTIATE_TEST_SUITE_P(Name, MetaStoreContract,                                             \
                             ::testing::Values(::rmk::contract::StoreFactory(factory_expression))); \
    INSTANTIATE_TEST_SUITE_P(Name, ConfigStoreContract,                                           \
                             ::testing::Values(::rmk::contract::StoreFactory(factory_expression)))
