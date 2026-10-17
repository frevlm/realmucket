#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>

#include "rmk/datanode.hpp"
#include "rmk/rpc.hpp"

namespace rmk {
namespace {

EnvLookup env_of(std::map<std::string, std::string> vars) {
    auto shared = std::make_shared<std::map<std::string, std::string>>(std::move(vars));
    return [shared](const char* name) -> const char* {
        auto it = shared->find(name);
        return it == shared->end() ? nullptr : it->second.c_str();
    };
}

constexpr Errc kAll[] = {
    Errc::not_found,         Errc::already_exists, Errc::precondition_failed, Errc::not_empty,
    Errc::invalid_argument,  Errc::expired,        Errc::no_space,            Errc::quota_exceeded,
    Errc::permission_denied, Errc::unauthenticated, Errc::unavailable,        Errc::aborted,
    Errc::corruption,        Errc::io_error,
};

TEST(Status, EveryErrcSurvivesTheWire) {
    for (Errc code : kAll) {
        grpc::Status wire = rpc::to_status(Error{code, "chunk 7: details: more"});
        EXPECT_EQ(wire.error_message(), std::string(errc_name(code)) + ": chunk 7: details: more");
        Error back = rpc::from_status(wire);
        EXPECT_EQ(back.code, code) << errc_name(code);
        EXPECT_EQ(back.message, "chunk 7: details: more");
    }
}

TEST(Status, GrpcCodesFollowTheTable) {
    EXPECT_EQ(rpc::to_status(Error{Errc::not_empty, ""}).error_code(), grpc::StatusCode::FAILED_PRECONDITION);
    EXPECT_EQ(rpc::to_status(Error{Errc::quota_exceeded, ""}).error_code(), grpc::StatusCode::RESOURCE_EXHAUSTED);
    EXPECT_EQ(rpc::to_status(Error{Errc::corruption, ""}).error_code(), grpc::StatusCode::DATA_LOSS);
    EXPECT_EQ(rpc::to_status(Error{Errc::io_error, ""}).error_code(), grpc::StatusCode::INTERNAL);
}

TEST(Status, ForeignStatusesMapByCode) {
    Error e = rpc::from_status(grpc::Status(grpc::StatusCode::UNAVAILABLE, "failed to connect"));
    EXPECT_EQ(e.code, Errc::unavailable);
    EXPECT_EQ(e.message, "failed to connect");
    EXPECT_EQ(rpc::from_status(grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "x")).code, Errc::unavailable);
    EXPECT_EQ(rpc::from_status(grpc::Status(grpc::StatusCode::UNKNOWN, "x")).code, Errc::io_error);
    // A name prefix that disagrees with the code is not trusted.
    EXPECT_EQ(rpc::from_status(grpc::Status(grpc::StatusCode::NOT_FOUND, "aborted: x")).code, Errc::not_found);
}

TEST(Tls, InsecureOnlyWhenAsked) {
    auto insecure = rpc::load_tls(env_of({{"RMK_INSECURE", "true"}}));
    ASSERT_TRUE(insecure);
    EXPECT_TRUE(insecure->insecure);
    EXPECT_NE(rpc::server_credentials(*insecure), nullptr);

    auto none = rpc::load_tls(env_of({}));
    ASSERT_FALSE(none);
    EXPECT_EQ(none.error().code, Errc::invalid_argument);
    EXPECT_EQ(rpc::load_tls(env_of({{"RMK_INSECURE", "yes"}})).error().code, Errc::invalid_argument);
}

TEST(Tls, CertificateFilesMustBeReadable) {
    auto dir = std::filesystem::temp_directory_path() / ("rmk-tls-" + std::to_string(::getpid()));
    std::filesystem::create_directories(dir);
    std::ofstream(dir / "ca.pem") << "ca";
    std::ofstream(dir / "cert.pem") << "cert";

    auto ca_only = rpc::load_tls(env_of({{"RMK_TLS_CA", (dir / "ca.pem").string()}}));
    ASSERT_TRUE(ca_only);
    EXPECT_FALSE(ca_only->insecure);
    EXPECT_EQ(ca_only->ca_file, (dir / "ca.pem").string());

    auto half = rpc::load_tls(env_of({{"RMK_TLS_CERT", (dir / "cert.pem").string()}}));
    ASSERT_FALSE(half);
    EXPECT_EQ(half.error().code, Errc::invalid_argument);

    auto missing = rpc::load_tls(env_of({{"RMK_TLS_CERT", (dir / "cert.pem").string()},
                                         {"RMK_TLS_KEY", (dir / "absent.pem").string()}}));
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, Errc::invalid_argument);
    std::filesystem::remove_all(dir);
}

TEST(Channels, OneChannelPerAddress) {
    rpc::Channels channels(rpc::TlsConfig{.insecure = true, .cert_file = {}, .key_file = {}, .ca_file = {}});
    auto a = channels.get("127.0.0.1:1");
    EXPECT_EQ(a, channels.get("127.0.0.1:1"));
    EXPECT_NE(a, channels.get("127.0.0.1:2"));
}

TEST(Stores, InMemoryWithoutClusterFile) {
    auto stores = rpc::open_stores(env_of({}), std::chrono::hours(1));
    ASSERT_TRUE(stores);
    EXPECT_NE(stores->meta, nullptr);
    EXPECT_NE(stores->config, nullptr);
    EXPECT_NE(stores->cluster, nullptr);
}

#ifndef RMK_HAVE_FDB
TEST(Stores, ClusterFileNeedsFoundationDb) {
    auto stores = rpc::open_stores(env_of({{"RMK_FDB_CLUSTER_FILE", "/etc/fdb.cluster"}}), std::chrono::hours(1));
    ASSERT_FALSE(stores);
    EXPECT_EQ(stores.error().code, Errc::invalid_argument);
}
#endif

TEST(DataNodeOptions, ReadsTheEnvironment) {
    auto options = datanode::load_options(env_of({{"RMK_NODE_ID", "dn-0"},
                                                  {"RMK_ADVERTISE_ADDRESS", "dn-0.dn:7001"},
                                                  {"RMK_DATA_DIR", "/data"},
                                                  {"RMK_ZONE", "z1"},
                                                  {"RMK_K8S_NODE", "worker-3"},
                                                  {"RMK_CAPACITY_BYTES", "1000000"}}));
    ASSERT_TRUE(options) << options.error().message;
    EXPECT_EQ(options->identity.id, "dn-0");
    EXPECT_EQ(options->identity.address, "dn-0.dn:7001");
    EXPECT_EQ(options->data_dir, "/data");
    EXPECT_EQ(options->identity.zone, "z1");
    EXPECT_EQ(options->identity.k8s_node, "worker-3");
    EXPECT_EQ(options->identity.capacity_bytes, 1000000u);
}

TEST(DataNodeOptions, RejectsMissingOrInvalidValues) {
    std::map<std::string, std::string> base{
        {"RMK_NODE_ID", "dn-0"}, {"RMK_ADVERTISE_ADDRESS", "a:1"}, {"RMK_DATA_DIR", "/data"}};
    for (const char* required : {"RMK_NODE_ID", "RMK_ADVERTISE_ADDRESS", "RMK_DATA_DIR"}) {
        auto vars = base;
        vars.erase(required);
        auto options = datanode::load_options(env_of(vars));
        ASSERT_FALSE(options) << required;
        EXPECT_EQ(options.error().code, Errc::invalid_argument);
    }
    auto bad_capacity = base;
    bad_capacity["RMK_CAPACITY_BYTES"] = "lots";
    EXPECT_EQ(datanode::load_options(env_of(bad_capacity)).error().code, Errc::invalid_argument);
    auto lookup_without_node = base;
    lookup_without_node["RMK_K8S_ZONE_LOOKUP"] = "true";
    EXPECT_EQ(datanode::load_options(env_of(lookup_without_node)).error().code, Errc::invalid_argument);
}

}  // namespace
}  // namespace rmk
