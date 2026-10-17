#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>

#include "rmk/datanode.hpp"
#include "rmk/memory_stores.hpp"
#include "rmk/rpc.hpp"
#include "rmk/v1/datanode.grpc.pb.h"

namespace rmk {
namespace {

namespace fs = std::filesystem;
namespace pb = rmk::internal::v1;

std::uint16_t free_port() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
    socklen_t len = sizeof addr;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    ::close(fd);
    return ntohs(addr.sin_port);
}

bool run(const std::string& command) {
    return std::system((command + " >/dev/null 2>&1").c_str()) == 0;
}

// Throwaway PKI: the cluster CA with one component certificate, plus an unrelated CA's
// certificate standing in for an intruder.
class InternalTls : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() / ("rmk-mtls-" + std::to_string(::getpid()));
        fs::remove_all(dir_);
        fs::create_directories(dir_ / "data");
        std::ofstream(dir_ / "ext.cnf") << "subjectAltName=IP:127.0.0.1,DNS:localhost\n"
                                           "extendedKeyUsage=serverAuth,clientAuth\n";
        const std::string d = dir_.string() + "/";
        for (const std::string ca : {"ca", "other-ca"})
            ASSERT_TRUE(run("openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=" + ca + " -keyout " + d +
                            ca + ".key -out " + d + ca + ".pem"));
        for (auto [name, ca] : {std::pair{"node", "ca"}, std::pair{"intruder", "other-ca"}}) {
            const std::string n = d + name;
            ASSERT_TRUE(run("openssl req -newkey rsa:2048 -nodes -subj /CN=" + std::string(name) + " -keyout " + n +
                            ".key -out " + n + ".csr"));
            ASSERT_TRUE(run("openssl x509 -req -days 2 -in " + n + ".csr -CA " + d + ca + ".pem -CAkey " + d + ca +
                            ".key -CAcreateserial -extfile " + d + "ext.cnf -out " + n + ".pem"));
        }

        node_tls_ = {.insecure = false, .cert_file = d + "node.pem", .key_file = d + "node.key", .ca_file = d + "ca.pem"};
        address_ = "127.0.0.1:" + std::to_string(free_port());
        datanode::Options options;
        options.identity.id = "dn-0";
        options.identity.address = address_;
        options.data_dir = dir_ / "data";
        auto node = datanode::Node::create(options, std::make_shared<MemoryClusterStore>(),
                                           std::make_shared<rpc::Channels>(node_tls_));
        ASSERT_TRUE(node);
        node_ = std::move(*node);
        auto credentials = rpc::server_credentials(node_tls_);
        ASSERT_NE(credentials, nullptr);
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address_, credentials);
        builder.RegisterService(node_->service());
        server_ = builder.BuildAndStart();
        ASSERT_NE(server_, nullptr);
    }

    void TearDown() override {
        if (server_)
            server_->Shutdown(std::chrono::system_clock::now());
        server_.reset();
        node_.reset();
        fs::remove_all(dir_);
    }

    grpc::Status stat_with(const rpc::TlsConfig& tls) {
        rpc::Channels channels(tls);
        auto stub = pb::DataNode::NewStub(channels.get(address_));
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        pb::FileRef ref;
        ref.set_chunk_id(1);
        pb::StatResponse response;
        return stub->Stat(&ctx, ref, &response);
    }

    fs::path dir_;
    rpc::TlsConfig node_tls_;
    std::string address_;
    std::unique_ptr<datanode::Node> node_;
    std::unique_ptr<grpc::Server> server_;
};

TEST_F(InternalTls, ComponentCertificateIsAccepted) {
    grpc::Status status = stat_with(node_tls_);
    EXPECT_TRUE(status.ok()) << status.error_message();
}

TEST_F(InternalTls, CallerWithoutCertificateIsRejected) {
    rpc::TlsConfig anonymous{.insecure = false, .cert_file = {}, .key_file = {}, .ca_file = node_tls_.ca_file};
    grpc::Status status = stat_with(anonymous);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAUTHENTICATED) << status.error_message();
}

TEST_F(InternalTls, CertificateFromAnotherCaIsRejected) {
    const std::string d = dir_.string() + "/";
    rpc::TlsConfig intruder{
        .insecure = false, .cert_file = d + "intruder.pem", .key_file = d + "intruder.key", .ca_file = node_tls_.ca_file};
    EXPECT_FALSE(stat_with(intruder).ok());
}

}  // namespace
}  // namespace rmk
