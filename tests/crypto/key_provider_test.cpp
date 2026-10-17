#include "rmk/crypto.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

namespace {

using namespace rmk;
using namespace rmk::crypto;
namespace fs = std::filesystem;

const std::string kHex1(64, 'a');
const std::string kHex2 = "00112233445566778899aabbccddeeff00112233445566778899AABBCCDDEEFF";

class DirectoryKeyProviderTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() / ("rmk_keys_" + std::to_string(::getpid()) + "_" +
                                            ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fs::remove_all(dir_);
        fs::create_directories(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }

    void write(const std::string& name, const std::string& content) {
        std::ofstream(dir_ / name) << content;
    }

    fs::path dir_;
};

TEST_F(DirectoryKeyProviderTest, LoadsKeysAndActive) {
    write("1", kHex1 + "\n");
    write("2", "  " + kHex2 + " \n");
    write("active", "2\n");
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->active_id(), 2u);

    auto k1 = p->master_key(1);
    ASSERT_TRUE(k1.has_value());
    EXPECT_EQ((*k1)[0], std::byte{0xaa});
    EXPECT_EQ((*k1)[31], std::byte{0xaa});

    auto k2 = p->master_key(2);
    ASSERT_TRUE(k2.has_value());
    EXPECT_EQ((*k2)[0], std::byte{0x00});
    EXPECT_EQ((*k2)[1], std::byte{0x11});
    EXPECT_EQ((*k2)[31], std::byte{0xff});
}

TEST_F(DirectoryKeyProviderTest, UnknownIdIsNotFound) {
    write("1", kHex1);
    write("active", "1");
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_TRUE(p.has_value());
    auto k = p->master_key(7);
    ASSERT_FALSE(k.has_value());
    EXPECT_EQ(k.error().code, Errc::not_found);
}

TEST_F(DirectoryKeyProviderTest, FollowsSymlinksAndIgnoresDotEntries) {
    fs::create_directories(dir_ / "..2024_data");
    std::ofstream(dir_ / "..2024_data" / "1") << kHex1;
    std::ofstream(dir_ / "..2024_data" / "active") << "1";
    fs::create_directory_symlink("..2024_data", dir_ / "..data");
    fs::create_symlink("..data/1", dir_ / "1");
    fs::create_symlink("..data/active", dir_ / "active");
    write(".hidden", "not a key");

    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->active_id(), 1u);
    auto k = p->master_key(1);
    ASSERT_TRUE(k.has_value());
    EXPECT_EQ((*k)[0], std::byte{0xaa});
}

TEST_F(DirectoryKeyProviderTest, MalformedKeyFile) {
    write("1", kHex1.substr(0, 62));
    write("active", "1");
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_FALSE(p.has_value());
    EXPECT_EQ(p.error().code, Errc::invalid_argument);
}

TEST_F(DirectoryKeyProviderTest, NonHexKeyFile) {
    write("1", std::string(63, 'a') + "g");
    write("active", "1");
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_FALSE(p.has_value());
    EXPECT_EQ(p.error().code, Errc::invalid_argument);
}

TEST_F(DirectoryKeyProviderTest, NonNumericName) {
    write("1", kHex1);
    write("backup", kHex1);
    write("active", "1");
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_FALSE(p.has_value());
    EXPECT_EQ(p.error().code, Errc::invalid_argument);
}

TEST_F(DirectoryKeyProviderTest, NonCanonicalNamesAreRejected) {
    for (const char* name : {"01", "+1", "-1", "1.0", "4294967296", "00000000001", " 1"}) {
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        write("1", kHex1);
        write(name, kHex2);
        write("active", "1");
        auto p = DirectoryKeyProvider::load(dir_);
        ASSERT_FALSE(p.has_value()) << name;
        EXPECT_EQ(p.error().code, Errc::invalid_argument);
    }
    fs::remove_all(dir_);
    fs::create_directories(dir_);
    write("0", kHex1);
    write("4294967295", kHex2);
    write("active", "4294967295");
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_TRUE(p.has_value());
    EXPECT_EQ(p->active_id(), 4294967295u);
}

TEST_F(DirectoryKeyProviderTest, MissingActive) {
    write("1", kHex1);
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_FALSE(p.has_value());
    EXPECT_EQ(p.error().code, Errc::invalid_argument);
}

TEST_F(DirectoryKeyProviderTest, ActiveWithoutKey) {
    write("1", kHex1);
    write("active", "2");
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_FALSE(p.has_value());
    EXPECT_EQ(p.error().code, Errc::invalid_argument);
}

TEST_F(DirectoryKeyProviderTest, MalformedActive) {
    write("1", kHex1);
    write("active", "one");
    auto p = DirectoryKeyProvider::load(dir_);
    ASSERT_FALSE(p.has_value());
    EXPECT_EQ(p.error().code, Errc::invalid_argument);
}

TEST_F(DirectoryKeyProviderTest, MissingDirectory) {
    auto p = DirectoryKeyProvider::load(dir_ / "nope");
    ASSERT_FALSE(p.has_value());
    EXPECT_EQ(p.error().code, Errc::invalid_argument);
}

}  // namespace
