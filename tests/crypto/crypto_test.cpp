#include "rmk/crypto.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

using namespace rmk;
using namespace rmk::crypto;

std::vector<std::byte> to_bytes(std::string_view s) {
    auto sp = std::as_bytes(std::span(s.data(), s.size()));
    return {sp.begin(), sp.end()};
}

TEST(Crypto, SealOpenRoundTrip) {
    Key key = random_key();
    auto plain = to_bytes("hello object storage");
    auto sealed = seal_part(key, 3, plain);
    ASSERT_EQ(sealed.size(), plain.size() + kPartOverhead);
    auto opened = open_part(key, 3, sealed);
    ASSERT_TRUE(opened.has_value());
    EXPECT_EQ(*opened, plain);
}

TEST(Crypto, SealOpenEmptyPlaintext) {
    Key key = random_key();
    auto sealed = seal_part(key, 1, {});
    ASSERT_EQ(sealed.size(), kPartOverhead);
    auto opened = open_part(key, 1, sealed);
    ASSERT_TRUE(opened.has_value());
    EXPECT_EQ(opened->size(), 0u);
}

TEST(Crypto, WrongPartNumberIsCorruption) {
    Key key = random_key();
    auto sealed = seal_part(key, 1, to_bytes("data"));
    auto opened = open_part(key, 2, sealed);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code, Errc::corruption);
}

TEST(Crypto, WrongKeyIsCorruption) {
    auto sealed = seal_part(random_key(), 1, to_bytes("data"));
    auto opened = open_part(random_key(), 1, sealed);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code, Errc::corruption);
}

TEST(Crypto, FlippedByteIsCorruptionAtEveryRegion) {
    Key key = random_key();
    auto sealed = seal_part(key, 1, to_bytes("some plaintext"));
    for (std::size_t pos : {std::size_t{0}, kNonceSize, sealed.size() - 1}) {
        auto bad = sealed;
        bad[pos] ^= std::byte{0x01};
        auto opened = open_part(key, 1, bad);
        ASSERT_FALSE(opened.has_value()) << "pos=" << pos;
        EXPECT_EQ(opened.error().code, Errc::corruption);
    }
}

TEST(Crypto, TooShortIsCorruption) {
    Key key = random_key();
    std::vector<std::byte> tiny(kPartOverhead - 1);
    auto opened = open_part(key, 1, tiny);
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code, Errc::corruption);
}

TEST(Crypto, FreshNonceEachSeal) {
    Key key = random_key();
    auto plain = to_bytes("same plaintext");
    auto a = seal_part(key, 1, plain);
    auto b = seal_part(key, 1, plain);
    EXPECT_NE(a, b);
    EXPECT_NE(std::vector<std::byte>(a.begin(), a.begin() + kNonceSize),
              std::vector<std::byte>(b.begin(), b.begin() + kNonceSize));
}

TEST(Crypto, Sha256Hex) {
    EXPECT_EQ(sha256_hex("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    EXPECT_EQ(sha256_hex(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(Crypto, ConstantTimeEqual) {
    EXPECT_TRUE(constant_time_equal("abc", "abc"));
    EXPECT_TRUE(constant_time_equal("", ""));
    EXPECT_FALSE(constant_time_equal("abc", "abd"));
    EXPECT_FALSE(constant_time_equal("abc", "abcd"));
}

TEST(Crypto, RandomBytesDiffer) {
    EXPECT_NE(random_key(), random_key());
}

TEST(DataKey, NewUnwrapRoundTrip) {
    StaticKeyProvider keys({{1, random_key()}, {2, random_key()}}, 2);
    auto fresh = new_data_key(keys);
    ASSERT_TRUE(fresh.has_value());
    EXPECT_EQ(fresh->encryption.master_key_id, 2u);
    EXPECT_EQ(fresh->encryption.wrapped_data_key.size(), kKeySize + kPartOverhead);
    auto key = unwrap(keys, fresh->encryption);
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, fresh->key);
}

TEST(DataKey, RewrapAcrossMasterKeys) {
    StaticKeyProvider keys({{1, random_key()}, {2, random_key()}}, 1);
    auto fresh = new_data_key(keys);
    ASSERT_TRUE(fresh.has_value());
    auto moved = rewrap(keys, fresh->encryption, 2);
    ASSERT_TRUE(moved.has_value());
    EXPECT_EQ(moved->master_key_id, 2u);
    EXPECT_NE(moved->wrapped_data_key, fresh->encryption.wrapped_data_key);
    auto key = unwrap(keys, *moved);
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(*key, fresh->key);
}

TEST(DataKey, UnknownMasterKeyIsNotFound) {
    StaticKeyProvider keys({{1, random_key()}}, 1);
    auto fresh = new_data_key(keys);
    ASSERT_TRUE(fresh.has_value());
    Encryption enc = fresh->encryption;
    enc.master_key_id = 9;
    auto key = unwrap(keys, enc);
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code, Errc::not_found);

    auto moved = rewrap(keys, fresh->encryption, 9);
    ASSERT_FALSE(moved.has_value());
    EXPECT_EQ(moved.error().code, Errc::not_found);
}

TEST(DataKey, WrongMasterKeyIsCorruption) {
    StaticKeyProvider writer({{1, random_key()}}, 1);
    StaticKeyProvider reader({{1, random_key()}}, 1);
    auto fresh = new_data_key(writer);
    ASSERT_TRUE(fresh.has_value());
    auto key = unwrap(reader, fresh->encryption);
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code, Errc::corruption);
}

}  // namespace
