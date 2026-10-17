#include "rmk/crypto.hpp"

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>

namespace rmk::crypto {
namespace {

constexpr std::string_view kDataKeyAad = "rmk-data-key-v1";

struct CtxDeleter {
    void operator()(EVP_CIPHER_CTX* ctx) const { EVP_CIPHER_CTX_free(ctx); }
};
using Ctx = std::unique_ptr<EVP_CIPHER_CTX, CtxDeleter>;

const unsigned char* uc(const std::byte* p) { return reinterpret_cast<const unsigned char*>(p); }
unsigned char* uc(std::byte* p) { return reinterpret_cast<unsigned char*>(p); }

std::span<const std::byte> as_span(std::string_view s) { return std::as_bytes(std::span(s.data(), s.size())); }

std::array<std::byte, 4> be32(std::uint32_t v) {
    return {static_cast<std::byte>(v >> 24), static_cast<std::byte>(v >> 16),
            static_cast<std::byte>(v >> 8), static_cast<std::byte>(v)};
}

Result<std::vector<std::byte>> gcm_seal(const Key& key, std::span<const std::byte> aad,
                                        std::span<const std::byte> plaintext) {
    std::vector<std::byte> out(kNonceSize + plaintext.size() + kTagSize);
    random_bytes(std::span(out).first(kNonceSize));
    std::byte* cipher = out.data() + kNonceSize;
    std::byte* tag = cipher + plaintext.size();

    Ctx ctx(EVP_CIPHER_CTX_new());
    int len = 0;
    if (!ctx || EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1 ||
        EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, kNonceSize, nullptr) != 1 ||
        EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr, uc(key.data()), uc(out.data())) != 1 ||
        (!aad.empty() && EVP_EncryptUpdate(ctx.get(), nullptr, &len, uc(aad.data()), static_cast<int>(aad.size())) != 1) ||
        (!plaintext.empty() &&
         EVP_EncryptUpdate(ctx.get(), uc(cipher), &len, uc(plaintext.data()), static_cast<int>(plaintext.size())) != 1) ||
        EVP_EncryptFinal_ex(ctx.get(), uc(cipher) + plaintext.size(), &len) != 1 ||
        EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, kTagSize, tag) != 1)
        return fail(Errc::io_error, "AES-256-GCM encryption failed");
    return out;
}

Result<std::vector<std::byte>> gcm_open(const Key& key, std::span<const std::byte> aad,
                                        std::span<const std::byte> stored) {
    if (stored.size() < kPartOverhead) return fail(Errc::corruption, "sealed data shorter than nonce and tag");
    const std::size_t n = stored.size() - kPartOverhead;
    const std::byte* nonce = stored.data();
    const std::byte* cipher = nonce + kNonceSize;
    std::byte tag[kTagSize];
    std::memcpy(tag, cipher + n, kTagSize);

    std::vector<std::byte> out(n);
    Ctx ctx(EVP_CIPHER_CTX_new());
    int len = 0;
    if (!ctx || EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1 ||
        EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, kNonceSize, nullptr) != 1 ||
        EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr, uc(key.data()), uc(nonce)) != 1 ||
        (!aad.empty() && EVP_DecryptUpdate(ctx.get(), nullptr, &len, uc(aad.data()), static_cast<int>(aad.size())) != 1) ||
        (n != 0 && EVP_DecryptUpdate(ctx.get(), uc(out.data()), &len, uc(cipher), static_cast<int>(n)) != 1) ||
        EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, kTagSize, tag) != 1 ||
        EVP_DecryptFinal_ex(ctx.get(), uc(out.data()) + n, &len) != 1)
        return fail(Errc::corruption, "authentication tag mismatch");
    return out;
}

Result<std::string> wrap(const Key& master, const Key& data_key) {
    auto sealed = gcm_seal(master, as_span(kDataKeyAad), data_key);
    if (!sealed) return std::unexpected(sealed.error());
    return std::string(reinterpret_cast<const char*>(sealed->data()), sealed->size());
}

Result<Key> unwrap_with(const Key& master, std::string_view wrapped) {
    auto plain = gcm_open(master, as_span(kDataKeyAad), as_span(wrapped));
    if (!plain) return std::unexpected(plain.error());
    if (plain->size() != kKeySize) return fail(Errc::corruption, "wrapped data key has wrong length");
    Key key;
    std::memcpy(key.data(), plain->data(), kKeySize);
    OPENSSL_cleanse(plain->data(), plain->size());
    return key;
}

}  // namespace

void random_bytes(std::span<std::byte> out) {
    if (out.empty()) return;
    if (RAND_bytes(uc(out.data()), static_cast<int>(out.size())) != 1) std::abort();
}

Key random_key() {
    Key key;
    random_bytes(key);
    return key;
}

std::vector<std::byte> seal_part(const Key& data_key, std::uint32_t part_number,
                                 std::span<const std::byte> plaintext) {
    auto aad = be32(part_number);
    auto sealed = gcm_seal(data_key, aad, plaintext);
    if (!sealed) std::abort();
    return std::move(*sealed);
}

Result<std::vector<std::byte>> open_part(const Key& data_key, std::uint32_t part_number,
                                         std::span<const std::byte> stored) {
    auto aad = be32(part_number);
    return gcm_open(data_key, aad, stored);
}

std::string sha256_hex(std::string_view data) {
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int md_len = 0;
    if (EVP_Digest(data.data(), data.size(), md, &md_len, EVP_sha256(), nullptr) != 1) std::abort();
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(md_len * 2);
    for (unsigned int i = 0; i < md_len; ++i) {
        out.push_back(kHex[md[i] >> 4]);
        out.push_back(kHex[md[i] & 0xF]);
    }
    return out;
}

bool constant_time_equal(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    return a.empty() || CRYPTO_memcmp(a.data(), b.data(), a.size()) == 0;
}

StaticKeyProvider::StaticKeyProvider(std::map<std::uint32_t, Key> keys, std::uint32_t active)
    : keys_(std::move(keys)), active_(active) {}

Result<Key> StaticKeyProvider::master_key(std::uint32_t id) const {
    auto it = keys_.find(id);
    if (it == keys_.end()) return fail(Errc::not_found, "unknown master key " + std::to_string(id));
    return it->second;
}

Result<NewDataKey> new_data_key(const KeyProvider& keys) {
    const std::uint32_t id = keys.active_id();
    auto master = keys.master_key(id);
    if (!master) return std::unexpected(master.error());
    NewDataKey out{random_key(), {}};
    auto wrapped = wrap(*master, out.key);
    if (!wrapped) return std::unexpected(wrapped.error());
    out.encryption = Encryption{id, std::move(*wrapped)};
    return out;
}

Result<Key> unwrap(const KeyProvider& keys, const Encryption& encryption) {
    auto master = keys.master_key(encryption.master_key_id);
    if (!master) return std::unexpected(master.error());
    return unwrap_with(*master, encryption.wrapped_data_key);
}

Result<Encryption> rewrap(const KeyProvider& keys, const Encryption& encryption,
                          std::uint32_t to_master_key_id) {
    auto data_key = unwrap(keys, encryption);
    if (!data_key) return std::unexpected(data_key.error());
    auto to = keys.master_key(to_master_key_id);
    if (!to) return std::unexpected(to.error());
    auto wrapped = wrap(*to, *data_key);
    if (!wrapped) return std::unexpected(wrapped.error());
    return Encryption{to_master_key_id, std::move(*wrapped)};
}

}  // namespace rmk::crypto
