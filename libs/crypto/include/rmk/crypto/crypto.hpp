#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "rmk/core/error.hpp"
#include "rmk/core/types.hpp"

namespace rmk::crypto {

inline constexpr std::size_t kKeySize = 32;    // AES-256
inline constexpr std::size_t kNonceSize = 12;  // GCM
inline constexpr std::size_t kTagSize = 16;
// Every stored Part is nonce || ciphertext || tag (ADR 0016).
inline constexpr std::size_t kPartOverhead = kNonceSize + kTagSize;

using Key = std::array<std::byte, kKeySize>;

// Cryptographically secure random bytes (OpenSSL RAND_bytes); aborts the process on failure.
void random_bytes(std::span<std::byte> out);
Key random_key();

// AES-256-GCM with a fresh random nonce. A re-sent Part therefore never reuses a nonce
// under the same Data Key. The part number is bound as associated data so Parts cannot be
// swapped. Output: nonce || ciphertext || tag.
std::vector<std::byte> seal_part(const Key& data_key, std::uint32_t part_number,
                                 std::span<const std::byte> plaintext);
// corruption if the tag does not verify (wrong key, wrong part number, altered bytes).
Result<std::vector<std::byte>> open_part(const Key& data_key, std::uint32_t part_number,
                                         std::span<const std::byte> stored);

std::string sha256_hex(std::string_view data);  // lowercase hex
bool constant_time_equal(std::string_view a, std::string_view b);

// Master Keys (ADR 0017). Several may exist; the active one wraps new Data Keys.
class KeyProvider {
public:
    virtual ~KeyProvider() = default;
    virtual Result<Key> master_key(std::uint32_t id) const = 0;
    virtual std::uint32_t active_id() const = 0;
};

// Reads a directory (e.g. a mounted Secret volume): one file per Master Key named by its decimal
// id holding 64 hex characters (surrounding whitespace ignored), plus a file "active"
// holding the active id. Any malformed file, or an "active" id without a key, is an error.
class DirectoryKeyProvider final : public KeyProvider {
public:
    static Result<DirectoryKeyProvider> load(const std::filesystem::path& dir);
    Result<Key> master_key(std::uint32_t id) const override;
    std::uint32_t active_id() const override { return active_; }

private:
    std::map<std::uint32_t, Key> keys_;
    std::uint32_t active_ = 0;
};

// Fixed keys, for tests and local development.
class StaticKeyProvider final : public KeyProvider {
public:
    StaticKeyProvider(std::map<std::uint32_t, Key> keys, std::uint32_t active);
    Result<Key> master_key(std::uint32_t id) const override;
    std::uint32_t active_id() const override { return active_; }

private:
    std::map<std::uint32_t, Key> keys_;
    std::uint32_t active_;
};

// Envelope encryption of Data Keys. wrapped_data_key = nonce || ciphertext || tag of the
// Data Key under the Master Key, with associated data "rmk-data-key-v1".
struct NewDataKey {
    Key key;
    Encryption encryption;
};
Result<NewDataKey> new_data_key(const KeyProvider& keys);
Result<Key> unwrap(const KeyProvider& keys, const Encryption& encryption);
Result<Encryption> rewrap(const KeyProvider& keys, const Encryption& encryption,
                          std::uint32_t to_master_key_id);

}  // namespace rmk::crypto
