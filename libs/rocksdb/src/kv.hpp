#pragma once

// Private to src/: key and value encoding, and the transaction interface the stores are written
// against. The layout is docs/fdb-layout.md; a transaction here is one logical operation run by the
// single writer (or a read-only view on a snapshot).

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "rmk/core/error.hpp"
#include "rmk/core/types.hpp"
#include "rmk/rocksdb/rocks_stores.hpp"

namespace rmk::rocks {

// One byte names each subspace (docs/fdb-layout.md).
enum Tag : char {
    kTenant = 'T',
    kUsage = 'u',
    kBucket = 'B',
    kHead = 'H',
    kVersion = 'V',
    kVersionPart = 'Y',
    kVersionRef = 'W',
    kChunkExtent = 'X',
    kUpload = 'P',
    kUploadByBucket = 'p',
    kUploadDeadline = 'D',
    kPendingInChunk = 'Q',
    kGarbage = 'G',
    kClusterConfig = 'K',
    kGeneration = 'Z',
};

// A version is named by the stamp of the transaction that wrote it: the Generation (8 bytes,
// big-endian, taken from a counter that only ever goes up) and 2 spare bytes.
inline constexpr std::size_t kStampSize = 10;

// ---------------------------------------------------------------------------------------------
// Keys: order-preserving. A string is its bytes with NUL escaped as 00 FF, then a 00
// terminator; integers are fixed-width big-endian; times are nanoseconds with the sign flipped.

inline void put_str(std::string& out, std::string_view s) {
    for (char c : s) {
        out += c;
        if (c == '\0') out += '\xff';
    }
    out += '\0';
}

inline void put_be(std::string& out, std::uint64_t v, int bytes) {
    for (int i = bytes - 1; i >= 0; --i) out += static_cast<char>((v >> (8 * i)) & 0xFF);
}

inline std::int64_t to_ns(Clock::time_point t) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

inline Clock::time_point from_ns(std::int64_t ns) {
    return Clock::time_point(std::chrono::duration_cast<Clock::duration>(std::chrono::nanoseconds(ns)));
}

inline void put_time(std::string& out, Clock::time_point t) {
    put_be(out, static_cast<std::uint64_t>(to_ns(t)) ^ (1ull << 63), 8);
}

// The first key after every key that starts with `key`.
inline std::string strinc(std::string key) {
    while (!key.empty() && static_cast<unsigned char>(key.back()) == 0xFF) key.pop_back();
    if (!key.empty()) key.back() = static_cast<char>(static_cast<unsigned char>(key.back()) + 1);
    return key;
}

inline std::uint64_t get_be(std::string_view in) {
    std::uint64_t v = 0;
    for (char c : in) v = (v << 8) | static_cast<unsigned char>(c);
    return v;
}

inline Generation stamp_generation(std::string_view stamp) { return get_be(stamp.substr(0, 8)); }

class KeyReader {
public:
    explicit KeyReader(std::string_view in) : in_(in) {}

    bool str(std::string& out) {
        out.clear();
        for (std::size_t i = 0; i < in_.size(); ++i) {
            if (in_[i] != '\0') {
                out += in_[i];
            } else if (i + 1 < in_.size() && in_[i + 1] == '\xff') {
                out += '\0';
                ++i;
            } else {
                in_.remove_prefix(i + 1);
                return true;
            }
        }
        return false;
    }

    bool be(std::uint64_t& out, std::size_t bytes) {
        if (in_.size() < bytes) return false;
        out = get_be(in_.substr(0, bytes));
        in_.remove_prefix(bytes);
        return true;
    }

    bool bytes(std::string& out, std::size_t n) {
        if (in_.size() < n) return false;
        out.assign(in_.substr(0, n));
        in_.remove_prefix(n);
        return true;
    }

    std::string_view rest() const { return in_; }

private:
    std::string_view in_;
};

// ---------------------------------------------------------------------------------------------
// Values: big-endian integers and length-prefixed strings.

class Writer {
public:
    Writer& u8(std::uint8_t v) { return be(v, 1); }
    Writer& u32(std::uint32_t v) { return be(v, 4); }
    Writer& u64(std::uint64_t v) { return be(v, 8); }
    Writer& time(Clock::time_point t) { return be(static_cast<std::uint64_t>(to_ns(t)), 8); }
    Writer& str(std::string_view s) {
        u32(static_cast<std::uint32_t>(s.size()));
        out_.append(s);
        return *this;
    }
    std::string take() { return std::move(out_); }

private:
    Writer& be(std::uint64_t v, int bytes) {
        put_be(out_, v, bytes);
        return *this;
    }
    std::string out_;
};

class Reader {
public:
    explicit Reader(std::string_view in) : in_(in) {}

    std::uint8_t u8() { return static_cast<std::uint8_t>(be(1)); }
    std::uint32_t u32() { return static_cast<std::uint32_t>(be(4)); }
    std::uint64_t u64() { return be(8); }
    Clock::time_point time() { return from_ns(static_cast<std::int64_t>(be(8))); }
    std::string str() {
        std::size_t n = u32();
        if (in_.size() < n) return bad<std::string>();
        std::string out(in_.substr(0, n));
        in_.remove_prefix(n);
        return out;
    }
    // True when every read fit and nothing is left over.
    bool ok() const { return ok_ && in_.empty(); }

private:
    std::uint64_t be(std::size_t n) {
        if (in_.size() < n) return bad<std::uint64_t>();
        std::uint64_t v = get_be(in_.substr(0, n));
        in_.remove_prefix(n);
        return v;
    }
    template <class T>
    T bad() {
        ok_ = false;
        in_ = {};
        return T{};
    }
    std::string_view in_;
    bool ok_ = true;
};

inline std::unexpected<Error> corrupt(std::string_view what) {
    return fail(Errc::corruption, "malformed " + std::string(what) + " record");
}

// Little-endian 64-bit counters (usage figures, the generation counter).
inline std::string le64(std::uint64_t v) {
    std::string out(8, '\0');
    for (int i = 0; i < 8; ++i) out[static_cast<std::size_t>(i)] = static_cast<char>((v >> (8 * i)) & 0xFF);
    return out;
}

inline std::uint64_t from_le64(std::string_view in) {
    std::uint64_t v = 0;
    for (std::size_t i = 0; i < in.size() && i < 8; ++i)
        v |= static_cast<std::uint64_t>(static_cast<unsigned char>(in[i])) << (8 * i);
    return v;
}

// ---------------------------------------------------------------------------------------------
// Transactions

struct KeyValue {
    std::string key;
    std::string value;
};

struct RangeOptions {
    std::size_t limit = 0;  // 0: no limit
    bool reverse = false;
    bool snapshot = false;  // ignored: a transaction always reads one consistent view
};

// What a store method sees. A write transaction reads its own writes; a read-only one sees the
// snapshot it was started on and refuses to write.
class Txn {
public:
    virtual ~Txn() = default;

    // Kept so the code ported from the FoundationDB store reads unchanged: a commit here is never
    // in doubt, so this is always false.
    bool maybe_committed = false;

    virtual Result<std::optional<std::string>> get(std::string_view key, bool snapshot = false) = 0;
    Result<std::vector<std::optional<std::string>>> get_many(const std::vector<std::string>& keys, bool snapshot = false);
    virtual Result<std::vector<KeyValue>> range(std::string_view begin, std::string_view end, RangeOptions options = {}) = 0;
    Result<std::vector<std::vector<KeyValue>>> ranges(const std::vector<std::pair<std::string, std::string>>& bounds,
                                                      bool snapshot = false);
    // Streams [begin, end); `visit` returns false to stop.
    virtual Status scan(std::string_view begin, std::string_view end, RangeOptions options,
                        const std::function<bool(KeyValue&&)>& visit) = 0;

    virtual void set(std::string_view key, std::string_view value) = 0;
    virtual void clear(std::string_view key) = 0;
    virtual void clear_range(std::string_view begin, std::string_view end) = 0;
    virtual void add(std::string_view key, std::uint64_t delta) = 0;  // wraps, so ~x + 1 subtracts x
    // `key`/`value` hold kStampSize placeholder bytes at `offset`, replaced by this transaction's stamp.
    virtual void set_stamped_key(std::string key, std::size_t offset, std::string_view value) = 0;
    virtual void set_stamped_value(std::string_view key, std::string value, std::size_t offset) = 0;
    void add_conflict(std::string_view, bool) {}

    // The stamp the stamped writes of this transaction carry (kStampSize bytes), once there was one.
    virtual std::string stamp() const = 0;
};

using Stamp = std::string;

template <class F>
auto write(Database& db, Database::Durability durability, F&& fn) -> std::invoke_result_t<F&, Txn&> {
    using R = std::invoke_result_t<F&, Txn&>;
    std::optional<R> result;
    Status committed = db.write(
        [&](Txn& tx) {
            result.emplace(fn(tx));
            return result->has_value();
        },
        durability);
    if (!committed) return R(std::unexpected(committed.error()));
    return std::move(*result);
}

template <class F>
auto read(Database& db, F&& fn) -> std::invoke_result_t<F&, Txn&> {
    using R = std::invoke_result_t<F&, Txn&>;
    std::optional<R> result;
    db.read([&](Txn& tx) { result.emplace(fn(tx)); });
    return std::move(*result);
}

}  // namespace rmk::rocks
