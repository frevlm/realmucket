#pragma once

// Private to src/fdb: key/value encoding, the transaction wrapper and the retry loop.

#include <foundationdb/fdb_c.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "rmk/fdb_stores.hpp"

namespace rmk::fdb {

// One byte after the store prefix names each subspace (docs/fdb-layout.md). The three stores
// may share a prefix, so the tags are unique across all of them.
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
    kReceipt = 'R',
    kGarbage = 'G',
    kClusterConfig = 'K',
    kNode = 'N',
    kChunk = 'C',
    kChunkByState = 'S',
    kChunkGarbage = 'g',
    kChunkCounter = 'A',
    kLeader = 'L',
};

inline constexpr std::size_t kStampSize = 10;  // commit version (8, big-endian) + batch order (2)

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

inline std::string encode_prefix(std::string_view key_prefix) {
    std::string out;
    put_str(out, key_prefix);
    return out;
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

// Little-endian 64-bit counters, the operand format of FDB's atomic ADD.
inline std::string le64(std::uint64_t v) {
    std::string out(8, '\0');
    for (int i = 0; i < 8; ++i) out[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
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
    bool snapshot = false;
};

std::unexpected<Error> fdb_fail(fdb_error_t code);

// One attempt of a transaction. A failed FDB call is remembered in error() and surfaces as an
// rmk error; the retry loop looks at error() to decide whether to retry.
class Txn {
public:
    explicit Txn(FDBTransaction* tr) : tr_(tr) {}

    // An earlier attempt of this same call may have committed (commit_unknown_result).
    bool maybe_committed = false;

    Result<std::optional<std::string>> get(std::string_view key, bool snapshot = false);
    Result<std::vector<std::optional<std::string>>> get_many(const std::vector<std::string>& keys,
                                                             bool snapshot = false);
    Result<std::vector<KeyValue>> range(std::string_view begin, std::string_view end,
                                        RangeOptions options = {});
    // Several whole ranges read concurrently.
    Result<std::vector<std::vector<KeyValue>>> ranges(
        const std::vector<std::pair<std::string, std::string>>& bounds, bool snapshot = false);
    // Streams [begin, end); `visit` returns false to stop.
    Status scan(std::string_view begin, std::string_view end, RangeOptions options,
                const std::function<bool(KeyValue&&)>& visit);

    void set(std::string_view key, std::string_view value);
    void clear(std::string_view key);
    void clear_range(std::string_view begin, std::string_view end);
    void add(std::string_view key, std::uint64_t delta);  // wraps, so ~x + 1 subtracts x
    // `key`/`value` hold kStampSize placeholder bytes at `offset`, replaced by the commit's
    // versionstamp.
    void set_stamped_key(std::string key, std::size_t offset, std::string_view value);
    void set_stamped_value(std::string_view key, std::string value, std::size_t offset);
    void add_conflict(std::string_view key, bool write);

    fdb_error_t error() const { return err_; }
    bool stamped() const { return stamped_; }

private:
    std::unexpected<Error> failed(fdb_error_t code);
    FDBTransaction* tr_;
    fdb_error_t err_ = 0;
    bool stamped_ = false;
};

struct FutureDeleter {
    void operator()(FDBFuture* f) const { fdb_future_destroy(f); }
};
using FuturePtr = std::unique_ptr<FDBFuture, FutureDeleter>;

inline fdb_error_t wait(const FuturePtr& f) {
    if (fdb_error_t e = fdb_future_block_until_ready(f.get())) return e;
    return fdb_future_get_error(f.get());
}

struct TransactionDeleter {
    void operator()(FDBTransaction* tr) const { fdb_transaction_destroy(tr); }
};

Result<std::unique_ptr<FDBTransaction, TransactionDeleter>> new_transaction(Database& db);

using Stamp = std::array<std::uint8_t, kStampSize>;

// Runs fn(Txn&) -> Result<T>/Status as one FDB transaction with the standard retry loop
// (fdb_transaction_on_error). An rmk error from fn aborts without committing. After a
// maybe-committed error the next attempt sees tx.maybe_committed and must detect its own
// earlier commit. When fn wrote versionstamped mutations and `stamp` is given, the commit's
// versionstamp is stored there.
template <class F>
auto run(Database& db, F&& fn, Stamp* stamp = nullptr) -> std::invoke_result_t<F&, Txn&> {
    using R = std::invoke_result_t<F&, Txn&>;
    auto tr = new_transaction(db);
    if (!tr) return R(std::unexpected(tr.error()));
    bool maybe_committed = false;
    for (;;) {
        Txn tx(tr->get());
        tx.maybe_committed = maybe_committed;
        R result = fn(tx);
        fdb_error_t e = tx.error();
        if (e == 0) {
            if (!result) return result;
            FuturePtr vs;
            if (stamp && tx.stamped()) vs.reset(fdb_transaction_get_versionstamp(tr->get()));
            FuturePtr commit(fdb_transaction_commit(tr->get()));
            e = wait(commit);
            if (e == 0 && db.take_simulated_unknown_result()) e = 1021;  // commit_unknown_result
            if (e == 0) {
                if (vs && stamp) {
                    if (fdb_error_t ve = wait(vs)) return R(fdb_fail(ve));
                    const std::uint8_t* key = nullptr;
                    int length = 0;
                    if (fdb_error_t ve = fdb_future_get_key(vs.get(), &key, &length))
                        return R(fdb_fail(ve));
                    if (!key || length != static_cast<int>(kStampSize))
                        return R(fail(Errc::corruption, "unexpected versionstamp"));
                    std::copy(key, key + kStampSize, stamp->begin());
                }
                return result;
            }
        }
        if (fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED, e)) maybe_committed = true;
        FuturePtr retry(fdb_transaction_on_error(tr->get(), e));
        if (fdb_error_t re = wait(retry)) return R(fdb_fail(re));
    }
}

}  // namespace rmk::fdb
