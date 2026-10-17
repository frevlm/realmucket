#include <cstdio>
#include <thread>

#include "fdb.hpp"

namespace rmk::fdb {
namespace {

constexpr std::int64_t kTransactionTimeoutMs = 10000;

// The FDB client allows one network thread per process, and it cannot be restarted.
class Network {
public:
    static Status ensure() {
        static Network network;
        return network.status_;
    }

private:
    Network() {
        if (fdb_error_t e = fdb_select_api_version(FDB_API_VERSION)) {
            status_ = fdb_fail(e);
            return;
        }
        if (fdb_error_t e = fdb_setup_network()) {
            status_ = fdb_fail(e);
            return;
        }
        thread_ = std::thread([] {
            if (fdb_error_t e = fdb_run_network()) std::fprintf(stderr, "fdb network thread: %s\n", fdb_get_error(e));
        });
    }

    ~Network() {
        if (!thread_.joinable()) return;
        if (fdb_stop_network() == 0) thread_.join();
        else thread_.detach();
    }

    Status status_;
    std::thread thread_;
};

std::string uint8s(const std::uint8_t* p, int n) { return std::string(reinterpret_cast<const char*>(p), static_cast<std::size_t>(n)); }

const std::uint8_t* bytes(std::string_view s) { return reinterpret_cast<const std::uint8_t*>(s.data()); }

int size(std::string_view s) { return static_cast<int>(s.size()); }

void put_le32(std::string& out, std::size_t v) {
    for (int i = 0; i < 4; ++i) out += static_cast<char>((v >> (8 * i)) & 0xFF);
}

}  // namespace

std::unexpected<Error> fdb_fail(fdb_error_t code) {
    std::string message = "fdb error " + std::to_string(code) + ": " + fdb_get_error(code);
    switch (code) {
        case 2101:  // transaction_too_large
        case 2102:  // key_too_large
        case 2103:  // value_too_large
            return fail(Errc::invalid_argument, std::move(message));
        default:
            return fail(Errc::unavailable, std::move(message));
    }
}

bool Database::take_simulated_unknown_result() {
    int left = unknown_results_.load();
    while (left > 0)
        if (unknown_results_.compare_exchange_weak(left, left - 1)) return true;
    return false;
}

Database::~Database() { fdb_database_destroy(db_); }

Result<std::shared_ptr<Database>> open_database(const std::string& cluster_file) {
    if (auto network = Network::ensure(); !network) return std::unexpected(network.error());
    FDBDatabase* db = nullptr;
    if (fdb_error_t e = fdb_create_database(cluster_file.c_str(), &db)) return fdb_fail(e);
    return std::shared_ptr<Database>(new Database(db));
}

Result<std::unique_ptr<FDBTransaction, TransactionDeleter>> new_transaction(Database& db) {
    FDBTransaction* tr = nullptr;
    if (fdb_error_t e = fdb_database_create_transaction(db.handle(), &tr)) return fdb_fail(e);
    std::unique_ptr<FDBTransaction, TransactionDeleter> owned(tr);
    std::string timeout;
    for (int i = 0; i < 8; ++i)
        timeout += static_cast<char>((static_cast<std::uint64_t>(kTransactionTimeoutMs) >> (8 * i)) & 0xFF);
    if (fdb_error_t e = fdb_transaction_set_option(tr, FDB_TR_OPTION_TIMEOUT, bytes(timeout), size(timeout)))
        return fdb_fail(e);
    return owned;
}

// --- Txn

std::unexpected<Error> Txn::failed(fdb_error_t code) {
    if (err_ == 0) err_ = code;
    return fdb_fail(code);
}

Result<std::optional<std::string>> Txn::get(std::string_view key, bool snapshot) {
    auto values = get_many({std::string(key)}, snapshot);
    if (!values) return std::unexpected(values.error());
    return std::move(values->front());
}

Result<std::vector<std::optional<std::string>>> Txn::get_many(const std::vector<std::string>& keys,
                                                              bool snapshot) {
    std::vector<FuturePtr> futures;
    futures.reserve(keys.size());
    for (const auto& key : keys) futures.emplace_back(fdb_transaction_get(tr_, bytes(key), size(key), snapshot));
    std::vector<std::optional<std::string>> out;
    out.reserve(keys.size());
    for (const auto& f : futures) {
        if (fdb_error_t e = wait(f)) return failed(e);
        fdb_bool_t present = 0;
        const std::uint8_t* value = nullptr;
        int length = 0;
        if (fdb_error_t e = fdb_future_get_value(f.get(), &present, &value, &length)) return failed(e);
        out.push_back(present ? std::optional(uint8s(value, length)) : std::nullopt);
    }
    return out;
}

Status Txn::scan(std::string_view begin_key, std::string_view end_key, RangeOptions options,
                 const std::function<bool(KeyValue&&)>& visit) {
    if (begin_key >= end_key) return {};
    std::string begin(begin_key), end(end_key);
    fdb_bool_t begin_or_equal = 0;  // first_greater_or_equal(begin), later first_greater_than(last)
    std::size_t seen = 0;
    for (int iteration = 1;; ++iteration) {
        int limit = options.limit ? static_cast<int>(options.limit - seen) : 0;
        FuturePtr f(fdb_transaction_get_range(tr_, bytes(begin), size(begin), begin_or_equal, 1,
                                              bytes(end), size(end), 0, 1, limit, 0,
                                              FDB_STREAMING_MODE_ITERATOR, iteration,
                                              options.snapshot, options.reverse));
        if (fdb_error_t e = wait(f)) return failed(e);
        const FDBKeyValue* kv = nullptr;
        int count = 0;
        fdb_bool_t more = 0;
        if (fdb_error_t e = fdb_future_get_keyvalue_array(f.get(), &kv, &count, &more)) return failed(e);
        if (count == 0) return {};
        std::string last = uint8s(kv[count - 1].key, kv[count - 1].key_length);
        for (int i = 0; i < count; ++i) {
            ++seen;
            if (!visit(KeyValue{uint8s(kv[i].key, kv[i].key_length), uint8s(kv[i].value, kv[i].value_length)}))
                return {};
        }
        if (!more || (options.limit && seen >= options.limit)) return {};
        if (options.reverse) {
            end = std::move(last);
        } else {
            begin = std::move(last);
            begin_or_equal = 1;
        }
    }
}

Result<std::vector<KeyValue>> Txn::range(std::string_view begin, std::string_view end,
                                         RangeOptions options) {
    std::vector<KeyValue> out;
    auto s = scan(begin, end, options, [&](KeyValue&& kv) {
        out.push_back(std::move(kv));
        return true;
    });
    if (!s) return std::unexpected(s.error());
    return out;
}

Result<std::vector<std::vector<KeyValue>>> Txn::ranges(
    const std::vector<std::pair<std::string, std::string>>& bounds, bool snapshot) {
    std::vector<FuturePtr> futures;
    futures.reserve(bounds.size());
    for (const auto& [b, e] : bounds)
        futures.emplace_back(fdb_transaction_get_range(tr_, bytes(b), size(b), 0, 1, bytes(e), size(e), 0, 1,
                                                       0, 0, FDB_STREAMING_MODE_WANT_ALL, 0, snapshot, 0));
    std::vector<std::vector<KeyValue>> out(bounds.size());
    for (std::size_t i = 0; i < bounds.size(); ++i) {
        if (fdb_error_t e = wait(futures[i])) return failed(e);
        const FDBKeyValue* kv = nullptr;
        int count = 0;
        fdb_bool_t more = 0;
        if (fdb_error_t e = fdb_future_get_keyvalue_array(futures[i].get(), &kv, &count, &more))
            return failed(e);
        for (int j = 0; j < count; ++j)
            out[i].push_back({uint8s(kv[j].key, kv[j].key_length), uint8s(kv[j].value, kv[j].value_length)});
        if (more && count > 0) {
            std::string resume = out[i].back().key + '\0';
            auto rest = range(resume, bounds[i].second, {.snapshot = snapshot});
            if (!rest) return std::unexpected(rest.error());
            for (auto& item : *rest) out[i].push_back(std::move(item));
        }
    }
    return out;
}

void Txn::set(std::string_view key, std::string_view value) {
    fdb_transaction_set(tr_, bytes(key), size(key), bytes(value), size(value));
}

void Txn::clear(std::string_view key) { fdb_transaction_clear(tr_, bytes(key), size(key)); }

void Txn::clear_range(std::string_view begin, std::string_view end) {
    if (begin < end) fdb_transaction_clear_range(tr_, bytes(begin), size(begin), bytes(end), size(end));
}

void Txn::add(std::string_view key, std::uint64_t delta) {
    std::string operand = le64(delta);
    fdb_transaction_atomic_op(tr_, bytes(key), size(key), bytes(operand), size(operand), FDB_MUTATION_TYPE_ADD);
}

void Txn::set_stamped_key(std::string key, std::size_t offset, std::string_view value) {
    put_le32(key, offset);
    fdb_transaction_atomic_op(tr_, bytes(key), size(key), bytes(value), size(value),
                              FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);
    stamped_ = true;
}

void Txn::set_stamped_value(std::string_view key, std::string value, std::size_t offset) {
    put_le32(value, offset);
    fdb_transaction_atomic_op(tr_, bytes(key), size(key), bytes(value), size(value),
                              FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);
    stamped_ = true;
}

void Txn::add_conflict(std::string_view key, bool write) {
    std::string end = std::string(key) + '\0';
    fdb_error_t e = fdb_transaction_add_conflict_range(tr_, bytes(key), size(key), bytes(end), size(end),
                                                       write ? FDB_CONFLICT_RANGE_TYPE_WRITE
                                                             : FDB_CONFLICT_RANGE_TYPE_READ);
    if (e && err_ == 0) err_ = e;
}

// --- Whole-prefix helpers

Result<std::vector<std::pair<std::string, std::string>>> dump_prefix(Database& db,
                                                                     std::string_view key_prefix) {
    std::string begin = encode_prefix(key_prefix);
    return run(db, [&](Txn& tx) -> Result<std::vector<std::pair<std::string, std::string>>> {
        auto kvs = tx.range(begin, strinc(begin), {.snapshot = true});
        if (!kvs) return std::unexpected(kvs.error());
        std::vector<std::pair<std::string, std::string>> out;
        for (auto& kv : *kvs) out.emplace_back(std::move(kv.key), std::move(kv.value));
        return out;
    });
}

Status clear_prefix(Database& db, std::string_view key_prefix) {
    std::string begin = encode_prefix(key_prefix);
    return run(db, [&](Txn& tx) -> Status {
        tx.clear_range(begin, strinc(begin));
        return {};
    });
}

}  // namespace rmk::fdb
