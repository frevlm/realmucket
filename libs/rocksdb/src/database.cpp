#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <mutex>
#include <thread>

#include "kv.hpp"

namespace rmk::rocks {

Result<std::vector<std::optional<std::string>>> Txn::get_many(const std::vector<std::string>& keys, bool snapshot) {
    std::vector<std::optional<std::string>> out;
    out.reserve(keys.size());
    for (const auto& key : keys) {
        auto v = get(key, snapshot);
        if (!v) return std::unexpected(v.error());
        out.push_back(std::move(*v));
    }
    return out;
}

Result<std::vector<std::vector<KeyValue>>> Txn::ranges(const std::vector<std::pair<std::string, std::string>>& bounds,
                                                      bool snapshot) {
    std::vector<std::vector<KeyValue>> out;
    out.reserve(bounds.size());
    for (const auto& [begin, end] : bounds) {
        auto r = range(begin, end, {.snapshot = snapshot});
        if (!r) return std::unexpected(r.error());
        out.push_back(std::move(*r));
    }
    return out;
}

namespace {

rocksdb::Slice slice(std::string_view s) {
    return {s.data(), s.size()};
}

std::unexpected<Error> io_error(const rocksdb::Status& s) {
    return fail(s.IsCorruption() ? Errc::corruption : Errc::io_error, "rocksdb: " + s.ToString());
}

// What the two kinds of transaction have in common: reading.
class ReadingTxn : public Txn {
public:
    explicit ReadingTxn(rocksdb::DB& db) : db_(db) {}

    Result<std::optional<std::string>> get(std::string_view key, bool) override {
        std::string value;
        rocksdb::Status s = raw_get(slice(key), &value);
        if (s.IsNotFound()) return std::optional<std::string>();
        if (!s.ok()) return io_error(s);
        return std::optional<std::string>(std::move(value));
    }

    Result<std::vector<KeyValue>> range(std::string_view begin, std::string_view end, RangeOptions options) override {
        std::vector<KeyValue> out;
        auto s = scan(begin, end, options, [&](KeyValue&& kv) {
            out.push_back(std::move(kv));
            return true;
        });
        if (!s) return std::unexpected(s.error());
        return out;
    }

    Status scan(std::string_view begin, std::string_view end, RangeOptions options,
                const std::function<bool(KeyValue&&)>& visit) override {
        const rocksdb::Slice lower = slice(begin), upper = slice(end);
        rocksdb::ReadOptions ro;
        ro.iterate_lower_bound = &lower;
        ro.iterate_upper_bound = &upper;
        std::unique_ptr<rocksdb::Iterator> it(iterator(ro));
        std::size_t n = 0;
        if (!options.reverse) {
            for (it->Seek(lower); it->Valid(); it->Next()) {
                if (it->key().ToStringView() >= end) break;
                if (!visit(KeyValue{it->key().ToString(), it->value().ToString()}) || ++n == options.limit) break;
            }
        } else {
            for (it->SeekForPrev(upper); it->Valid(); it->Prev()) {
                if (it->key().ToStringView() >= end) continue;  // SeekForPrev lands on `end` itself if present
                if (it->key().ToStringView() < begin) break;
                if (!visit(KeyValue{it->key().ToString(), it->value().ToString()}) || ++n == options.limit) break;
            }
        }
        if (!it->status().ok()) return std::unexpected(io_error(it->status()).error());
        return {};
    }

    void set(std::string_view, std::string_view) override { misuse(); }
    void clear(std::string_view) override { misuse(); }
    void clear_range(std::string_view, std::string_view) override { misuse(); }
    void add(std::string_view, std::uint64_t) override { misuse(); }
    void set_stamped_key(std::string, std::size_t, std::string_view) override { misuse(); }
    void set_stamped_value(std::string_view, std::string, std::size_t) override { misuse(); }
    std::string stamp() const override { return {}; }

protected:
    virtual rocksdb::Status raw_get(const rocksdb::Slice& key, std::string* value) = 0;
    virtual rocksdb::Iterator* iterator(const rocksdb::ReadOptions& options) = 0;

    [[noreturn]] static void misuse() {
        std::fprintf(stderr, "rocksdb store: a read-only transaction tried to write\n");
        std::abort();
    }

    rocksdb::DB& db_;
};

// A consistent view of the database as of the moment it was created.
class SnapshotTxn final : public ReadingTxn {
public:
    explicit SnapshotTxn(rocksdb::DB& db) : ReadingTxn(db), snapshot_(db.GetSnapshot()) {}
    ~SnapshotTxn() override { db_.ReleaseSnapshot(snapshot_); }

protected:
    rocksdb::Status raw_get(const rocksdb::Slice& key, std::string* value) override {
        rocksdb::ReadOptions ro;
        ro.snapshot = snapshot_;
        return db_.Get(ro, key, value);
    }
    rocksdb::Iterator* iterator(const rocksdb::ReadOptions& options) override {
        rocksdb::ReadOptions ro = options;
        ro.snapshot = snapshot_;
        return db_.NewIterator(ro);
    }

private:
    const rocksdb::Snapshot* snapshot_;
};

// One logical transaction inside the writer's batch: reads the database plus everything the batch
// has written so far, writes into the batch.
class BatchTxn final : public ReadingTxn {
public:
    BatchTxn(rocksdb::DB& db, rocksdb::WriteBatchWithIndex& batch) : ReadingTxn(db), batch_(batch) {}

    void set(std::string_view key, std::string_view value) override { check(batch_.Put(slice(key), slice(value))); }
    void clear(std::string_view key) override { check(batch_.Delete(slice(key))); }

    void clear_range(std::string_view begin, std::string_view end) override {
        std::vector<std::string> keys;
        (void)scan(begin, end, {}, [&](KeyValue&& kv) {
            keys.push_back(std::move(kv.key));
            return true;
        });
        for (const auto& key : keys) clear(key);
    }

    void add(std::string_view key, std::uint64_t delta) override {
        auto cur = get(key, false);
        set(key, le64((cur && *cur ? from_le64(**cur) : 0) + delta));
    }

    void set_stamped_key(std::string key, std::size_t offset, std::string_view value) override {
        key.replace(offset, kStampSize, stamp_for_write());
        set(key, value);
    }

    void set_stamped_value(std::string_view key, std::string value, std::size_t offset) override {
        value.replace(offset, kStampSize, stamp_for_write());
        set(key, value);
    }

    std::string stamp() const override { return stamp_; }

protected:
    rocksdb::Status raw_get(const rocksdb::Slice& key, std::string* value) override {
        return batch_.GetFromBatchAndDB(&db_, rocksdb::ReadOptions(), key, value);
    }
    rocksdb::Iterator* iterator(const rocksdb::ReadOptions& options) override {
        return batch_.NewIteratorWithBase(db_.DefaultColumnFamily(), db_.NewIterator(options), &options);
    }

private:
    static void check(const rocksdb::Status& s) {
        if (!s.ok()) {
            std::fprintf(stderr, "rocksdb store: write into a batch failed: %s\n", s.ToString().c_str());
            std::abort();
        }
    }

    // The first stamped write of a transaction takes the next Generation from a persistent counter;
    // the counter lives in the batch like everything else, so a transaction that rolls back gives its
    // number back.
    const std::string& stamp_for_write() {
        if (stamp_.empty()) {
            const std::string counter = std::string(1, kGeneration) + "gen";
            auto cur = get(counter, false);
            const std::uint64_t next = (cur && *cur ? from_le64(**cur) : 0) + 1;
            set(counter, le64(next));
            stamp_ = std::string();
            put_be(stamp_, next, 8);
            stamp_.append(kStampSize - 8, '\0');
        }
        return stamp_;
    }

    rocksdb::WriteBatchWithIndex& batch_;
    std::string stamp_;
};

}  // namespace

// ================================================================================== Database

struct Database::Impl {
    struct Item {
        Item(const std::function<bool(Txn&)>* b, bool d) : body(b), durable(d) {}
        const std::function<bool(Txn&)>* body;
        bool durable;
        bool done = false;
        Status status;
        std::uint64_t seq = 0;
        std::condition_variable cv;
    };

    static constexpr std::size_t kMaxBatch = 1024;

    Options options;
    std::unique_ptr<rocksdb::DB> db;

    std::mutex mu;
    std::condition_variable work_cv, flush_cv;
    std::deque<Item*> queue;
    std::deque<Item*> waiting;  // applied, durable, not yet flushed
    std::uint64_t applied = 0;
    bool stopping = false;
    std::atomic<std::uint64_t> batches{0}, flushes{0}, transactions{0};
    std::thread applier, flusher;

    void apply_loop() {
        std::unique_lock lock(mu);
        for (;;) {
            work_cv.wait(lock, [&] { return stopping || !queue.empty(); });
            if (queue.empty()) return;
            std::vector<Item*> batch_items;
            while (!queue.empty() && batch_items.size() < kMaxBatch) {
                batch_items.push_back(queue.front());
                queue.pop_front();
            }
            lock.unlock();

            rocksdb::WriteBatchWithIndex batch(rocksdb::BytewiseComparator(), 0, true);
            for (Item* item : batch_items) {
                batch.SetSavePoint();
                BatchTxn tx(*db, batch);
                if ((*item->body)(tx))
                    (void)batch.PopSavePoint();
                else
                    (void)batch.RollbackToSavePoint();
            }
            Status written;
            if (batch.GetWriteBatch()->Count() > 0) {
                rocksdb::WriteOptions wo;
                wo.sync = false;
                rocksdb::Status s = db->Write(wo, batch.GetWriteBatch());
                if (!s.ok()) written = std::unexpected(io_error(s).error());
            }
            ++batches;
            transactions += batch_items.size();

            lock.lock();
            const std::uint64_t seq = ++applied;
            for (Item* item : batch_items) {
                item->status = written;
                item->seq = seq;
                if (item->durable && options.sync && written) {
                    waiting.push_back(item);  // released by the flusher
                    continue;
                }
                item->done = true;
                item->cv.notify_one();
            }
            if (!waiting.empty()) flush_cv.notify_all();
        }
    }

    void flush_loop() {
        std::unique_lock lock(mu);
        for (;;) {
            flush_cv.wait(lock, [&] { return stopping || !waiting.empty(); });
            if (waiting.empty()) return;
            const std::uint64_t target = applied;  // everything applied so far
            lock.unlock();
            rocksdb::Status s = db->FlushWAL(true);
            ++flushes;
            lock.lock();
            for (auto it = waiting.begin(); it != waiting.end();) {
                if ((*it)->seq <= target) {
                    if (!s.ok()) (*it)->status = std::unexpected(io_error(s).error());
                    (*it)->done = true;
                    (*it)->cv.notify_one();
                    it = waiting.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
};

Result<std::shared_ptr<Database>> Database::open(const Options& options) {
    std::error_code ec;
    std::filesystem::create_directories(options.dir, ec);
    if (ec) return fail(Errc::io_error, "create " + options.dir.string() + ": " + ec.message());

    std::shared_ptr<Database> database(new Database());
    database->created_ = !std::filesystem::exists(options.dir / "CURRENT");
    database->impl_ = std::make_unique<Impl>();
    Impl& impl = *database->impl_;
    impl.options = options;

    rocksdb::Options o;
    o.create_if_missing = true;
    o.compression = rocksdb::kNoCompression;
    o.write_buffer_size = std::size_t{64} << 20;
    o.max_write_buffer_number = 4;
    o.max_background_jobs = 4;
    o.max_open_files = 512;
    o.keep_log_file_num = 5;
    o.use_fsync = true;  // on macOS this is what reaches for a flush that really hits the disk
    rocksdb::BlockBasedTableOptions table;
    table.block_cache = rocksdb::NewLRUCache(std::size_t{256} << 20);
    table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    o.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
    rocksdb::Status s = rocksdb::DB::Open(o, options.dir.string(), &impl.db);
    if (!s.ok()) return std::unexpected(io_error(s).error());

    impl.applier = std::thread([&impl] { impl.apply_loop(); });
    impl.flusher = std::thread([&impl] { impl.flush_loop(); });
    return database;
}

Database::~Database() {
    if (!impl_) return;
    {
        std::lock_guard lock(impl_->mu);
        impl_->stopping = true;
    }
    impl_->work_cv.notify_all();
    impl_->flush_cv.notify_all();
    impl_->applier.join();
    impl_->flusher.join();
    if (impl_->db) (void)impl_->db->Close();
}

Database::Stats Database::stats() const {
    return {impl_->batches.load(), impl_->flushes.load(), impl_->transactions.load()};
}

Status Database::write(const std::function<bool(Txn&)>& body, Durability durability) {
    Impl::Item item(&body, durability == Durability::durable);
    std::unique_lock lock(impl_->mu);
    impl_->queue.push_back(&item);
    impl_->work_cv.notify_all();
    item.cv.wait(lock, [&] { return item.done; });
    return item.status;
}

void Database::read(const std::function<void(Txn&)>& body) {
    SnapshotTxn tx(*impl_->db);
    body(tx);
}

}  // namespace rmk::rocks
