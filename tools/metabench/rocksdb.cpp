#ifdef MB_HAVE_ROCKSDB
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <stdexcept>

#include "engine.hpp"

namespace metabench {
namespace {

void check(const rocksdb::Status& s, const char* what) {
    if (!s.ok())
        throw std::runtime_error(std::string("rocksdb ") + what + ": " + s.ToString());
}

rocksdb::Slice slice(std::string_view s) {
    return {s.data(), s.size()};
}

class RocksEngine final : public Engine {
public:
    explicit RocksEngine(const Options& options) : options_(options) {
        std::filesystem::create_directories(options.dir);
        rocksdb::Options o;
        o.create_if_missing = true;
        o.compression = rocksdb::kNoCompression;
        o.write_buffer_size = std::size_t{128} << 20;
        o.max_write_buffer_number = 4;
        o.max_background_jobs = 6;
        o.use_fsync = true;  // on macOS this is the call that reaches for a full flush
        rocksdb::BlockBasedTableOptions table;
        table.block_cache = rocksdb::NewLRUCache(std::size_t{512} << 20);
        table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
        o.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
        check(rocksdb::DB::Open(o, options.dir.string(), &db_), "open");
    }

    ~RocksEngine() override {
        db_->Close();
    }

    std::string name() const override { return "rocksdb"; }

    void apply_batch(const std::vector<const TxBody*>& bodies, std::vector<char>& ok, bool durable) override {
        write(bodies, ok, durable && options_.sync);
    }

    bool can_decouple() const override { return true; }
    void apply_unsynced(const std::vector<const TxBody*>& bodies, std::vector<char>& ok) override { write(bodies, ok, false); }
    void flush() override {
        if (options_.sync)
            check(db_->FlushWAL(true), "FlushWAL");
    }

    void write(const std::vector<const TxBody*>& bodies, std::vector<char>& ok, bool sync) {
        rocksdb::WriteBatchWithIndex batch(rocksdb::BytewiseComparator(), 0, true);
        for (std::size_t i = 0; i < bodies.size(); ++i) {
            batch.SetSavePoint();
            Writer tx(*db_, batch);
            ok[i] = (*bodies[i])(tx) ? 1 : 0;
            if (ok[i])
                batch.PopSavePoint();
            else
                batch.RollbackToSavePoint();
        }
        rocksdb::WriteOptions wo;
        wo.sync = sync;
        check(db_->Write(wo, batch.GetWriteBatch()), "write");
    }

    std::optional<std::string> read_get(std::string_view key) override {
        std::string value;
        auto s = db_->Get(rocksdb::ReadOptions(), slice(key), &value);
        if (s.IsNotFound())
            return std::nullopt;
        check(s, "get");
        return value;
    }

    std::size_t read_scan(std::string_view from, std::string_view to, std::size_t limit) override {
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
        std::size_t n = 0;
        for (it->Seek(slice(from)); it->Valid() && n < limit; it->Next()) {
            if (it->key().ToStringView() >= to)
                break;
            ++n;
        }
        return n;
    }

    std::uint64_t disk_bytes() const override { return directory_bytes(options_.dir); }

private:
    class Writer final : public Tx {
    public:
        Writer(rocksdb::DB& db, rocksdb::WriteBatchWithIndex& batch) : db_(db), batch_(batch) {}
        std::optional<std::string> get(std::string_view key) override {
            std::string value;
            auto s = batch_.GetFromBatchAndDB(&db_, rocksdb::ReadOptions(), slice(key), &value);
            if (s.IsNotFound())
                return std::nullopt;
            check(s, "get");
            return value;
        }
        void put(std::string_view key, std::string_view value) override { check(batch_.Put(slice(key), slice(value)), "put"); }
        void del(std::string_view key) override { check(batch_.Delete(slice(key)), "delete"); }

    private:
        rocksdb::DB& db_;
        rocksdb::WriteBatchWithIndex& batch_;
    };

    Options options_;
    std::unique_ptr<rocksdb::DB> db_;
};

}  // namespace

std::unique_ptr<Engine> make_rocksdb(const Options& options) {
    return std::make_unique<RocksEngine>(options);
}

}  // namespace metabench
#endif
