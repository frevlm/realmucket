#ifdef MB_HAVE_LMDB
#include <lmdb.h>

#include <stdexcept>

#include "engine.hpp"

namespace metabench {
namespace {

void check(int rc, const char* what) {
    if (rc != 0)
        throw std::runtime_error(std::string("lmdb ") + what + ": " + mdb_strerror(rc));
}

class LmdbEngine final : public Engine {
public:
    explicit LmdbEngine(const Options& options) : options_(options) {
        std::filesystem::create_directories(options.dir);
        check(mdb_env_create(&env_), "env_create");
        check(mdb_env_set_maxreaders(env_, 1024), "maxreaders");
        check(mdb_env_set_mapsize(env_, std::size_t{64} << 30), "mapsize");
        check(mdb_env_open(env_, options.dir.c_str(), MDB_NOSYNC, 0644), "env_open");  // flushed by hand, below
        MDB_txn* txn;
        check(mdb_txn_begin(env_, nullptr, 0, &txn), "txn_begin");
        check(mdb_dbi_open(txn, nullptr, 0, &dbi_), "dbi_open");
        check(mdb_txn_commit(txn), "txn_commit");
    }

    ~LmdbEngine() override { mdb_env_close(env_); }

    std::string name() const override { return "lmdb"; }

    void apply_batch(const std::vector<const TxBody*>& bodies, std::vector<char>& ok, bool durable) override {
        apply_unsynced(bodies, ok);
        if (durable)
            flush();
    }

    bool can_decouple() const override { return true; }

    void flush() override {
        if (options_.sync)
            check(mdb_env_sync(env_, 1), "env_sync");
    }

    void apply_unsynced(const std::vector<const TxBody*>& bodies, std::vector<char>& ok) override {
        MDB_txn* parent;
        check(mdb_txn_begin(env_, nullptr, 0, &parent), "txn_begin");
        for (std::size_t i = 0; i < bodies.size(); ++i) {
            MDB_txn* child;
            check(mdb_txn_begin(env_, parent, 0, &child), "child txn_begin");
            Writer tx(child, dbi_);
            ok[i] = (*bodies[i])(tx) ? 1 : 0;
            if (ok[i])
                check(mdb_txn_commit(child), "child commit");
            else
                mdb_txn_abort(child);
        }
        check(mdb_txn_commit(parent), "commit");
    }

    std::optional<std::string> read_get(std::string_view key) override {
        MDB_txn* txn;
        check(mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn), "read txn_begin");
        MDB_val k{key.size(), const_cast<char*>(key.data())}, v;
        int rc = mdb_get(txn, dbi_, &k, &v);
        std::optional<std::string> out;
        if (rc == 0)
            out.emplace(static_cast<const char*>(v.mv_data), v.mv_size);
        else if (rc != MDB_NOTFOUND)
            check(rc, "get");
        mdb_txn_abort(txn);
        return out;
    }

    std::size_t read_scan(std::string_view from, std::string_view to, std::size_t limit) override {
        MDB_txn* txn;
        check(mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn), "read txn_begin");
        MDB_cursor* cursor;
        check(mdb_cursor_open(txn, dbi_, &cursor), "cursor_open");
        MDB_val k{from.size(), const_cast<char*>(from.data())}, v;
        std::size_t n = 0;
        for (int rc = mdb_cursor_get(cursor, &k, &v, MDB_SET_RANGE); rc == 0 && n < limit;
             rc = mdb_cursor_get(cursor, &k, &v, MDB_NEXT)) {
            if (std::string_view(static_cast<const char*>(k.mv_data), k.mv_size) >= to)
                break;
            ++n;
        }
        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        return n;
    }

    std::uint64_t disk_bytes() const override { return directory_bytes(options_.dir); }

private:
    class Writer final : public Tx {
    public:
        Writer(MDB_txn* txn, MDB_dbi dbi) : txn_(txn), dbi_(dbi) {}
        std::optional<std::string> get(std::string_view key) override {
            MDB_val k{key.size(), const_cast<char*>(key.data())}, v;
            int rc = mdb_get(txn_, dbi_, &k, &v);
            if (rc == MDB_NOTFOUND)
                return std::nullopt;
            check(rc, "get");
            return std::string(static_cast<const char*>(v.mv_data), v.mv_size);
        }
        void put(std::string_view key, std::string_view value) override {
            MDB_val k{key.size(), const_cast<char*>(key.data())}, v{value.size(), const_cast<char*>(value.data())};
            check(mdb_put(txn_, dbi_, &k, &v, 0), "put");
        }
        void del(std::string_view key) override {
            MDB_val k{key.size(), const_cast<char*>(key.data())};
            int rc = mdb_del(txn_, dbi_, &k, nullptr);
            if (rc != MDB_NOTFOUND)
                check(rc, "del");
        }

    private:
        MDB_txn* txn_;
        MDB_dbi dbi_;
    };

    Options options_;
    MDB_env* env_ = nullptr;
    MDB_dbi dbi_ = 0;
};

}  // namespace

std::unique_ptr<Engine> make_lmdb(const Options& options) {
    return std::make_unique<LmdbEngine>(options);
}

}  // namespace metabench
#endif
