// SQLite: one write connection (WAL), readers each with a connection of their own.
#include <sqlite3.h>

#include <map>
#include <memory>
#include <stdexcept>

#include "engine.hpp"

namespace metabench {
namespace {

void check(int rc, sqlite3* db, const char* what) {
    if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW)
        throw std::runtime_error(std::string("sqlite ") + what + ": " + (db ? sqlite3_errmsg(db) : "?"));
}

struct Stmt {
    sqlite3_stmt* s = nullptr;
    sqlite3* db = nullptr;
    Stmt(sqlite3* d, const char* sql) : db(d) { check(sqlite3_prepare_v2(d, sql, -1, &s, nullptr), d, sql); }
    ~Stmt() { sqlite3_finalize(s); }
    Stmt(const Stmt&) = delete;
};

sqlite3* open_db(const std::filesystem::path& file) {
    sqlite3* db = nullptr;
    check(sqlite3_open_v2(file.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr),
          db, "open");
    sqlite3_busy_timeout(db, 10000);
    return db;
}

void exec(sqlite3* db, const char* sql) {
    check(sqlite3_exec(db, sql, nullptr, nullptr, nullptr), db, sql);
}

std::optional<std::string> get_value(Stmt& st, std::string_view key) {
    sqlite3_reset(st.s);
    sqlite3_bind_blob(st.s, 1, key.data(), static_cast<int>(key.size()), SQLITE_STATIC);
    int rc = sqlite3_step(st.s);
    std::optional<std::string> out;
    if (rc == SQLITE_ROW)
        out.emplace(static_cast<const char*>(sqlite3_column_blob(st.s, 0)), static_cast<std::size_t>(sqlite3_column_bytes(st.s, 0)));
    else
        check(rc, st.db, "get");
    sqlite3_reset(st.s);
    return out;
}

class SqliteEngine final : public Engine {
public:
    explicit SqliteEngine(const Options& options) : options_(options), file_(options.dir / "meta.db") {
        std::filesystem::create_directories(options.dir);
        db_ = open_db(file_);
        exec(db_, "PRAGMA journal_mode=WAL");
        exec(db_, "PRAGMA fullfsync=ON");            // macOS: a real flush, not just fsync()
        exec(db_, "PRAGMA checkpoint_fullfsync=ON");
        exec(db_, "PRAGMA cache_size=-262144");
        exec(db_, "CREATE TABLE IF NOT EXISTS kv(k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID");
        get_ = std::make_unique<Stmt>(db_, "SELECT v FROM kv WHERE k=?1");
        put_ = std::make_unique<Stmt>(db_, "INSERT OR REPLACE INTO kv(k,v) VALUES(?1,?2)");
        del_ = std::make_unique<Stmt>(db_, "DELETE FROM kv WHERE k=?1");
        begin_ = std::make_unique<Stmt>(db_, "BEGIN IMMEDIATE");
        commit_ = std::make_unique<Stmt>(db_, "COMMIT");
        save_ = std::make_unique<Stmt>(db_, "SAVEPOINT b");
        release_ = std::make_unique<Stmt>(db_, "RELEASE b");
        rollback_ = std::make_unique<Stmt>(db_, "ROLLBACK TO b");
    }

    ~SqliteEngine() override {
        get_.reset(); put_.reset(); del_.reset(); begin_.reset(); commit_.reset();
        save_.reset(); release_.reset(); rollback_.reset();
        sqlite3_close(db_);
    }

    std::string name() const override { return "sqlite"; }

    void apply_batch(const std::vector<const TxBody*>& bodies, std::vector<char>& ok, bool durable) override {
        const bool flush = durable && options_.sync;
        if (static_cast<int>(flush) != flush_) {  // the setting cannot change inside a transaction
            exec(db_, flush ? "PRAGMA synchronous=FULL" : "PRAGMA synchronous=OFF");
            flush_ = flush;
        }
        step(*begin_);
        Writer tx(*this);
        for (std::size_t i = 0; i < bodies.size(); ++i) {
            step(*save_);
            ok[i] = (*bodies[i])(tx) ? 1 : 0;
            if (!ok[i])
                step(*rollback_);
            step(*release_);
        }
        step(*commit_);
    }

    std::optional<std::string> read_get(std::string_view key) override { return reader().get(key); }

    std::size_t read_scan(std::string_view from, std::string_view to, std::size_t limit) override {
        Reader& r = reader();
        sqlite3_reset(r.scan.s);
        sqlite3_bind_blob(r.scan.s, 1, from.data(), static_cast<int>(from.size()), SQLITE_STATIC);
        sqlite3_bind_blob(r.scan.s, 2, to.data(), static_cast<int>(to.size()), SQLITE_STATIC);
        sqlite3_bind_int64(r.scan.s, 3, static_cast<sqlite3_int64>(limit));
        std::size_t n = 0;
        while (sqlite3_step(r.scan.s) == SQLITE_ROW)
            ++n;
        sqlite3_reset(r.scan.s);
        return n;
    }

    std::uint64_t disk_bytes() const override { return directory_bytes(options_.dir); }

private:
    void step(Stmt& st) {
        sqlite3_reset(st.s);
        check(sqlite3_step(st.s), db_, "step");
        sqlite3_reset(st.s);
    }

    class Writer final : public Tx {
    public:
        explicit Writer(SqliteEngine& e) : e_(e) {}
        std::optional<std::string> get(std::string_view key) override { return get_value(*e_.get_, key); }
        void put(std::string_view key, std::string_view value) override {
            Stmt& st = *e_.put_;
            sqlite3_reset(st.s);
            sqlite3_bind_blob(st.s, 1, key.data(), static_cast<int>(key.size()), SQLITE_STATIC);
            sqlite3_bind_blob(st.s, 2, value.data(), static_cast<int>(value.size()), SQLITE_STATIC);
            check(sqlite3_step(st.s), e_.db_, "put");
            sqlite3_reset(st.s);
        }
        void del(std::string_view key) override {
            Stmt& st = *e_.del_;
            sqlite3_reset(st.s);
            sqlite3_bind_blob(st.s, 1, key.data(), static_cast<int>(key.size()), SQLITE_STATIC);
            check(sqlite3_step(st.s), e_.db_, "del");
            sqlite3_reset(st.s);
        }

    private:
        SqliteEngine& e_;
    };

    struct Reader {
        sqlite3* db;
        Stmt get_stmt;
        Stmt scan;
        explicit Reader(const std::filesystem::path& file)
            : db(open_db(file)),
              get_stmt(db, "SELECT v FROM kv WHERE k=?1"),
              scan(db, "SELECT k FROM kv WHERE k>=?1 AND k<?2 ORDER BY k LIMIT ?3") {
            exec(db, "PRAGMA cache_size=-65536");
        }
        ~Reader() {
            sqlite3_finalize(get_stmt.s); get_stmt.s = nullptr;
            sqlite3_finalize(scan.s); scan.s = nullptr;
            sqlite3_close(db);
        }
        std::optional<std::string> get(std::string_view key) { return get_value(get_stmt, key); }
    };

    Reader& reader() {
        thread_local std::map<const SqliteEngine*, std::unique_ptr<Reader>> readers;
        auto& slot = readers[this];
        if (!slot)
            slot = std::make_unique<Reader>(file_);
        return *slot;
    }

    Options options_;
    std::filesystem::path file_;
    sqlite3* db_ = nullptr;
    int flush_ = -1;  // PRAGMA synchronous currently in force: 1 FULL, 0 OFF, -1 not set yet (the default is not FULL everywhere)
    std::unique_ptr<Stmt> get_, put_, del_, begin_, commit_, save_, release_, rollback_;
};

}  // namespace

std::unique_ptr<Engine> make_sqlite(const Options& options) {
    return std::make_unique<SqliteEngine>(options);
}

}  // namespace metabench
