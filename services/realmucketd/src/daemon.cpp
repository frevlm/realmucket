#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "rmk/core/chunk_file.hpp"
#include "rmk/local/chunk_store.hpp"
#include "rmk/realmucketd/realmucketd.hpp"
#include "rmk/rocksdb/rocks_stores.hpp"

namespace rmk::realmucketd {
namespace {

constexpr int kMaxMessageBytes = 32 << 20;

// An exclusive lock on <data_dir>/LOCK for as long as the daemon runs: two processes on one data
// directory would write the same chunk files and the same database.
class DirectoryLock {
public:
    static Result<DirectoryLock> acquire(const std::filesystem::path& data_dir) {
        std::error_code ec;
        std::filesystem::create_directories(data_dir, ec);
        if (ec) return fail(Errc::io_error, "create " + data_dir.string() + ": " + ec.message());
        const auto path = data_dir / "LOCK";
        int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (fd < 0) return fail(Errc::io_error, "open " + path.string() + ": " + std::strerror(errno));
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            const int err = errno;
            ::close(fd);
            return fail(err == EWOULDBLOCK ? Errc::unavailable : Errc::io_error,
                        err == EWOULDBLOCK ? data_dir.string() + " is in use by another process"
                                           : "lock " + path.string() + ": " + std::strerror(err));
        }
        return DirectoryLock(fd);
    }

    DirectoryLock(DirectoryLock&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    DirectoryLock& operator=(DirectoryLock&&) = delete;
    ~DirectoryLock() {
        if (fd_ >= 0) ::close(fd_);  // closing releases the lock
    }

private:
    explicit DirectoryLock(int fd) : fd_(fd) {}
    int fd_;
};

}  // namespace

struct Daemon::Impl {
    DirectoryLock lock;
    std::shared_ptr<rocks::Database> database;
    std::shared_ptr<local::ChunkStore> store;
    std::unique_ptr<gateway::Gateway> gateway;
    std::unique_ptr<grpc::Server> server;
    std::unique_ptr<Maintenance> maintenance;
    int port = 0;

    explicit Impl(DirectoryLock l) : lock(std::move(l)) {}

    ~Impl() {
        if (server) {
            server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds{30});
            server->Wait();
        }
        maintenance.reset();
        gateway.reset();
        store.reset();
        database.reset();
    }
};

Result<std::unique_ptr<Daemon>> Daemon::start(Options options, std::shared_ptr<crypto::KeyProvider> keys) {
    auto credentials = rpc::server_credentials(options.tls);
    if (!credentials)
        return fail(Errc::invalid_argument, "serving needs RMK_TLS_CERT and RMK_TLS_KEY (or RMK_INSECURE=true)");

    auto lock = DirectoryLock::acquire(options.data_dir);
    if (!lock)
        return std::unexpected(lock.error());

    auto store = local::ChunkStore::open({options.data_dir, options.chunk_capacity});
    if (!store)
        return std::unexpected(store.error());
    std::shared_ptr<local::ChunkStore> shared = std::move(*store);

    const auto meta_dir = options.meta_dir.empty() ? options.data_dir / "meta" : options.meta_dir;
    auto database = rocks::Database::open({meta_dir, options.sync_metadata});
    if (!database)
        return std::unexpected(database.error());
    // Chunk files without a database to explain them would be taken for garbage and deleted.
    // Without the database the objects cannot be read anyway (their Data Keys lived in it), but that
    // is for the operator to decide, not for a restart to do quietly.
    if ((*database)->created() && !shared->chunks().empty())
        return fail(Errc::invalid_argument,
                    "found " + std::to_string(shared->chunks().size()) + " chunk files in " + options.data_dir.string() +
                        " but no metadata database in " + meta_dir.string() +
                        ": refusing to start (restore the database, or move the chunk files away)");

    gateway::Dependencies deps;
    std::shared_ptr<MetaStore> meta = rocks::make_meta_store(*database, options.gateway.pending_upload_retention);
    deps.stores.meta = meta;
    deps.stores.config = rocks::make_config_store(*database);
    deps.keys = keys;
    deps.writer = shared;
    deps.reader = shared;
    deps.garbage = shared;
    auto gateway = gateway::Gateway::create(options.gateway, std::move(deps));
    if (!gateway)
        return std::unexpected(gateway.error());
    auto maintenance = Maintenance::create(options.maintenance, meta, shared, std::move(keys));
    if (!maintenance)
        return std::unexpected(maintenance.error());

    std::unique_ptr<Daemon> daemon(new Daemon());
    daemon->impl_ = std::make_unique<Impl>(std::move(*lock));
    daemon->impl_->database = std::move(*database);
    daemon->impl_->store = std::move(shared);
    daemon->impl_->gateway = std::move(*gateway);
    daemon->impl_->maintenance = std::move(*maintenance);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(options.listen, credentials, &daemon->impl_->port);
    builder.RegisterService(daemon->impl_->gateway->service());
    builder.SetMaxReceiveMessageSize(kMaxMessageBytes);
    builder.SetMaxSendMessageSize(kMaxMessageBytes);
    daemon->impl_->server = builder.BuildAndStart();
    if (!daemon->impl_->server || daemon->impl_->port == 0)
        return fail(Errc::unavailable, "cannot listen on " + options.listen);
    daemon->impl_->maintenance->start();
    return daemon;
}

Daemon::~Daemon() = default;

int Daemon::port() const {
    return impl_->port;
}

}  // namespace rmk::realmucketd
