#include <memory>
#include <string>

#include "rmk/memory_stores.hpp"
#include "rmk/rpc.hpp"

#ifdef RMK_HAVE_FDB
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "rmk/fdb_stores.hpp"
#endif

namespace rmk::rpc {
namespace {

#ifdef RMK_HAVE_FDB
// The FDB client rewrites its cluster file when coordinators change, so it must be writable;
// a mounted ConfigMap is not. Work on a private copy.
Result<std::string> writable_copy(const std::string& cluster_file) {
    std::ifstream in(cluster_file, std::ios::binary);
    if (!in)
        return fail(Errc::invalid_argument, "cannot read RMK_FDB_CLUSTER_FILE " + cluster_file);
    std::ostringstream content;
    content << in.rdbuf();

    std::error_code ec;
    auto dir = std::filesystem::temp_directory_path(ec);
    if (ec)
        dir = "/tmp";
    std::string path = (dir / "rmk-fdb-XXXXXX.cluster").string();
    int fd = ::mkstemps(path.data(), static_cast<int>(std::string_view(".cluster").size()));
    if (fd < 0)
        return fail(Errc::io_error, std::string("create cluster file copy: ") + std::strerror(errno));
    const std::string bytes = content.str();
    std::size_t done = 0;
    while (done < bytes.size()) {
        ssize_t w = ::write(fd, bytes.data() + done, bytes.size() - done);
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0) {
            ::close(fd);
            return fail(Errc::io_error, std::string("write cluster file copy: ") + std::strerror(errno));
        }
        done += static_cast<std::size_t>(w);
    }
    ::close(fd);
    return path;
}

Result<Stores> open_fdb(const EnvLookup& env, const std::string& cluster_file,
                        std::chrono::seconds pending_upload_retention) {
    std::string prefix = "rmk";
    if (auto s = env_string(env, "RMK_FDB_PREFIX", false, prefix); !s)
        return std::unexpected(s.error());
    auto copy = writable_copy(cluster_file);
    if (!copy)
        return std::unexpected(copy.error());
    auto db = fdb::open_database(*copy);
    if (!db)
        return std::unexpected(db.error());
    Stores stores;
    stores.meta = std::make_shared<fdb::FdbMetaStore>(*db, prefix, pending_upload_retention);
    stores.config = std::make_shared<fdb::FdbConfigStore>(*db, prefix);
    stores.cluster = std::make_shared<fdb::FdbClusterStore>(*db, prefix);
    return stores;
}
#endif

}  // namespace

Result<Stores> open_stores(const EnvLookup& env, std::chrono::seconds pending_upload_retention) {
    std::string cluster_file;
    if (auto s = env_string(env, "RMK_FDB_CLUSTER_FILE", false, cluster_file); !s)
        return std::unexpected(s.error());
    if (!cluster_file.empty()) {
#ifdef RMK_HAVE_FDB
        return open_fdb(env, cluster_file, pending_upload_retention);
#else
        return fail(Errc::invalid_argument, "RMK_FDB_CLUSTER_FILE is set but this build is without FoundationDB");
#endif
    }
    Stores stores;
    stores.meta = std::make_shared<MemoryMetaStore>(pending_upload_retention);
    stores.config = std::make_shared<MemoryConfigStore>();
    stores.cluster = std::make_shared<MemoryClusterStore>();
    return stores;
}

}  // namespace rmk::rpc
