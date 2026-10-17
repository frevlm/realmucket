#include <fcntl.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <list>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rmk/chunk_file.hpp"
#include "rmk/crc32c.hpp"
#include "rmk/datanode.hpp"
#include "rmk/ec.hpp"
#include "rmk/http.hpp"
#include "rmk/v1/datanode.grpc.pb.h"

namespace rmk::datanode {
namespace {

namespace pb = rmk::internal::v1;
namespace fs = std::filesystem;

constexpr std::size_t kMessageBytes = std::size_t{1} << 20;  // per streamed message
constexpr std::uint64_t kBlockBytes = std::uint64_t{4} << 20;  // per shard position (encode, rebuild, copy)
constexpr std::size_t kMaxOpenFiles = 256;
constexpr std::string_view kTempMarker = ".tmp.";

void log_line(const char* level, const std::string& message) {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
    ::gmtime_r(&now, &tm);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &tm);
    std::fprintf(stderr, "%s %s datanode %s\n", stamp, level, message.c_str());
}

std::chrono::system_clock::time_point io_deadline(std::uint64_t bytes) {
    return std::chrono::system_clock::now() + std::chrono::seconds(30) + std::chrono::seconds(bytes >> 20);
}

std::string file_name(const pb::FileRef& ref) {
    char hex[17];
    std::snprintf(hex, sizeof hex, "%016llx", static_cast<unsigned long long>(ref.chunk_id()));
    return ref.shard() ? std::string(hex) + ".s" + std::to_string(ref.shard_index()) : std::string(hex) + ".r";
}

pb::FileRef shard_ref(ChunkId id, std::uint32_t index) {
    pb::FileRef ref;
    ref.set_chunk_id(id);
    ref.set_shard(true);
    ref.set_shard_index(index);
    return ref;
}

grpc::Status to_grpc(const Error& error) {
    return rpc::to_status(error);
}

Status sync_dir(const fs::path& dir) {
    int fd = ::open(dir.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return fail(Errc::io_error, std::string("open data directory: ") + std::strerror(errno));
    Status s = ::fsync(fd) == 0 ? Status{} : fail(Errc::io_error, std::string("fsync data directory: ") + std::strerror(errno));
    ::close(fd);
    return s;
}

// ----- Client side of other Data Nodes (copy, encode, rebuild).

Result<pb::StatResponse> remote_stat(const std::shared_ptr<grpc::Channel>& channel, const pb::FileRef& ref) {
    auto stub = pb::DataNode::NewStub(channel);
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    pb::StatResponse response;
    if (grpc::Status status = stub->Stat(&ctx, ref, &response); !status.ok())
        return std::unexpected(rpc::from_status(status));
    return response;
}

Result<std::vector<std::byte>> remote_read(const std::shared_ptr<grpc::Channel>& channel, const pb::FileRef& ref,
                                           std::uint64_t offset, std::uint64_t length) {
    auto stub = pb::DataNode::NewStub(channel);
    grpc::ClientContext ctx;
    ctx.set_deadline(io_deadline(length));
    pb::ReadRequest request;
    *request.mutable_file() = ref;
    request.set_offset(offset);
    request.set_length(length);
    std::vector<std::byte> data;
    data.reserve(length);
    auto stream = stub->Read(&ctx, request);
    pb::ReadResponse response;
    while (stream->Read(&response)) {
        const std::string& piece = response.data();
        if (piece.size() > length - data.size()) {
            ctx.TryCancel();
            break;
        }
        const auto* p = reinterpret_cast<const std::byte*>(piece.data());
        data.insert(data.end(), p, p + piece.size());
    }
    grpc::Status status = stream->Finish();
    if (!status.ok() && status.error_code() != grpc::StatusCode::CANCELLED)
        return std::unexpected(rpc::from_status(status));
    if (data.size() != length)
        return fail(Errc::io_error, "short read of " + file_name(ref));
    return data;
}

// A streaming Write to one remote file, fed block by block.
class RemoteWriter {
public:
    RemoteWriter(const std::shared_ptr<grpc::Channel>& channel, const pb::FileRef& ref, std::uint64_t offset,
                 std::uint64_t total)
        : stub_(pb::DataNode::NewStub(channel)), name_(file_name(ref)) {
        ctx_.set_deadline(io_deadline(total));
        stream_ = stub_->Write(&ctx_, &response_);
        pb::WriteRequest request;
        *request.mutable_header()->mutable_file() = ref;
        request.mutable_header()->set_offset(offset);
        ok_ = stream_->Write(request);
    }

    RemoteWriter(const RemoteWriter&) = delete;
    RemoteWriter& operator=(const RemoteWriter&) = delete;

    // An unfinished stream is abandoned: the target keeps whatever arrived, which a retry replaces.
    ~RemoteWriter() {
        if (finished_)
            return;
        ctx_.TryCancel();
        (void)stream_->Finish();
    }

    bool send(std::span<const std::byte> bytes) {
        pb::WriteRequest request;
        for (std::size_t pos = 0; ok_ && pos < bytes.size(); pos += kMessageBytes) {
            auto piece = bytes.subspan(pos, std::min(kMessageBytes, bytes.size() - pos));
            request.set_data(reinterpret_cast<const char*>(piece.data()), piece.size());
            ok_ = stream_->Write(request);
        }
        crc_ = crc32c(bytes, crc_);
        length_ += bytes.size();
        return ok_;
    }

    // Completes the stream and checks that the remote end stored exactly what was sent.
    Status finish() {
        finished_ = true;
        if (ok_)
            stream_->WritesDone();
        grpc::Status status = stream_->Finish();
        if (!status.ok())
            return std::unexpected(rpc::from_status(status));
        if (response_.length() != length_ || response_.crc32c() != crc_)
            return fail(Errc::corruption, "remote copy of " + name_ + " does not match");
        return {};
    }

private:
    std::unique_ptr<pb::DataNode::Stub> stub_;
    std::string name_;
    grpc::ClientContext ctx_;
    pb::WriteResponse response_;
    std::unique_ptr<grpc::ClientWriter<pb::WriteRequest>> stream_;
    bool ok_ = false;
    bool finished_ = false;
    std::uint32_t crc_ = 0;
    std::uint64_t length_ = 0;
};

Status remote_create(const std::shared_ptr<grpc::Channel>& channel, const pb::FileRef& ref, std::uint64_t capacity,
                     std::uint32_t data_units, std::uint32_t parity_units) {
    auto stub = pb::DataNode::NewStub(channel);
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    pb::CreateChunkRequest request;
    *request.mutable_file() = ref;
    request.set_capacity(capacity);
    request.set_data_units(data_units);
    request.set_parity_units(parity_units);
    pb::Empty response;
    if (grpc::Status status = stub->CreateChunk(&ctx, request, &response); !status.ok())
        return std::unexpected(rpc::from_status(status));
    return {};
}

// ----- Local files: one mutex per file name, open handles kept in an LRU.

struct OpenFile {
    std::mutex mu;
    std::optional<ChunkFile> file;  // opened lazily, closed on eviction or delete
    int writers = 0;                // Write streams in progress
};

// Shard files are never written in place: CreateChunk prepares this staging file, a Write
// stream fills it, and only a complete, synced shard is renamed over the real name.
std::string staging_name(const std::string& name) {
    return name + std::string(kTempMarker) + "staged";
}

class FileTable {
public:
    explicit FileTable(fs::path dir) : dir_(std::move(dir)) {}

    const fs::path& dir() const { return dir_; }

    std::shared_ptr<OpenFile> get(const std::string& name) {
        std::lock_guard lock(mu_);
        if (auto it = entries_.find(name); it != entries_.end()) {
            lru_.splice(lru_.begin(), lru_, it->second.second);
            return it->second.first;
        }
        lru_.push_front(name);
        auto entry = std::make_shared<OpenFile>();
        entries_.emplace(name, std::pair{entry, lru_.begin()});
        // Evict idle entries only: an entry in use must stay the one mutex for its file.
        for (auto it = std::prev(lru_.end()); entries_.size() > kMaxOpenFiles && it != lru_.begin();) {
            auto victim = entries_.find(*it);
            auto prev = std::prev(it);
            if (victim->second.first.use_count() == 1) {
                entries_.erase(victim);
                lru_.erase(it);
            }
            it = prev;
        }
        return entry;
    }

private:
    fs::path dir_;
    std::mutex mu_;
    std::list<std::string> lru_;
    std::unordered_map<std::string, std::pair<std::shared_ptr<OpenFile>, std::list<std::string>::iterator>> entries_;
};

// Caller holds entry.mu.
Result<ChunkFile*> ensure_open(OpenFile& entry, const fs::path& path) {
    if (!entry.file) {
        auto file = ChunkFile::open(path);
        if (!file)
            return std::unexpected(file.error());
        entry.file = std::move(*file);
    }
    return &*entry.file;
}

}  // namespace

struct Node::Impl {
    class Service final : public pb::DataNode::Service {
    public:
        explicit Service(Impl& impl) : impl_(impl) {}

        grpc::Status CreateChunk(grpc::ServerContext* ctx, const pb::CreateChunkRequest* request, pb::Empty*) override {
            return serve(ctx, [&] { return impl_.create_chunk(*request); });
        }
        grpc::Status Stat(grpc::ServerContext* ctx, const pb::FileRef* request, pb::StatResponse* response) override {
            return serve(ctx, [&] { return impl_.stat(*request, *response); });
        }
        grpc::Status Write(grpc::ServerContext* ctx, grpc::ServerReader<pb::WriteRequest>* reader,
                           pb::WriteResponse* response) override {
            return serve(ctx, [&] { return impl_.write(*reader, *response); });
        }
        grpc::Status Read(grpc::ServerContext* ctx, const pb::ReadRequest* request,
                          grpc::ServerWriter<pb::ReadResponse>* writer) override {
            return serve(ctx, [&] { return impl_.read(*request, *writer); });
        }
        grpc::Status DeleteChunk(grpc::ServerContext* ctx, const pb::FileRef* request, pb::Empty*) override {
            return serve(ctx, [&] { return impl_.delete_file(*request); });
        }
        grpc::Status CopyChunkFrom(grpc::ServerContext* ctx, const pb::CopyChunkRequest* request, pb::Empty*) override {
            return serve(ctx, [&] { return impl_.copy_from(*request); });
        }
        grpc::Status EncodeChunk(grpc::ServerContext* ctx, const pb::EncodeChunkRequest* request, pb::Empty*) override {
            return serve(ctx, [&] { return impl_.encode(*request); });
        }
        grpc::Status RebuildShard(grpc::ServerContext* ctx, const pb::RebuildShardRequest* request, pb::Empty*) override {
            return serve(ctx, [&] { return impl_.rebuild(*request); });
        }

    private:
        // Over TLS only realmucket components may call: their client certificate must have been
        // verified against RMK_TLS_CA (§6 safety rule 4). A plaintext server exists only with
        // RMK_INSECURE=true.
        static Status authorize(const grpc::ServerContext& ctx) {
            auto auth = ctx.auth_context();
            if (!auth)
                return fail(Errc::unauthenticated, "no transport security context");
            auto type = auth->FindPropertyValues(GRPC_TRANSPORT_SECURITY_TYPE_PROPERTY_NAME);
            const bool tls = !type.empty() && std::string_view(type.front().data(), type.front().size()) ==
                                                  GRPC_SSL_TRANSPORT_SECURITY_TYPE;
            if (tls && !auth->IsPeerAuthenticated())
                return fail(Errc::unauthenticated, "a client certificate signed by the cluster CA is required");
            return {};
        }

        template <class F>
        static grpc::Status serve(grpc::ServerContext* ctx, F handler) {
            Status s = authorize(*ctx);
            if (s)
                s = handler();
            return s ? grpc::Status::OK : to_grpc(s.error());
        }

        Impl& impl_;
    };

    Options options;
    std::shared_ptr<ClusterStore> cluster;
    std::shared_ptr<rpc::Channels> channels;
    FileTable files;
    Service service{*this};
    std::atomic<std::uint64_t> temp_counter{0};
    metrics::Counter& written_bytes;
    metrics::Counter& read_bytes;

    std::mutex stop_mu;
    std::condition_variable stop_cv;
    bool stopping = false;
    std::thread heartbeats;

    Impl(Options o, std::shared_ptr<ClusterStore> c, std::shared_ptr<rpc::Channels> ch)
        : options(std::move(o)),
          cluster(std::move(c)),
          channels(std::move(ch)),
          files(options.data_dir),
          written_bytes(metrics::global().counter("rmk_datanode_written_bytes_total", "Bytes written to chunk files")),
          read_bytes(metrics::global().counter("rmk_datanode_read_bytes_total", "Bytes served from chunk files")) {}

    fs::path path_of(const std::string& name) const { return options.data_dir / name; }

    fs::path temp_path(const std::string& name) {
        return path_of(name + std::string(kTempMarker) + std::to_string(temp_counter++));
    }

    // Puts a fully written and synced temporary file in place of `name`.
    Status install(const std::string& name, const fs::path& temp, ChunkFile file) {
        auto entry = files.get(name);
        std::lock_guard lock(entry->mu);
        entry->file.reset();
        std::error_code ec;
        fs::rename(temp, path_of(name), ec);
        if (ec)
            return fail(Errc::io_error, "rename " + temp.string() + ": " + ec.message());
        if (auto s = sync_dir(options.data_dir); !s)
            return s;
        entry->file = std::move(file);
        return {};
    }

    Status create_chunk(const pb::CreateChunkRequest& request) {
        if (request.data_units() == 0 || request.data_units() + request.parity_units() > 255 ||
            request.file().shard_index() >= request.data_units() + request.parity_units())
            return fail(Errc::invalid_argument, "invalid redundancy for " + file_name(request.file()));
        ChunkHeader header;
        header.chunk_id = request.file().chunk_id();
        header.capacity = request.capacity();
        header.data_units = static_cast<std::uint8_t>(request.data_units());
        header.parity_units = static_cast<std::uint8_t>(request.parity_units());
        header.shard_index = request.file().shard() ? static_cast<std::uint8_t>(request.file().shard_index()) : 0;

        const std::string name = file_name(request.file());
        if (request.file().shard())
            return stage_shard(name, header);
        auto entry = files.get(name);
        std::lock_guard lock(entry->mu);
        auto created = ChunkFile::create(path_of(name), header);
        if (created) {
            entry->file = std::move(*created);
            return {};
        }
        if (created.error().code != Errc::already_exists)
            return std::unexpected(created.error());
        // Creating the same file twice is fine (the Controller may retry); a different one is not.
        auto existing = ensure_open(*entry, path_of(name));
        if (!existing)
            return std::unexpected(existing.error());
        if ((*existing)->header() != header)
            return fail(Errc::already_exists, name + " exists with a different header");
        return {};
    }

    // A fresh staging file for shard `name`. One still being written is left alone.
    Status stage_shard(const std::string& name, const ChunkHeader& header) {
        const std::string staged = staging_name(name);
        auto entry = files.get(staged);
        std::lock_guard lock(entry->mu);
        if (entry->writers > 0)
            return fail(Errc::aborted, name + " is being written by another stream");
        entry->file.reset();
        std::error_code ec;
        fs::remove(path_of(staged), ec);
        auto created = ChunkFile::create(path_of(staged), header);
        if (!created)
            return std::unexpected(created.error());
        entry->file = std::move(*created);
        return {};
    }

    Status stat(const pb::FileRef& ref, pb::StatResponse& response) {
        const std::string name = file_name(ref);
        auto entry = files.get(name);
        std::lock_guard lock(entry->mu);
        auto file = ensure_open(*entry, path_of(name));
        if (!file) {
            if (file.error().code == Errc::not_found) {
                response.set_exists(false);
                return {};
            }
            return std::unexpected(file.error());
        }
        response.set_exists(true);
        response.set_size((*file)->size());
        response.set_capacity((*file)->header().capacity);
        response.set_data_units((*file)->header().data_units);
        response.set_parity_units((*file)->header().parity_units);
        return {};
    }

    Status write(grpc::ServerReader<pb::WriteRequest>& reader, pb::WriteResponse& response) {
        pb::WriteRequest request;
        if (!reader.Read(&request) || !request.has_header())
            return fail(Errc::invalid_argument, "a write must start with its header");
        const bool shard = request.header().file().shard();
        const std::string final_name = file_name(request.header().file());
        const std::string name = shard ? staging_name(final_name) : final_name;
        const std::uint64_t offset = request.header().offset();
        auto entry = files.get(name);
        {
            std::lock_guard lock(entry->mu);
            ++entry->writers;
        }
        struct Done {
            OpenFile& entry;
            ~Done() {
                std::lock_guard lock(entry.mu);
                --entry.writers;
            }
        } done{*entry};

        std::uint64_t pos = offset;
        std::uint32_t crc = 0;
        {
            std::lock_guard lock(entry->mu);
            auto file = ensure_open(*entry, path_of(name));
            if (!file)
                return std::unexpected(file.error());
            if (offset < (*file)->size())
                return fail(Errc::invalid_argument, "write at " + std::to_string(offset) + " would overwrite " + name);
            if (offset > (*file)->header().capacity)
                return fail(Errc::no_space, name + " is full");
        }
        while (reader.Read(&request)) {
            if (request.item_case() != pb::WriteRequest::kData)
                return fail(Errc::invalid_argument, "only data may follow the write header");
            const std::string& data = request.data();
            std::span bytes(reinterpret_cast<const std::byte*>(data.data()), data.size());
            std::lock_guard lock(entry->mu);
            auto file = ensure_open(*entry, path_of(name));
            if (!file)
                return std::unexpected(file.error());
            if (auto written = (*file)->write_at(pos, bytes); !written)
                return std::unexpected(written.error());
            crc = crc32c(bytes, crc);
            pos += bytes.size();
        }
        {
            std::lock_guard lock(entry->mu);
            auto file = ensure_open(*entry, path_of(name));
            if (!file)
                return std::unexpected(file.error());
            if (auto s = (*file)->sync(); !s)
                return s;
            if (shard) {
                if ((*file)->size() != (*file)->header().capacity)
                    return fail(Errc::invalid_argument, "incomplete shard " + final_name);
                ChunkFile complete = std::move(**file);
                entry->file.reset();
                if (auto s = install(final_name, path_of(name), std::move(complete)); !s)
                    return s;
            }
        }
        written_bytes.inc(static_cast<double>(pos - offset));
        response.set_offset(offset);
        response.set_length(pos - offset);
        response.set_crc32c(crc);
        return {};
    }

    Status read(const pb::ReadRequest& request, grpc::ServerWriter<pb::ReadResponse>& writer) {
        if (request.offset() < kChunkHeaderSize)
            return fail(Errc::invalid_argument, "reads start at offset 64 or later");
        const std::string name = file_name(request.file());
        auto entry = files.get(name);
        pb::ReadResponse response;
        for (std::uint64_t done = 0; done < request.length();) {
            const std::uint64_t n = std::min<std::uint64_t>(kMessageBytes, request.length() - done);
            Result<std::vector<std::byte>> data;
            {
                std::lock_guard lock(entry->mu);
                auto file = ensure_open(*entry, path_of(name));
                if (!file)
                    return std::unexpected(file.error());
                if (request.offset() > (*file)->size() || request.length() > (*file)->size() - request.offset())
                    return fail(Errc::invalid_argument, "range beyond the end of " + name);
                data = (*file)->read_range(request.offset() + done, n);
            }
            if (!data)
                return std::unexpected(data.error());
            response.set_data(reinterpret_cast<const char*>(data->data()), data->size());
            if (!writer.Write(response))
                return fail(Errc::unavailable, "reader went away");
            done += n;
        }
        read_bytes.inc(static_cast<double>(request.length()));
        return {};
    }

    Status delete_file(const pb::FileRef& ref) {
        const std::string name = file_name(ref);
        auto entry = files.get(name);
        std::lock_guard lock(entry->mu);
        entry->file.reset();
        std::error_code ec;
        const bool removed = fs::remove(path_of(name), ec);
        if (ec)
            return fail(Errc::io_error, "delete " + name + ": " + ec.message());
        // Without this a crash could bring the file back as an orphan.
        return removed ? sync_dir(options.data_dir) : Status{};
    }

    Status copy_from(const pb::CopyChunkRequest& request) {
        const pb::FileRef& ref = request.file();
        const std::string name = file_name(ref);
        auto source = channels->get(request.source_address());
        auto st = remote_stat(source, ref);
        if (!st)
            return std::unexpected(st.error());
        if (!st->exists())
            return fail(Errc::not_found, name + " does not exist on " + request.source_address());
        if (auto s = check_source(ref, *st); !s)
            return fail(s.error().code, request.source_address() + ": " + s.error().message);

        ChunkHeader header;
        header.chunk_id = ref.chunk_id();
        header.capacity = st->capacity();
        header.data_units = static_cast<std::uint8_t>(st->data_units());
        header.parity_units = static_cast<std::uint8_t>(st->parity_units());
        header.shard_index = ref.shard() ? static_cast<std::uint8_t>(ref.shard_index()) : 0;
        const fs::path temp = temp_path(name);
        auto file = ChunkFile::create(temp, header);
        if (!file)
            return std::unexpected(file.error());
        Status copied = [&]() -> Status {
            for (std::uint64_t pos = kChunkHeaderSize; pos < st->size(); pos += kBlockBytes) {
                auto data = remote_read(source, ref, pos, std::min(kBlockBytes, st->size() - pos));
                if (!data)
                    return std::unexpected(data.error());
                if (auto w = file->write_at(pos, *data); !w)
                    return std::unexpected(w.error());
            }
            if (auto again = remote_stat(source, ref); !again || again->size() != st->size())
                return fail(Errc::aborted, name + " changed on the source while it was copied");
            return file->sync();
        }();
        if (copied)
            copied = install(name, temp, std::move(*file));
        if (!copied) {
            std::error_code ec;
            fs::remove(temp, ec);
            return copied;
        }
        log_line("info", "copied " + name + " (" + std::to_string(st->size()) + " bytes) from " +
                             request.source_address());
        return {};
    }

    // What can be checked about a copy source without per-block checksums: the file must hold
    // every byte the Chunk's recorded size says it has (a replica at least ChunkInfo.size, a
    // shard exactly its shard length) and match the recorded layout.
    Status check_source(const pb::FileRef& ref, const pb::StatResponse& st) {
        auto info = cluster->get_chunk(ref.chunk_id());
        if (!info)
            return std::unexpected(info.error());
        if (ref.shard()) {
            if (info->state != ChunkState::erasure_coded || st.data_units() != info->data_units ||
                st.parity_units() != info->parity_units)
                return fail(Errc::precondition_failed, "shard does not match the chunk's layout");
            const std::uint64_t expected = kChunkHeaderSize + ec::shard_length(info->size, info->data_units);
            if (st.size() != expected)
                return fail(Errc::corruption, "shard holds " + std::to_string(st.size()) + " bytes, expected " +
                                                  std::to_string(expected));
            return {};
        }
        if (st.size() < info->size)
            return fail(Errc::corruption, "replica holds " + std::to_string(st.size()) + " bytes, chunk has " +
                                              std::to_string(info->size));
        return {};
    }

    // Reads logical bytes [offset, offset + out.size()) of a sealed local replica, header
    // included; bytes past `logical_size` are zero (the last shard's padding).
    Status read_logical(OpenFile& entry, const fs::path& path, std::uint64_t logical_size, std::uint64_t offset,
                        std::span<std::byte> out) {
        std::ranges::fill(out, std::byte{0});
        if (offset >= logical_size)
            return {};
        const std::uint64_t end = std::min<std::uint64_t>(offset + out.size(), logical_size);
        if (offset < kChunkHeaderSize) {
            int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
            if (fd < 0)
                return fail(Errc::io_error, "open " + path.string() + ": " + std::strerror(errno));
            std::array<std::byte, kChunkHeaderSize> head{};
            ssize_t got = ::pread(fd, head.data(), head.size(), 0);
            ::close(fd);
            if (got != static_cast<ssize_t>(head.size()))
                return fail(Errc::io_error, "read header of " + path.string());
            const std::uint64_t stop = std::min<std::uint64_t>(end, kChunkHeaderSize);
            std::copy(head.begin() + static_cast<std::ptrdiff_t>(offset), head.begin() + static_cast<std::ptrdiff_t>(stop),
                      out.begin());
        }
        const std::uint64_t from = std::max<std::uint64_t>(offset, kChunkHeaderSize);
        if (from >= end)
            return {};
        std::lock_guard lock(entry.mu);
        auto file = ensure_open(entry, path);
        if (!file)
            return std::unexpected(file.error());
        auto data = (*file)->read_range(from, end - from);
        if (!data)
            return std::unexpected(data.error());
        std::ranges::copy(*data, out.begin() + static_cast<std::ptrdiff_t>(from - offset));
        return {};
    }

    Status encode(const pb::EncodeChunkRequest& request) {
        const std::uint32_t k = request.data_units();
        const std::uint32_t m = request.parity_units();
        if (k == 0 || k + m > 255 || static_cast<std::uint32_t>(request.target_addresses_size()) != k + m)
            return fail(Errc::invalid_argument, "EncodeChunk needs data_units + parity_units target addresses");
        auto codec = ec::Codec::create(static_cast<std::uint8_t>(k), static_cast<std::uint8_t>(m));
        if (!codec)
            return std::unexpected(codec.error());
        // The logical size is the sealed size recorded by the writer; anything a failed write
        // left beyond it on this replica is not part of the Chunk.
        auto info = cluster->get_chunk(request.chunk_id());
        if (!info)
            return std::unexpected(info.error());
        if (info->state != ChunkState::sealed)
            return fail(Errc::precondition_failed, "chunk " + std::to_string(info->id) + " is not sealed");
        const std::uint64_t logical = info->size;

        pb::FileRef replica;
        replica.set_chunk_id(request.chunk_id());
        const std::string name = file_name(replica);
        auto entry = files.get(name);
        {
            std::lock_guard lock(entry->mu);
            auto file = ensure_open(*entry, path_of(name));
            if (!file)
                return std::unexpected(file.error());
            if ((*file)->size() < logical)
                return fail(Errc::corruption, name + " is shorter than the sealed chunk");
        }

        const std::uint64_t shard_len = ec::shard_length(logical, static_cast<std::uint8_t>(k));
        // Each target stages its shard and renames it into place only once complete.
        for (std::uint32_t i = 0; i < k + m; ++i) {
            const std::string& address = request.target_addresses(static_cast<int>(i));
            Status s = remote_create(channels->get(address), shard_ref(request.chunk_id(), i),
                                     kChunkHeaderSize + shard_len, k, m);
            if (!s)
                return fail(s.error().code, "shard " + std::to_string(i) + " on " + address + ": " + s.error().message);
        }
        std::vector<std::unique_ptr<RemoteWriter>> targets;
        for (std::uint32_t i = 0; i < k + m; ++i)
            targets.push_back(std::make_unique<RemoteWriter>(channels->get(request.target_addresses(static_cast<int>(i))),
                                                             shard_ref(request.chunk_id(), i), kChunkHeaderSize,
                                                             shard_len));

        std::vector<std::vector<std::byte>> blocks(k + m);
        for (std::uint64_t pos = 0; pos < shard_len; pos += kBlockBytes) {
            const std::uint64_t len = std::min(kBlockBytes, shard_len - pos);
            std::vector<std::span<const std::byte>> data;
            std::vector<std::span<std::byte>> parity;
            for (std::uint32_t i = 0; i < k + m; ++i) {
                blocks[i].resize(len);
                if (i < k) {
                    if (auto s = read_logical(*entry, path_of(name), logical, i * shard_len + pos, blocks[i]); !s)
                        return s;
                    data.emplace_back(blocks[i]);
                } else {
                    parity.emplace_back(blocks[i]);
                }
            }
            codec->encode(data, parity);
            for (std::uint32_t i = 0; i < k + m; ++i)
                if (!targets[i]->send(blocks[i]))
                    return fail(Errc::unavailable, "shard target " + request.target_addresses(static_cast<int>(i)) +
                                                       " stopped accepting data");
        }
        for (std::uint32_t i = 0; i < k + m; ++i)
            if (auto s = targets[i]->finish(); !s)
                return fail(s.error().code, "shard " + std::to_string(i) + " on " +
                                                request.target_addresses(static_cast<int>(i)) + ": " + s.error().message);
        log_line("info", "encoded chunk " + std::to_string(request.chunk_id()) + " as " + std::to_string(k) + "+" +
                             std::to_string(m));
        return {};
    }

    Status rebuild(const pb::RebuildShardRequest& request) {
        const std::uint32_t k = request.data_units();
        const std::uint32_t m = request.parity_units();
        const std::uint32_t want = request.shard_index();
        if (k == 0 || k + m > 255 || static_cast<std::uint32_t>(request.source_addresses_size()) != k + m || want >= k + m)
            return fail(Errc::invalid_argument, "RebuildShard needs one source address per shard");
        auto codec = ec::Codec::create(static_cast<std::uint8_t>(k), static_cast<std::uint8_t>(m));
        if (!codec)
            return std::unexpected(codec.error());
        const std::uint64_t shard_len = ec::shard_length(request.logical_size(), static_cast<std::uint8_t>(k));

        ChunkHeader header;
        header.chunk_id = request.chunk_id();
        header.capacity = kChunkHeaderSize + shard_len;
        header.data_units = static_cast<std::uint8_t>(k);
        header.parity_units = static_cast<std::uint8_t>(m);
        header.shard_index = static_cast<std::uint8_t>(want);
        const std::string name = file_name(shard_ref(request.chunk_id(), want));
        const fs::path temp = temp_path(name);
        auto file = ChunkFile::create(temp, header);
        if (!file)
            return std::unexpected(file.error());

        std::vector<bool> usable(k + m);
        for (std::uint32_t i = 0; i < k + m; ++i)
            usable[i] = i != want && !request.source_addresses(static_cast<int>(i)).empty();
        Status rebuilt = [&]() -> Status {
            std::vector<std::vector<std::byte>> shards(k + m);
            for (std::uint64_t pos = 0; pos < shard_len; pos += kBlockBytes) {
                const std::uint64_t len = std::min(kBlockBytes, shard_len - pos);
                std::vector<std::uint32_t> have;
                // Reads sources until `count` blocks are in hand (or none are left).
                auto gather = [&](std::size_t count) {
                    for (std::uint32_t i = 0; i < k + m && have.size() < count; ++i) {
                        if (!usable[i] || std::ranges::find(have, i) != have.end())
                            continue;
                        auto data = remote_read(channels->get(request.source_addresses(static_cast<int>(i))),
                                                shard_ref(request.chunk_id(), i), kChunkHeaderSize + pos, len);
                        if (!data) {
                            usable[i] = false;
                            continue;
                        }
                        shards[i] = std::move(*data);
                        have.push_back(i);
                    }
                };
                // One spare source verifies the result; on a mismatch, find the bad source.
                gather(std::size_t{k} + 1);
                auto block = reconstruct_block(*codec, shards, have, want, std::nullopt, len);
                if (!block && block.error().code == Errc::corruption) {
                    gather(std::size_t{k} + m);
                    for (std::uint32_t bad : have)
                        if (have.size() > k + 1 && (block = reconstruct_block(*codec, shards, have, want, bad, len)))
                            break;
                }
                if (!block)
                    return std::unexpected(block.error());
                if (auto w = file->write_at(kChunkHeaderSize + pos, *block); !w)
                    return std::unexpected(w.error());
            }
            return file->sync();
        }();
        if (rebuilt)
            rebuilt = install(name, temp, std::move(*file));
        if (!rebuilt) {
            std::error_code ec;
            fs::remove(temp, ec);
            return fail(rebuilt.error().code, "rebuild " + name + ": " + rebuilt.error().message);
        }
        log_line("info", "rebuilt " + name);
        return {};
    }

    // Rebuilds shard `want` from the first k shards in `have` (leaving out `skip`) and accepts
    // it only if every further shard in `have` equals its rebuilt value. With exactly k sources
    // there is nothing to compare against and the result is taken as is.
    static Result<std::vector<std::byte>> reconstruct_block(const ec::Codec& codec,
                                                            const std::vector<std::vector<std::byte>>& shards,
                                                            const std::vector<std::uint32_t>& have, std::uint32_t want,
                                                            std::optional<std::uint32_t> skip, std::uint64_t len) {
        const std::size_t k = codec.data_units();
        std::vector<std::uint32_t> use;
        for (std::uint32_t i : have)
            if (i != skip)
                use.push_back(i);
        if (use.size() < k)
            return fail(Errc::unavailable, "only " + std::to_string(use.size()) + " of " + std::to_string(k) +
                                               " shards readable");
        std::vector<std::vector<std::byte>> work(shards.size(), std::vector<std::byte>(len));
        std::array<bool, 256> present{};
        for (std::size_t j = 0; j < k; ++j) {
            work[use[j]] = shards[use[j]];
            present[use[j]] = true;
        }
        std::vector<std::span<std::byte>> spans(work.begin(), work.end());
        if (auto s = codec.reconstruct(spans, std::span<const bool>(present.data(), shards.size())); !s)
            return std::unexpected(s.error());
        for (std::size_t j = k; j < use.size(); ++j)
            if (work[use[j]] != shards[use[j]])
                return fail(Errc::corruption, "source shards disagree");
        return std::move(work[want]);
    }

    Status heartbeat() {
        NodeInfo info = options.identity;
        if (info.capacity_bytes == 0) {
            struct statvfs vfs {};
            if (::statvfs(options.data_dir.c_str(), &vfs) != 0)
                return fail(Errc::io_error, std::string("statvfs: ") + std::strerror(errno));
            info.capacity_bytes = static_cast<std::uint64_t>(vfs.f_blocks) * vfs.f_frsize;
        }
        std::error_code ec;
        for (const auto& item : fs::directory_iterator(options.data_dir, ec)) {
            std::error_code size_ec;
            if (item.is_regular_file(size_ec))
                info.used_bytes += item.file_size(size_ec);
        }
        return cluster->heartbeat(info, Clock::now());
    }

    void heartbeat_loop() {
        std::unique_lock lock(stop_mu);
        while (!stop_cv.wait_for(lock, options.heartbeat_interval, [&] { return stopping; })) {
            lock.unlock();
            if (auto s = heartbeat(); !s)
                log_line("warn", "heartbeat failed: " + s.error().message);
            lock.lock();
        }
    }
};

Result<std::unique_ptr<Node>> Node::create(Options options, std::shared_ptr<ClusterStore> cluster,
                                           std::shared_ptr<rpc::Channels> channels) {
    if (options.identity.id.empty() || options.identity.address.empty() || options.data_dir.empty())
        return fail(Errc::invalid_argument, "node id, address and data directory are required");
    std::error_code ec;
    fs::create_directories(options.data_dir, ec);
    if (ec || !fs::is_directory(options.data_dir))
        return fail(Errc::io_error, "data directory " + options.data_dir.string() + " is unusable");
    // Leftovers of copies or rebuilds interrupted by a crash.
    for (const auto& item : fs::directory_iterator(options.data_dir, ec))
        if (item.path().filename().string().find(kTempMarker) != std::string::npos) {
            std::error_code remove_ec;
            fs::remove(item.path(), remove_ec);
        }

    std::unique_ptr<Node> node(new Node());
    node->impl_ = std::make_unique<Impl>(std::move(options), std::move(cluster), std::move(channels));
    return node;
}

Node::~Node() {
    if (!impl_)
        return;
    {
        std::lock_guard lock(impl_->stop_mu);
        impl_->stopping = true;
    }
    impl_->stop_cv.notify_all();
    if (impl_->heartbeats.joinable())
        impl_->heartbeats.join();
}

grpc::Service* Node::service() {
    return &impl_->service;
}

Status Node::heartbeat() {
    return impl_->heartbeat();
}

void Node::start_heartbeats() {
    if (impl_->heartbeats.joinable())
        return;
    impl_->heartbeats = std::thread([impl = impl_.get()] { impl->heartbeat_loop(); });
}

}  // namespace rmk::datanode
