#include "rmk/core/chunk_file.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include "rmk/core/crc32c.hpp"

namespace rmk {
namespace {

// Header layout, little-endian:
//   [0, 8)   magic "RMKCHUNK"
//   [8, 12)  format_version
//   [12]     data_units
//   [13]     parity_units
//   [14]     shard_index
//   [15]     reserved, zero
//   [16, 24) chunk_id
//   [24, 32) capacity
//   [32, 36) crc32c of [0, 32)
//   [36, 64) reserved, zero
constexpr std::array<char, 8> kMagic{'R', 'M', 'K', 'C', 'H', 'U', 'N', 'K'};
constexpr std::size_t kCrcOffset = 32;

using HeaderBytes = std::array<std::byte, kChunkHeaderSize>;

template <class T>
void put_le(HeaderBytes& out, std::size_t offset, T value) {
    for (std::size_t i = 0; i < sizeof(T); ++i)
        out[offset + i] = static_cast<std::byte>((static_cast<std::uint64_t>(value) >> (8 * i)) & 0xFF);
}

template <class T>
T get_le(const HeaderBytes& in, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i)
        value |= std::to_integer<std::uint64_t>(in[offset + i]) << (8 * i);
    return static_cast<T>(value);
}

HeaderBytes encode(const ChunkHeader& h) {
    HeaderBytes out{};
    std::memcpy(out.data(), kMagic.data(), kMagic.size());
    put_le<std::uint32_t>(out, 8, h.format_version);
    put_le<std::uint8_t>(out, 12, h.data_units);
    put_le<std::uint8_t>(out, 13, h.parity_units);
    put_le<std::uint8_t>(out, 14, h.shard_index);
    put_le<std::uint64_t>(out, 16, h.chunk_id);
    put_le<std::uint64_t>(out, 24, h.capacity);
    put_le<std::uint32_t>(out, kCrcOffset, crc32c(std::span(out).first(kCrcOffset)));
    return out;
}

Result<ChunkHeader> decode(const HeaderBytes& in) {
    if (std::memcmp(in.data(), kMagic.data(), kMagic.size()) != 0)
        return fail(Errc::corruption, "not a chunk file");
    if (get_le<std::uint32_t>(in, kCrcOffset) != crc32c(std::span(in).first(kCrcOffset)))
        return fail(Errc::corruption, "chunk header checksum mismatch");

    ChunkHeader h;
    h.format_version = get_le<std::uint32_t>(in, 8);
    h.data_units = get_le<std::uint8_t>(in, 12);
    h.parity_units = get_le<std::uint8_t>(in, 13);
    h.shard_index = get_le<std::uint8_t>(in, 14);
    h.chunk_id = get_le<std::uint64_t>(in, 16);
    h.capacity = get_le<std::uint64_t>(in, 24);
    // Every format up to the current one stays readable forever (ADR 0015).
    if (h.format_version == 0 || h.format_version > kChunkFormatVersion)
        return fail(Errc::corruption, "unsupported chunk format version " + std::to_string(h.format_version));
    return h;
}

std::unexpected<Error> io_fail(const char* what) {
    return fail(Errc::io_error, std::string(what) + ": " + std::strerror(errno));
}

Status write_all(int fd, const std::byte* p, std::size_t n, std::uint64_t offset) {
    while (n > 0) {
        ssize_t w = ::pwrite(fd, p, n, static_cast<off_t>(offset));
        if (w < 0) {
            if (errno == EINTR)
                continue;
            return io_fail("pwrite");
        }
        p += w;
        n -= static_cast<std::size_t>(w);
        offset += static_cast<std::uint64_t>(w);
    }
    return {};
}

Status read_all(int fd, std::byte* p, std::size_t n, std::uint64_t offset) {
    while (n > 0) {
        ssize_t r = ::pread(fd, p, n, static_cast<off_t>(offset));
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return io_fail("pread");
        }
        if (r == 0)
            return fail(Errc::corruption, "unexpected end of chunk file");
        p += r;
        n -= static_cast<std::size_t>(r);
        offset += static_cast<std::uint64_t>(r);
    }
    return {};
}

Status sync_fd(int fd) {
#ifdef __APPLE__
    if (::fcntl(fd, F_FULLFSYNC) == 0)
        return {};
#else
    if (::fdatasync(fd) == 0)
        return {};
#endif
    return io_fail("sync");
}

// A newly created file only survives a crash once its directory entry is durable too.
Status sync_parent_dir(const std::filesystem::path& path) {
    auto dir = path.parent_path();
    int fd = ::open(dir.empty() ? "." : dir.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return io_fail("open parent directory");
    Status s = ::fsync(fd) == 0 ? Status{} : io_fail("fsync parent directory");
    ::close(fd);
    return s;
}

}  // namespace

ChunkFile::ChunkFile(int fd, ChunkHeader header, std::uint64_t size)
    : fd_(fd), header_(header), size_(size) {}

ChunkFile::ChunkFile(ChunkFile&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)), header_(other.header_), size_(other.size_) {}

ChunkFile& ChunkFile::operator=(ChunkFile&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0)
            ::close(fd_);
        fd_ = std::exchange(other.fd_, -1);
        header_ = other.header_;
        size_ = other.size_;
    }
    return *this;
}

ChunkFile::~ChunkFile() {
    if (fd_ >= 0)
        ::close(fd_);
}

Result<ChunkFile> ChunkFile::create(const std::filesystem::path& path, const ChunkHeader& header) {
    if (header.capacity <= kChunkHeaderSize || header.data_units == 0)
        return fail(Errc::invalid_argument, "invalid chunk header");

    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0)
        return errno == EEXIST ? fail(Errc::already_exists, path.string()) : io_fail("create chunk");
    ChunkFile file(fd, header, kChunkHeaderSize);

    HeaderBytes bytes = encode(header);
    if (auto s = write_all(fd, bytes.data(), bytes.size(), 0); !s)
        return std::unexpected(s.error());
    if (auto s = file.sync(); !s)
        return std::unexpected(s.error());
    if (auto s = sync_parent_dir(path); !s)
        return std::unexpected(s.error());
    return file;
}

Result<ChunkFile> ChunkFile::open(const std::filesystem::path& path) {
    int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0)
        return errno == ENOENT ? fail(Errc::not_found, path.string()) : io_fail("open chunk");
    ChunkFile file(fd, ChunkHeader{}, 0);

    HeaderBytes bytes{};
    if (auto s = read_all(fd, bytes.data(), bytes.size(), 0); !s)
        return std::unexpected(s.error());
    auto header = decode(bytes);
    if (!header)
        return std::unexpected(header.error());

    struct stat st {};
    if (::fstat(fd, &st) != 0)
        return io_fail("fstat chunk");
    auto size = static_cast<std::uint64_t>(st.st_size);
    if (size > header->capacity)
        return fail(Errc::corruption, "chunk file larger than its capacity");

    file.header_ = *header;
    file.size_ = size;
    return file;
}

Result<Extent> ChunkFile::append(std::span<const std::byte> data) {
    return write_at(size_, data);
}

Result<Extent> ChunkFile::write_at(std::uint64_t offset, std::span<const std::byte> data) {
    if (offset < size_)
        return fail(Errc::invalid_argument, "write at " + std::to_string(offset) + " would overwrite chunk " +
                                                std::to_string(header_.chunk_id) + " (end " + std::to_string(size_) + ")");
    if (offset > header_.capacity || data.size() > header_.capacity - offset)
        return fail(Errc::no_space, "chunk " + std::to_string(header_.chunk_id) + " is full");

    Extent extent{header_.chunk_id, offset, data.size(), crc32c(data)};
    if (auto s = write_all(fd_, data.data(), data.size(), offset); !s)
        return std::unexpected(s.error());
    size_ = offset + data.size();
    return extent;
}

Result<std::vector<std::byte>> ChunkFile::read(const Extent& extent) const {
    if (extent.chunk_id != header_.chunk_id)
        return fail(Errc::invalid_argument, "extent belongs to chunk " + std::to_string(extent.chunk_id));
    auto data = read_range(extent.offset, extent.length);
    if (!data)
        return data;
    if (crc32c(*data) != extent.crc32c)
        return fail(Errc::corruption, "extent checksum mismatch in chunk " + std::to_string(header_.chunk_id));
    return data;
}

Result<std::vector<std::byte>> ChunkFile::read_range(std::uint64_t offset, std::uint64_t length) const {
    if (offset < kChunkHeaderSize || offset > size_ || length > size_ - offset)
        return fail(Errc::invalid_argument, "range outside chunk " + std::to_string(header_.chunk_id));
    std::vector<std::byte> data(length);
    if (auto s = read_all(fd_, data.data(), data.size(), offset); !s)
        return std::unexpected(s.error());
    return data;
}

Status ChunkFile::sync() {
    return sync_fd(fd_);
}

}  // namespace rmk
