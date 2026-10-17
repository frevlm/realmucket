#include "rmk/core/chunk_file.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

namespace {

namespace fs = std::filesystem;

std::span<const std::byte> bytes(std::string_view s) {
    return std::as_bytes(std::span(s.data(), s.size()));
}

std::string text(const std::vector<std::byte>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

class ChunkFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("rmk-chunk-" + std::to_string(::getpid()) + "-" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fs::remove_all(dir_);
        fs::create_directories(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }

    fs::path path() const { return dir_ / "chunk"; }
    rmk::ChunkHeader header(std::uint64_t capacity = 4096) const {
        return rmk::ChunkHeader{.chunk_id = 42, .capacity = capacity};
    }

    fs::path dir_;
};

TEST_F(ChunkFileTest, AppendedDataReadsBack) {
    auto chunk = rmk::ChunkFile::create(path(), header());
    ASSERT_TRUE(chunk);

    auto a = chunk->append(bytes("hello"));
    auto b = chunk->append(bytes("world"));
    ASSERT_TRUE(a && b);
    EXPECT_EQ(a->offset, rmk::kChunkHeaderSize);
    EXPECT_EQ(b->offset, rmk::kChunkHeaderSize + 5);

    auto read = chunk->read(*b);
    ASSERT_TRUE(read);
    EXPECT_EQ(text(*read), "world");
}

TEST_F(ChunkFileTest, ReopenRestoresHeaderAndContinuesAppending) {
    rmk::Extent first;
    {
        auto chunk = rmk::ChunkFile::create(path(), header());
        ASSERT_TRUE(chunk);
        first = *chunk->append(bytes("first"));
        ASSERT_TRUE(chunk->sync());
    }

    auto chunk = rmk::ChunkFile::open(path());
    ASSERT_TRUE(chunk);
    EXPECT_EQ(chunk->header(), header());

    auto second = chunk->append(bytes("second"));
    ASSERT_TRUE(second);
    EXPECT_EQ(second->offset, first.offset + first.length);
    EXPECT_EQ(text(*chunk->read(first)), "first");
}

TEST_F(ChunkFileTest, DetectsCorruptedData) {
    rmk::Extent extent;
    {
        auto chunk = rmk::ChunkFile::create(path(), header());
        ASSERT_TRUE(chunk);
        extent = *chunk->append(bytes("payload"));
        ASSERT_TRUE(chunk->sync());
    }
    {
        std::fstream f(path(), std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(static_cast<std::streamoff>(extent.offset));
        f.put('X');
    }

    auto chunk = rmk::ChunkFile::open(path());
    ASSERT_TRUE(chunk);
    auto read = chunk->read(extent);
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error().code, rmk::Errc::corruption);
}

TEST_F(ChunkFileTest, RejectsAppendBeyondCapacity) {
    auto chunk = rmk::ChunkFile::create(path(), header(rmk::kChunkHeaderSize + 4));
    ASSERT_TRUE(chunk);
    ASSERT_TRUE(chunk->append(bytes("1234")));

    auto overflow = chunk->append(bytes("5"));
    ASSERT_FALSE(overflow);
    EXPECT_EQ(overflow.error().code, rmk::Errc::no_space);
}

TEST_F(ChunkFileTest, WriteAtLeavesHoleAndRefusesToOverwrite) {
    auto chunk = rmk::ChunkFile::create(path(), header());
    ASSERT_TRUE(chunk);
    ASSERT_TRUE(chunk->append(bytes("abc")));

    auto far = chunk->write_at(rmk::kChunkHeaderSize + 100, bytes("xyz"));
    ASSERT_TRUE(far);
    EXPECT_EQ(chunk->size(), rmk::kChunkHeaderSize + 103);
    EXPECT_EQ(text(*chunk->read(*far)), "xyz");

    auto back = chunk->write_at(rmk::kChunkHeaderSize, bytes("!"));
    ASSERT_FALSE(back);
    EXPECT_EQ(back.error().code, rmk::Errc::invalid_argument);
}

TEST_F(ChunkFileTest, ShardIndexSurvivesReopen) {
    auto shard = header();
    shard.data_units = 4;
    shard.parity_units = 2;
    shard.shard_index = 5;
    ASSERT_TRUE(rmk::ChunkFile::create(path(), shard));

    auto chunk = rmk::ChunkFile::open(path());
    ASSERT_TRUE(chunk);
    EXPECT_EQ(chunk->header(), shard);
}

TEST_F(ChunkFileTest, RejectsFileWithoutChunkHeader) {
    {
        std::ofstream f(path(), std::ios::binary);
        f << std::string(rmk::kChunkHeaderSize, 'z');
    }
    auto chunk = rmk::ChunkFile::open(path());
    ASSERT_FALSE(chunk);
    EXPECT_EQ(chunk.error().code, rmk::Errc::corruption);
}

}  // namespace
