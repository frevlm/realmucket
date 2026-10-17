// Data Node loss, repair, erasure coding and compaction, with the Controller driven by run_once.

#include <set>

#include "e2e/cluster.hpp"

namespace rmk::e2e {
namespace {

using rc::ErrorCode;

using Content = std::map<std::string, std::string>;  // key -> bytes

Content put_objects(rc::Client& client, const std::string& bucket, int count, std::size_t size, std::uint64_t seed) {
    Content content;
    for (int i = 0; i < count; ++i) {
        char key[16];
        std::snprintf(key, sizeof key, "obj-%03d", i);
        content[key] = pattern(size, seed + static_cast<std::uint64_t>(i));
        take(client.put({"rec", bucket, key}, content[key]));
    }
    return content;
}

void expect_readable(rc::Client& client, const std::string& bucket, const Content& content) {
    for (const auto& [key, bytes] : content) {
        auto got = client.get({"rec", bucket, key});
        ASSERT_TRUE(got.has_value()) << key << ": " << got.error().message;
        EXPECT_TRUE(*got == bytes) << key << " differs";
    }
}

TEST_F(E2E, ReadsSurviveTheLossOfOneDataNode) {
    auto rec = tenant_admin("rec");
    ASSERT_TRUE(rec);
    take(rec->create_bucket("rec", "data"));
    Content content = put_objects(*rec, "data", 10, 5000, 100);
    content["big"] = pattern(5 * 64 * kKiB + 1, 99);  // several Parts, several Chunks
    take(rec->put({"rec", "data", "big"}, content["big"]));

    stop_node("dn1");
    expect_readable(*rec, "data", content);
}

TEST_F(E2E, ChunkIsRepairedOntoAnotherNodeAfterTheRepairDelay) {
    auto rec = tenant_admin("rec");
    ASSERT_TRUE(rec);
    take(rec->create_bucket("rec", "data"));
    const std::string body = pattern(20000, 7);
    take(rec->put({"rec", "data", "obj"}, body));

    const ChunkId id = stored("rec", "data", "obj").parts.at(0).extent.chunk_id;
    const ChunkInfo before = chunk(id);
    ASSERT_EQ(before.nodes.size(), 3u);
    const NodeId victim = before.nodes[0];
    stop_node(victim);
    EXPECT_EQ(take(rec->get({"rec", "data", "obj"})), body);

    // Silent for a moment is not dead: nothing moves yet.
    auto early = pass();
    EXPECT_EQ(early.chunks_repaired, 0u);
    EXPECT_EQ(chunk(id).nodes, before.nodes);

    // The Controller's clock runs past the repair delay; the live nodes have just checked in.
    std::this_thread::sleep_for(std::chrono::seconds{1});
    beat_live();
    auto late = pass_at(Clock::now() + cfg.repair_delay - std::chrono::milliseconds{500});
    EXPECT_GE(late.chunks_repaired, 1u);

    const ChunkInfo after = chunk(id);
    ASSERT_EQ(after.nodes.size(), 3u);
    EXPECT_EQ(std::ranges::count(after.nodes, victim), 0);
    EXPECT_NE(after.nodes[0], victim);
    EXPECT_EQ(after.nodes[1], before.nodes[1]);
    EXPECT_EQ(after.nodes[2], before.nodes[2]);
    EXPECT_EQ(after.state, ChunkState::sealed);

    // The new copy alone serves the object once the original survivors are gone.
    stop_node(before.nodes[1]);
    stop_node(before.nodes[2]);
    EXPECT_EQ(take(rec->get({"rec", "data", "obj"})), body);
}

TEST_F(E2EWide, ErasureCodedBucketSurvivesTheLossOfAShardNode) {
    auto rec = tenant_admin("rec");
    ASSERT_TRUE(rec);
    rc::BucketConfig cold;
    cold.storage_policy = {4, 2};
    take(rec->create_bucket("rec", "cold", cold));
    pass();  // open Chunks for 4+2

    // 40 * ~60 KiB overfills the two Chunks the Gateway writes to, so at least one seals.
    const Content content = put_objects(*rec, "cold", 40, 60000, 1000);
    auto sealed = chunks_in(ChunkState::sealed);
    ASSERT_FALSE(sealed.empty());
    for (const auto& c : sealed)
        EXPECT_EQ(c.target_policy, (StoragePolicy{4, 2}));

    auto report = pass();
    EXPECT_GE(report.chunks_encoded, 1u);
    auto coded = chunks_in(ChunkState::erasure_coded);
    ASSERT_FALSE(coded.empty());
    for (const auto& c : coded) {
        EXPECT_EQ(c.data_units, 4);
        EXPECT_EQ(c.parity_units, 2);
        EXPECT_EQ(c.nodes.size(), 6u);
    }
    std::set<ChunkId> coded_ids;
    for (const auto& c : coded)
        coded_ids.insert(c.id);
    int in_coded = 0;
    for (const auto& [key, bytes] : content)
        in_coded += coded_ids.contains(stored("rec", "cold", key).parts.at(0).extent.chunk_id);
    EXPECT_GE(in_coded, 1);
    expect_readable(*rec, "cold", content);

    stop_node(coded.front().nodes.front());
    expect_readable(*rec, "cold", content);
}

TEST_F(E2E, CompactionLeavesTheRemainingObjectsReadable) {
    auto rec = tenant_admin("rec");
    ASSERT_TRUE(rec);
    take(rec->create_bucket("rec", "data"));
    Content content = put_objects(*rec, "data", 60, 40000, 5000);

    std::set<ChunkId> sealed_ids;
    for (const auto& c : chunks_in(ChunkState::sealed))
        sealed_ids.insert(c.id);
    ASSERT_FALSE(sealed_ids.empty());

    // Keep one object per sealed Chunk, delete the rest of what they hold.
    std::set<ChunkId> seen;
    std::vector<std::string> removed;
    for (const auto& [key, bytes] : content) {
        const ChunkId id = stored("rec", "data", key).parts.at(0).extent.chunk_id;
        if (!sealed_ids.contains(id))
            continue;
        if (seen.insert(id).second)
            continue;  // the survivor of this Chunk
        removed.push_back(key);
    }
    ASSERT_FALSE(removed.empty());
    for (const auto& key : removed) {
        take(rec->remove({"rec", "data", key}));
        content.erase(key);
    }

    controller::Report total = pass();
    auto second = pass();  // finishes what the first left (an emptied Chunk is deleted)
    total.chunks_compacted += second.chunks_compacted;
    total.chunks_deleted += second.chunks_deleted;
    EXPECT_GE(total.chunks_compacted, 1u);
    EXPECT_GE(total.chunks_deleted, static_cast<std::uint32_t>(sealed_ids.size()));

    for (ChunkId id : sealed_ids)
        EXPECT_FALSE(cluster->get_chunk(id).has_value()) << "chunk " << id << " still exists";
    for (const auto& [key, bytes] : content)
        EXPECT_FALSE(sealed_ids.contains(stored("rec", "data", key).parts.at(0).extent.chunk_id)) << key;
    expect_readable(*rec, "data", content);
    for (const auto& key : removed)
        expect_error(rec->get({"rec", "data", key}), ErrorCode::not_found);
}

}  // namespace
}  // namespace rmk::e2e
