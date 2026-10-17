#pragma once

#include <algorithm>
#include <set>
#include <string>

#include "store_contract/common.hpp"

class ClusterStoreContract : public ::testing::TestWithParam<rmk::contract::StoreFactory> {
protected:
    void SetUp() override {
        rmk::contract::StoreSet set = GetParam()(std::chrono::hours(1));
        if (!set.meta) GTEST_SKIP() << "store unavailable";
        store = set.cluster;
    }

    static rmk::NodeInfo node(std::string id, std::string zone = "z1") {
        rmk::NodeInfo n;
        n.id = std::move(id);
        n.address = n.id + ".svc:7001";
        n.k8s_node = "machine-" + n.id;
        n.zone = std::move(zone);
        n.capacity_bytes = 1000;
        n.used_bytes = 100;
        return n;
    }

    static rmk::ChunkInfo chunk(rmk::ChunkId id, rmk::ChunkState state = rmk::ChunkState::open,
                                rmk::StoragePolicy policy = rmk::kReplicated3) {
        rmk::ChunkInfo c;
        c.id = id;
        c.state = state;
        c.target_policy = policy;
        c.nodes = {"n1", "n2", "n3"};
        c.capacity = 1000;
        c.size = 64;
        return c;
    }

    // Allocates `count` ids and returns them in ascending order.
    std::vector<rmk::ChunkId> allocate(int count) {
        std::vector<rmk::ChunkId> ids;
        for (int i = 0; i < count; ++i) {
            auto id = store->allocate_chunk_id();
            if (!id) {
                ADD_FAILURE() << rmk::errc_name(id.error().code);
                break;
            }
            ids.push_back(*id);
        }
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    static std::vector<rmk::ChunkId> ids_of(const std::vector<rmk::ChunkInfo>& chunks) {
        std::vector<rmk::ChunkId> out;
        for (const auto& c : chunks) out.push_back(c.id);
        return out;
    }

    std::shared_ptr<rmk::ClusterStore> store;
};

using namespace rmk;
using namespace rmk::contract;

// --- Data Nodes

TEST_P(ClusterStoreContract, HeartbeatRegistersNodeAndStampsTime) {
    NodeInfo n = node("n1");
    n.last_heartbeat = at(-999);  // ignored: the store stamps `now`
    RMK_ASSERT_OK(store->heartbeat(n, at(100)));
    auto nodes = store->list_nodes();
    RMK_ASSERT_OK(nodes);
    ASSERT_EQ(nodes->size(), 1u);
    NodeInfo expected = n;
    expected.last_heartbeat = at(100);
    EXPECT_EQ((*nodes)[0], expected);
}

TEST_P(ClusterStoreContract, HeartbeatUpsertsAndRestamps) {
    RMK_ASSERT_OK(store->heartbeat(node("n1"), at(100)));
    NodeInfo changed = node("n1", "z9");
    changed.used_bytes = 555;
    changed.address = "moved:7001";
    RMK_ASSERT_OK(store->heartbeat(changed, at(200)));
    auto nodes = store->list_nodes();
    RMK_ASSERT_OK(nodes);
    ASSERT_EQ(nodes->size(), 1u);
    changed.last_heartbeat = at(200);
    EXPECT_EQ((*nodes)[0], changed);
}

TEST_P(ClusterStoreContract, HeartbeatRejectsEmptyId) {
    RMK_EXPECT_ERR(store->heartbeat(node(""), at(1)), invalid_argument);
}

TEST_P(ClusterStoreContract, ListNodesIsInIdOrder) {
    for (const char* id : {"n3", "n1", "n2"}) RMK_ASSERT_OK(store->heartbeat(node(id), at(1)));
    auto nodes = store->list_nodes();
    RMK_ASSERT_OK(nodes);
    ASSERT_EQ(nodes->size(), 3u);
    EXPECT_EQ((*nodes)[0].id, "n1");
    EXPECT_EQ((*nodes)[1].id, "n2");
    EXPECT_EQ((*nodes)[2].id, "n3");
}

TEST_P(ClusterStoreContract, ListNodesOfEmptyClusterIsEmpty) {
    auto nodes = store->list_nodes();
    RMK_ASSERT_OK(nodes);
    EXPECT_EQ(nodes->size(), 0u);
}

TEST_P(ClusterStoreContract, RemoveNodeDeletesIt) {
    RMK_ASSERT_OK(store->heartbeat(node("n1"), at(1)));
    RMK_ASSERT_OK(store->heartbeat(node("n2"), at(1)));
    RMK_ASSERT_OK(store->remove_node("n1"));
    auto nodes = store->list_nodes();
    RMK_ASSERT_OK(nodes);
    ASSERT_EQ(nodes->size(), 1u);
    EXPECT_EQ((*nodes)[0].id, "n2");
}

TEST_P(ClusterStoreContract, RemoveMissingNodeIsNotFound) {
    RMK_EXPECT_ERR(store->remove_node("ghost"), not_found);
}

// --- Chunks

TEST_P(ClusterStoreContract, AllocatedChunkIdsAreUniqueAndNonZero) {
    std::set<ChunkId> seen;
    for (int i = 0; i < 100; ++i) {
        auto id = store->allocate_chunk_id();
        RMK_ASSERT_OK(id);
        EXPECT_NE(*id, 0u);
        seen.insert(*id);
    }
    EXPECT_EQ(seen.size(), 100u);
}

TEST_P(ClusterStoreContract, PutAndGetChunkRoundTrip) {
    auto ids = allocate(1);
    ChunkInfo c = chunk(ids[0]);
    c.garbage_bytes = 12;
    c.data_units = 4;
    c.parity_units = 2;
    c.writer = "gw-1";
    c.writer_lease_expiry = at(500);
    RMK_ASSERT_OK(store->put_chunk(c));
    auto got = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(got);
    EXPECT_EQ(got->id, c.id);
    EXPECT_EQ(got->state, c.state);
    EXPECT_EQ(got->target_policy, c.target_policy);
    EXPECT_EQ(got->data_units, 4);
    EXPECT_EQ(got->parity_units, 2);
    EXPECT_EQ(got->nodes, c.nodes);
    EXPECT_EQ(got->capacity, c.capacity);
    EXPECT_EQ(got->size, c.size);
    EXPECT_EQ(got->garbage_bytes, 12u);
    EXPECT_EQ(got->writer, "gw-1");
    EXPECT_EQ(got->writer_lease_expiry, at(500));
    EXPECT_EQ(got->revision, c.revision);
}

TEST_P(ClusterStoreContract, PutChunkTwiceIsAlreadyExists) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    RMK_EXPECT_ERR(store->put_chunk(chunk(ids[0])), already_exists);
}

TEST_P(ClusterStoreContract, GetMissingChunkIsNotFound) {
    RMK_EXPECT_ERR(store->get_chunk(987654), not_found);
}

TEST_P(ClusterStoreContract, ListChunksInIdOrderWithAfterAndLimit) {
    auto ids = allocate(5);
    for (auto it = ids.rbegin(); it != ids.rend(); ++it) RMK_ASSERT_OK(store->put_chunk(chunk(*it)));
    auto all = store->list_chunks(std::nullopt, std::nullopt, 100);
    RMK_ASSERT_OK(all);
    EXPECT_EQ(ids_of(*all), ids);
    auto limited = store->list_chunks(std::nullopt, std::nullopt, 2);
    RMK_ASSERT_OK(limited);
    EXPECT_EQ(ids_of(*limited), (std::vector<ChunkId>{ids[0], ids[1]}));
    auto after = store->list_chunks(std::nullopt, ids[1], 100);
    RMK_ASSERT_OK(after);
    EXPECT_EQ(ids_of(*after), (std::vector<ChunkId>{ids[2], ids[3], ids[4]}));
    auto paged = store->list_chunks(std::nullopt, ids[1], 2);
    RMK_ASSERT_OK(paged);
    EXPECT_EQ(ids_of(*paged), (std::vector<ChunkId>{ids[2], ids[3]}));
    auto end = store->list_chunks(std::nullopt, ids[4], 100);
    RMK_ASSERT_OK(end);
    EXPECT_EQ(end->size(), 0u);
}

TEST_P(ClusterStoreContract, ListChunksFiltersByState) {
    auto ids = allocate(4);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0], ChunkState::open)));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[1], ChunkState::sealed)));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[2], ChunkState::erasure_coded)));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[3], ChunkState::sealed)));
    auto sealed = store->list_chunks(ChunkState::sealed, std::nullopt, 100);
    RMK_ASSERT_OK(sealed);
    EXPECT_EQ(ids_of(*sealed), (std::vector<ChunkId>{ids[1], ids[3]}));
    auto ec = store->list_chunks(ChunkState::erasure_coded, std::nullopt, 100);
    RMK_ASSERT_OK(ec);
    EXPECT_EQ(ids_of(*ec), (std::vector<ChunkId>{ids[2]}));
    // The limit applies to matching chunks.
    auto first_sealed = store->list_chunks(ChunkState::sealed, std::nullopt, 1);
    RMK_ASSERT_OK(first_sealed);
    EXPECT_EQ(ids_of(*first_sealed), (std::vector<ChunkId>{ids[1]}));
    auto after = store->list_chunks(ChunkState::sealed, ids[1], 1);
    RMK_ASSERT_OK(after);
    EXPECT_EQ(ids_of(*after), (std::vector<ChunkId>{ids[3]}));
}

TEST_P(ClusterStoreContract, UpdateChunkStoresNextWithBumpedRevision) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto current = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(current);
    ChunkInfo next = *current;
    next.state = ChunkState::sealed;
    next.size = 900;
    next.writer.clear();
    auto stored = store->update_chunk(next);
    RMK_ASSERT_OK(stored);
    EXPECT_EQ(stored->revision, current->revision + 1);
    EXPECT_EQ(stored->state, ChunkState::sealed);
    EXPECT_EQ(stored->size, 900u);
    auto got = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(got);
    EXPECT_EQ(got->revision, current->revision + 1);
    EXPECT_EQ(got->state, ChunkState::sealed);
    EXPECT_EQ(got->size, 900u);
    EXPECT_EQ(got->nodes, current->nodes);
}

TEST_P(ClusterStoreContract, UpdateChunkWithStaleRevisionIsAborted) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto current = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(current);
    ChunkInfo first = *current;
    first.size = 100;
    ChunkInfo second = *current;
    second.size = 200;
    RMK_ASSERT_OK(store->update_chunk(first));
    RMK_EXPECT_ERR(store->update_chunk(second), aborted);
    auto got = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(got);
    EXPECT_EQ(got->size, 100u);
}

TEST_P(ClusterStoreContract, UpdateChunkChainsWithReturnedRevision) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto a = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(a);
    ChunkInfo next = *a;
    next.size = 100;
    auto b = store->update_chunk(next);
    RMK_ASSERT_OK(b);
    ChunkInfo next2 = *b;
    next2.size = 200;
    auto c = store->update_chunk(next2);
    RMK_ASSERT_OK(c);
    EXPECT_EQ(c->revision, a->revision + 2);
    EXPECT_EQ(c->size, 200u);
}

TEST_P(ClusterStoreContract, UpdateChunkKeepsStoredGarbageBytes) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto stale = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(stale);
    RMK_ASSERT_OK(store->add_garbage(ids[0], 100));
    ChunkInfo next = *stale;
    next.state = ChunkState::sealed;
    next.garbage_bytes = 7;
    auto stored = store->update_chunk(next);
    RMK_ASSERT_OK(stored);
    EXPECT_EQ(stored->garbage_bytes, stale->garbage_bytes + 100);
    auto got = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(got);
    EXPECT_EQ(got->garbage_bytes, stale->garbage_bytes + 100);
    EXPECT_EQ(got->state, ChunkState::sealed);
}

TEST_P(ClusterStoreContract, UpdateMissingChunkIsNotFound) {
    RMK_EXPECT_ERR(store->update_chunk(chunk(424242)), not_found);
}

TEST_P(ClusterStoreContract, DeleteChunkRemovesIt) {
    auto ids = allocate(2);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[1])));
    RMK_ASSERT_OK(store->delete_chunk(ids[0]));
    RMK_EXPECT_ERR(store->get_chunk(ids[0]), not_found);
    auto all = store->list_chunks(std::nullopt, std::nullopt, 10);
    RMK_ASSERT_OK(all);
    EXPECT_EQ(ids_of(*all), (std::vector<ChunkId>{ids[1]}));
    RMK_EXPECT_ERR(store->delete_chunk(ids[0]), not_found);
}

TEST_P(ClusterStoreContract, AddGarbageAccumulatesWithoutTouchingRevision) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto before = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(before);
    RMK_ASSERT_OK(store->add_garbage(ids[0], 100));
    RMK_ASSERT_OK(store->add_garbage(ids[0], 23));
    auto after = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(after);
    EXPECT_EQ(after->garbage_bytes, before->garbage_bytes + 123);
    EXPECT_EQ(after->revision, before->revision);
}

TEST_P(ClusterStoreContract, AddGarbageToMissingChunkIsNotFound) {
    RMK_EXPECT_ERR(store->add_garbage(31337, 1), not_found);
}

// --- claim_open_chunk

TEST_P(ClusterStoreContract, ClaimPicksLowestIdAndSetsWriterLeaseAndRevision) {
    auto ids = allocate(3);
    for (auto it = ids.rbegin(); it != ids.rend(); ++it) RMK_ASSERT_OK(store->put_chunk(chunk(*it)));
    auto before = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(before);
    auto claimed = store->claim_open_chunk(kReplicated3, "gw-1", 100, at(130), at(100));
    RMK_ASSERT_OK(claimed);
    EXPECT_EQ(claimed->id, ids[0]);
    EXPECT_EQ(claimed->writer, "gw-1");
    EXPECT_EQ(claimed->writer_lease_expiry, at(130));
    EXPECT_EQ(claimed->revision, before->revision + 1);
    auto got = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(got);
    EXPECT_EQ(got->writer, "gw-1");
    EXPECT_EQ(got->writer_lease_expiry, at(130));
    EXPECT_EQ(got->revision, before->revision + 1);
    EXPECT_EQ(got->nodes, before->nodes);
    EXPECT_EQ(got->size, before->size);
}

TEST_P(ClusterStoreContract, ClaimedChunkCanBeUpdatedWithReturnedRevision) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto claimed = store->claim_open_chunk(kReplicated3, "gw-1", 1, at(130), at(100));
    RMK_ASSERT_OK(claimed);
    ChunkInfo sealed = *claimed;
    sealed.state = ChunkState::sealed;
    sealed.writer.clear();
    RMK_EXPECT_OK(store->update_chunk(sealed));
}

TEST_P(ClusterStoreContract, ClaimWithNoChunkIsNotFound) {
    RMK_EXPECT_ERR(store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100)), not_found);
}

TEST_P(ClusterStoreContract, ClaimSkipsChunksThatAreNotOpen) {
    auto ids = allocate(3);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0], ChunkState::sealed)));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[1], ChunkState::erasure_coded)));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[2], ChunkState::open)));
    auto claimed = store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100));
    RMK_ASSERT_OK(claimed);
    EXPECT_EQ(claimed->id, ids[2]);
}

TEST_P(ClusterStoreContract, ClaimMatchesTargetPolicy) {
    auto ids = allocate(2);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0], ChunkState::open, StoragePolicy{4, 2})));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[1], ChunkState::open, kReplicated3)));
    auto replicated = store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100));
    RMK_ASSERT_OK(replicated);
    EXPECT_EQ(replicated->id, ids[1]);
    auto coded = store->claim_open_chunk(StoragePolicy{4, 2}, "gw-1", 0, at(130), at(100));
    RMK_ASSERT_OK(coded);
    EXPECT_EQ(coded->id, ids[0]);
    RMK_EXPECT_ERR(store->claim_open_chunk(StoragePolicy{6, 3}, "gw-1", 0, at(130), at(100)), not_found);
}

TEST_P(ClusterStoreContract, ClaimRequiresMinFreeBytes) {
    auto ids = allocate(2);
    ChunkInfo small = chunk(ids[0]);
    small.capacity = 1000;
    small.size = 950;  // 50 free
    ChunkInfo roomy = chunk(ids[1]);
    roomy.capacity = 1000;
    roomy.size = 100;  // 900 free
    RMK_ASSERT_OK(store->put_chunk(small));
    RMK_ASSERT_OK(store->put_chunk(roomy));
    auto exact = store->claim_open_chunk(kReplicated3, "gw-1", 50, at(130), at(100));
    RMK_ASSERT_OK(exact);
    EXPECT_EQ(exact->id, ids[0]);
    auto larger = store->claim_open_chunk(kReplicated3, "gw-2", 51, at(130), at(100));
    RMK_ASSERT_OK(larger);
    EXPECT_EQ(larger->id, ids[1]);
    RMK_EXPECT_ERR(store->claim_open_chunk(kReplicated3, "gw-3", 1, at(130), at(100)), not_found);
}

TEST_P(ClusterStoreContract, ClaimSkipsChunksLeasedToAnotherWriter) {
    auto ids = allocate(2);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[1])));
    auto a = store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100));
    auto b = store->claim_open_chunk(kReplicated3, "gw-2", 0, at(130), at(101));
    RMK_ASSERT_OK(a);
    RMK_ASSERT_OK(b);
    EXPECT_EQ(a->id, ids[0]);
    EXPECT_EQ(b->id, ids[1]);
    RMK_EXPECT_ERR(store->claim_open_chunk(kReplicated3, "gw-3", 0, at(160), at(102)), not_found);
}

TEST_P(ClusterStoreContract, ClaimTakesOverExpiredLease) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    RMK_ASSERT_OK(store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100)));
    RMK_EXPECT_ERR(store->claim_open_chunk(kReplicated3, "gw-2", 0, at(260), at(129)), not_found);
    auto taken = store->claim_open_chunk(kReplicated3, "gw-2", 0, at(260), at(131));
    RMK_ASSERT_OK(taken);
    EXPECT_EQ(taken->id, ids[0]);
    EXPECT_EQ(taken->writer, "gw-2");
    EXPECT_EQ(taken->writer_lease_expiry, at(260));
}

TEST_P(ClusterStoreContract, ClaimAgainByTheSameWriterRenewsTheLease) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto first = store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100));
    RMK_ASSERT_OK(first);
    auto again = store->claim_open_chunk(kReplicated3, "gw-1", 0, at(250), at(120));
    RMK_ASSERT_OK(again);
    EXPECT_EQ(again->id, ids[0]);
    EXPECT_EQ(again->writer_lease_expiry, at(250));
    EXPECT_EQ(again->revision, first->revision + 1);
    RMK_EXPECT_ERR(store->claim_open_chunk(kReplicated3, "gw-2", 0, at(300), at(200)), not_found);
}

TEST_P(ClusterStoreContract, ClaimTakesChunkWithEmptyWriterEvenIfExpiryIsInTheFuture) {
    auto ids = allocate(1);
    ChunkInfo c = chunk(ids[0]);
    c.writer.clear();
    c.writer_lease_expiry = at(100000);
    RMK_ASSERT_OK(store->put_chunk(c));
    auto claimed = store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100));
    RMK_ASSERT_OK(claimed);
    EXPECT_EQ(claimed->writer, "gw-1");
}

TEST_P(ClusterStoreContract, ClaimInvalidatesStaleUpdates) {
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto stale = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(stale);
    RMK_ASSERT_OK(store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100)));
    ChunkInfo next = *stale;
    next.state = ChunkState::sealed;
    RMK_EXPECT_ERR(store->update_chunk(next), aborted);
}

// --- try_lead

TEST_P(ClusterStoreContract, FirstTryLeadAcquires) {
    auto epoch = store->try_lead("ctl-1", std::chrono::seconds(15), at(100));
    RMK_EXPECT_OK(epoch);
}

TEST_P(ClusterStoreContract, HolderRenewsWithTheSameEpoch) {
    auto first = store->try_lead("ctl-1", std::chrono::seconds(15), at(100));
    RMK_ASSERT_OK(first);
    auto renewed = store->try_lead("ctl-1", std::chrono::seconds(15), at(110));
    RMK_ASSERT_OK(renewed);
    EXPECT_EQ(*renewed, *first);
    auto again = store->try_lead("ctl-1", std::chrono::seconds(15), at(120));
    RMK_ASSERT_OK(again);
    EXPECT_EQ(*again, *first);
}

TEST_P(ClusterStoreContract, OthersAreAbortedWhileTheLeaseLives) {
    RMK_ASSERT_OK(store->try_lead("ctl-1", std::chrono::seconds(15), at(100)));
    RMK_EXPECT_ERR(store->try_lead("ctl-2", std::chrono::seconds(15), at(101)), aborted);
    RMK_EXPECT_ERR(store->try_lead("ctl-2", std::chrono::seconds(15), at(114)), aborted);
}

TEST_P(ClusterStoreContract, RenewalExtendsTheLease) {
    RMK_ASSERT_OK(store->try_lead("ctl-1", std::chrono::seconds(15), at(100)));
    RMK_ASSERT_OK(store->try_lead("ctl-1", std::chrono::seconds(15), at(110)));
    RMK_EXPECT_ERR(store->try_lead("ctl-2", std::chrono::seconds(15), at(120)), aborted);
    RMK_EXPECT_OK(store->try_lead("ctl-2", std::chrono::seconds(15), at(126)));
}

TEST_P(ClusterStoreContract, TakeoverAfterExpiryIncrementsTheEpoch) {
    auto first = store->try_lead("ctl-1", std::chrono::seconds(15), at(100));
    RMK_ASSERT_OK(first);
    auto second = store->try_lead("ctl-2", std::chrono::seconds(15), at(120));
    RMK_ASSERT_OK(second);
    EXPECT_GT(*second, *first);
    // The new holder renews under its own epoch; the old one is locked out.
    auto renewed = store->try_lead("ctl-2", std::chrono::seconds(15), at(125));
    RMK_ASSERT_OK(renewed);
    EXPECT_EQ(*renewed, *second);
    RMK_EXPECT_ERR(store->try_lead("ctl-1", std::chrono::seconds(15), at(126)), aborted);
}

TEST_P(ClusterStoreContract, EpochGrowsWithEveryChangeOfHands) {
    auto e1 = store->try_lead("ctl-1", std::chrono::seconds(10), at(0));
    auto e2 = store->try_lead("ctl-2", std::chrono::seconds(10), at(20));
    auto e3 = store->try_lead("ctl-3", std::chrono::seconds(10), at(40));
    auto e4 = store->try_lead("ctl-1", std::chrono::seconds(10), at(60));
    RMK_ASSERT_OK(e1);
    RMK_ASSERT_OK(e2);
    RMK_ASSERT_OK(e3);
    RMK_ASSERT_OK(e4);
    EXPECT_LT(*e1, *e2);
    EXPECT_LT(*e2, *e3);
    EXPECT_LT(*e3, *e4);
}

TEST_P(ClusterStoreContract, TtlIsPerCall) {
    RMK_ASSERT_OK(store->try_lead("ctl-1", std::chrono::seconds(60), at(100)));
    RMK_EXPECT_ERR(store->try_lead("ctl-2", std::chrono::seconds(5), at(150)), aborted);
    RMK_EXPECT_OK(store->try_lead("ctl-2", std::chrono::seconds(5), at(161)));
}

// --- Fencing and sealed_at

TEST_P(ClusterStoreContract, FencedWritesSucceedForTheCurrentLeader) {
    auto epoch = store->try_lead("ctl-1", std::chrono::seconds(15), at(100));
    RMK_ASSERT_OK(epoch);
    Fence fence{"ctl-1", *epoch};
    auto ids = allocate(2);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0]), fence));
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[1]), fence));
    auto current = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(current);
    ChunkInfo next = *current;
    next.state = ChunkState::sealed;
    RMK_ASSERT_OK(store->update_chunk(next, fence));
    RMK_ASSERT_OK(store->delete_chunk(ids[1], fence));
    RMK_ASSERT_OK(store->heartbeat(node("n1"), at(100)));
    RMK_ASSERT_OK(store->remove_node("n1", fence));
    auto got = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(got);
    EXPECT_EQ(got->state, ChunkState::sealed);
    RMK_EXPECT_ERR(store->get_chunk(ids[1]), not_found);
}

TEST_P(ClusterStoreContract, FencedWritesAreAbortedAfterAnotherHolderTookOver) {
    auto epoch = store->try_lead("ctl-1", std::chrono::seconds(15), at(100));
    RMK_ASSERT_OK(epoch);
    Fence old_fence{"ctl-1", *epoch};
    auto ids = allocate(2);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0]), old_fence));
    RMK_ASSERT_OK(store->heartbeat(node("n1"), at(100)));
    auto current = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(current);

    auto taken = store->try_lead("ctl-2", std::chrono::seconds(15), at(200));
    RMK_ASSERT_OK(taken);

    ChunkInfo next = *current;
    next.size = 500;
    RMK_EXPECT_ERR(store->put_chunk(chunk(ids[1]), old_fence), aborted);
    RMK_EXPECT_ERR(store->update_chunk(next, old_fence), aborted);
    RMK_EXPECT_ERR(store->delete_chunk(ids[0], old_fence), aborted);
    RMK_EXPECT_ERR(store->remove_node("n1", old_fence), aborted);
    // Nothing changed.
    auto got = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(got);
    EXPECT_EQ(got->size, current->size);
    RMK_EXPECT_ERR(store->get_chunk(ids[1]), not_found);
    auto nodes = store->list_nodes();
    RMK_ASSERT_OK(nodes);
    EXPECT_EQ(nodes->size(), 1u);
    // The new leader's fence works.
    RMK_EXPECT_OK(store->update_chunk(next, Fence{"ctl-2", *taken}));
}

TEST_P(ClusterStoreContract, FencedWriteWithWrongEpochOrHolderIsAborted) {
    auto epoch = store->try_lead("ctl-1", std::chrono::seconds(15), at(100));
    RMK_ASSERT_OK(epoch);
    auto ids = allocate(1);
    RMK_EXPECT_ERR(store->put_chunk(chunk(ids[0]), Fence{"ctl-1", *epoch + 1}), aborted);
    RMK_EXPECT_ERR(store->put_chunk(chunk(ids[0]), Fence{"ctl-2", *epoch}), aborted);
    RMK_EXPECT_OK(store->put_chunk(chunk(ids[0]), Fence{"ctl-1", *epoch}));
}

TEST_P(ClusterStoreContract, FencedWriteWithoutAnyLeaderIsAborted) {
    auto ids = allocate(1);
    RMK_EXPECT_ERR(store->put_chunk(chunk(ids[0]), Fence{"ctl-1", 1}), aborted);
}

TEST_P(ClusterStoreContract, UnfencedWritesIgnoreLeadership) {
    RMK_ASSERT_OK(store->try_lead("ctl-1", std::chrono::seconds(15), at(100)));
    auto ids = allocate(1);
    RMK_ASSERT_OK(store->put_chunk(chunk(ids[0])));
    auto current = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(current);
    RMK_EXPECT_OK(store->update_chunk(*current));
    RMK_EXPECT_OK(store->delete_chunk(ids[0]));
}

TEST_P(ClusterStoreContract, SealedAtRoundTripsThroughPutGetUpdateAndClaim) {
    auto ids = allocate(2);
    ChunkInfo open = chunk(ids[0]);
    open.sealed_at = at(77);
    ChunkInfo sealed = chunk(ids[1], ChunkState::sealed);
    sealed.sealed_at = at(88);
    RMK_ASSERT_OK(store->put_chunk(open));
    RMK_ASSERT_OK(store->put_chunk(sealed));
    auto got_open = store->get_chunk(ids[0]);
    auto got_sealed = store->get_chunk(ids[1]);
    RMK_ASSERT_OK(got_open);
    RMK_ASSERT_OK(got_sealed);
    EXPECT_EQ(got_open->sealed_at, at(77));
    EXPECT_EQ(got_sealed->sealed_at, at(88));

    auto claimed = store->claim_open_chunk(kReplicated3, "gw-1", 0, at(130), at(100));
    RMK_ASSERT_OK(claimed);
    EXPECT_EQ(claimed->id, ids[0]);
    EXPECT_EQ(claimed->sealed_at, at(77));

    ChunkInfo next = *claimed;
    next.state = ChunkState::sealed;
    next.writer.clear();
    next.sealed_at = at(150);
    auto updated = store->update_chunk(next);
    RMK_ASSERT_OK(updated);
    EXPECT_EQ(updated->sealed_at, at(150));
    auto after = store->get_chunk(ids[0]);
    RMK_ASSERT_OK(after);
    EXPECT_EQ(after->sealed_at, at(150));
}
