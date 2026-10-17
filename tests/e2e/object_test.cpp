// Tenants and the object API, through rmk::client::Client only.

#include <sstream>

#include "e2e/cluster.hpp"

namespace rmk::e2e {
namespace {

using rc::ErrorCode;

TEST_F(E2E, CreateTenantAndReportUsage) {
    auto tenant = take(admin->create_tenant("rec", rc::Quota{8 * kMiB, 100}));
    EXPECT_EQ(tenant.name, "rec");
    EXPECT_EQ(tenant.quota.max_bytes, 8 * kMiB);
    EXPECT_EQ(tenant.quota.max_objects, 100u);

    take(admin->create_bucket("rec", "models"));
    take(admin->put({"rec", "models", "hello"}, "world"));
    auto own = take(admin->get_tenant("rec"));
    EXPECT_EQ(own.name, "rec");
    EXPECT_EQ(own.quota.max_bytes, 8 * kMiB);
    EXPECT_EQ(own.used_objects, 1u);
    EXPECT_EQ(own.used_bytes, 5u);
}

TEST_F(E2E, SmallAndMultipartObjectsRoundTrip) {
    auto rec = tenant_admin("rec", rc::Quota{64 * kMiB, 1000});
    ASSERT_TRUE(rec);
    take(rec->create_bucket("rec", "models"));

    const std::string small = "a small object";
    const rc::Location small_at{"rec", "models", "small"};
    auto small_info = take(rec->put(small_at, small));
    EXPECT_EQ(small_info.size, small.size());
    EXPECT_EQ(small_info.checksum, crc_of(small));
    EXPECT_EQ(take(rec->get(small_at)), small);
    EXPECT_EQ(stored("rec", "models", "small").parts.size(), 1u);

    // Above the 100 KiB threshold the SDK sends 64 KiB Parts, four at a time.
    const std::string big = pattern(5 * 64 * kKiB + 123, 1);
    const rc::Location big_at{"rec", "models", "big"};
    auto big_info = take(rec->put(big_at, big));
    EXPECT_EQ(big_info.size, big.size());
    EXPECT_EQ(big_info.checksum, crc_of(big));
    EXPECT_EQ(stored("rec", "models", "big").parts.size(), 6u);
    EXPECT_EQ(take(rec->get(big_at)), big);
    EXPECT_EQ(take(rec->head(big_at)).checksum, crc_of(big));

    const std::string file_content = pattern(7 * 64 * kKiB + 5, 2);
    const fs::path file = root / "upload.bin";
    {
        std::ofstream out(file, std::ios::binary);
        out << file_content;
    }
    const rc::Location file_at{"rec", "models", "from-file"};
    auto file_info = take(rec->put_file(file_at, file));
    EXPECT_EQ(file_info.checksum, crc_of(file_content));
    EXPECT_EQ(stored("rec", "models", "from-file").parts.size(), 8u);
    std::ostringstream streamed;
    take(rec->get(file_at, streamed));
    EXPECT_EQ(streamed.str(), file_content);

    // A range that straddles the first Part boundary.
    rc::GetOptions range;
    range.offset = 64 * kKiB - 10;
    range.length = 100;
    EXPECT_EQ(take(rec->get(big_at, range)), big.substr(range.offset, range.length));

    auto listing = take(rec->list("rec", "models"));
    ASSERT_EQ(listing.objects.size(), 3u);
    EXPECT_EQ(listing.objects[0].location.key, "big");
    EXPECT_EQ(listing.objects[1].location.key, "from-file");
    EXPECT_EQ(listing.objects[2].location.key, "small");
    EXPECT_EQ(take(rec->get_tenant("rec")).used_objects, 3u);
}

TEST_F(E2E, ConditionalWrites) {
    auto rec = tenant_admin("rec");
    ASSERT_TRUE(rec);
    take(rec->create_bucket("rec", "data"));
    const rc::Location where{"rec", "data", "doc"};

    auto first = take(rec->put(where, "v1", rc::Precondition::if_not_exists()));
    expect_error(rec->put(where, "v1b", rc::Precondition::if_not_exists()), ErrorCode::precondition_failed);
    EXPECT_EQ(take(rec->get(where)), "v1");

    auto second = take(rec->put(where, "v2", rc::Precondition::if_generation(first.generation)));
    EXPECT_GT(second.generation, first.generation);
    expect_error(rec->put(where, "v3", rc::Precondition::if_generation(first.generation)),
                 ErrorCode::precondition_failed);
    EXPECT_EQ(take(rec->get(where)), "v2");

    expect_error(rec->remove(where, rc::Precondition::if_generation(first.generation)),
                 ErrorCode::precondition_failed);
    take(rec->remove(where, rc::Precondition::if_generation(second.generation)));
    expect_error(rec->get(where), ErrorCode::not_found);
    take(rec->put(where, "v4", rc::Precondition::if_not_exists()));
    EXPECT_EQ(take(rec->get(where)), "v4");
}

TEST_F(E2E, QuotaExceeded) {
    auto tiny = tenant_admin("tiny", rc::Quota{1000, 0});
    ASSERT_TRUE(tiny);
    take(tiny->create_bucket("tiny", "data"));
    const std::string six_hundred = pattern(600, 3);

    take(tiny->put({"tiny", "data", "k1"}, six_hundred));
    expect_error(tiny->put({"tiny", "data", "k2"}, six_hundred), ErrorCode::quota_exceeded);
    expect_error(tiny->put({"tiny", "data", "k-big"}, pattern(3 * 64 * kKiB, 4)), ErrorCode::quota_exceeded);
    EXPECT_EQ(take(tiny->get_tenant("tiny")).used_bytes, 600u);

    take(tiny->remove({"tiny", "data", "k1"}));
    take(tiny->put({"tiny", "data", "k2"}, six_hundred));
    EXPECT_EQ(take(tiny->get_tenant("tiny")).used_bytes, 600u);

    // The object count is limited separately; replacing an object stays within it.
    take(admin->set_quota("tiny", rc::Quota{0, 1}));
    expect_error(tiny->put({"tiny", "data", "k3"}, "x"), ErrorCode::quota_exceeded);
    take(tiny->put({"tiny", "data", "k2"}, "replacement"));
    EXPECT_EQ(take(tiny->get({"tiny", "data", "k2"})), "replacement");
}

TEST_F(E2E, MinimumSdkVersionRejectsTheClient) {
    auto rec = tenant_admin("rec");
    ASSERT_TRUE(rec);
    take(rec->create_bucket("rec", "data"));
    expect_error(admin->set_min_sdk_version("one"), ErrorCode::invalid_argument);

    take(admin->set_min_sdk_version(rc::kVersion));
    take(rec->put({"rec", "data", "k"}, "still fine"));

    take(admin->set_min_sdk_version("1.0.1"));
    auto rejected = rec->put({"rec", "data", "k2"}, "too old");
    expect_error(rejected, ErrorCode::precondition_failed);
    if (!rejected) {
        EXPECT_NE(rejected.error().message.find("1.0.1"), std::string::npos) << rejected.error().message;
        EXPECT_NE(rejected.error().message.find("upgrade"), std::string::npos) << rejected.error().message;
    }
    expect_error(rec->get({"rec", "data", "k"}), ErrorCode::precondition_failed);
    expect_error(admin->get_tenant("rec"), ErrorCode::precondition_failed);
}

TEST_F(E2E, VersioningKeepsHistoryAndRemoveVersionIsPermanent) {
    auto rec = tenant_admin("rec");
    ASSERT_TRUE(rec);
    rc::BucketConfig versioned;
    versioned.versioning = true;
    take(rec->create_bucket("rec", "docs", versioned));
    const rc::Location where{"rec", "docs", "readme"};

    const auto g1 = take(rec->put(where, "v1")).generation;
    const auto g2 = take(rec->put(where, "v2")).generation;
    EXPECT_GT(g2, g1);
    EXPECT_EQ(take(rec->get(where)), "v2");
    auto at = [&](std::uint64_t generation) {
        rc::GetOptions o;
        o.generation = generation;
        return rec->get(where, o);
    };
    EXPECT_EQ(take(at(g1)), "v1");
    EXPECT_EQ(take(rec->list_versions(where)).size(), 2u);

    // Removing adds a delete marker; the history stays.
    const auto marker = take(rec->remove(where));
    EXPECT_NE(marker, 0u);
    expect_error(rec->get(where), ErrorCode::not_found);
    EXPECT_TRUE(take(rec->list("rec", "docs")).objects.empty());
    auto versions = take(rec->list_versions(where));
    ASSERT_EQ(versions.size(), 3u);
    EXPECT_EQ(std::ranges::count_if(versions, [](const rc::ObjectInfo& v) { return v.delete_marker; }), 1);
    EXPECT_EQ(take(at(g1)), "v1");

    // remove_version deletes exactly that version, for good.
    take(rec->remove_version(where, g1));
    expect_error(at(g1), ErrorCode::not_found);
    EXPECT_FALSE(meta->head_version(ObjectRef{{"rec", "docs"}, "readme"}, g1).has_value());
    EXPECT_EQ(take(rec->list_versions(where)).size(), 2u);

    take(rec->remove_version(where, marker));  // dropping the marker brings v2 back
    EXPECT_EQ(take(rec->get(where)), "v2");
    take(rec->remove_version(where, g2));
    expect_error(rec->get(where), ErrorCode::not_found);
    EXPECT_TRUE(take(rec->list_versions(where)).empty());
}

}  // namespace
}  // namespace rmk::e2e
