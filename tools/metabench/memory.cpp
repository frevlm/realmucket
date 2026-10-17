// The ceiling: an ordered map in memory. Costs only the batching thread and the transaction logic.
#include <map>
#include <shared_mutex>

#include "engine.hpp"

namespace metabench {
namespace {

class MemoryEngine final : public Engine {
public:
    std::string name() const override { return "memory"; }

    void apply_batch(const std::vector<const TxBody*>& bodies, std::vector<char>& ok, bool) override {
        std::unique_lock lock(mu_);
        for (std::size_t i = 0; i < bodies.size(); ++i) {
            UndoTx tx(map_);
            ok[i] = (*bodies[i])(tx) ? 1 : 0;
            if (!ok[i])
                tx.rollback();
        }
    }

    bool can_decouple() const override { return true; }
    void apply_unsynced(const std::vector<const TxBody*>& bodies, std::vector<char>& ok) override {
        apply_batch(bodies, ok, false);
    }
    void flush() override {}

    std::optional<std::string> read_get(std::string_view key) override {
        std::shared_lock lock(mu_);
        auto it = map_.find(std::string(key));
        return it == map_.end() ? std::nullopt : std::optional<std::string>(it->second);
    }

    std::size_t read_scan(std::string_view from, std::string_view to, std::size_t limit) override {
        std::shared_lock lock(mu_);
        std::size_t n = 0;
        for (auto it = map_.lower_bound(std::string(from)); it != map_.end() && it->first < to && n < limit; ++it)
            ++n;
        return n;
    }

    std::uint64_t disk_bytes() const override { return 0; }

private:
    using Map = std::map<std::string, std::string, std::less<>>;

    class UndoTx final : public Tx {
    public:
        explicit UndoTx(Map& map) : map_(map) {}
        std::optional<std::string> get(std::string_view key) override {
            auto it = map_.find(key);
            return it == map_.end() ? std::nullopt : std::optional<std::string>(it->second);
        }
        void put(std::string_view key, std::string_view value) override {
            remember(key);
            map_[std::string(key)] = std::string(value);
        }
        void del(std::string_view key) override {
            remember(key);
            map_.erase(map_.find(key));
        }
        void rollback() {
            for (auto it = undo_.rbegin(); it != undo_.rend(); ++it) {
                if (it->second)
                    map_[it->first] = *it->second;
                else
                    map_.erase(it->first);
            }
        }

    private:
        void remember(std::string_view key) {
            auto it = map_.find(key);
            undo_.emplace_back(std::string(key), it == map_.end() ? std::nullopt : std::optional<std::string>(it->second));
        }
        Map& map_;
        std::vector<std::pair<std::string, std::optional<std::string>>> undo_;
    };

    mutable std::shared_mutex mu_;
    Map map_;
};

}  // namespace

std::unique_ptr<Engine> make_memory(const Options&) {
    return std::make_unique<MemoryEngine>();
}

std::uint64_t directory_bytes(const std::filesystem::path& dir) {
    std::uint64_t total = 0;
    std::error_code ec;
    for (const auto& item : std::filesystem::recursive_directory_iterator(dir, ec))
        if (item.is_regular_file(ec))
            total += item.file_size(ec);
    return total;
}

}  // namespace metabench
