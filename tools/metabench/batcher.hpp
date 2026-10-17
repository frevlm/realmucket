// The single writer thread. Clients hand it a transaction and block until it has been applied (and
// flushed, if it is durable). Whatever queued up while the previous batch was being committed goes
// into the next one, so one commit serves many transactions.
//
// For engines whose flush is a step of its own (can_decouple), applying and flushing run on two
// threads: the applier never waits for a flush, non-durable transactions are released as soon as
// they are applied, and the flusher makes everything applied so far durable with one flush, then
// releases the durable transactions waiting for it (group commit).
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

#include "engine.hpp"

namespace metabench {

class Batcher {
public:
    Batcher(Engine& engine, std::size_t max_batch, bool decouple)
        : engine_(engine), max_batch_(max_batch), decoupled_(decouple && engine.can_decouple()) {
        applier_ = std::thread([this] { run_applier(); });
        if (decoupled_)
            flusher_ = std::thread([this] { run_flusher(); });
    }

    ~Batcher() {
        {
            std::lock_guard lock(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        flush_cv_.notify_all();
        applier_.join();
        if (flusher_.joinable())
            flusher_.join();
    }

    bool decoupled() const { return decoupled_; }

    // Blocks until the transaction has been applied (and flushed, if durable). False if it aborted.
    bool submit(const TxBody& body, bool durable) {
        Item item(&body, durable);
        std::unique_lock lock(mu_);
        queue_.push_back(&item);
        cv_.notify_all();
        item.cv.wait(lock, [&] { return item.done; });
        return item.ok;
    }

    std::uint64_t batches() const { return batches_.load(); }
    std::uint64_t flushes() const { return flushes_.load(); }
    std::uint64_t transactions() const { return txs_.load(); }

private:
    struct Item {
        Item(const TxBody* b, bool d) : body(b), durable(d) {}
        const TxBody* body;
        bool durable;
        bool done = false;
        bool ok = false;
        std::uint64_t seq = 0;
        std::condition_variable cv;
    };

    void run_applier() {
        std::unique_lock lock(mu_);
        for (;;) {
            cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
            if (queue_.empty())
                return;  // stopping, nothing left
            std::vector<Item*> batch;
            while (!queue_.empty() && batch.size() < max_batch_) {
                batch.push_back(queue_.front());
                queue_.pop_front();
            }
            lock.unlock();

            std::vector<const TxBody*> bodies;
            bool durable = false;
            for (Item* item : batch) {
                bodies.push_back(item->body);
                durable |= item->durable;
            }
            std::vector<char> ok(batch.size(), 0);
            if (decoupled_)
                engine_.apply_unsynced(bodies, ok);
            else
                engine_.apply_batch(bodies, ok, durable);
            ++batches_;
            txs_ += batch.size();
            if (!decoupled_ && durable)
                ++flushes_;

            lock.lock();
            const std::uint64_t seq = ++applied_;
            for (std::size_t i = 0; i < batch.size(); ++i) {
                batch[i]->ok = ok[i] != 0;
                batch[i]->seq = seq;
                if (decoupled_ && batch[i]->durable) {
                    waiting_.push_back(batch[i]);  // released by the flusher
                    continue;
                }
                batch[i]->done = true;
                batch[i]->cv.notify_one();
            }
            if (decoupled_ && !waiting_.empty())
                flush_cv_.notify_all();
        }
    }

    void run_flusher() {
        std::unique_lock lock(mu_);
        for (;;) {
            flush_cv_.wait(lock, [&] { return stop_ || !waiting_.empty(); });
            if (waiting_.empty())
                return;
            const std::uint64_t target = applied_;  // everything applied so far
            lock.unlock();
            engine_.flush();
            ++flushes_;
            lock.lock();
            for (auto it = waiting_.begin(); it != waiting_.end();) {
                if ((*it)->seq <= target) {
                    (*it)->done = true;
                    (*it)->cv.notify_one();
                    it = waiting_.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }

    Engine& engine_;
    std::size_t max_batch_;
    bool decoupled_;
    std::mutex mu_;
    std::condition_variable cv_, flush_cv_;
    std::deque<Item*> queue_;
    std::deque<Item*> waiting_;  // applied, durable, not yet flushed
    std::uint64_t applied_ = 0;
    bool stop_ = false;
    std::atomic<std::uint64_t> batches_{0}, flushes_{0}, txs_{0};
    std::thread applier_, flusher_;
};

}  // namespace metabench
