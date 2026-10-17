#include "rmk/ec/ec.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <numeric>
#include <string>
#include <utility>

namespace rmk::ec {
namespace {

struct Tables {
    std::array<std::uint8_t, 512> exp{};
    std::array<std::uint8_t, 256> log{};
    std::array<std::array<std::uint8_t, 256>, 256> mul{};

    Tables() {
        unsigned x = 1;
        for (unsigned i = 0; i < 255; ++i) {
            exp[i] = static_cast<std::uint8_t>(x);
            log[x] = static_cast<std::uint8_t>(i);
            x <<= 1;
            if (x & 0x100) x ^= 0x11D;
        }
        for (unsigned i = 255; i < 512; ++i) exp[i] = exp[i - 255];
        for (unsigned a = 1; a < 256; ++a)
            for (unsigned b = 1; b < 256; ++b)
                mul[a][b] = exp[log[a] + log[b]];
    }
};

const Tables& tables() {
    static const Tables t;
    return t;
}

std::uint8_t gf_inv(std::uint8_t a) {
    const Tables& t = tables();
    return t.exp[255 - t.log[a]];
}

// dst ^= coef * src
void mul_add(std::byte* dst, const std::byte* src, std::size_t n, std::uint8_t coef) {
    if (coef == 0) return;
    const auto& row = tables().mul[coef];
    for (std::size_t i = 0; i < n; ++i)
        dst[i] ^= static_cast<std::byte>(row[std::to_integer<std::uint8_t>(src[i])]);
}

// Inverts the n x n matrix in place (row-major); false if singular.
bool invert(std::vector<std::uint8_t>& a, std::size_t n) {
    const Tables& t = tables();
    std::vector<std::uint8_t> inv(n * n, 0);
    for (std::size_t i = 0; i < n; ++i) inv[i * n + i] = 1;

    for (std::size_t col = 0; col < n; ++col) {
        std::size_t pivot = col;
        while (pivot < n && a[pivot * n + col] == 0) ++pivot;
        if (pivot == n) return false;
        if (pivot != col) {
            for (std::size_t j = 0; j < n; ++j) {
                std::swap(a[pivot * n + j], a[col * n + j]);
                std::swap(inv[pivot * n + j], inv[col * n + j]);
            }
        }
        const std::uint8_t scale = gf_inv(a[col * n + col]);
        for (std::size_t j = 0; j < n; ++j) {
            a[col * n + j] = t.mul[scale][a[col * n + j]];
            inv[col * n + j] = t.mul[scale][inv[col * n + j]];
        }
        for (std::size_t r = 0; r < n; ++r) {
            const std::uint8_t f = a[r * n + col];
            if (r == col || f == 0) continue;
            for (std::size_t j = 0; j < n; ++j) {
                a[r * n + j] ^= t.mul[f][a[col * n + j]];
                inv[r * n + j] ^= t.mul[f][inv[col * n + j]];
            }
        }
    }
    a = std::move(inv);
    return true;
}

}  // namespace

Codec::Codec(std::uint8_t k, std::uint8_t m, std::vector<std::uint8_t> matrix)
    : k_(k), m_(m), matrix_(std::move(matrix)) {}

Result<Codec> Codec::create(std::uint8_t data_units, std::uint8_t parity_units) {
    if (data_units < 1 || static_cast<unsigned>(data_units) + parity_units > 255)
        return fail(Errc::invalid_argument, "need 1 <= data_units and data_units + parity_units <= 255");

    const std::size_t k = data_units;
    const std::size_t m = parity_units;
    std::vector<std::uint8_t> matrix((k + m) * k, 0);
    for (std::size_t j = 0; j < k; ++j) matrix[j * k + j] = 1;
    for (std::size_t i = 0; i < m; ++i)
        for (std::size_t j = 0; j < k; ++j)
            matrix[(k + i) * k + j] = gf_inv(static_cast<std::uint8_t>((k + i) ^ j));
    return Codec(data_units, parity_units, std::move(matrix));
}

void Codec::encode(std::span<const std::span<const std::byte>> data,
                   std::span<const std::span<std::byte>> parity) const {
    assert(data.size() == k_ && parity.size() == m_);
    const std::size_t len = data.empty() ? 0 : data[0].size();
    for (std::size_t i = 0; i < m_; ++i) {
        assert(parity[i].size() == len);
        std::fill(parity[i].begin(), parity[i].end(), std::byte{0});
        for (std::size_t j = 0; j < k_; ++j) {
            assert(data[j].size() == len);
            mul_add(parity[i].data(), data[j].data(), len, matrix_[(k_ + i) * k_ + j]);
        }
    }
}

Status Codec::reconstruct(std::span<const std::span<std::byte>> shards,
                          std::span<const bool> present) const {
    const std::size_t k = k_;
    const std::size_t n = k + m_;
    if (shards.size() != n || present.size() != n)
        return fail(Errc::invalid_argument, "expected " + std::to_string(n) + " shards");
    const std::size_t len = shards[0].size();
    for (const auto& s : shards)
        if (s.size() != len) return fail(Errc::invalid_argument, "shard lengths differ");

    std::vector<std::size_t> use;
    for (std::size_t i = 0; i < n && use.size() < k; ++i)
        if (present[i]) use.push_back(i);
    if (use.size() < k)
        return fail(Errc::invalid_argument, "fewer than " + std::to_string(k) + " shards present");

    bool missing_data = false;
    for (std::size_t i = 0; i < k; ++i) missing_data = missing_data || !present[i];

    if (missing_data) {
        std::vector<std::uint8_t> sub(k * k);
        for (std::size_t r = 0; r < k; ++r)
            std::copy_n(matrix_.begin() + use[r] * k, k, sub.begin() + r * k);
        if (!invert(sub, k)) return fail(Errc::corruption, "generator submatrix is singular");

        for (std::size_t d = 0; d < k; ++d) {
            if (present[d]) continue;
            std::fill(shards[d].begin(), shards[d].end(), std::byte{0});
            for (std::size_t r = 0; r < k; ++r)
                mul_add(shards[d].data(), shards[use[r]].data(), len, sub[d * k + r]);
        }
    }

    for (std::size_t p = k; p < n; ++p) {
        if (present[p]) continue;
        std::fill(shards[p].begin(), shards[p].end(), std::byte{0});
        for (std::size_t j = 0; j < k; ++j)
            mul_add(shards[p].data(), shards[j].data(), len, matrix_[p * k + j]);
    }
    return {};
}

std::uint64_t shard_length(std::uint64_t logical_size, std::uint8_t data_units) {
    return (logical_size + data_units - 1) / data_units;
}

std::vector<ShardPiece> map_range(std::uint64_t logical_size, std::uint8_t data_units,
                                  std::uint64_t offset, std::uint64_t length) {
    std::vector<ShardPiece> pieces;
    const std::uint64_t sl = shard_length(logical_size, data_units);
    std::uint64_t pos = offset;
    std::uint64_t remaining = length;
    while (remaining > 0) {
        const std::uint64_t in_shard = pos % sl;
        const std::uint64_t take = std::min(remaining, sl - in_shard);
        pieces.push_back({static_cast<std::uint8_t>(pos / sl), in_shard, take, pos});
        pos += take;
        remaining -= take;
    }
    return pieces;
}

}  // namespace rmk::ec
