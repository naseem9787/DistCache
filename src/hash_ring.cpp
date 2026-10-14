#include "hash_ring.hpp"

#include <algorithm>

#include "hash.hpp"

namespace {

// Position of the i-th virtual point of a node. Derived from the NAME only, so a node keeps
// its points when other nodes come and go (that is what makes movement minimal).
uint64_t point_hash(const std::string& name, uint32_t i) {
    std::string label = name;
    label += '#';
    label += std::to_string(i);
    return murmur64a(label.data(), label.size());
}

}  // namespace

HashRing::HashRing(std::vector<Node> nodes, uint32_t vnodes_per_weight)
    : vnodes_(vnodes_per_weight == 0 ? 1 : vnodes_per_weight) {
    std::stable_sort(nodes.begin(), nodes.end(), [](const Node& a, const Node& b) { return a.name < b.name; });
    for (Node& n : nodes) {
        if (!nodes_.empty() && nodes_.back().name == n.name) continue;  // duplicate: first wins
        if (n.weight == 0) n.weight = 1;
        nodes_.push_back(std::move(n));
    }
    size_t total = 0;
    for (const Node& n : nodes_) total += static_cast<size_t>(n.weight) * vnodes_;
    points_.reserve(total);
    for (uint32_t idx = 0; idx < nodes_.size(); ++idx) {
        const uint32_t count = nodes_[idx].weight * vnodes_;
        for (uint32_t i = 0; i < count; ++i) points_.push_back({point_hash(nodes_[idx].name, i), idx});
    }
    // Ties (two points with the same hash) are broken by node index, i.e. by name: deterministic.
    std::sort(points_.begin(), points_.end(), [](const Point& a, const Point& b) {
        return a.hash != b.hash ? a.hash < b.hash : a.node < b.node;
    });
}

bool HashRing::contains(std::string_view name) const {
    return std::any_of(nodes_.begin(), nodes_.end(), [&](const Node& n) { return n.name == name; });
}

int HashRing::owner_index(uint64_t h) const {
    if (points_.empty()) return -1;
    auto it = std::lower_bound(points_.begin(), points_.end(), h,
                               [](const Point& p, uint64_t v) { return p.hash < v; });
    if (it == points_.end()) it = points_.begin();  // wrap around the circle
    return static_cast<int>(it->node);
}

const std::string* HashRing::lookup(std::string_view key) const {
    const int i = owner_index(key_hash(key));
    return i < 0 ? nullptr : &nodes_[static_cast<size_t>(i)].name;
}

std::vector<int> HashRing::owner_indices(uint64_t h, size_t n) const {
    std::vector<int> out;
    if (points_.empty() || n == 0) return out;
    n = std::min(n, nodes_.size());
    size_t start = static_cast<size_t>(
        std::lower_bound(points_.begin(), points_.end(), h, [](const Point& p, uint64_t v) { return p.hash < v; }) -
        points_.begin());
    for (size_t step = 0; step < points_.size() && out.size() < n; ++step) {
        const uint32_t node = points_[(start + step) % points_.size()].node;
        if (std::find(out.begin(), out.end(), static_cast<int>(node)) == out.end()) out.push_back(static_cast<int>(node));
    }
    return out;
}

HashRing HashRing::with_node(Node node) const {
    std::vector<Node> v;
    for (const Node& n : nodes_) if (n.name != node.name) v.push_back(n);
    v.push_back(std::move(node));
    return HashRing(std::move(v), vnodes_);
}

HashRing HashRing::without_node(std::string_view name) const {
    std::vector<Node> v;
    for (const Node& n : nodes_) if (n.name != name) v.push_back(n);
    return HashRing(std::move(v), vnodes_);
}

std::vector<double> HashRing::ownership() const {
    std::vector<double> share(nodes_.size(), 0.0);
    if (points_.empty()) return share;
    constexpr double kCircle = 18446744073709551616.0;  // 2^64
    // The arc ending at points_[i] (from the previous point, exclusive) belongs to points_[i].
    for (size_t i = 0; i < points_.size(); ++i) {
        const uint64_t prev = points_[i == 0 ? points_.size() - 1 : i - 1].hash;
        const uint64_t arc = points_[i].hash - prev;  // unsigned wrap-around is the circle distance
        double frac = static_cast<double>(arc) / kCircle;
        if (points_.size() == 1) frac = 1.0;          // a single point owns the whole circle
        share[points_[i].node] += frac;
    }
    return share;
}
