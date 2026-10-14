#pragma once
// Consistent hash ring with virtual nodes.
//
//   hash space: 0 .. 2^64-1, treated as a circle.
//   each node places many points ("virtual nodes") on the circle at hash(name "#" i).
//   a key belongs to the node owning the first point at or after hash(key), wrapping around.
//
// Why a ring instead of hash(key) % N:
//   modulo remaps almost every key when N changes (~N/(N+1) of them); on a ring, adding a node
//   takes over only the arcs just before its own points, so ~1/(N+1) of the keys move, and
//   every key that moves goes TO the new node (never between two old nodes). Removing a node
//   hands its keys to the neighbours and nothing else moves.
// Why virtual nodes: with one point per node the arcs have wildly different lengths (the load
//   per node is exponential-ish). Many points per node average that out: the relative standard
//   deviation of a node's share falls like 1/sqrt(points). 160 points per node (the ketama
//   default) gives about 8 %.
// Weights: a node with weight w gets w * vnodes_per_weight points, so w times the keys.
//
// A HashRing is immutable after construction and therefore safe to read from any number of
// threads without locking. "Changing" the ring means building a new one (with_node /
// without_node) and swapping the pointer the readers use. The ring depends only on the SET of
// (name, weight) pairs and vnodes_per_weight, never on insertion order: two processes given the
// same membership compute identical placements.
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

class HashRing {
public:
    struct Node {
        std::string name;
        uint32_t weight = 1;
    };
    static constexpr uint32_t kDefaultVnodes = 160;

    // Duplicate names are ignored (the first wins); weight 0 is treated as 1.
    explicit HashRing(std::vector<Node> nodes = {}, uint32_t vnodes_per_weight = kDefaultVnodes);

    bool empty() const { return nodes_.empty(); }
    size_t node_count() const { return nodes_.size(); }
    const std::vector<Node>& nodes() const { return nodes_; }  // sorted by name
    uint32_t vnodes_per_weight() const { return vnodes_; }
    size_t point_count() const { return points_.size(); }
    bool contains(std::string_view name) const;

    // Index (into nodes()) of the owner of ring position `h`; -1 when the ring is empty.
    int owner_index(uint64_t h) const;
    // Name of the node that owns `key` (hash tags honoured); nullptr when the ring is empty.
    const std::string* lookup(std::string_view key) const;
    // Up to n DISTINCT nodes in clockwise order starting with the owner: the owner followed by
    // the nodes that would take over its keys if it left (used by replication).
    std::vector<int> owner_indices(uint64_t h, size_t n) const;

    // Derived rings. with_node replaces an existing node of the same name.
    HashRing with_node(Node node) const;
    HashRing without_node(std::string_view name) const;

    // Exact share of the hash circle owned by each node (sums to 1), same order as nodes().
    std::vector<double> ownership() const;

private:
    struct Point {
        uint64_t hash;
        uint32_t node;  // index into nodes_
    };
    std::vector<Node> nodes_;
    uint32_t vnodes_;
    std::vector<Point> points_;  // sorted by (hash, node index)
};
