/*******************************************************************************
 * examples/bfs/bfs.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Robert Williger <willigerrobert@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <cereal/types/vector.hpp>
#include <thrill/data/serialization_cereal.hpp>

#include <limits>
#include <vector>

namespace bfs {

const size_t INVALID = std::numeric_limits<size_t>::max();
using Node = size_t;
using EdgeList = std::vector<Node>;

struct NodeParentPair
{
    Node node = INVALID;
    Node parent = INVALID;

    NodeParentPair() = default;
    NodeParentPair(Node node, Node parent) : node(node), parent(parent) { }

    template <typename Archive>
    void serialize(Archive& archive) {
        archive(node, parent);
    }
};

std::ostream& operator << (std::ostream& os, const NodeParentPair& pair) {
    os << '(' << pair.node << ',' << pair.parent << ')';
    return os;
}

class BfsNode
{
public:

    EdgeList edges;
    Node nodeIndex = INVALID;
    size_t treeIndex = INVALID;
    Node parent = INVALID;
    size_t level = INVALID;

    BfsNode() = default;

    template <typename Archive>
    void serialize(Archive& archive) {
        archive(edges, nodeIndex, treeIndex, parent, level);
    }
};

std::ostream& operator << (std::ostream& os, const BfsNode& node) {
    os << "(" << node.nodeIndex << ": [";

    if (!node.edges.empty()) {
        os << node.edges[0];
        for (size_t i = 1; i != node.edges.size(); ++i)
            os << ',' << node.edges[i];
    }

    os << "], par: " << node.parent << ", lvl: " << node.treeIndex << '_' << node.level << ')';

    return os;
}

struct TreeInfo {
    size_t startIndex = INVALID;
    size_t levels = INVALID;

    TreeInfo() = default;
    TreeInfo(size_t startIndex) : TreeInfo(startIndex, INVALID){}
    TreeInfo(size_t startIndex, size_t levels) : startIndex(startIndex), levels(levels){}
};

} // namespace bfs
