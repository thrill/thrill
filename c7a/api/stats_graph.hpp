/*******************************************************************************
 * c7a/api/stats_graph.hpp
 *
 * Simple Graph to represent Execution Stages
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_STATS_GRAPH_HEADER
#define C7A_API_STATS_GRAPH_HEADER

#include <c7a/common/logger.hpp>

#include <fstream>
#include <string>
#include <vector>

namespace c7a {
namespace api {

class StatsNode
{
public:
    StatsNode(const std::string& label, const std::string& type)
        : label_(label),
          type_(type)
    { }

    StatsNode(const StatsNode& other) = delete;

    void AddNeighbor(StatsNode* neighbor) {
        adjacent_nodes_.push_back(neighbor);
    }

    const std::vector<StatsNode*> & adjacent_nodes() const {
        return adjacent_nodes_;
    }

    std::string type() const {
        return type_;
    }

    std::string label() const {
        return label_;
    }

    void AddStatsMsg(const std::string& msg) {
        stats_msg_.push_back(msg);
    }

    friend std::ostream& operator << (std::ostream& os, const StatsNode& c) {
        return os << c.label_;
    }

    std::string NodeStyle() const {
        if (type_.compare("DOp") == 0) {
            return label_ + " [style=filled, fillcolor=red, shape=box]";
        } else if (type_.compare("Action") == 0) {
            return label_ + " [style=filled, fillcolor=yellow, shape=diamond]";
        } else if (type_.compare("LOp") == 0) {
            return label_ + " [style=filled, fillcolor=blue, shape=hexagon]";
        } else {
            return label_;
        }
    }

private:
    //! Adjacent nodes
    std::vector<StatsNode*> adjacent_nodes_;

    //! Label of node
    std::string label_;

    //! Type of node
    std::string type_;

    //! Stats messages
    std::vector<std::string> stats_msg_;
};

class StatsGraph
{
public:
    StatsGraph() : nodes_id_(0) { }

    StatsGraph(const StatsGraph& other) = delete;

    virtual ~StatsGraph() { }

    StatsNode* AddNode(const std::string& label, const std::string& type) {
        StatsNode* node = new StatsNode(label + std::to_string(nodes_id_++), type);
        nodes_.push_back(node);
        return node;
    }

    void AddEdge(StatsNode* source, StatsNode* target) {
        for (const auto& node : nodes_) {
            if (source == node) node->AddNeighbor(target);
        }
    }

    void BuildLayout(const std::string& path) {
        std::ofstream file(path);
        file << "digraph {\n";
        for (const auto& node : nodes_) {
            file << "\t" << node->NodeStyle() << ";\n"; 
        }
        file << "\n";
        for (const auto& node : nodes_) {
            for (const auto& neighbor : node->adjacent_nodes()) {
                file << "\t" << *node << " -> " << *neighbor << ";\n";
            }
        }
        file << "}";
        file.close();
    }

private:
    //! Nodes of the graph.
    std::vector<StatsNode*> nodes_;

    size_t nodes_id_;
};

} // namespace api
} // namespace c7a

#endif // !C7A_API_STATS_GRAPH_HEADER

/******************************************************************************/
