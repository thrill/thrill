/*******************************************************************************
 * c7a/api/graph.hpp
 *
 * Simple Graph to represent Execution Stages
 *
 * Part of Project c7a.
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_GRAPH_HEADER
#define C7A_API_GRAPH_HEADER

#include <c7a/common/logger.hpp>

#include <string>
#include <vector>
#include <fstream>

namespace c7a {
namespace api {

class StatsNode {
public:
    StatsNode(const std::string& type) 
        : type_(type) { }

    StatsNode(const StatsNode& other) = delete; 

    void AddNeighbor(StatsNode* const neighbor) {
        adjacent_nodes_.push_back(neighbor);
    }

    const std::vector<StatsNode*> & adjacent_nodes() const {
        return adjacent_nodes_;
    }

    std::string type() const {
        return type_;
    }

    friend std::ostream& operator << (std::ostream& os, const StatsNode& c) {
        return os << c.type_;
    }

private:
    //! Adjacent nodes
    std::vector<StatsNode*> adjacent_nodes_;

    //! Type of node
    std::string type_; 
};

class StatsGraph
{
public:
    StatsGraph(const std::string& path_out) 
        : path_out_(path_out),
          file_(path_out_) { }

    StatsGraph(const StatsGraph& other) = delete; 

    virtual ~StatsGraph() { 
        for (auto node : nodes_) {
            delete node;
            node = NULL;
        }
    }

    void AddNode(StatsNode* const node) {
        nodes_.push_back(node);
    }

    void AddEdge(StatsNode* const source, StatsNode* const target) {
        for (auto& node : nodes_) {
            if (source == node) node->AddNeighbor(target);
        }
    }

    void BuildLayout() { 
        file_ << "digraph {\n";
        for (const auto& node : nodes_) {
            for (const auto& neighbor : node->adjacent_nodes()) {
                file_ << "\t" << *node << " -> " << *neighbor << ";\n";
            }
        }
        file_ << "}";
        file_.close();
    }

private:
    //! Nodes of the graph.
    std::vector<StatsNode*> nodes_;

    //! Path of the output file.
    std::string path_out_;

    //! File to write to
    std::ofstream file_;
};

} // namespace api
} // namespace c7a

#endif // !C7A_API_GRAPH_HEADER

/******************************************************************************/
