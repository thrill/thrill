/*******************************************************************************
 * thrill/api/stats_graph.hpp
 *
 * Simple Graph to represent execution stages.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_STATS_GRAPH_HEADER
#define THRILL_API_STATS_GRAPH_HEADER

#include <thrill/common/logger.hpp>

#include <fstream>
#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

enum class DIANodeType {
    DOP,
    ACTION,
    COLLAPSE,
    CACHE,
    LAMBDA
};

enum class LogType {
    EXECUTION,
    NETWORK,
    INFO
};

class StatsNode
{
public:
    /*!
     * Create a new stats node.
     *
     * \param label Label of the node in the graphical representation.
     * \param type Switch for choosing the layout of the node.
     */
    StatsNode(size_t id, const std::string& label, const DIANodeType& type)
        : id_(id),
          label_(label),
          type_(type)
    { }

    //! Delete copy-constructor
    StatsNode(const StatsNode& other) = delete;

    /*!
     * Add a new neighbor to the stats node.
     *
     * \param neighbor Neighboring node.
     */
    void AddNeighbor(StatsNode* neighbor) {
        adjacent_nodes_.push_back(neighbor);
    }

    /*!
     * Returns the current neighbors.
     */
    const std::vector<StatsNode*> & adjacent_nodes() const {
        return adjacent_nodes_;
    }

    /*!
     * Returns the type of the node.
     */
    const size_t & id() const {
        return id_;
    }

    /*!
     * Returns the label of the node.
     */
    std::string label() const {
        return label_;
    }

    /*!
     * Returns the type of the node.
     */
    const DIANodeType & type() const {
        return type_;
    }

    /*!
     * Add a new message (label) to the graphical representation.
     *
     * \param msg Message to be displayed.
     */
    void AddStatsMsg(const std::string& msg, const LogType& type) {
        stats_msg_.push_back(msg);
        switch (type) {
        case LogType::EXECUTION:
            sLOG1 << "[Execution]" << msg;
            break;
        case LogType::NETWORK:
            sLOG1 << "[Network]" << msg;
            break;
        case LogType::INFO:
            sLOG1 << "[Info]" << msg;
            break;
        default:
            break;
        }
    }

    /*!
     * Print the label of the node.
     */
    friend std::ostream& operator << (std::ostream& os, const StatsNode& c) {
        return os << c.label_;
    }

    /*!
     * Set the node style according to the nodes type.
     */
    std::string NodeStyle() const {
        std::string style = label_ + " [";
        switch (type_) {
        case DIANodeType::DOP:
            style += "style=filled, fillcolor=red, shape=box";
            break;
        case DIANodeType::ACTION:
            style += "style=filled, fillcolor=yellow, shape=diamond";
            break;
        case DIANodeType::CACHE:
        case DIANodeType::COLLAPSE:
            style += "style=filled, fillcolor=blue, shape=hexagon";
            break;
        default:
            break;
        }
        style += StatsLabels();
        style += "]";

        return style;
    }

    std::string StatsLabels() const {
        std::string labels = "";
        for (const std::string& msg : stats_msg_) {
            labels += ", xlabel=\"" + msg + "\"";
        }
        return labels;
    }

private:
    //! Adjacent nodes
    std::vector<StatsNode*> adjacent_nodes_;

    //! Serial number as id of node
    size_t id_;

    //! Label of node
    std::string label_;

    //! Type of node
    DIANodeType type_;

    //! Stats messages
    std::vector<std::string> stats_msg_;
};

class StatsGraph
{
public:
    /*!
     * Create a new stats graph.
     * The node counter is initialized to zero.
     */
    StatsGraph() : nodes_id_(0) { }

    //! Delete copy-constructor
    StatsGraph(const StatsGraph& other) = delete;

    /*!
     * Clear all nodes.
     */
    virtual ~StatsGraph() {
        for (auto node : nodes_) {
            delete node;
            node = nullptr;
        }
    }

    /*!
     * Add a new node with a given label and type.
     *
     * \param label Label of the new node.
     * \param type Type of the new node.
     *
     * \return Pointer to the new node.
     */
    StatsNode * AddNode(const std::string& label, const DIANodeType& type) {
        size_t id = nodes_id_++;
        StatsNode* node = new StatsNode(id, label + std::to_string(id), type);
        nodes_.push_back(node);
        return node;
    }

    /*!
     * Add a new directed edge between two given nodes.
     *
     * \param source Source node.
     * \param target Target node.
     */
    void AddEdge(StatsNode* source, StatsNode* target) {
        for (const auto& node : nodes_) {
            if (source == node) node->AddNeighbor(target);
        }
    }

    /*!
     * Build the layout based on the nodes styles.
     *
     * \param path Filepath where the layout will be saved.
     */
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

    //! Current node id.
    size_t nodes_id_;
};

} // namespace api
} // namespace thrill

#endif // !THRILL_API_STATS_GRAPH_HEADER

/******************************************************************************/
