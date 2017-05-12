/*******************************************************************************
 * examples/bfs/bfs.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Robert Williger <willigerrobert@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include "bfs.hpp"

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/group_by_iterator.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/min.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/common/cmdline_parser.hpp>

#include <algorithm>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

using thrill::DIA;
using namespace examples::bfs;

struct BfsResult {
    DIA<BfsNode>          graph;
    std::vector<TreeInfo> treeInfos;
};

// load graph from file
DIA<BfsNode> LoadBFSGraph(thrill::Context& ctx, size_t& graphSize,
                          const std::string& path, VertexId startIndex) {

    // read graph lines from file and add index
    auto lines =
        ReadLines(ctx, path)
        .ZipWithIndex(
            [](const std::string& node, const size_t& index) {
                return std::make_pair(node, index);
            });

    auto size = lines.SizeFuture();

    // parse lines into BfsNode structs
    auto graph = lines.Map(
        [startIndex](const std::pair<std::string, size_t>& input) {
            std::istringstream iss(input.first);
            BfsNode node;

            node.edges = EdgeList(std::istream_iterator<VertexId>(iss),
                                  std::istream_iterator<VertexId>());
            node.nodeIndex = input.second;

            if (node.nodeIndex == startIndex)
            {
                node.parent = startIndex;
                node.level = 0;
                node.treeIndex = 0;
            }

            return node;
        });

    graphSize = size.get();

    return graph.Cache();
}

// returns true if new nodes have been possibly added to the next BFS level
bool BFSNextLevel(DIA<BfsNode>& graph, size_t& currentLevel,
                  const size_t currentTreeIndex, const size_t graphSize) {

    auto neighbors =
        graph
        .FlatMap<NodeParentPair>(
            [=](const BfsNode& node, auto emit) {
                if (node.level == currentLevel && node.treeIndex == currentTreeIndex) {
                    for (auto neighbor : node.edges) {
                        emit(NodeParentPair { neighbor, node.nodeIndex });
                    }
                }
            });

    if (neighbors.Size() == 0)
        return false;

    auto reducedNeighbors = neighbors.ReduceToIndex(
        [](const NodeParentPair& pair) {
            return pair.node == INVALID ? 0 : pair.node;
        },
        [](const NodeParentPair& pair1, const NodeParentPair& pair2) {
            // pair1.node is INVALID iff it is the default constructed value for
            // its index
            return pair1.node == INVALID ? pair2 : pair1;
        },
        graphSize);

    currentLevel++;

    graph = Zip(
        [=](BfsNode node, NodeParentPair pair) {
            if (pair.node != INVALID && node.level == INVALID) {
                node.level = currentLevel;
                node.parent = pair.parent;
                node.treeIndex = currentTreeIndex;
            }
            return node;
        },
        graph,
        reducedNeighbors);

    return true;
}

// returns true if not all nodes have been reached yet
bool PrepareNextTree(DIA<BfsNode>& graph, size_t& startIndex,
                     const size_t currentTreeIndex) {

    BfsNode validDummy;
    validDummy.level = 0;

    // find a node which has not yet been traversed (level == INVALID)
    auto node = graph.Sum(
        [](const BfsNode& node1, const BfsNode& node2) {
            return node1.level == INVALID ? node1 : node2;
        },
        validDummy);

    if (node.level != INVALID)
        return false;   // all nodes have already been traversed

    startIndex = node.nodeIndex;

    // initialize start index
    graph = graph.Map(
        [=](BfsNode node) {
            if (node.nodeIndex == startIndex) {
                node.level = 0;
                node.parent = node.nodeIndex;
                node.treeIndex = currentTreeIndex;
            }

            return node;
        });

    return true;
}

void outputBFSResult(DIA<BfsNode>& graph, size_t num_trees,
                     std::string output_path) {

    if (output_path.empty())
        return;

    auto grouped =
        graph
        .Filter([](const BfsNode& i) { return i.treeIndex != INVALID; })
        .template GroupToIndex<std::string>(
            [](const BfsNode& i) { return i.treeIndex; },
            [](auto& iter, const size_t& key) mutable {
                std::ostringstream str;
                str << "BFS tree " << key << ":\n";

                std::vector<std::pair<size_t, size_t> > nodes;

                while (iter.HasNext()) {
                    BfsNode node = iter.Next();
                    nodes.emplace_back(node.level, node.nodeIndex);
                }

                std::sort(nodes.begin(), nodes.end(),
                          [](const std::pair<size_t, size_t>& l,
                             const std::pair<size_t, size_t>& r) {
                              return l.first < r.first;
                          });

                size_t lastLevel = 0;
                str << "0: ";
                for (const auto& node : nodes)
                {
                    if (lastLevel != node.first)
                    {
                        lastLevel++;
                        str << '\n' << lastLevel << ": ";
                    }
                    str << node.second << ' ';
                }
                str << '\n';

                return str.str();
            },
            num_trees);

    grouped.WriteLines(output_path);
}

/*!
 * runs A BFS on graph starting at startIndex. If full_bfs is true then all
 * nodes will eventually be reached possibly resulting in a forest instead of a
 * simple tree
*/
BfsResult BFS(DIA<BfsNode>& graph, size_t graphSize,
              VertexId startIndex, bool full_bfs = false) {

    std::vector<TreeInfo> treeInfos;
    size_t currentTreeIndex = 0;

    do {
        size_t currentLevel = 0;

        while (BFSNextLevel(graph, currentLevel, currentTreeIndex, graphSize))
        { }

        treeInfos.emplace_back(TreeInfo { startIndex, currentLevel });

        currentTreeIndex++;
    } while (full_bfs && PrepareNextTree(graph, startIndex, currentTreeIndex));

    return BfsResult({ graph, treeInfos });
}

BfsResult BFS(thrill::Context& ctx,
              std::string input_path, std::string output_path,
              VertexId startIndex, bool full_bfs = false) {

    size_t graphSize;
    DIA<BfsNode> graph = LoadBFSGraph(ctx, graphSize, input_path, startIndex);

    auto result = BFS(graph, graphSize, startIndex, full_bfs);
    outputBFSResult(result.graph, result.treeInfos.size(), output_path);
    return result;
}

size_t doubleSweepDiameter(
    thrill::Context& ctx,
    std::string input_path, std::string output_path, std::string pathOut2,
    VertexId startIndex) {

    size_t graphSize;
    DIA<BfsNode> graph = LoadBFSGraph(ctx, graphSize, input_path, startIndex);
    auto firstBFS = BFS(graph, graphSize, startIndex);

    outputBFSResult(firstBFS.graph, firstBFS.treeInfos.size(), output_path);

    // choose node from last level as new start index
    auto targetLevel = firstBFS.treeInfos.front().levels - 1;
    startIndex =
        firstBFS.graph
        .Filter([=](const BfsNode& node) {
                    return node.level == targetLevel;
                })
        .Map([](const BfsNode& node) {
                 return node.nodeIndex;
             })
        .Min(INVALID);

    // create clean graph with new start index
    DIA<BfsNode> secondGraph =
        graph
        .Map([=](const BfsNode& node) {
                 BfsNode emitNode;
                 emitNode.nodeIndex = node.nodeIndex;
                 emitNode.edges = node.edges;

                 if (emitNode.nodeIndex == startIndex)
                 {
                     emitNode.parent = startIndex;
                     emitNode.level = 0;
                     emitNode.treeIndex = 0;
                 }
                 return emitNode;
             });

    auto secondBFS = BFS(secondGraph, graphSize, startIndex);

    auto diameter = secondBFS.treeInfos.front().levels;

    outputBFSResult(secondBFS.graph, secondBFS.treeInfos.size(), pathOut2);

    return diameter;
}

int main(int argc, char* argv[]) {

    thrill::common::CmdlineParser clp;

    std::string input_path;
    clp.AddParamString("input", input_path,
                       "read graph from this file");

    std::string output_path;
    clp.AddOptParamString("output", output_path,
                          "output bfs tree to this file");

    bool full_bfs = false;
    clp.AddFlag('f', "full-bfs", full_bfs,
                "traverse all nodes even if this produces a disconnected bfs forest");

    if (!clp.Process(argc, argv))
        return -1;

    clp.PrintResult();

    return thrill::Run(
        [&](thrill::Context& ctx) {
            BFS(ctx, input_path, output_path, /* startIndex */ 0, full_bfs);
            // doubleSweepDiameter(ctx, input_path, output_path, "bfs2.graph", 1);
        });
}

/******************************************************************************/
