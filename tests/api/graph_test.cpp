/*******************************************************************************
 * tests/api/graph_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/stats_graph.hpp>
#include <c7a/common/logger.hpp>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace c7a;
using c7a::api::StatsGraph;
using c7a::api::StatsNode;

TEST(API, SimpleGraphTest) {

    StatsGraph g;
    StatsNode* a = new StatsNode("LOp1");
    StatsNode* b = new StatsNode("LOp2");
    StatsNode* c = new StatsNode("Sum");
    StatsNode* d = new StatsNode("AllGather");
    g.AddNode(a);
    g.AddNode(b);
    g.AddNode(c);
    g.AddNode(d);
    g.AddEdge(a, b);
    g.AddEdge(b, c);
    g.AddEdge(b, d);
    g.BuildLayout("simple_graph.out");

    return;
}

/******************************************************************************/
