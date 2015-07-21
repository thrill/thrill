/*******************************************************************************
 * tests/api/graph_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/graph.hpp>
#include <c7a/common/logger.hpp>

#include <gtest/gtest.h>

#include <string>
#include <vector>

TEST(API, SimpleGraphTest) {
    using c7a::api::StatsGraph;
    using c7a::api::StatsNode;

    static const bool debug = false;

    StatsGraph g("graph_test.out");
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
    g.BuildLayout();

    return;
}

/******************************************************************************/
