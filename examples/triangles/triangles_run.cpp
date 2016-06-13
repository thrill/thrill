/*******************************************************************************
 * examples/triangles/triangles.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 * 
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank/zipf_graph_gen.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/join.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <cmath>
#include <functional>
#include <utility>
#include <vector>
#include <bits/functional_hash.h>

using namespace thrill;              // NOLINT
using examples::page_rank::ZipfGraphGen;
static constexpr bool debug = true;

using Node = size_t;
using Edge = std::pair<Node, Node>;

namespace std { //i am sorry.

	template <> struct hash<Edge> {
		size_t operator()(const Edge& e) const {
			return hash<Node>()(e.first) ^ hash<Node>()(e.second);
		}
	};

} //namespace std
 
static size_t CountTrianglesGenerated(
	api::Context& ctx,
	const ZipfGraphGen& base_graph_gen,
	const size_t& num_vertices) {

	auto edge_lists = Generate(
		ctx,
		[graph_gen = ZipfGraphGen(base_graph_gen, num_vertices),
         rng = std::default_random_engine(std::random_device { } ())](
            size_t index) mutable {
            return std::make_pair(index, graph_gen.GenerateOutgoing(rng));
        },
        num_vertices);

	auto edges = edge_lists.template FlatMap<Edge>(
		[](std::pair<Node, std::vector<Node>> neighbors, auto emit) {
			for (auto neighbor : neighbors.second) {
				if (neighbors.first > neighbor) {
					emit(std::make_pair(neighbor, neighbors.first));
				} else {
					if (neighbors.first < neighbor) {
						emit(std::make_pair(neighbors.first, neighbor));
					}
					//self-loop: do not emit
				}
			}
		}).Cache();

	auto edges_length_2 = edges.InnerJoinWith(edges, [](const Edge& e) {
			return e.second;
		}, [](const Edge& e) {
			return e.first;
		}, [](const Edge& e1, const Edge& e2) {
			return std::make_pair(e1.first, e2.second);
		});

	auto triangles = edges_length_2.InnerJoinWith(edges, [](const Edge& e) {
			return e;
		}, [](const Edge& e) {
			return e;
		}, [](const Edge& /* e1 */, const Edge& /* e2 */) {
			return (size_t)1;
		});

	return triangles.Size();
}

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

	size_t num_vertices;

	clp.AddSizeT('n', "vertices", num_vertices, "Number of vertices");

    // Graph Generator
    ZipfGraphGen gg(1);

    clp.AddDouble(0, "size_mean", gg.size_mean,
                  "generated: mean of number of outgoing links, "
                  "default: " + std::to_string(gg.size_mean));

    clp.AddDouble(0, "size_var", gg.size_var,
                  "generated: variance of number of outgoing links, "
                  "default: " + std::to_string(gg.size_var));

    clp.AddDouble(0, "link_scale", gg.link_zipf_scale,
                  "generated: Zipf scale parameter for outgoing links, "
                  "default: " + std::to_string(gg.link_zipf_scale));

    clp.AddDouble(0, "link_exponent", gg.link_zipf_exponent,
                  "generated: Zipf exponent parameter for outgoing links, "
                  "default: " + std::to_string(gg.link_zipf_exponent));

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
		    size_t triangles = CountTrianglesGenerated(
				ctx, gg, num_vertices);

			LOG1 << "#triangles=" << triangles;
        });
}

/******************************************************************************/
