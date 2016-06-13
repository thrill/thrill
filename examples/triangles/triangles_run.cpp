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
#include <examples/triangles/triangles.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <cmath>
#include <functional>
#include <utility>
#include <vector>

using namespace thrill;              // NOLINT
using examples::page_rank::ZipfGraphGen;
static constexpr bool debug = true;

using Node = size_t;
using Edge = std::pair<Node, Node>;


static size_t CountTrianglesPerLine(
	api::Context& ctx,
	const std::vector<std::string>& input_path) {
	auto edges = ReadLines(ctx, input_path).template FlatMap<Edge>(
		[](const std::string& input, auto emit) {
			// parse "source\ttarget\n" lines
			char* endptr;
		    size_t src = std::strtoul(input.c_str(), &endptr, 10);
			die_unless(endptr && *endptr == '\t' &&
					   "Could not parse src tgt line");
			size_t tgt = std::strtoul(endptr + 1, &endptr, 10);
			die_unless(endptr && *endptr == 0 &&
					   "Could not parse src tgt line");

			if (src < tgt) {
				emit(std::make_pair(src, tgt));
			} else {
				if (src > tgt) {
					emit(std::make_pair(tgt, src));
				} 
				//self-loop: do not emit;
			}			
		});
	
	return examples::triangles::CountTriangles(edges);
}
 
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
		});

	return examples::triangles::CountTriangles(edges);
}

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;


    bool generate = false;
    clp.AddFlag('g', "generate", generate,
                "generate graph data, set input = #pages");

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

	std::vector<std::string> input_path;
    clp.AddParamStringlist("input", input_path,
                           "input file pattern(s)");

    if (!clp.Process(argc, argv)) {
        return -1;
    }



    die_unless(!generate || input_path.size() == 1);

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
			size_t triangles;
			if (generate) {
				triangles = CountTrianglesGenerated(
					ctx, gg, num_vertices);
			} else {
				triangles = CountTrianglesPerLine(
					ctx, input_path);
			}

			LOG1 << "#triangles=" << triangles;
			return triangles;
        });
}

/******************************************************************************/
