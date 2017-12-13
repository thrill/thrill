/*******************************************************************************
 * examples/triangles/triangles_run.cpp
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
#include <thrill/common/logger.hpp>
#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <cmath>
#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;              // NOLINT
using examples::page_rank::ZipfGraphGen;

using Node = size_t;
using Edge = std::pair<Node, Node>;

static size_t CountTrianglesPerLine(
    api::Context& ctx,
    const std::vector<std::string>& input_path) {
    auto edges = ReadLines(ctx, input_path).template FlatMap<Edge>(
        [](const std::string& input, auto emit) {
            // parse "source\ttarget\ttarget...\n" lines
            char* endptr;
            unsigned long src = std::strtoul(input.c_str(), &endptr, 10);
            // die_unless(endptr && *endptr == ' ' &&
            //         "Could not parse src tgt line");
            while (*endptr != 0) {
                unsigned long tgt = std::strtoul(endptr + 1, &endptr, 10);

                if (src < tgt) {
                    emit(std::make_pair(src, tgt));
                }
                else {
                    // do not emit when src >= tgt (will be emitted when on other
                    // side of edge)
                }
            }
        }).Keep();

    return examples::triangles::CountTriangles(edges);
}

static size_t CountTrianglesGenerated(
    api::Context& ctx,
    const ZipfGraphGen& base_graph_gen,
    const size_t& num_vertices) {

    auto edge_lists = Generate(
        ctx, num_vertices,
        [graph_gen = ZipfGraphGen(base_graph_gen, num_vertices),
         rng = std::default_random_engine(std::random_device { } ())](
            size_t index) mutable {
            return std::make_pair(index, graph_gen.GenerateOutgoing(rng));
        });

    auto edges = edge_lists.template FlatMap<Edge>(
        [](std::pair<Node, std::vector<Node> > neighbors, auto emit) {
            for (auto neighbor : neighbors.second) {
                if (neighbors.first > neighbor) {
                    emit(std::make_pair(neighbor, neighbors.first));
                }
                else {
                    if (neighbors.first < neighbor) {
                    // emit(std::make_pair(neighbors.first, neighbor));
                    }
                    // self-loop: do not emit
                }
            }
        }).Keep().Cache().Execute();

    ctx.net.Barrier();
    common::StatsTimerStart timer;

    const bool use_detection = true;

    size_t triangles = examples::triangles::CountTriangles<use_detection>(edges);

    ctx.net.Barrier();

    if (ctx.my_rank() == 0) {
        if (use_detection) {
            LOG1 << "RESULT " << "benchmark=triangles " << "detection=ON"
                 << " vertices=" << num_vertices
                 << " time=" << timer
                 << " traffic=" << ctx.net_manager().Traffic()
                 << " hosts=" << ctx.num_hosts();
        }
        else {
            LOG1 << "RESULT " << "benchmark=triangles " << "detection=OFF"
                 << " vertices=" << num_vertices
                 << " time=" << timer
                 << " traffic=" << ctx.net_manager().Traffic()
                 << " hosts=" << ctx.num_hosts();
        }
    }

    return triangles;
}

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    bool generate = false;
    clp.add_bool('g', "generate", generate,
                 "generate graph data, set input = #pages");

    size_t num_vertices;

    clp.add_size_t('n', "vertices", num_vertices, "Number of vertices");

    // Graph Generator
    ZipfGraphGen gg(1);

    clp.add_double(0, "size_mean", gg.size_mean,
                   "generated: mean of number of outgoing links, "
                   "default: " + std::to_string(gg.size_mean));

    clp.add_double(0, "size_var", gg.size_var,
                   "generated: variance of number of outgoing links, "
                   "default: " + std::to_string(gg.size_var));

    clp.add_double(0, "link_scale", gg.link_zipf_scale,
                   "generated: Zipf scale parameter for outgoing links, "
                   "default: " + std::to_string(gg.link_zipf_scale));

    clp.add_double(0, "link_exponent", gg.link_zipf_exponent,
                   "generated: Zipf exponent parameter for outgoing links, "
                   "default: " + std::to_string(gg.link_zipf_exponent));

    std::vector<std::string> input_path;
    clp.add_param_stringlist("input", input_path,
                             "input file pattern(s)");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    die_unless(!generate || input_path.size() == 1);

    clp.print_result();

    return api::Run(
        [&](api::Context& ctx) {
            ctx.enable_consume();

            size_t triangles;
            if (generate) {
                triangles = CountTrianglesGenerated(
                    ctx, gg, num_vertices);
            }
            else {
                triangles = CountTrianglesPerLine(
                    ctx, input_path);
            }

            return triangles;
        });
}

/******************************************************************************/
