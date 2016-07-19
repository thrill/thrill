/*******************************************************************************
 * examples/page_rank/page_rank_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank/page_rank.hpp>
#include <examples/page_rank/zipf_graph_gen.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;              // NOLINT
using namespace examples::page_rank; // NOLINT

static void RunPageRankEdgePerLine(
    api::Context& ctx,
    const std::vector<std::string>& input_path, const std::string& output_path,
    size_t iterations) {
    ctx.enable_consume();

    common::StatsTimerStart timer;

    // read input file and create links in this format:
    //
    // url linked_url
    // url linked_url
    // url linked_url
    // ...
    auto input =
        ReadLines(ctx, input_path)
        .Map([](const std::string& input) {
                 // parse "source\ttarget\n" lines
                 char* endptr;
                 unsigned long src = std::strtoul(input.c_str(), &endptr, 10);
                 die_unless(endptr && *endptr == '\t' &&
                            "Could not parse src tgt line");
                 unsigned long tgt = std::strtoul(endptr + 1, &endptr, 10);
                 die_unless(endptr && *endptr == 0 &&
                            "Could not parse src tgt line");
                 return PagePageLink { src, tgt };
             });

    size_t num_pages =
        input.Keep()
        .Map([](const PagePageLink& ppl) { return std::max(ppl.src, ppl.tgt); })
        .Max() + 1;

    auto number_edges = input.Keep().Size();

    // aggregate all outgoing links of a page in this format: by index
    // ([linked_url, linked_url, ...])

    // group outgoing links from input file

    auto links = input.template GroupToIndex<OutgoingLinks>(
        [](const PagePageLink& p) { return p.src; },
        [num_pages,
         all = std::vector < PageId > ()](auto& r, const PageId&) mutable {
            all.clear();
            while (r.HasNext()) {
                all.push_back(r.Next().tgt);
            }
            return all;
        },
        num_pages).Cache();

    // perform actual page rank calculation iterations

    auto ranks = PageRank(links, num_pages, iterations);

    // construct output as "pageid: rank"

    if (output_path.size()) {
        ranks.Zip(
            // generate index numbers: 0...num_pages-1
            Generate(ctx, num_pages),
            [](const Rank& r, const PageId& p) {
                return common::str_sprintf("%zu: %g", p, r);
            })
        .WriteLinesMany(output_path);
    }
    else {
        ranks.Execute();
    }

    timer.Stop();

    if (ctx.my_rank() == 0) {
        LOG1 << "FINISHED PAGERANK COMPUTATION";
        LOG1 << "#pages: " << num_pages;
        LOG1 << "#edges: " << number_edges;
        LOG1 << "#iterations: " << iterations;
        LOG1 << "time: " << timer << "s";
    }
}

static void RunPageRankGenerated(
    api::Context& ctx,
    const std::string& input_path, const ZipfGraphGen& base_graph_gen,
    const std::string& output_path, size_t iterations) {
    ctx.enable_consume();

    common::StatsTimerStart timer;

    size_t num_pages;
    if (!common::from_str<size_t>(input_path, num_pages))
        die("For generated graph data, set input_path to the number of pages.");

    auto links = Generate(
        ctx,
        [graph_gen = ZipfGraphGen(base_graph_gen, num_pages),
         rng = std::default_random_engine(std::random_device { } ())](
            size_t /* index */) mutable {
            return graph_gen.GenerateOutgoing(rng);
        },
        num_pages).Cache();

    auto number_edges =
        links.Keep().Map([](const OutgoingLinks& ol) { return ol.size(); }).Sum();

    // perform actual page rank calculation iterations

    auto ranks = PageRank(links, num_pages, iterations);

    // construct output as "pageid: rank"

    if (output_path.size()) {
        ranks.Zip(
            // generate index numbers: 0...num_pages-1
            Generate(ctx, num_pages),
            [](const Rank& r, const PageId& p) {
                return std::to_string(p) + ": " + std::to_string(r);
            })
        .WriteLinesMany(output_path);
    }
    else {
        ranks.Execute();
    }

    timer.Stop();

    if (ctx.my_rank() == 0) {
        LOG1 << "FINISHED PAGERANK COMPUTATION";
        LOG1 << "#pages: " << num_pages;
        LOG1 << "#edges: " << number_edges;
        LOG1 << "#iterations: " << iterations;
        LOG1 << "time: " << timer << "s";
    }
}

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    bool generate = false;
    clp.AddFlag('g', "generate", generate,
                "generate graph data, set input = #pages");

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

    std::string output_path;
    clp.AddString('o', "output", output_path,
                  "output file pattern");

    size_t iter = 10;
    clp.AddSizeT('n', "iterations", iter, "PageRank iterations, default: 10");

    std::vector<std::string> input_path;
    clp.AddParamStringlist("input", input_path,
                           "input file pattern(s)");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    die_unless(!generate || input_path.size() == 1);

    return api::Run(
        [&](api::Context& ctx) {
            if (generate)
                return RunPageRankGenerated(
                    ctx, input_path[0], gg, output_path, iter);
            else
                return RunPageRankEdgePerLine(
                    ctx, input_path, output_path, iter);
        });
}

/******************************************************************************/
