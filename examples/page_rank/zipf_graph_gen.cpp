/*******************************************************************************
 * examples/page_rank/zipf_graph_gen.cpp
 *
 * A simple graph generator for the PageRank benchmark inspired by HiBench's
 * generator. The number of outgoing links of each page is Gaussian distributed,
 * by default with mean 50 and variance 10, and the link targets themselves
 * follow a Zipf-Mandelbrot distribution with very small scale parameter, such
 * that the pages with low id numbers have a slightly higher probability than
 * the rest.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/page_rank/zipf_graph_gen.hpp>

#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <string>
#include <vector>

using namespace thrill;              // NOLINT
using namespace examples::page_rank; // NOLINT

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    // Graph Generator
    ZipfGraphGen gg(1);

    uint64_t pages;
    clp.AddParamBytes("pages", pages, "number of pages");

    bool group = false;
    clp.AddFlag('g', "group", group, "group outgoing links");

    clp.AddSizeT('m', "size_mean", gg.size_mean,
                 "mean of number of outgoing links, default: "
                 + std::to_string(gg.size_mean));

    clp.AddDouble(0, "size_var", gg.size_var,
                  "variance of number of outgoing links, default: "
                  + std::to_string(gg.size_var));

    clp.AddDouble(0, "link_scale", gg.link_zipf_scale,
                  "Zipf scale parameter for outgoing links, default: "
                  + std::to_string(gg.link_zipf_scale));

    clp.AddDouble(0, "link_exponent", gg.link_zipf_exponent,
                  "Zipf exponent parameter for outgoing links, default: "
                  + std::to_string(gg.link_zipf_exponent));

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    // reinitialize graph generator with parameters from the command line.
    gg.Initialize(pages);

    //! underlying random number generator
    std::default_random_engine rng(std::random_device { } ());

    for (size_t p = 0; p < pages; ++p)
    {
        std::vector<size_t> result = gg.GenerateOutgoing(rng);
        if (group) {
            for (size_t i = 0; i < result.size(); ++i) {
                if (i != 0) std::cout << ' ';
                std::cout << result[i];
            }
            std::cout << '\n';
        }
        else {
            for (const size_t& out : result) {
                std::cout << p << '\t' << out << '\n';
            }
        }
    }

    return 0;
}

/******************************************************************************/
