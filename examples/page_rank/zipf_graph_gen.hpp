/*******************************************************************************
 * examples/page_rank/zipf_graph_gen.hpp
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

#pragma once
#ifndef THRILL_EXAMPLES_PAGE_RANK_ZIPF_GRAPH_GEN_HEADER
#define THRILL_EXAMPLES_PAGE_RANK_ZIPF_GRAPH_GEN_HEADER

#include <thrill/common/string.hpp>
#include <thrill/common/zipf_distribution.hpp>

#include <algorithm>
#include <string>
#include <vector>

class ZipfGraphGen
{
public:
    using ZipfDistribution = thrill::common::ZipfDistribution;

    //! number of pages in graph
    uint64_t pages;

    //! Gaussian mean and variance of content length
    size_t size_mean = 50;
    double size_var = 10;

    //! Zipf distribution scale and exponent for generating outgoing links over
    //! the page number universe.
    double link_zipf_scale = 0.3;
    double link_zipf_exponent = 0.5;

    explicit ZipfGraphGen(uint64_t _pages)
        : pages(_pages) { Initialize(); }

    //! reinitialize the random generator if parameters were changed.
    void Initialize(uint64_t _pages) {
        pages = _pages;

        content_length_dist_ = std::normal_distribution<double>(
            size_mean, size_var);

        link_zipf_ = ZipfDistribution(
            pages, link_zipf_scale, link_zipf_exponent);
    }

    //! reinitialize the random generator if parameters were changed.
    void Initialize() {
        return Initialize(pages);
    }

    template <typename Generator>
    std::vector<size_t> GenerateOutgoing(Generator& rng) {
        double dsize = content_length_dist_(rng);
        if (dsize < 0) dsize = 0;

        size_t size = static_cast<size_t>(std::round(dsize));

        std::vector<size_t> result;
        result.reserve(size);

        for (size_t i = 0; i < size; ++i) {
            result.emplace_back(link_zipf_(rng) - 1);
        }
        std::sort(result.begin(), result.end());
        return result;
    }

private:
    //! Gaussian random variable for content length of a page
    std::normal_distribution<double> content_length_dist_;

    //! Zipf random variable for outgoing links.
    ZipfDistribution link_zipf_;
};

#endif // !THRILL_EXAMPLES_PAGE_RANK_ZIPF_GRAPH_GEN_HEADER

/******************************************************************************/
