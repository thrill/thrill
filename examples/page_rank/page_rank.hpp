/*******************************************************************************
 * examples/page_rank/page_rank.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_PAGE_RANK_PAGE_RANK_HEADER
#define THRILL_EXAMPLES_PAGE_RANK_PAGE_RANK_HEADER

#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace examples {
namespace page_rank {

using namespace thrill; // NOLINT

static constexpr bool debug = false;

static constexpr double dampening = 0.85;

using PageId = std::size_t;
using Rank = double;

//! A pair (page source, page target)
struct PagePageLink {
    PageId src, tgt;

    friend std::ostream& operator << (std::ostream& os, const PagePageLink& a) {
        return os << '(' << a.src << '>' << a.tgt << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

//! A pair (page, rank)
struct PageRankPair {
    PageId page;
    Rank   rank;

    friend std::ostream& operator << (std::ostream& os, const PageRankPair& a) {
        return os << '(' << a.page << '|' << a.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

using OutgoingLinks = std::vector<PageId>;
using OutgoingLinksRank = std::pair<std::vector<PageId>, Rank>;

template <typename InStack>
auto PageRank(const DIA<OutgoingLinks, InStack>&links,
              size_t num_pages, size_t iterations) {

    api::Context& ctx = links.context();

    // initialize all ranks to 1.0 / n: (url, rank)

    DIA<Rank> ranks =
        Generate(
            ctx,
            [num_pages](const size_t&) { return Rank(1.0) / num_pages; },
            num_pages)
        .Collapse();

    // do iterations
    for (size_t iter = 0; iter < iterations; ++iter) {

        // for all outgoing link, get their rank contribution from all
        // links by doing:
        //
        // 1) group all outgoing links with rank of its parent page: (Zip)
        // ([linked_url, linked_url, ...], rank_parent)
        //
        // 2) compute rank contribution for each linked_url: (FlatMap)
        // (linked_url, rank / outgoing.size)

        auto outs_rank = links.Zip(
            ranks,
            [](const OutgoingLinks& ol, const Rank& r) {
                return OutgoingLinksRank(ol, r);
            });

        if (debug) {
            outs_rank
            .Map([](const OutgoingLinksRank& ol) {
                     return common::Join(',', ol.first)
                     + " <- " + std::to_string(ol.second);
                 })
            .Print("outs_rank");
        }

        auto contribs = outs_rank.template FlatMap<PageRankPair>(
            [](const OutgoingLinksRank& p, auto emit) {
                if (p.first.size() > 0) {
                    Rank rank_contrib = p.second / p.first.size();
                    for (const PageId& tgt : p.first)
                        emit(PageRankPair { tgt, rank_contrib });
                }
            });

        // reduce all rank contributions by adding all rank contributions and
        // compute the new rank: (url, rank)

        ranks =
            contribs
            .ReduceToIndex(
                [](const PageRankPair& p) { return p.page; },
                [](const PageRankPair& p1, const PageRankPair& p2) {
                    return PageRankPair { p1.page, p1.rank + p2.rank };
                }, num_pages)
            .Map([num_pages](const PageRankPair& p) {
                     return dampening * p.rank + (1 - dampening) / num_pages;
                 })
            .Collapse();
    }

    return ranks;
}

} // namespace page_rank
} // namespace examples

#endif // !THRILL_EXAMPLES_PAGE_RANK_PAGE_RANK_HEADER

/******************************************************************************/
