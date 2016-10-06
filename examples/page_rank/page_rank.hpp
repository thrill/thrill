/*******************************************************************************
 * examples/page_rank/page_rank.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_PAGE_RANK_PAGE_RANK_HEADER
#define THRILL_EXAMPLES_PAGE_RANK_PAGE_RANK_HEADER

#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/join.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/reduce_by_key.hpp>
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

using PageRankPair = std::pair<PageId, Rank>;
using OutgoingLinks = std::vector<PageId>;
using OutgoingLinksRank = std::pair<std::vector<PageId>, Rank>;
using LinkedPage = std::pair<PageId, OutgoingLinks>;
using RankedPage = std::pair<PageId, Rank>;

template <typename InStack>
auto PageRank(const DIA<OutgoingLinks, InStack>&links,
              size_t num_pages, size_t iterations) {

    api::Context& ctx = links.context();
    double num_pages_d = static_cast<double>(num_pages);

    // initialize all ranks to 1.0 / n: (url, rank)

    DIA<Rank> ranks =
        Generate(
            ctx,
            [num_pages_d](size_t) { return Rank(1.0) / num_pages_d; },
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
                    Rank rank_contrib = p.second / static_cast<double>(p.first.size());
                    for (const PageId& tgt : p.first)
                        emit(PageRankPair { tgt, rank_contrib });
                }
            });

        // reduce all rank contributions by adding all rank contributions and
        // compute the new rank: (url, rank)

        ranks =
            contribs
            .ReduceToIndex(
                [](const PageRankPair& p) { return p.first; },
                [](const PageRankPair& p1, const PageRankPair& p2) {
                    return std::make_pair(p1.first, p1.second + p2.second);
                }, num_pages)
            .Map([num_pages_d](const PageRankPair& p) {
                     return dampening * p.second + (1 - dampening) / num_pages_d;
                 })
            .Collapse();
    }

    return ranks;
}

template <const bool UseLocationDetection = false, typename InStack>
auto PageRankJoin(const DIA<LinkedPage, InStack>&links, size_t num_pages,
                  size_t iterations) {

    api::Context& ctx = links.context();
    double num_pages_d = static_cast<double>(num_pages);

    // initialize all ranks to 1.0 / n: (url, rank)

    DIA<RankedPage> ranks =
        Generate(
            ctx,
            [num_pages_d](size_t idx) {
                return std::make_pair(idx, Rank(1.0) / num_pages_d);
            },
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

        auto outs_rank = links.template InnerJoinWith<UseLocationDetection>(
            ranks,
            [](const LinkedPage& lp) {
                return lp.first;
            },
            [](const RankedPage& r) {
                return r.first;
            },
            [](const LinkedPage& lp, const RankedPage& r) {
                return std::make_pair(lp.second, r.second);
            }, thrill::hash());

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
                    Rank rank_contrib = p.second / static_cast<double>(p.first.size());
                    for (const PageId& tgt : p.first)
                        emit(std::make_pair(tgt, rank_contrib));
                }
            });

        // reduce all rank contributions by adding all rank contributions and
        // compute the new rank: (url, rank)

        ranks =
            contribs
            .ReducePair(
                [](const Rank& p1, const Rank& p2) {
                    return p1 + p2;
                })
            .Map([num_pages_d](const PageRankPair& p) {
                     return std::make_pair(p.first,
                                           dampening * p.second +
                                           (1 - dampening) / num_pages_d);
                 }).Execute();
    }

    return ranks;
}

} // namespace page_rank
} // namespace examples

#endif // !THRILL_EXAMPLES_PAGE_RANK_PAGE_RANK_HEADER

/******************************************************************************/
