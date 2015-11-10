/*******************************************************************************
 * examples/page_rank.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/groupby_index.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <iostream>
#include <iterator>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

using thrill::DIA;
using thrill::Context;

using namespace thrill; // NOLINT

namespace examples {

static const bool debug = false;

static const double s = 0.85;
static const double f = 0.15;

using Key = std::size_t;
using Node = std::size_t;
using Rank = double;
using Page_Rank = std::pair<Node, Rank>;
using Page_Link = std::pair<Node, Node>;
using Outgoings_Rank = std::pair<std::vector<Node>, Rank>;
using Outgoings = std::vector<Node>;

template<typename InStack>
auto PageRank(const DIA<std::string, InStack> &in, api::Context &ctx, int iter) {

    DIA<Page_Link> input = in.Map(
            [](const std::string &input) {
                auto split = thrill::common::Split(input, " ");
                LOG0 << "input "
                        << (std::stoi(split[0]) - 1)
                        << " "
                        << (std::stoi(split[1]) - 1);
                // set base of page_id to 0
                return std::make_pair((size_t) (std::stoi(split[0]) - 1),
                                      (size_t) (std::stoi(split[1]) - 1));
            });

    // aggregate all outgoing links of a page in this format:
    //
    //  URL   OUTGOING
    // ([linked_url, linked_url, ...])
    // ([linked_url, linked_url, ...])
    // ([linked_url, linked_url, ...])
    // ...

    // get number of nodes by finding max page_id
    // add 1 to max node_id to get number of nodes because of node_id 0
    const auto number_nodes = input.Sum(
            [](const Page_Link &in1, const Page_Link &in2) {
                Node first = std::max(in1.first, in2.first);
                Node second = std::max(in1.second, in2.second);
                return std::make_pair(std::max(first, second), first);
            }).first + 1;

    LOG << "number_nodes " << number_nodes;

    // group outgoing links
    DIA<Outgoings> links = input.GroupByIndex<Outgoings>(
            [](Page_Link p) { return p.first; },
            [](auto &r, Key) {
                std::vector<Node> all;
                while (r.HasNext()) {
                    all.push_back(r.Next().second);
                }

                // std::string s = "{";
                // for (auto e : all) {
                //     s+= std::to_string(e) + ", ";
                // }
                // LOG << "links " << s << "}";

                return all;
            },
            number_nodes).Cache();

    // initialize all ranks to 1.0
    //
    // (url, rank)
    // (url, rank)
    // (url, rank)
    // ...
    // auto ranks = Generate(ctx, [](const size_t& index) {
    //     return std::make_pair(index, 1.0);
    // }, number_nodes).Cache();
    auto ranks = Generate(ctx,
                          [](const size_t &) {
                              return (Rank) 1.0;
                          }, number_nodes).Cache();

    auto node_ids = Generate(ctx,
                             [](const size_t &index) {
                                 return index + 1;
                             }, number_nodes);

    // do iterations
    for (int i = 0; i < iter; ++i) {
        LOG << "iteration " << i;

        // for all outgoing link, get their rank contribution from all
        // links by doing:
        //
        // 1) group all outgoing links with rank of its parent page: (Zip)
        //
        // ([linked_url, linked_url, ...], rank_parent)
        // ([linked_url, linked_url, ...], rank_parent)
        // ([linked_url, linked_url, ...], rank_parent)
        //
        // 2) compute rank contribution for each linked_url: (FlatMap)
        //
        // (linked_url, rank / OUTGOING.size)
        // (linked_url, rank / OUTGOING.size)
        // (linked_url, rank / OUTGOING.size)
        // ...

        std::cout << links.Size() << std::endl;
        std::cout << ranks.Size() << std::endl;

        assert(links.Size() == ranks.Size());

        // TODO(SL): when Zip/FlatMap chained, code doesn't compile, please check
        DIA<Outgoings_Rank> outs_rank = links.Zip(ranks,
                                                  [](const Outgoings &l, const Rank r) {
                                                      // std::string s = "{";
                                                      // for (auto e : l) {
                                                      //     s += std::to_string(e) + ", ";
                                                      // }
                                                      // s += "}";
                                                      // LOG << "contribs1 " << s << " " << r;

                                                      return std::make_pair(l, r);
                                                  });
        DIA<Page_Rank> contribs = outs_rank.FlatMap<Page_Rank>(
                [](const Outgoings_Rank &p, auto emit) {
                    if (p.first.size() > 0) {
                        Rank rank_contrib = p.second / p.first.size();
                        // assert (rank_contrib <= 1);
                        for (auto e : p.first) {
                            LOG << "contribs2 " << e << " " << rank_contrib;
                            emit(std::make_pair(e, rank_contrib));
                        }
                    }
                });

        // reduce all rank contributions by adding all rank contributions
        // and compute the new rank with 0.15 * 0.85 * sum_rank_contribs
        //
        // (url, rank)
        // (url, rank)
        // (url, rank)
        // ...

        // auto sum_rank_contrib_fn = [](const Page_Rank& p1, const Page_Rank& p2) {
        //     assert(p1.first == p2.first);
        //     return p1.second + p2.second;
        // };
        ranks = contribs.ReduceToIndex(
                        [](const Page_Rank &p) { return p.first; },
                        [](const Page_Rank &p1, const Page_Rank &p2) {
                            return std::make_pair(p1.first, p1.second + p2.second);
                        }, number_nodes)
                .Map(
                        [](const Page_Rank p) {
                            LOG << "ranks2 in " << p.first << "-" << p.second;
                            if (std::fabs(p.second) <= 1E-5) {
                                LOG << "ranks2 " << 0.0;
                                return (Rank) 0.0;
                            }
                            else {
                                LOG << "ranks2 " << f + s * p.second;
                                return f + s * p.second;
                            }
                        }).Keep().Collapse();
    }

    // write result to line. add 1 to node_ids to revert back to normal
    auto res = ranks.Zip(node_ids,
                         [](const Rank r, const Node n) {
                             return std::to_string(n)
                                    + ": " + std::to_string(r);
                         });

    assert (res.Size() == links.Size());

    return res;
}

}