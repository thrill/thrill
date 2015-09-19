/*******************************************************************************
 * benchmarks/page_rank/page_rank.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/groupby_index.hpp>
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

using thrill::DIARef;
using thrill::Context;

//! The PageRank user program
void page_rank(Context& ctx) {

    static const double s = 0.85;

    using PageWithLinks = std::pair<size_t, std::vector<int> >;
    using PageWithRank = std::pair<size_t, double>;
    using Page = std::tuple<size_t, double, std::vector<int> >;

    DIARef<PageWithRank> ranks =
        ReadLines(ctx, "pagerank.in")
        .Map([](const std::string& input) {
                 auto splitted = thrill::common::split(input, " ");
                 return std::make_pair((size_t)std::stoi(splitted[0]), 1.0);
             }).Cache();

    size_t size = ranks.Size();

    auto links = ReadLines(ctx, "pagerank.in")
                 .Map([](const std::string& line) {
                          auto splitted = thrill::common::split(line, " ");
                          std::vector<int> links;
                          links.reserve(splitted.size() - 1);
                          for (size_t i = 1; i < splitted.size(); i++) {
                              links.push_back(std::stoi(splitted[i]));
                          }
                          return std::make_pair(std::stoi(splitted[0]), links);
                      });

    for (size_t i = 1; i <= 10; ++i) {
        std::cout << "Iteration: " << i << std::endl;

        auto pages =
            links
            .Zip(ranks, [](PageWithLinks first, PageWithRank second) {
                     return std::make_tuple(first.first, second.second, first.second);
                 });

        auto contribs =
            pages
            .FlatMap<PageWithRank>(
                [](Page page, auto emit) {
                    std::vector<int> urls = std::get<2>(page);
                    double rank = std::get<1>(page);
                    double num_urls = static_cast<double>(urls.size());
                    for (int url : urls) {
                        emit(std::make_pair(url, rank / num_urls));
                    }
                });

        ranks =
            contribs
            .ReducePairToIndex(
                [](double rank1, double rank2) {
                    return rank1 + rank2;
                },
                size)
            .Map([](PageWithRank page) {
                     return std::make_pair(page.first, (1 - s) + page.second * s);
                 })
            .Cache();
    }

    ranks.Map([](PageWithRank item) {
                  return std::to_string(item.first)
                  + ": " + std::to_string(item.second);
              }).
    WriteLines("pagerank.out");
}

//! The PageRank user program with group by
void page_rank_with_reduce_sort(Context& ctx) {

    static const double s = 0.85;

    using PageWithLinks = std::pair<size_t, std::vector<int> >;
    using PageWithRank = std::pair<size_t, double>;
    using Page = std::tuple<size_t, double, std::vector<int> >;

    // url linked_url
    // url linked_url
    // url linked_url
    // ...
    auto in = ReadLines(ctx, "pagerank_2.in");

    auto key_fn = [](const std::pair<int, std::vector<int> >& p) { return p.first; };

    auto red_fn = [](const std::pair<int, std::vector<int> >& in1, const std::pair<int, std::vector<int> >& in2) {
                      std::vector<int> v;
                      v.reserve(in1.second.size() + in2.second.size());
                      v.insert(v.end(), in1.second.begin(), in1.second.end());
                      v.insert(v.end(), in2.second.begin(), in2.second.end());
                      return std::make_pair(in1.first, v);
                  };

    //  URL   OUTGOING
    // (url, [linked_url, linked_url, ...])
    // (url, [linked_url, linked_url, ...])
    // (url, [linked_url, linked_url, ...])
    // ...
    auto links = in.Map(
        [](const std::string& input) {
            auto split = thrill::common::split(input, " ");
            return std::make_pair<int, std::vector<int> >(std::stoi(split[0]), std::vector<int>(1, std::stoi(split[1])));
        }).ReduceByKey(key_fn, red_fn);

    auto compare_fn = [](const std::pair<int, std::vector<int> >& in1, const std::pair<int, std::vector<int> >& in2) {
                          return in1.first < in2.first;
                      };

    auto links_sorted = links.Sort(compare_fn).Keep();

    // (url, rank)
    // (url, rank)
    // (url, rank)
    // ...
    DIARef<PageWithRank> ranks = links_sorted.Map(
        [](const std::pair<int, std::vector<int> >& l) {
            return std::make_pair((size_t)l.first, 1.0);
        }).Cache();

    size_t size = ranks.Size();

    for (size_t i = 1; i <= 10; ++i) {
        std::cout << "Iteration: " << i << std::endl;

        auto pages =
            links_sorted
            .Zip(ranks, [](PageWithLinks first, PageWithRank second) {
                     return std::make_tuple(first.first, second.second, first.second);
                 });

        auto contribs =
            pages
            .FlatMap<PageWithRank>(
                [](Page page, auto emit) {
                    std::vector<int> urls = std::get<2>(page);
                    double rank = std::get<1>(page);
                    double num_urls = static_cast<double>(urls.size());
                    for (int url : urls) {
                        emit(std::make_pair(url, rank / num_urls));
                    }
                });

        ranks =
            contribs
            .ReducePairToIndex(
                [](double rank1, double rank2) {
                    return rank1 + rank2;
                },
                size)
            .Map([](PageWithRank page) {
                     return std::make_pair(page.first, (1 - s) + page.second * s);
                 })
            .Cache();
    }

    ranks.Map([](PageWithRank item) {
                  return std::to_string(item.first)
                  + ": " + std::to_string(item.second);
              }).
    WriteLines("pagerank.out");
}



//! The PageRank user program with group by
void page_rank_with_groupbyindex(Context& ctx) {
    thrill::common::StatsTimer<true> timer(false);
    static const bool debug = true;
    static const double s = 0.85;
    static const double f = 0.15;
    static const std::size_t iters = 10;

    using Key = std::size_t;
    using Node = std::size_t;
    using PageWithLinks = std::pair<std::size_t, std::vector<Node> >;
    using PageWithRank = std::pair<std::size_t, double>;
    using Link = std::pair<std::size_t, std::size_t>;

    ////////////////////////////////////////////////////////////////////////////
    ////////////////////////// ALL KINDS OF LAMBDAS ////////////////////////////
    ////////////////////////////////////////////////////////////////////////////

    auto create_links_fn = [](const std::string& input) {
        auto split = thrill::common::split(input, " ");
        // set node ids base to zero
        return std::make_pair((std::size_t)(std::stoi(split[0]) - 1),
            (std::size_t)(std::stoi(split[1]) - 1));
    };

    auto max_fn = [](const Link &in1, const Link &in2) {
        return std::make_pair(((in1.first > in2.first) ? in1.first : in2.first), in2.second);
    };

    auto key_link_fn = [](Link p) { return p.first; };
    auto key_page_with_ranks_fn = [](const PageWithRank& p) { return p.first; };

    auto group_fn = [](auto& r, Key k) {
        std::vector<Node> all;
        while (r.HasNext()) {
            all.push_back(r.Next().second);
        }
        return std::make_pair(k, all);
    };

    auto set_rank_fn = [](PageWithLinks p) {
        return std::make_pair(p.first, 1.0);
    };

    auto compute_rank_contrib_fn = [](const PageWithLinks& l, const PageWithRank& r){
        return std::make_pair(l.first, r.second/(l.second.size()));
    };

    //spark computation is:
    // ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    auto update_rank_fn = [](const PageWithRank& p1, const PageWithRank& p2) {
        return std::make_pair(p1.first, f + s * (p1.second + p2.second));
    };

    PageWithLinks neutral_page = std::make_pair(0, std::vector<Node>());


    ////////////////////////////////////////////////////////////////////////////
    //////////////////////// START OF COMPUTATION HERE /////////////////////////
    ////////////////////////////////////////////////////////////////////////////

    timer.Start();
    // url linked_url
    // url linked_url
    // url linked_url
    // ...
    auto in = ReadLines(ctx, "../../../bench_in/eu-2005.in");
    auto input = in.Map(create_links_fn);

    //  URL   OUTGOING
    // (url, [linked_url, linked_url, ...])
    // (url, [linked_url, linked_url, ...])
    // (url, [linked_url, linked_url, ...])
    // ...
    auto number_nodes = input.Sum(max_fn).first + 1;
    auto links = input.GroupByIndex<PageWithLinks>(key_link_fn, group_fn, number_nodes).Keep();

    // (url, rank)
    // (url, rank)
    // (url, rank)
    // ...
    auto ranks = links.Map(set_rank_fn).Cache();

    // do iterations
    for (size_t i = 1; i <= iters; ++i) {
        // (linked_url, rank / OUTGOING.size)
        // (linked_url, rank / OUTGOING.size)
        // (linked_url, rank / OUTGOING.size)
        // ...
        auto contribs = links.Zip(ranks, compute_rank_contrib_fn);

        // (url, rank)
        // (url, rank)
        // (url, rank)
        // ...
        ranks = contribs.ReduceToIndex(key_page_with_ranks_fn, update_rank_fn, number_nodes);
    }

    ranks.Map([](PageWithRank item) {
        return std::to_string(item.first + 1)
        + ": " + std::to_string(item.second);
    }).
    WriteLines("pagerank.out");
    timer.Stop();

    auto number_edges = in.Size();
    LOG << "FINISHED PAGERANK COMPUTATION"
        << "\n"
        << std::left << std::setfill(' ')
        << std::setw(10) << "#nodes: " << number_nodes
        << "\n"
        << std::setw(10) << "#edges: " << number_edges
        << "\n"
        << std::setw(10) << "#iter: " << iters
        << "\n"
        << std::setw(10) << "time: " << timer.Seconds();
}

int main(int, char**) {
    return thrill::api::Run(page_rank_with_groupbyindex);
}

/******************************************************************************/
