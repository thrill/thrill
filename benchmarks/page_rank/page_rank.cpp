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

using thrill::DIARef;
using thrill::Context;

using namespace thrill; // NOLINT

//! The PageRank user program
void page_rank(Context& ctx) {

    static const double s = 0.85;

    using PageWithLinks = std::pair<size_t, std::vector<int> >;
    using PageWithRank = std::pair<size_t, double>;
    using Page = std::tuple<size_t, double, std::vector<int> >;

    DIARef<PageWithRank> ranks =
        ReadLines(ctx, "pagerank.in")
        .Map([](const std::string& input) {
                 auto splitted = thrill::common::Split(input, " ");
                 return std::make_pair((size_t)std::stoi(splitted[0]), 1.0);
             }).Cache();

    size_t size = ranks.Size();

    auto links = ReadLines(ctx, "pagerank.in")
                 .Map([](const std::string& line) {
                          auto splitted = thrill::common::Split(line, " ");
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
            auto split = thrill::common::Split(input, " ");
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

int main(int argc, char* argv[]) {
    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    std::string input;
    clp.AddParamString("input", input,
                       "input file pattern");

    std::string output;
    clp.AddParamString("output", output,
                       "output file pattern");

    int iter;
    clp.AddParamInt("n", iter, "Iterations");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func =
        [&input, &output, &iter](api::Context& ctx) {
            thrill::common::StatsTimer<true> timer(false);
            static const bool debug = false;
            static const double s = 0.85;
            static const double f = 0.15;

            using Key = size_t;
            using Node = size_t;
            using Page_Outgoings = std::pair<size_t, std::vector<Node> >;
            using Page_Rank = std::pair<size_t, double>;
            using Page_Link = std::pair<size_t, size_t>;
            using Outgoings_Rank = std::pair<std::vector<Node>, double>;

            ////////////////////////////////////////////////////////////////////////////
            ////////////////////////// ALL KINDS OF LAMBDAS ////////////////////////////
            ////////////////////////////////////////////////////////////////////////////

            auto create_links_fn =
                [](const std::string& input) {
                    auto split = thrill::common::Split(input, " ");
                    // set node ids base to zero
                    // LOG << (size_t)(std::stoi(split[0]) - 1);
                    // LOG << (size_t)(std::stoi(split[1]) - 1);
                    return std::make_pair((size_t)(std::stoi(split[0]) - 1),
                                          (size_t)(std::stoi(split[1]) - 1));
                };

            auto max_fn = [](const Page_Link& in1, const Page_Link& in2) {
                              Node first = std::max(in1.first, in2.first);
                              Node second = std::max(in1.second, in2.second);
                              return std::make_pair(std::max(first, second), first);
                          };

            auto key_link_fn = [](Page_Link p) { return p.first; };
            auto key_page_with_ranks_fn = [](const Page_Rank& p) { return p.first; };

            auto group_fn = [](auto& r, Key k) {
                                // LOG << k << " has outgoings to";
                                std::vector<Node> all;
                                while (r.HasNext()) {
                                    // auto out = r.Next().second;
                                    // LOG << out;
                                    all.push_back(r.Next().second);
                                }
                                return std::make_pair(k, all);
                            };

            auto set_rank_fn = [](Page_Outgoings p) {
                                   return std::make_pair(p.first, 1.0);
                               };

            Page_Outgoings neutral_page = std::make_pair(0, std::vector<Node>());

            ////////////////////////////////////////////////////////////////////////////
            //////////////////////// START OF COMPUTATION HERE /////////////////////////
            ////////////////////////////////////////////////////////////////////////////

            timer.Start();
            // url linked_url
            // url linked_url
            // url linked_url
            // ...
            auto in = ReadLines(ctx, input);
            auto input = in.Map(create_links_fn);

            //  URL   OUTGOING
            // (url, [linked_url, linked_url, ...])
            // (url, [linked_url, linked_url, ...])
            // (url, [linked_url, linked_url, ...])
            // ...
            const auto number_nodes = input.Sum(max_fn).first + 1;
            auto links = input.GroupByIndex<Page_Outgoings>(key_link_fn, group_fn, number_nodes).Keep();

            // (url, rank)
            // (url, rank)
            // (url, rank)
            // ...
            auto ranks = links.Map(set_rank_fn).Cache();
            // do iterations
            for (int i = 1; i <= iter; ++i) {
                LOG << "iteration " << i;

                // (linked_url, rank / OUTGOING.size)
                // (linked_url, rank / OUTGOING.size)
                // (linked_url, rank / OUTGOING.size)
                // ...
                assert(links.Size() == ranks.Size());
                auto merge_outgoings_w_rank_fn = [](const Page_Outgoings& l, const Page_Rank& r) {
                                                     return std::make_pair(l.second, r.second);
                                                 };
                auto compute_rank_contrib_fn = [](const Outgoings_Rank& p, auto emit) {
                                                   if (p.first.size() > 0) {
                                                       double rank_contrib = p.second / p.first.size();
                                                       // assert (rank_contrib <= 1);
                                                       for (auto e : p.first) {
                                                           emit(std::make_pair(e, rank_contrib));
                                                       }
                                                   }
                                               };

                auto contribs = links.Zip(ranks, merge_outgoings_w_rank_fn).FlatMap<Page_Rank>(compute_rank_contrib_fn);

                // (url, rank)
                // (url, rank)
                // (url, rank)
                // ...

                // spark computation is:
                // ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
                auto update_rank_fn = [](const Page_Rank& p1, const Page_Rank& p2) {
                                          assert(p1.first == p2.first);
                                          // assert(f + s * (p1.second + p2.second) <= 1);
                                          return std::make_pair(p1.first, f + s * (p1.second + p2.second));
                                      };
                ranks = contribs.ReduceToIndex(key_page_with_ranks_fn, update_rank_fn, number_nodes).Cache();
            }

            auto res = ranks.Map([](Page_Rank item) {
                                     if (item.first == 0 && item.second == 0.0) {
                                         return std::string("");
                                     }
                                     else {
                                         return std::to_string(item.first + 1)
                                         + ": " + std::to_string(item.second);
                                     }
                                 });

            assert(res.Size() == links.Size());

            res.WriteLines(output);
            timer.Stop();

            auto number_edges = in.Size();
            LOG << "\n"
                << "FINISHED PAGERANK COMPUTATION"
                << "\n"
                << std::left << std::setfill(' ')
                << std::setw(10) << "#nodes: " << number_nodes
                << "\n"
                << std::setw(10) << "#edges: " << number_edges
                << "\n"
                << std::setw(10) << "#iter: " << iter
                << "\n"
                << std::setw(10) << "time: " << timer.Milliseconds() << "ms";
        };

    return api::Run(start_func);
}

/******************************************************************************/
