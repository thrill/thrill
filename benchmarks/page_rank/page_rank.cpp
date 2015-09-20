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
#include <thrill/api/generate.hpp>
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

using namespace thrill; // NOLINT

// //! The PageRank user program
// void page_rank(Context& ctx) {

//     static const double s = 0.85;

//     using PageWithLinks = std::pair<size_t, std::vector<int> >;
//     using PageWithRank = std::pair<size_t, double>;
//     using Page = std::tuple<size_t, double, std::vector<int> >;

//     DIARef<PageWithRank> ranks =
//         ReadLines(ctx, "pagerank.in")
//         .Map([](const std::string& input) {
//                  auto splitted = thrill::common::split(input, " ");
//                  return std::make_pair((size_t)std::stoi(splitted[0]), 1.0);
//              }).Cache();

//     size_t size = ranks.Size();

//     auto links = ReadLines(ctx, "pagerank.in")
//                  .Map([](const std::string& line) {
//                           auto splitted = thrill::common::split(line, " ");
//                           std::vector<int> links;
//                           links.reserve(splitted.size() - 1);
//                           for (size_t i = 1; i < splitted.size(); i++) {
//                               links.push_back(std::stoi(splitted[i]));
//                           }
//                           return std::make_pair(std::stoi(splitted[0]), links);
//                       });

//     for (size_t i = 1; i <= 10; ++i) {
//         std::cout << "Iteration: " << i << std::endl;

//         auto pages =
//             links
//             .Zip(ranks, [](PageWithLinks first, PageWithRank second) {
//                      return std::make_tuple(first.first, second.second, first.second);
//                  });

//         auto contribs =
//             pages
//             .FlatMap<PageWithRank>(
//                 [](Page page, auto emit) {
//                     std::vector<int> urls = std::get<2>(page);
//                     double rank = std::get<1>(page);
//                     double num_urls = static_cast<double>(urls.size());
//                     for (int url : urls) {
//                         emit(std::make_pair(url, rank / num_urls));
//                     }
//                 });

//         ranks =
//             contribs
//             .ReducePairToIndex(
//                 [](double rank1, double rank2) {
//                     return rank1 + rank2;
//                 },
//                 size)
//             .Map([](PageWithRank page) {
//                      return std::make_pair(page.first, (1 - s) + page.second * s);
//                  })
//             .Cache();
//     }

//     ranks.Map([](PageWithRank item) {
//                   return std::to_string(item.first)
//                   + ": " + std::to_string(item.second);
//               }).
//     WriteLines("pagerank.out");
// }

// //! The PageRank user program with group by
// void page_rank_with_reduce_sort(Context& ctx) {

//     static const double s = 0.85;

//     using PageWithLinks = std::pair<size_t, std::vector<int> >;
//     using PageWithRank = std::pair<size_t, double>;
//     using Page = std::tuple<size_t, double, std::vector<int> >;

//     // url linked_url
//     // url linked_url
//     // url linked_url
//     // ...
//     auto in = ReadLines(ctx, "pagerank_2.in");

//     auto key_fn = [](const std::pair<int, std::vector<int> >& p) { return p.first; };

//     auto red_fn = [](const std::pair<int, std::vector<int> >& in1, const std::pair<int, std::vector<int> >& in2) {
//                       std::vector<int> v;
//                       v.reserve(in1.second.size() + in2.second.size());
//                       v.insert(v.end(), in1.second.begin(), in1.second.end());
//                       v.insert(v.end(), in2.second.begin(), in2.second.end());
//                       return std::make_pair(in1.first, v);
//                   };

//     //  URL   OUTGOING
//     // (url, [linked_url, linked_url, ...])
//     // (url, [linked_url, linked_url, ...])
//     // (url, [linked_url, linked_url, ...])
//     // ...
//     auto links = in.Map(
//         [](const std::string& input) {
//             auto split = thrill::common::split(input, " ");
//             return std::make_pair<int, std::vector<int> >(std::stoi(split[0]), std::vector<int>(1, std::stoi(split[1])));
//         }).ReduceByKey(key_fn, red_fn);

//     auto compare_fn = [](const std::pair<int, std::vector<int> >& in1, const std::pair<int, std::vector<int> >& in2) {
//                           return in1.first < in2.first;
//                       };

//     auto links_sorted = links.Sort(compare_fn).Keep();

//     // (url, rank)
//     // (url, rank)
//     // (url, rank)
//     // ...
//     DIARef<PageWithRank> ranks = links_sorted.Map(
//         [](const std::pair<int, std::vector<int> >& l) {
//             return std::make_pair((size_t)l.first, 1.0);
//         }).Cache();

//     size_t size = ranks.Size();

//     for (size_t i = 1; i <= 10; ++i) {
//         std::cout << "Iteration: " << i << std::endl;

//         auto pages =
//             links_sorted
//             .Zip(ranks, [](PageWithLinks first, PageWithRank second) {
//                      return std::make_tuple(first.first, second.second, first.second);
//                  });

//         auto contribs =
//             pages
//             .FlatMap<PageWithRank>(
//                 [](Page page, auto emit) {
//                     std::vector<int> urls = std::get<2>(page);
//                     double rank = std::get<1>(page);
//                     double num_urls = static_cast<double>(urls.size());
//                     for (int url : urls) {
//                         emit(std::make_pair(url, rank / num_urls));
//                     }
//                 });

//         ranks =
//             contribs
//             .ReducePairToIndex(
//                 [](double rank1, double rank2) {
//                     return rank1 + rank2;
//                 },
//                 size)
//             .Map([](PageWithRank page) {
//                      return std::make_pair(page.first, (1 - s) + page.second * s);
//                  })
//             .Cache();
//     }

//     ranks.Map([](PageWithRank item) {
//                   return std::to_string(item.first)
//                   + ": " + std::to_string(item.second);
//               }).
//     WriteLines("pagerank.out");
// }

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
        [&input, &output, & iter](api::Context& ctx) {
            thrill::common::StatsTimer<true> timer(false);
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


            ////////////////////////////////////////////////////////////////////////////
            //////////////////////// START OF COMPUTATION HERE /////////////////////////
            ////////////////////////////////////////////////////////////////////////////

            timer.Start();
            // read input file and create links in this format:
            //
            // url linked_url
            // url linked_url
            // url linked_url
            // ...

            auto in = ReadLines(ctx, input);
            auto input = in.Map(
                [](const std::string& input) {
                    auto split = thrill::common::split(input, " ");
                    LOG << "input "
                        << (std::size_t)(std::stoi(split[0]) - 1)
                        << " "
                        << (std::size_t)(std::stoi(split[1]) - 1);
                    // set base of page_id to 0
                    return std::make_pair((size_t)(std::stoi(split[0]) - 1),
                                          (size_t)(std::stoi(split[1]) - 1));
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
            auto links = input.GroupByIndex<Outgoings>(
                [](Page_Link p) { return p.first; },
                [](auto& r, Key) {
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
                }, number_nodes).Keep();

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
                [](const size_t&) {
                    return (Rank)1.0;
                }, number_nodes).Cache();

            auto node_ids = Generate(ctx,
                [](const size_t& index) {
                    return index+1;
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

                assert(links.Size() == ranks.Size());

                auto contribs = links.Zip(ranks,
                    [](const Outgoings& l, const Rank r){
                        // std::string s = "{";
                        // for (auto e : l) {
                        //     s += std::to_string(e) + ", ";
                        // }
                        // s += "}";
                        // LOG << "contribs1 " << s << " " << r;

                        return std::make_pair(l, r);
                    })
                    .FlatMap<Page_Rank>(
                    [](const Outgoings_Rank& p, auto emit){
                        if (p.first.size() > 0) {
                            Rank rank_contrib = p.second / p.first.size();
                            // assert (rank_contrib <= 1);
                            for(auto e : p.first) {
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
                    [](const Page_Rank& p) { return p.first; },
                    [](const Page_Rank& p1, const Page_Rank& p2) {
                        return std::make_pair(p1.first, p1.second+p2.second);
                    }, number_nodes)
                    .Map(
                    [](const Page_Rank p) {
                        LOG << "ranks2 in " << p;
                        if (std::fabs(p.second) <= 1E-5) {
                            LOG << "ranks2 " << 0.0;
                            return (Rank)0.0;
                        }else {
                            LOG << "ranks2 " << f + s * p.second;
                            return f + s * p.second;
                        }
                    }).Cache();
            }

            // write result to line. add 1 to node_ids to revert back to normal
            auto res = ranks.Zip(node_ids,
                [](const Rank r, const Node n) {
                    return std::to_string(n)
                    + ": " + std::to_string(r);
                });

            // assert (res.Size() == links.Size());

            res.WriteLines(output);
            timer.Stop();

            auto number_edges = in.Size();
            LOG1 << "\n"
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
