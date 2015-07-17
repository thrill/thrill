/*******************************************************************************
 * examples/page_rank.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <random>
#include <thread>
#include <string>

#include <c7a/api/bootstrap.hpp>
#include <c7a/common/cmdline_parser.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/c7a.hpp>

using c7a::Context;
using c7a::DIARef;

int page_rank(Context& context);

int main(int argc, char* argv[]) {
    return c7a::api::Execute(argc, argv, page_rank);
}

//! The PageRank user program
int page_rank(Context& ctx) {

    double s = 0.85;

    using PageWithLinks = std::tuple<int, std::vector<int> >;
    using PageWithRank = std::tuple<int, double>;
    using Page = std::tuple<int, double, std::vector<int> >;

    auto key_page_with_links = [](PageWithLinks in) {
                                   return in.first;
                               };

    auto key_page_with_rank = [](PageWithRank in) {
                                  return in.first;
                              };

    auto links = ReadLines(
        ctx,
        "pagerank.in",
        [](const std::string& line) {
            return line;
        });

    DIARef<PageWithRank> ranks =
        links
        .Map([](PageWithLinks input) {
                 return std::make_tuple(std::get<0>(input), 1.0);
             });

    for (std::size_t i = 1; i <= 10; ++i) {
        std::cout << "Iteration: " << i << std::endl;

        auto pages =
            links
            .Zip([](PageWithLinks first, PageWithRank second) {
                     return std::make_tuple(std::get<0>(first),
                                            std::get<1>(second),
                                            std::get<1>(first));
                 }, ranks);

        auto contribs = pages.FlatMap<PageWithRank>(
            [](Page page, auto emit) {
                std::vector<int> urls = std::get<2>(page);
                double rank = std::get<1>(page);
                int num_urls = urls.size();
                for (int url : urls) {
                    emit(std::make_tuple(url, rank / num_urls));
                }
                emit(std::make_tuple(std::get<0>(page), 0.0));
            });

        double dangling_sum = pages.Filter(
            [](Page input) {
                int num_urls = std::get<2>(input).size();
                return (num_urls == 0);
            }).Map([](Page page) {
                       return std::get<1>(page);
                   }).Sum([](double first, double second) {
                              return first + second;
                          });

        ranks = contribs
                .ReduceToIndex(key_page_with_rank, [](PageWithRank input) {
                                   return std::get<0>(input);
                               }, [](std::vector<PageWithRank> ranks) {
                                   double sum = 0.0;
                                   int url = 0;
                                   for (PageWithRank r : ranks) {
                                       sum += std::get<1>(r);
                                       url = std::get<0>(r);
                                   }
                                   return std::make_tuple(url, sum);
                               })
                .Map([&s, &dangling_sum](PageWithRank input) {
                         int url = std::get<0>(input);
                         double rank = std::get<1>(input);
                         return std::make_tuple(url, rank * s + dangling_sum * s + (1 - s));
                     });
    }

    ranks.WriteToFileSystem(
        "pagerank_" + std::to_string(ctx.rank()) + ".out",
        [](const WordCount& item) {
            return item.first + ": " + std::to_string(item.second);
        });

    return 0;
}

/******************************************************************************/
