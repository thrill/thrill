/*******************************************************************************
 * examples/page_rank.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/bootstrap.hpp>
#include <c7a/api/dia.hpp>

int page_rank(c7a::Context& context);

int main(int argc, char* argv[]) {
    return c7a::Execute(argc, argv, page_rank);
}

//! The PageRank user program
int page_rank(c7a::Context& ctx) {
    using c7a::Context;

    double s = 0.85;

    using PageWithLinks = std::tuple<int, std::vector<int>>;
    using PageWithRank = std::tuple<int, double>;
    using Page = std::tuple<int, double, std::vector<int>>;

    auto link_print = [](PageWithLinks input) {
        std::stringstream ss;
        ss << "Page: " << std::get<0>(input);
        ss << " [ ";
        for (int p : std::get<1>(input)) ss << p << " ";
        ss << "]";
        return ss.str();
    };

    auto rank_print = [](PageWithRank input) {
        std::stringstream ss;
        ss << "Page: " << std::get<0>(input);
        ss << " Rank: " << std::get<1>(input);;
        return ss.str();
    };

    auto rank_write = [](PageWithRank input) {
        std::stringstream ss;
        ss << "Page: " << std::get<0>(input) << " Rank: " << std::get<1>(input);
        return ss.str();
    };

    auto key_page_with_links = [](PageWithLinks in) {
        return in.first;
    };

    auto key_page_with_rank = [](PageWithRank in) {
        return in.first;
    };

    auto links = ReadFromFileSystem(
        ctx,
        ctx.get_current_dir() + "/tests/inputs/page_rank.in",
        [](const std::string& line) {
            return line;
        });

    std::cout << "Initial Links:" << std::endl;
    links.Print(link_print);
    std::cout << std::endl;

    auto ranks = links.Map([](PageWithLinks input) {
        return std::make_tuple(std::get<0>(input), 1.0);
    });

    std::cout << "Initial Ranks:" << std::endl;
    ranks.Print(rank_print);
    std::cout << std::endl;

    for (std::size_t i = 1; i <= 10; ++i) {
        std::cout << "Iteration: " << i << std::endl;

        auto pages = links.Zip(ranks, [](PageWithLinks first, PageWithRank second) {
            return std::make_tuple(std::get<0>(first), std::get<1>(second), std::get<1>(first));
        });

        auto contribs = pages.FlatMap([](Page page, std::function<void(std::tuple<int, double>)> emit) {
            std::vector<int> urls = std::get<2>(page);
            double rank = std::get<1>(page);
            int num_urls = urls.size();
            for (int url : urls) {
                emit(std::make_tuple(url, rank / num_urls));
            }
            emit(std::make_tuple(std::get<0>(page), 0.0));
        });

        double dangling_sum = pages.Filter([](Page input) {
            int num_urls = std::get<2>(input).size();
            return (num_urls == 0);
        }).Map([](Page page) {
            return std::get<1>(page);
        }).Sum([](double first, double second) {
            return first + second;
        });

        auto red_words = word_pairs.ReduceBy(key).With(red_fn);


        ranks = contribs.ReduceBy(key_page_with_rank).With([](PageWithRank input) {
            return std::get<0>(input);
        }, [](std::vector<PageWithRank> ranks) {
            double sum = 0.0;
            int url = 0;
            for (PageWithRank r : ranks) {
                sum += std::get<1>(r);
                url = std::get<0>(r);
            }
            return std::make_tuple(url, sum);
        }).Map([&s, &dangling_sum](PageWithRank input) {
            int url = std::get<0>(input);
            double rank = std::get<1>(input);
            return std::make_tuple(url, rank * s + dangling_sum * s + (1 - s));
        });
    }

    std::cout << "Final Ranks:" << std::endl;
    ranks.Print(rank_print);
    std::cout << std::endl;

    ranks.WriteToFileSystem(ctx.get_current_dir() +
                            "/tests/outputs/page_rank.out",
                            rank_write);
    std::cout << "Final Ranks written to disk!" << std::endl;

    return 0;
}

/******************************************************************************/
