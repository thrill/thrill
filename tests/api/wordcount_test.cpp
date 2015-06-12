/*******************************************************************************
 * tests/api/wordcount_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia_base.hpp>
#include <c7a/net/endpoint.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/api/bootstrap.hpp>
#include <c7a/common/string.hpp>

#include <examples/word_count_user_program.cpp>

#include <map>

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;

TEST(WordCount, WordCountSingleWorker) {
    using c7a::Context;
    using WordPair = std::pair<std::string, int>;

    Context ctx;
    std::vector<std::string> self = { "127.0.0.1:1234" };
    ctx.job_manager().Connect(0, Endpoint::ParseEndpointList(self));

    auto lines = ReadLines(
        ctx,
        "wordcounttest",
        [](const std::string& line) {
            return line;
        });

    auto red_words = word_count_user(lines);

    //TODO(an) : Assert size == 5

    red_words.WriteToFileSystem("wordcount.out",
                                [](const WordPair& item) {
                                    std::string str;
                                    str += item.first;
                                    str += ":";
                                    str.append(std::to_string(item.second));
                                    return str;
                                });
}



/*TEST(WordCount, WordcountLocalMultipleWorkers) {
    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(30000, 65000);
    const size_t port_base = distribution(generator);
    
    std::uniform_int_distribution<int> distr(2, 4);

    unsigned int workers = distr(generator);

    unsigned int elements = distribution(generator);

    auto lines = ReadLines(
        ctx,
        "wordcounttest",
        [](const std::string& line) {
            return line;
        });

    auto red_words = word_count_user(lines);

    //TODO(an) : Assert size == 5

    red_words.WriteToFileSystem("wordcount.out",
                                [](const WordPair& item) {
                                    std::string str;
                                    str += item.first;
                                    str += ":";
                                    str.append(std::to_string(item.second));
                                    return str;
                                });
    
                                }*/

/******************************************************************************/
