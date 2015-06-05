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

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;

TEST(WordCount, WordCountExample) {
    using c7a::Context;
    using WordPair = std::pair<std::string, int>;

    Context ctx;
    std::vector<std::string> self = { "127.0.0.1:1234" };
    ctx.job_manager().Connect(0, Endpoint::ParseEndpointList(self));

    auto line_to_words = [](std::string line, std::function<void(WordPair)> emit) {
                             std::string word;
                             std::istringstream iss(line);
                             while (iss >> word) {
                                 WordPair wp = std::make_pair(word, 1);
                                 emit(wp);
                             }
                         };
    auto key = [](WordPair in) {
                   return in.first;
               };
    auto red_fn = [](WordPair in1, WordPair in2) {
                      WordPair wp = std::make_pair(in1.first, in1.second + in2.second);
                      return wp;
                  };

    auto lines = ReadFromFileSystem(
        ctx,
        "wordcount.in",
        [](const std::string& line) {
            return line;
        });

    auto word_pairs = lines.FlatMap(line_to_words);

    auto red_words = word_pairs.ReduceBy(key).With(red_fn);

    red_words.WriteToFileSystem("wordcount.out",
                                [](const WordPair& item) {
                                    std::string str;
                                    str += item.first;
                                    str += ": ";
                                    str += item.second;
                                    return str;
                                });
}

int word_count_generated_nored(c7a::Context& ctx, size_t size) {
    using c7a::Context;
    using WordPair = std::pair<std::string, int>;

    auto word_pair_gen = [](std::string line) {
                             WordPair wp = std::make_pair(line, 1);
                             return wp;
                         };

    auto key = [](WordPair in) {
                   return in.first;
               };

    auto red_fn = [](WordPair in1, WordPair in2) {
                      WordPair wp = std::make_pair(in1.first, in1.second + in2.second);
                      return wp;
                  };

    auto lines = GenerateFromFile(
        ctx,
        "headwords",
        [](const std::string& line) {
            return line;
        },
        size);

    auto word_pairs = lines.Map(word_pair_gen);

    word_pairs.WriteToFileSystem("wordcount" + std::to_string(ctx.rank()) + ".out",
                                 [](const WordPair& item) {
                                     std::string str;
                                     str += item.first;
                                     str += ": ";
                                     str += std::to_string(item.second);
                                     //  std::cout << str << std::endl;
                                     return str;
                                 });
    return 0;
}

TEST(WordCount, GenerateAndWriteWith2Workers) {
    using c7a::Execute;

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(30000, 65000);
    const size_t port_base = distribution(generator);

    unsigned int workers = 2;

    unsigned int elements = 100;

    std::vector<std::thread> threads(workers);
    std::vector<char**> arguments(workers);
    std::vector<std::vector<std::string> > strargs(workers);

    for (size_t i = 0; i < workers; i++) {

        arguments[i] = new char*[workers + 3];
        strargs[i].resize(workers + 3);

        for (size_t j = 0; j < workers; j++) {
            strargs[i][j + 3] += "127.0.0.1:";
            strargs[i][j + 3] += std::to_string(port_base + j);
            arguments[i][j + 3] = const_cast<char*>(strargs[i][j + 2].c_str());
        }

        std::function<int(c7a::Context&)> start_func = [elements](c7a::Context& ctx) {
                                                           return word_count_generated_nored(ctx, elements);
                                                       };

        strargs[i][0] = "wordcount";
        arguments[i][0] = const_cast<char*>(strargs[i][0].c_str());
        strargs[i][1] = "-r";
        arguments[i][1] = const_cast<char*>(strargs[i][1].c_str());
        strargs[i][2] = std::to_string(i);
        arguments[i][2] = const_cast<char*>(strargs[i][2].c_str());
        threads[i] = std::thread([=]() { Execute(workers + 2, arguments[i], start_func); });
    }

    for (size_t i = 0; i < workers; i++) {
        threads[i].join();
    }
}

/******************************************************************************/
