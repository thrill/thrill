/*******************************************************************************
 * tests/api/wordcount_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia_base.hpp>
#include <c7a/net/net_endpoint.hpp>
#include <c7a/core/job_manager.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/api/dia.hpp>
#include <tests/c7a_tests.hpp>

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;

TEST(WordCount, WordCountExample) {
    using c7a::Context;
    using WordPair = std::pair<std::string, int>;

    Context ctx;
    std::vector<std::string> self = { "127.0.0.1:1234" };
    ctx.job_manager().Connect(0, NetEndpoint::ParseEndpointList(self));
    ctx.job_manager().StartDispatcher();

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
        g_workpath + "/inputs/wordcount.in",
        [](const std::string& line) {
            return line;
        });

    auto word_pairs = lines.FlatMap(line_to_words);

    auto red_words = word_pairs.ReduceBy(key).With(red_fn);

    red_words.WriteToFileSystem(g_workpath + "/outputs/wordcount.out",
                                [](const WordPair& item) {
                                    std::string str;
                                    str += item.first;
                                    str += ": ";
                                    str += item.second;
                                    return str;
                                });
    ctx.job_manager().StopDispatcher();
}

/******************************************************************************/
