/*******************************************************************************
 * tests/api/test_wordcount.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include <tests/c7a-tests.hpp>
#include "c7a/api/dia_base.hpp"
#include "c7a/engine/stage_builder.hpp"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

using namespace c7a::engine;

TEST(WordCount, PreOP) {
    using c7a::DIA;
    using c7a::Context;

    Context ctx;
    auto doubles = ReadFromFileSystem(ctx,
        g_workpath + "/inputs/wordcount.in",
        [](const std::string& line) {
            return std::make_pair(line, 1);
        });

    using WordPair = std::pair<std::string, int>;

    auto key = [](WordPair in) { return in.first; };
    auto red_fn =
        [](WordPair in1, WordPair in2) {
            return std::make_pair(in1.first, in1.second + in2.second);
        };

    auto rem_duplicates = doubles.ReduceBy(key).With(red_fn);

    std::vector<Stage> result;
    FindStages(rem_duplicates.get_node(), result);
    for (auto s : result)
    {
        s.Run();
    }
}

/******************************************************************************/
