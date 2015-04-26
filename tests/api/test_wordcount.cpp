#include "gtest/gtest.h"
#include "c7a/api/dia_base.hpp"
#include "c7a/engine/stage_builder.hpp"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

using namespace c7a::engine;

TEST(WordCount, PreOP) {
    using c7a::DIA;
    using c7a::Context;

    Context ctx;
    auto doubles = ctx.ReadFromFileSystem(
        "tests/inputs/wordcount.in",
        [](const std::string& line) {
	  return std::make_pair(line,1);
        });

    using WordPair = std::pair<std::string,int>;

    auto key_ex = [](WordPair in) { return in.first; };
    auto red_fn = [](WordPair in1, WordPair in2) { return std::make_pair(in1.first, in1.second + in2.second); };

    auto rem_duplicates = doubles.Reduce(key_ex, red_fn);

    std::vector<Stage> result;
    FindStages(rem_duplicates.get_node(), result);
    for (auto s : result)
    {
        s.Run();
    }
}
