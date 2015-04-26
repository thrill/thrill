#include "gtest/gtest.h"
#include "c7a/api/dia_base.hpp"
#include "c7a/engine/stage_builder.hpp"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

using namespace c7a::engine;

TEST(WordCount, PreOP) {
    using c7a::DIA;
    using c7a::Context;

    auto doubles = Context().ReadFromFileSystem("tests/inputs/wordcount.in", [](std::string line) {
            return line;
        });

    auto key_ex = [](std::string in) { return in; };
    auto red_fn = [](std::string in1, std::string in2) { return in1; };

    auto rem_duplicates = doubles.Reduce(key_ex, red_fn);

    std::vector<Stage> result;
    FindStages(rem_duplicates.get_node(), result);
    for (auto s : result)
    {
        s.Run();
    }
}
