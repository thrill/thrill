#include "gtest/gtest.h"
#include "c7a/api/dia_base.hpp"
#include "c7a/engine/stage_builder.hpp"
#include "c7a/api/dia.hpp"
#include "c7a/api/context.hpp"

using namespace c7a::engine;

TEST(Stage, GetStagesFromBuilder) {
    using c7a::DIA;
    using c7a::Context;
    
    auto doubles = Context().ReadFromFileSystem("tests/inputs/test1", [](std::string line) {
            return std::stod(line);
        });

    auto key_ex = [](double in) { return (int) in; };
    auto red_fn = [](double in1, double in2) { return in1 + in2; };
    auto map_fn = [](double input, std::function<void(double)> emit_func) {
            emit_func(input);
            emit_func(input);
        };

    auto duplicates = doubles.FlatMap(map_fn);
    auto reduced_doubles = doubles.Reduce(key_ex, red_fn);
    auto reduced_duplicates = reduced_doubles.FlatMap(map_fn);
    auto reduced_duplicates2 = reduced_doubles.FlatMap(map_fn);
    auto reduced_duplicates3 = reduced_doubles.FlatMap(map_fn);
    auto reduced_duplicates4 = reduced_duplicates.FlatMap(map_fn);
    auto reduced_doubles2 = doubles.Reduce(key_ex, red_fn);


    auto stages = FindStages(&(*reduced_doubles2));
    for (auto it = stages.first; it != stages.second; ++it)
    {
        it->Run();
    }
}
