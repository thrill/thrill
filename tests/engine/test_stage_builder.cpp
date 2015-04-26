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
    auto map_fn = [](double input) {
            std::cout << "Map" << std::endl;
            return input;
        };
    auto fmap_fn = [](double input, std::function<void(double)> emit_func) {
            std::cout << "FlatMap" << std::endl;
            emit_func(input);
            emit_func(input);
        };

    auto duplicates = doubles.Map(map_fn);
    auto duplicates2 = duplicates.Map(map_fn);
    auto red_duplicates = duplicates2.Reduce(key_ex, red_fn);

    auto duplicates3 = red_duplicates.Map(map_fn);
    auto red_duplicates2 = duplicates3.Reduce(key_ex, red_fn);

    auto stages = FindStages(duplicates3.get_node());
    for (auto it = stages.first; it != stages.second; ++it) {
        it->Run();
    }
}
