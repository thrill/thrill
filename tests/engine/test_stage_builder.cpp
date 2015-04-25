#include "gtest/gtest.h"
#include "c7a/api/dia_base.hpp"
#include "c7a/engine/stage_builder.hpp"

using namespace c7a::engine;

TEST(Stage, GetStagesFromBuilder) {
    //create DIABase graph
    c7a::DIABase a = c7a::DIABase();
    c7a::DIABase b = c7a::DIABase();
    c7a::DIABase c = c7a::DIABase();
    a.add_child(b);
    a.add_child(c);

    auto stages = FindStages(a);
    for (auto s : stages)
    {
        s.Run();
    }
}
