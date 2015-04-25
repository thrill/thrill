/*******************************************************************************
 * c7a/engine/stage_builder.hpp
 *
 * Functions to build stages
 ******************************************************************************/

#pragma once

#include "../api/dia_base.hpp"
#include "../common/logger.hpp"
#include <stack>

namespace c7a { 
namespace engine {

bool test = true;

class Stage
{
public:
    Stage(DIABase &dop_root) : dop_root_(dop_root)
    {
        SpacingLogger(test) << "I'm creating a stage.";   
    }
    void Run() {
        SpacingLogger(test) << "I'm running a stage.";
        //GOAL: Make sure the stage is executed efficiently. 
        dop_root_.execute();
    };
private:
    DIABase dop_root_;
};

// Returns a list of stages of graph scope
std::vector<Stage> FindStages(DIABase &scope_root)
{
    SpacingLogger(test) << "I'm looking for stages:";

    std::vector<Stage> result_stages;

    // GOAL: Returns a vector with stages
    // TEMP SOLUTION: Every node is a stage
    std::stack<DIABase> dia_stack;
    dia_stack.push(scope_root);
    while (!dia_stack.empty()) {
        auto curr = dia_stack.top();
        dia_stack.pop();
        result_stages.emplace_back(Stage(curr));
        auto children = curr.get_childs();
        for (auto c : children) {
            dia_stack.push(c);
        }
    }

    return result_stages;
};

}}
