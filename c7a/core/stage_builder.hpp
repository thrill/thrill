/*******************************************************************************
 * c7a/core/stage_builder.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_ENGINE_STAGE_BUILDER_HEADER
#define C7A_ENGINE_STAGE_BUILDER_HEADER

#include "../api/dia_base.hpp"
#include "../common/logger.hpp"
#include <stack>
#include <string>
#include <utility>
#include <algorithm>
#include <set>

namespace c7a { 
namespace core {

class Stage
{
public:
    Stage(DIABase* node) : node_(node)
    {
        SpacingLogger(true) << "CREATING stage" << node_->ToString() << "node" << node_;
    }
    void Run()
    {
        SpacingLogger(true) << "RUNNING stage " << node_->ToString() << "node" << node_;
        //GOAL: Make sure the stage is executed efficiently.
        node_->execute();
    }

private:
    DIABase* node_;
};

// Returns a list of stages of graph scope
inline void FindStages(DIABase* action, std::vector<Stage>& stages_result)
{
    SpacingLogger(true) << "FINDING stages:";
    std::set<DIABase*> stages_found;
    // GOAL: Returns a vector with stages
    // TEMP SOLUTION: Every node is a stage
    std::stack<DIABase*> dia_stack;
    dia_stack.push(action);
    stages_found.insert(action);
    while (!dia_stack.empty()) {
        DIABase* curr = dia_stack.top();
        dia_stack.pop();
        stages_result.emplace_back(Stage(curr));
        std::vector<DIABase*> parents = curr->get_parents();
        for (DIABase* p : parents) {
            if (p && (stages_found.find(p) == stages_found.end())) {
                dia_stack.push(p);
                stages_found.insert(p);
            }
            else SpacingLogger(true) << "OMG NULLPTR";
        }
    }

    std::reverse(stages_result.begin(), stages_result.end());
}

} // namespace engine

} // namespace c7a

#endif // !C7A_ENGINE_STAGE_BUILDER_HEADER

/******************************************************************************/
