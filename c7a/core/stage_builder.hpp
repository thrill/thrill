/*******************************************************************************
 * c7a/core/stage_builder.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#ifndef DEBUG
#define DEBUG = FALSE
#endif

#pragma once
#ifndef C7A_CORE_STAGE_BUILDER_HEADER
#define C7A_CORE_STAGE_BUILDER_HEADER

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

    // Do a reverse DFS and find all stages
    std::stack<DIABase*> dia_stack;
    dia_stack.push(action);
    stages_found.insert(action);
    while (!dia_stack.empty()) {
        DIABase* curr = dia_stack.top();
        dia_stack.pop();
        stages_result.emplace_back(Stage(curr));

        std::vector<DIABase*> parents = curr->get_parents();
        for (DIABase* p : parents) {
            // if p is not a nullpointer and p is not cached mark it and save stage
            if (p && (stages_found.find(p) == stages_found.end()) && p->state() != CACHED) {
                dia_stack.push(p);
                stages_found.insert(p);
            }
            else SpacingLogger(true) << "OMG NULLPTR";
        }
    }

    std::reverse(stages_result.begin(), stages_result.end());
}

inline void RunScope(DIABase* action) 
{
    std::vector<Stage> result;
    FindStages(action, result);
    for (auto s : result)
    {
        s.Run();
    }
}


} // namespace engine
} // namespace c7a

#endif // !C7A_CORE_STAGE_BUILDER_HEADER

/******************************************************************************/
