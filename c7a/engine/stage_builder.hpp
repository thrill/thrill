/*******************************************************************************
 * c7a/engine/stage_builder.hpp
 *
 * Functions to build stages
 ******************************************************************************/

#pragma once

#include "../api/dia_base.hpp"
#include "../common/logger.hpp"
#include <stack>
 #include <utility>

namespace c7a { 
namespace engine {

bool test = true;

class Stage
{
public:
    Stage(DIABase* node) : node_(node)
    {
        SpacingLogger(test) << "I'm creating a stage.";   
    }
    void Run() {
        SpacingLogger(test) << "I'm running a stage.";
        //GOAL: Make sure the stage is executed efficiently. 
        node_->execute();
    };
private:
    DIABase* node_;
};

// Returns a list of stages of graph scope
typedef std::pair<std::vector<Stage>::reverse_iterator, std::vector<Stage>::reverse_iterator> vec_it;
vec_it  FindStages(DIABase* action)
{
    SpacingLogger(test) << "I'm looking for stages:";

    std::vector<Stage> result_stages;

    // GOAL: Returns a vector with stages
    // TEMP SOLUTION: Every node is a stage
    std::stack<DIABase*> dia_stack;
    dia_stack.push(action);
    while (!dia_stack.empty()) {
        auto curr = dia_stack.top();
        dia_stack.pop();
        result_stages.emplace_back(Stage(curr));
        auto parents = curr->get_parents();
        for (auto p : parents) {
            dia_stack.push(p);
        }
    }

    auto it_begin = result_stages.rbegin();
    auto it_end = result_stages.rend();

    return std::make_pair(it_begin, it_end);
};

}}
