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

static const bool test = true;

class Stage
{
public:
    Stage(DIABase* node) : node_(node)
    {
        SpacingLogger(test) << "I'm creating stage" << node_->ToString() << node_;   
    }
    void Run() {
        SpacingLogger(test) << "I'm running stage" << node_->ToString() << node_;
        //GOAL: Make sure the stage is executed efficiently. 
        node_->execute();
    };
private:
    DIABase* node_;
};

// Returns a list of stages of graph scope
typedef std::pair<std::vector<Stage>::reverse_iterator, std::vector<Stage>::reverse_iterator> vec_it;

static inline vec_it  FindStages(DIABase* action)
{
    SpacingLogger(test) << "I'm looking for stages:";

    std::vector<Stage> result_stages;

    // GOAL: Returns a vector with stages
    // TEMP SOLUTION: Every node is a stage
    std::stack<DIABase*> dia_stack;
    dia_stack.push(action);
    while (!dia_stack.empty()) {
        DIABase* curr = dia_stack.top();
        dia_stack.pop();
        result_stages.emplace_back(Stage(curr));
        std::vector<DIABase*> parents = curr->get_parents();
        for (DIABase* p : parents) {
            dia_stack.push(p);
        }
    }

    auto it_begin = result_stages.rbegin();
    auto it_end = result_stages.rend();

    return std::make_pair(it_begin, it_end);
};

}}// !C7A_ENGINE_STAGE_BUILD
