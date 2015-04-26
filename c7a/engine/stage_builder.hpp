/*******************************************************************************
 * c7a/engine/stage_builder.hpp
 *
 * Functions to build stages
 ******************************************************************************/

#pragma once

#include "../api/dia_base.hpp"
#include "../common/logger.hpp"
#include <stack>
#include <string>
#include <utility>
#include <algorithm>

namespace c7a { 
namespace engine {



class Stage {
public:
    Stage(DIABase* node) : node_(node) {
        SpacingLogger(true) << "CREATING stage" << node_->ToString() << "node" << node_;   
    }
    void Run() {
        SpacingLogger(true) << "RUNNING stage " << node_->ToString() << "node" << node_;
        //GOAL: Make sure the stage is executed efficiently. 
        node_->execute();
    };
private:
    DIABase* node_;
};

// Returns a list of stages of graph scope
void  FindStages(DIABase* action, std::vector<Stage> & stages_result) {
    SpacingLogger(true) << "FINDING stages:";

    // GOAL: Returns a vector with stages
    // TEMP SOLUTION: Every node is a stage
    std::stack<DIABase*> dia_stack;
    dia_stack.push(action);
    while (!dia_stack.empty()) {
        DIABase* curr = dia_stack.top();
        // SpacingLogger(true) << curr;
        dia_stack.pop();
        stages_result.emplace_back(Stage(curr));
        std::vector<DIABase*> parents = curr->get_parents();
        for (DIABase* p : parents) {
            // SpacingLogger(true) << "PTR IS" << p; 
            if (p) dia_stack.push(p);
            else SpacingLogger(true) << "OMG NULLPTR";
        }
    }

    std::reverse(stages_result.begin(),stages_result.end());
};

}}// !C7A_ENGINE_STAGE_BUILD
