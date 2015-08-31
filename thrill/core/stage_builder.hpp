/*******************************************************************************
 * thrill/core/stage_builder.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_STAGE_BUILDER_HEADER
#define THRILL_CORE_STAGE_BUILDER_HEADER

#include <thrill/api/collapse.hpp>
#include <thrill/api/dia_base.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <set>
#include <stack>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

using api::DIABase;

class Stage
{
public:
    explicit Stage(DIABase* node) : node_(node) {
        sLOG << "CREATING stage" << node_->NameString() << "id" << node_->id()
             << "node" << node_;
    }

    void Execute() {
        sLOG << "EXECUTING stage " << node_->NameString() << "id" << node_->id()
             << "node" << node_;
        node_->StartExecutionTimer();
        node_->Execute();
        node_->StopExecutionTimer();

        node_->PushData();
        node_->set_state(api::DIAState::EXECUTED);
    }

    void PushData() {
        sLOG << "PUSHING stage " << node_->NameString() << "id" << node_->id()
             << "node" << node_;
        node_->PushData();
        node_->set_state(api::DIAState::EXECUTED);
    }

    void Dispose() {
        sLOG << "DISPOSING stage " << node_->NameString() << "id" << node_->id()
             << "node" << node_;
        node_->Dispose();
        node_->set_state(api::DIAState::DISPOSED);
    }

    DIABase * node() {
        return node_;
    }

private:
    static const bool debug = false;
    DIABase* node_;
};

class StageBuilder
{
public:
    void FindStages(DIABase* action, std::vector<Stage>& stages_result) {
        LOG << "FINDING stages:";
        std::set<const DIABase*> stages_found;
        // Do a reverse DFS and find all stages
        std::stack<DIABase*> dia_stack;
        dia_stack.push(action);
        stages_found.insert(action);
        stages_result.push_back(Stage(action));
        while (!dia_stack.empty()) {
            DIABase* curr = dia_stack.top();
            dia_stack.pop();
            const auto parents = curr->parents();
            for (size_t i = 0; i < parents.size(); ++i) {
                // Check if parent was already added
                auto p = parents[i].get();
                if (p) {
                    // If not add parent to stages found and result stages
                    stages_found.insert(p);
                    stages_result.push_back(Stage(p));
                    // If parent was not executed push it to the DFS
                    if (p->state() != api::DIAState::EXECUTED ||
                        p->type() == api::DIANodeType::COLLAPSE) {
                        dia_stack.push(p);
                    }
                }
            }
        }
        // Reverse the execution order
        std::reverse(stages_result.begin(), stages_result.end());
    }

    void RunScope(DIABase* action) {
        std::vector<Stage> result;
        FindStages(action, result);
        for (auto s : result)
        {
            if (s.node()->state() == api::DIAState::EXECUTED) s.PushData();
            if (s.node()->state() == api::DIAState::NEW) s.Execute();
            s.node()->UnregisterChilds();
        }
    }

    static const bool debug = false;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_STAGE_BUILDER_HEADER

/******************************************************************************/
