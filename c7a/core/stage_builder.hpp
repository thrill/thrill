/*******************************************************************************
 * c7a/core/stage_builder.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_STAGE_BUILDER_HEADER
#define C7A_CORE_STAGE_BUILDER_HEADER

#include <c7a/api/collapse.hpp>
#include <c7a/api/dia_base.hpp>
#include <c7a/common/logger.hpp>

#include <algorithm>
#include <set>
#include <stack>
#include <string>
#include <utility>
#include <vector>

namespace c7a {
namespace core {

using c7a::api::DIABase;

class Stage
{
public:
    explicit Stage(DIABase* node) : node_(node) {
        LOG << "CREATING stage" << node_->ToString() << "node" << node_;
    }

    void Execute() {
        LOG << "EXECUTING stage " << node_->ToString() << "node" << node_;
        node_->StartExecutionTimer();
        node_->Execute();
        node_->StopExecutionTimer();

        node_->PushData();
        node_->set_state(api::DIAState::EXECUTED);
    }

    void PushData() {
        LOG << "PUSHING stage " << node_->ToString() << "node" << node_;
        node_->PushData();
        node_->set_state(api::DIAState::EXECUTED);
    }

    void Dispose() {
        LOG << "DISPOSING stage " << node_->ToString() << "node" << node_;
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
                if (p && (stages_found.find(p) == stages_found.end())) {
                    // If not add parent to stages found and result stages
                    stages_found.insert(p);
                    stages_result.push_back(Stage(p));
                    // If parent was not executed push it to the DFS
                    if (p->state() != api::DIAState::EXECUTED || p->type() == api::NodeType::COLLAPSE) {
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
} // namespace c7a

#endif // !C7A_CORE_STAGE_BUILDER_HEADER

/******************************************************************************/
