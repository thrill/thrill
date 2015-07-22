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
        node_->Execute();
        node_->PushData();
        node_->UnregisterChilds();
        node_->set_state(c7a::api::EXECUTED);
    }

    void PushData() {
        LOG << "PUSHING stage " << node_->ToString() << "node" << node_;
        node_->PushData();
    }

    void Dispose() {
        LOG << "DISPOSING stage " << node_->ToString() << "node" << node_;
        node_->Dispose();
        node_->set_state(c7a::api::DISPOSED);
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
        // std::set<const DIABase*> stages_found;
        std::vector<DIABase*> stages_found;
        // Do a reverse DFS and find all stages
        std::stack<DIABase*> dia_stack;
        dia_stack.push(action);
        // stages_found.insert(action);
        stages_found.push_back(action);
        while (!dia_stack.empty()) {
            DIABase* curr = dia_stack.top();
            dia_stack.pop();
            const auto parents = curr->parents();
            for (size_t i = 0; i < parents.size(); ++i) {
                auto p = parents[i].get();
                stages_found.push_back(p);
                if (p->state() != c7a::api::EXECUTED) {
                    dia_stack.push(p);
                }
            }
        }
        for (auto stage : stages_found) {
            stages_result.emplace_back(stage);
        }
        std::reverse(stages_result.begin(), stages_result.end());
    }

    void RunScope(DIABase* action) {
        std::vector<Stage> result;
        FindStages(action, result);
        for (auto s : result)
        {
            // TODO(sl): This is nonsense -tb, fix it:
            if (s.node()->state() == c7a::api::EXECUTED) s.PushData();
            if (s.node()->state() == c7a::api::NEW) s.Execute();
        }
    }

    static const bool debug = false;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_STAGE_BUILDER_HEADER

/******************************************************************************/
