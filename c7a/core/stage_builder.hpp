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
    void Run() {
        LOG << "RUNNING stage " << node_->ToString() << "node" << node_;
        node_->Execute();
    }
    DIABase* node() {
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
                if (p) {
                    stages_found.push_back(p);
                    if (p->state() != c7a::api::CALCULATED) {
                        dia_stack.push(p);
                    }
                }
            }
        }

        for (auto stage : stages_found) {
            stages_result.emplace_back(Stage(stage));
        }
        std::reverse(stages_result.begin(), stages_result.end());
    }

    void RunScope(DIABase* action) {
        std::vector<Stage> result;
        FindStages(action, result);
        for (auto s : result)
        {
            s.Run();
            s.node()->set_state(c7a::api::CALCULATED);
        }
    }

    static const bool debug = false;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_STAGE_BUILDER_HEADER

/******************************************************************************/
