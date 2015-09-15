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
#include <thrill/common/stat_logger.hpp>
#include <thrill/common/stats_timer.hpp>

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
        STAT(node_->context()) << "CREATING stage" << node_->label() << "id" << node_->id()
                     << "node" << node_;
    }

    void Execute() {
        STAT(node_->context()) << "START(EXECUTING) stage" << node_->label() << "id" << node_->id()
                     << "node" << node_;
        timer.Start();
        // node_->StartExecutionTimer();
        node_->Execute();
        // node_->StopExecutionTimer();
        timer.Stop();
        STAT(node_->context()) << "FINISH (EXECUTING) stage" << node_->label() << "id" << node_->id() 
                     << "node" << node_ << "took" << timer.Microseconds();

        node_->PushData(node_->consume_on_push_data());
        node_->set_state(api::DIAState::EXECUTED);
    }

    void PushData() {
        STAT(node_->context()) << "START (PUSHING) stage" << node_->label() << "id" << node_->id()
                     << "node" << node_;
        timer.Start();
        die_unless(!node_->consume_on_push_data());
        node_->PushData(node_->consume_on_push_data());
        node_->set_state(api::DIAState::EXECUTED);
        timer.Stop();
        STAT(node_->context()) << "FINISH (PUSHING) stage" << node_->label() << "id" << node_->id()
                     << "node" << node_ << "took" << timer.Microseconds();
    }

    void Dispose() {
        STAT(node_->context()) << "START (DISPOSING) stage" << node_->label() << "id" << node_->id()
                     << "node" << node_;
        timer.Start();
        node_->Dispose();
        node_->set_state(api::DIAState::DISPOSED);
        timer.Stop();
        STAT(node_->context()) << "FINISH (DISPOSING) stage" << node_->label() << "id" << node_->id()
                     << "node" << node_ << "took" << timer.Microseconds();
    }

    DIABase * node() {
        return node_;
    }

private:
    static const bool debug = false;
    common::StatsTimer<true> timer;
    DIABase* node_;
};

class StageBuilder
{
public:
    template <typename T>
    using mm_set = std::set<T, std::less<T>, mem::Allocator<T> >;

    void FindStages(DIABase* action, mem::mm_vector<Stage>& stages_result) {
        LOG << "FINDING stages:";
        mm_set<const DIABase*> stages_found(
            mem::Allocator<const DIABase*>(action->mem_manager()));

        // Do a reverse DFS and find all stages
        mem::mm_deque<DIABase*> dia_stack(
            mem::Allocator<DIABase*>(action->mem_manager()));

        dia_stack.push_back(action);
        stages_found.insert(action);
        stages_result.push_back(Stage(action));
        while (!dia_stack.empty()) {
            DIABase* curr = dia_stack.front();
            dia_stack.pop_front();
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
                        dia_stack.push_back(p);
                    }
                }
            }
        }
        // Reverse the execution order
        std::reverse(stages_result.begin(), stages_result.end());
    }

    void RunScope(DIABase* action) {
        mem::mm_vector<Stage> result(
            mem::Allocator<Stage>(action->mem_manager()));

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
