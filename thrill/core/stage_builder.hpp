/*******************************************************************************
 * thrill/core/stage_builder.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
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
#include <chrono>
#include <ctime>
#include <functional>
#include <iomanip>
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
    explicit Stage(DIABase* node) : node_(node) { }

    void Execute() {
        // time_t tt;
        // timer.Start();
        node_->Execute();
        // timer.Stop();
        // tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        // STAT(node_->context())
        //    << "FINISH (EXECUTING) stage" << node_->label() << node_->id();
        //    << "took (ms)" << timer.Milliseconds()
        //    << "time:" << std::put_time(std::localtime(&tt), "%T");

        // tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        // timer.Start();
        node_->DoPushData(node_->consume_on_push_data());
        node_->set_state(api::DIAState::EXECUTED);
        // timer.Stop();
        // tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        // STAT(node_->context())
        //    << "FINISH (PUSHING) stage" << node_->label() << node_->id();
        //    << "took (ms)" << timer.Milliseconds()
        //    << "time:" << std::put_time(std::localtime(&tt), "%T");
    }

    void PushData() {
        // time_t tt;
        if (node_->consume_on_push_data()) {
            sLOG1 << "StageBuilder: Attempt to PushData on"
                  << "stage" << node_->label()
                  << "failed, it was already consumed. Add .Keep()";
            abort();
        }
        die_unless(!node_->consume_on_push_data());
        // timer.Start();
        node_->DoPushData(node_->consume_on_push_data());
        node_->set_state(api::DIAState::EXECUTED);
        // timer.Stop();
        // tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        // STAT(node_->context())
        //    << "FINISH (PUSHING) stage" << node_->label() << node_->id();
        //    << "took (ms)" << timer.Milliseconds()
        //    << "time: " << std::put_time(std::localtime(&tt), "%T");
    }

    void Dispose() {
        // time_t tt;
        // timer.Start();
        node_->Dispose();
        // timer.Stop();
        node_->set_state(api::DIAState::DISPOSED);
        // tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        // STAT(node_->context())
        //    << "FINISH (DISPOSING) stage" << node_->label()
        //    << "took (ms)" << timer.Milliseconds()
        //    << "time:" << std::put_time(std::localtime(&tt), "%T");
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
                    LOG1 << "FOUND: " << p->label() << " " << p->id();
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
            if (s.node()->state() == api::DIAState::EXECUTED) {
                bool skip = true;
                for (DIABase* child : s.node()->children()) 
                    if(child->state() != api::DIAState::EXECUTED 
                            || child->type() == api::DIANodeType::COLLAPSE) 
                        skip = false;
                if (skip) continue;
                else s.PushData();
            }
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
