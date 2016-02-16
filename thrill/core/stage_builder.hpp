/*******************************************************************************
 * thrill/core/stage_builder.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
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

struct tm localtime_from(const time_t& t) {
#if __MINGW32__
    return *localtime(&t); // NOLINT
#else
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    localtime_r(&t, &tm);
    return tm;
#endif
}

//! format string using time structure
std::string format_time(const char* format, const struct tm& t) {
    char buffer[256];
    strftime(buffer, sizeof(buffer), format, &t);
    return buffer;
}

//! format string using time in localtime representation
std::string format_time(const char* format, const time_t& t) {
    return format_time(format, localtime_from(t));
}

class Stage
{
    static const bool debug = true;

public:
    using system_clock = std::chrono::system_clock;

    explicit Stage(DIABase* node) : node_(node) { }

    void Execute() {

        time_t tt = system_clock::to_time_t(system_clock::now());
        sLOG << "START  (EXECUTE) stage" << node_->label() << node_->id()
             << "time:" << format_time("%T", tt);

        timer.Start();
        node_->Execute();
        timer.Stop();

        tt = system_clock::to_time_t(system_clock::now());
        sLOG << "FINISH (EXECUTE) stage" << node_->label() << node_->id()
             << "took" << timer.Milliseconds() << "ms"
             << "time:" << format_time("%T", tt);

        timer.Start();
        node_->RunPushData(node_->consume_on_push_data());
        node_->set_state(api::DIAState::EXECUTED);
        timer.Stop();

        tt = system_clock::to_time_t(system_clock::now());
        sLOG << "FINISH (PUSHDATA) stage" << node_->label() << node_->id()
             << "took" << timer.Milliseconds() << "ms"
             << "time:" << format_time("%T", tt);
    }

    void PushData() {
        if (node_->consume_on_push_data() && node_->context().consume()) {
            sLOG1 << "StageBuilder: attempt to PushData on"
                  << "stage" << node_->label()
                  << "failed, it was already consumed. Add .Keep()";
            abort();
        }

        time_t tt = system_clock::to_time_t(system_clock::now());
        sLOG << "START  (PUSHDATA) stage" << node_->label() << node_->id()
             << "time:" << format_time("%T", tt);

        timer.Start();
        node_->RunPushData(node_->consume_on_push_data());
        node_->set_state(api::DIAState::EXECUTED);
        timer.Stop();

        tt = system_clock::to_time_t(system_clock::now());
        sLOG << "FINISH (PUSHDATA) stage" << node_->label() << node_->id()
             << "took" << timer.Milliseconds() << "ms"
             << "time:" << format_time("%T", tt);
    }

    DIABase * node() { return node_; }

private:
    common::StatsTimer<true> timer;
    DIABase* node_;
};

class StageBuilder
{
    static const bool debug = true;

public:
    template <typename T>
    using mm_set = std::set<T, std::less<T>, mem::Allocator<T> >;

    void FindStages(DIABase* action, mem::mm_vector<Stage>& stages_result) {
        LOG << "FINDING stages:";
        mm_set<const DIABase*> stages_found(
            mem::Allocator<const DIABase*>(action->mem_manager()));

        // Do a reverse BFS and find all stages
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
                    LOG << "FOUND: " << p->label() << '.' << p->id();
                    // If parent was not executed push it to the BFS queue
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

        for (Stage& s : result)
        {
            if (s.node()->state() == api::DIAState::NEW) {
                s.Execute();
            }
            else if (s.node()->state() == api::DIAState::EXECUTED) {
                bool skip = false;
                // for (const DIABase::Child& child : s.node()->children())
                //     if (child.node->state() != api::DIAState::EXECUTED ||
                //         child.node->type() == api::DIANodeType::COLLAPSE)
                //         skip = false;

                if (skip) continue;
                else s.PushData();
            }
            s.node()->UnregisterChilds();
        }
    }
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_STAGE_BUILDER_HEADER

/******************************************************************************/
