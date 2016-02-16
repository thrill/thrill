/*******************************************************************************
 * thrill/api/dia_base.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/dia_base.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stat_logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/mem/allocator.hpp>

#include <algorithm>
#include <chrono>
#include <deque>
#include <functional>
#include <iomanip>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

/******************************************************************************/
// DIABase StageBuilder

static inline
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
static inline
std::string format_time(const char* format, const struct tm& t) {
    char buffer[256];
    strftime(buffer, sizeof(buffer), format, &t);
    return buffer;
}

//! format string using time in localtime representation
static inline
std::string format_time(const char* format, const time_t& t) {
    return format_time(format, localtime_from(t));
}

class Stage
{
    static const bool debug = false;

public:
    using system_clock = std::chrono::system_clock;

    explicit Stage(DIABase* node) : node_(node) { }

    //! compute a string to show all target nodes into which this Stage pushes.
    std::string Targets() const {
        std::ostringstream oss;
        std::vector<DIABase*> children = node_->children();
        std::reverse(children.begin(), children.end());

        oss << '[';
        while (children.size())
        {
            DIABase* child = children.back();
            children.pop_back();

            if (child == nullptr) {
                oss << ']';
            }
            else if (child->type() == DIANodeType::COLLAPSE) {
                // push children of Collapse onto stack
                std::vector<DIABase*> sub = child->children();
                children.push_back(nullptr);
                children.insert(children.end(), sub.begin(), sub.end());
                oss << *child << ' ' << '[';
            }
            else {
                oss << *child << ' ';
            }
        }
        oss << ']';
        return oss.str();
    }

    void Execute() {

        time_t tt = system_clock::to_time_t(system_clock::now());
        sLOG << "START  (EXECUTE) stage" << *node_ << "targets" << Targets()
             << "time:" << format_time("%T", tt);

        timer.Start();
        node_->Execute();
        timer.Stop();

        tt = system_clock::to_time_t(system_clock::now());
        sLOG << "FINISH (EXECUTE) stage" << *node_ << "targets" << Targets()
             << "took" << timer.Milliseconds() << "ms"
             << "time:" << format_time("%T", tt);

        timer.Start();
        node_->RunPushData(node_->consume_on_push_data());
        node_->set_state(DIAState::EXECUTED);
        timer.Stop();

        tt = system_clock::to_time_t(system_clock::now());
        sLOG << "FINISH (PUSHDATA) stage" << *node_ << "targets" << Targets()
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
        sLOG << "START  (PUSHDATA) stage" << *node_ << "targets" << Targets()
             << "time:" << format_time("%T", tt);

        timer.Start();
        node_->RunPushData(node_->consume_on_push_data());
        node_->set_state(DIAState::EXECUTED);
        timer.Stop();

        tt = system_clock::to_time_t(system_clock::now());
        sLOG << "FINISH (PUSHDATA) stage" << *node_ << "targets" << Targets()
             << "took" << timer.Milliseconds() << "ms"
             << "time:" << format_time("%T", tt);
    }

    DIABase * node() { return node_; }

private:
    common::StatsTimer<true> timer;
    DIABase* node_;
};

template <typename T>
using mm_set = std::set<T, std::less<T>, mem::Allocator<T> >;

static void FindStages(DIABase* action, std::vector<Stage>& stages_result) {
    static const bool debug = false;

    LOG << "FINDING stages:";
    mm_set<const DIABase*> stages_found(
        mem::Allocator<const DIABase*>(action->mem_manager()));

    // Do a BFS on parents and find all stages needed to PushData to calculate
    // this node.
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

            // if parents where not already seen, push onto stages
            if (stages_found.count(p)) continue;
            stages_found.insert(p);

            stages_result.push_back(Stage(p));
            LOG << "FOUND: " << p->label() << '.' << p->id();
            if (p->CanExecute()) {
                // If parent was not executed push it to the BFS queue
                if (p->state() != DIAState::EXECUTED)
                    dia_stack.push_back(p);
            }
            else {
                // If parent cannot be executed (hold data) continue upward.
                dia_stack.push_back(p);
            }
        }
    }
    // Reverse the execution order
    std::reverse(stages_result.begin(), stages_result.end());
}

void DIABase::RunScope() {
    static const bool debug = false;

    LOG << "DIABase::RunScope() this=" << *this;

    std::vector<Stage> result;
    FindStages(this, result);

    for (Stage& s : result)
    {
        if (!s.node()->CanExecute())
            continue;

        if (s.node()->state() == DIAState::NEW) {
            s.Execute();
        }
        else if (s.node()->state() == DIAState::EXECUTED) {
            s.PushData();
        }
        s.node()->RemoveAllChildren();
    }
}

/******************************************************************************/
// DIABase

//! make ostream-able.
std::ostream& operator << (std::ostream& os, const DIABase& d) {
    return os << d.label() << '.' << d.id();
}

//! Returns the state of this DIANode as a string. Used by ToString.
const char* DIABase::state_string() {
    switch (state_) {
    case DIAState::NEW:
        return "NEW";
    case DIAState::EXECUTED:
        return "EXECUTED";
    case DIAState::DISPOSED:
        return "DISPOSED";
    default:
        return "UNDEFINED";
    }
}

} // namespace api
} // namespace thrill

/******************************************************************************/
