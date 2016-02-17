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
public:
    static const bool debug = false;

    using system_clock = std::chrono::system_clock;

    explicit Stage(const DIABasePtr& node) : node_(node) { }

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
        common::StatsTimer<true> timer;
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
                  << "stage" << *node_
                  << "failed, it was already consumed. Add .Keep()";
            abort();
        }

        common::StatsTimer<true> timer;
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

    bool operator < (const Stage& s) const { return node_ < s.node_; }

    //! shared pointer to node
    DIABasePtr node_;

    //! temporary marker for toposort to detect cycles
    mutable bool cycle_mark_ = false;

    //! toposort seen marker
    mutable bool topo_seen_ = false;
};

template <typename T>
using mm_set = std::set<T, std::less<T>, mem::Allocator<T> >;

//! Do a BFS on parents to find all DIANodes (Stages) needed to Execute or
//! PushData to calculate this action node.
static void FindStages(const DIABasePtr& action, mm_set<Stage>& stages) {
    static const bool debug = Stage::debug;

    LOG << "Finding Stages:";

    mem::mm_deque<DIABasePtr> bfs_stack(
        mem::Allocator<DIABasePtr>(action->mem_manager()));

    bfs_stack.push_back(action);
    stages.insert(Stage(action));

    while (!bfs_stack.empty()) {
        DIABasePtr curr = bfs_stack.front();
        bfs_stack.pop_front();

        for (const DIABasePtr& p : curr->parents()) {
            // if parents where not already seen, push onto stages
            if (stages.count(Stage(p))) continue;

            LOG << "  Stage: " << *p;
            stages.insert(Stage(p));

            if (p->CanExecute()) {
                // If parent was not executed push it to the BFS queue
                if (p->state() != DIAState::EXECUTED)
                    bfs_stack.push_back(p);
            }
            else {
                // If parent cannot be executed (hold data) continue upward.
                bfs_stack.push_back(p);
            }
        }
    }
}

static void TopoSortVisit(
    const Stage& s, mm_set<Stage>& stages, mem::mm_vector<Stage>& result) {
    // check markers
    die_unless(!s.cycle_mark_ && "Cycle in toposort of Stages? Impossible.");
    if (s.topo_seen_) return;

    s.cycle_mark_ = true;
    // iterate over all children of s which are in the to-be-calculate stages
    for (DIABase* child : s.node_->children()) {
        auto it = stages.find(Stage(child->shared_from_this()));

        // child not in stage set
        if (it == stages.end()) continue;

        // depth-first search
        TopoSortVisit(*it, stages, result);
    }

    s.topo_seen_ = true;
    s.cycle_mark_ = false;
    result.push_back(s);
}

static void TopoSortStages(mm_set<Stage>& stages, mem::mm_vector<Stage>& result) {
    // iterate over all stages and visit nodes in DFS search
    for (const Stage& s : stages) {
        if (s.topo_seen_) continue;
        TopoSortVisit(s, stages, result);
    }
}

void DIABase::RunScope() {
    static const bool debug = Stage::debug;

    LOG << "DIABase::RunScope() this=" << *this;

    mm_set<Stage> stages {
        mem::Allocator<Stage>(mem_manager())
    };
    FindStages(shared_from_this(), stages);

    mem::mm_vector<Stage> toporder {
        mem::Allocator<Stage>(mem_manager())
    };
    TopoSortStages(stages, toporder);

    LOG << "Topological order";
    for (auto top = toporder.rbegin(); top != toporder.rend(); ++top) {
        LOG << "  " << *top->node_;
    }

    while (toporder.size())
    {
        Stage& s = toporder.back();

        if (!s.node_->CanExecute()) {
            toporder.pop_back();
            continue;
        }

        if (debug)
            mem::malloc_tracker_print_status();

        if (s.node_->state() == DIAState::NEW) {
            s.Execute();
        }
        else if (s.node_->state() == DIAState::EXECUTED) {
            s.PushData();
        }
        s.node_->RemoveAllChildren();

        // remove from result stack, this may destroy the last shared_ptr
        // reference to a node.
        toporder.pop_back();
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
