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
#include <thrill/common/json_logger.hpp>
#include <thrill/common/logger.hpp>
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

class Stage
{
public:
    static const bool debug = false;

    explicit Stage(const DIABasePtr& node) : node_(node) { }

    //! iterate over all target nodes into which this Stage pushes
    template <typename Lambda>
    void Targets(const Lambda& lambda) const {
        std::vector<DIABase*> children = node_->children();
        std::reverse(children.begin(), children.end());

        while (children.size()) {
            DIABase* child = children.back();
            children.pop_back();

            if (!child->CanExecute()) {
                // push children of Collapse onto stack
                std::vector<DIABase*> sub = child->children();
                children.insert(children.end(), sub.begin(), sub.end());
                lambda(child);
            }
            else {
                lambda(child);
            }
        }
    }

    //! compute a string to show all target nodes into which this Stage pushes.
    std::string TargetsString() const {
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
            else if (!child->CanExecute()) {
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

    std::vector<size_t> TargetIds() const {
        std::vector<size_t> ids;
        Targets([&ids](DIABase* child) { ids.emplace_back(child->id()); });
        return ids;
    }

    std::vector<DIABase*> TargetPtrs() const {
        std::vector<DIABase*> children;
        Targets([&children](DIABase* child) { children.emplace_back(child); });
        return children;
    }

    void Execute() {
        sLOG << "START  (EXECUTE) stage" << *node_ << "targets" << TargetsString();

        std::vector<size_t> target_ids = TargetIds();

        logger_ << "class" << "StageBuilder" << "event" << "execute-start"
                << "targets" << target_ids;

        common::StatsTimerStart timer;
        node_->set_mem_use(node_->context().mem_limit());
        node_->Execute();
        node_->set_state(DIAState::EXECUTED);
        timer.Stop();

        sLOG << "FINISH (EXECUTE) stage" << *node_ << "targets" << TargetsString()
             << "took" << timer << "ms";

        logger_ << "class" << "StageBuilder" << "event" << "execute-done"
                << "targets" << target_ids << "elapsed" << timer;
    }

    void PushData() {
        if (node_->context().consume() && node_->consume_counter() == 0) {
            sLOG1 << "StageBuilder: attempt to PushData on"
                  << "stage" << *node_
                  << "failed, it was already consumed. Add .Keep()";
            abort();
        }

        sLOG << "START  (PUSHDATA) stage" << *node_ << "targets" << TargetsString();

        std::vector<size_t> target_ids = TargetIds();

        logger_ << "class" << "StageBuilder" << "event" << "pushdata-start"
                << "targets" << target_ids;

        // collect memory requests of all targets children

        std::vector<DIABase*> targets = TargetPtrs();

        const size_t mem_limit = node_->context().mem_limit();
        std::vector<DIABase*> max_mem_nodes;
        size_t const_mem = 0;

        {
            // process node to PushData()
            DIAMemUse m = node_->PreOpMemUse();
            if (m.is_max()) {
                max_mem_nodes.emplace_back(node_.get());
            }
            else {
                const_mem += m.limit();
                node_->set_mem_use(m.limit());
            }
        }

        for (DIABase* target : TargetPtrs()) {
            DIAMemUse m = target->PreOpMemUse();
            if (m.is_max()) {
                max_mem_nodes.emplace_back(target);
            }
            else {
                const_mem += m.limit();
                target->set_mem_use(m.limit());
            }
        }

        if (const_mem > mem_limit) {
            sLOG1 << "StageBuilder: constant memory usage of DIANodes in Stage: "
                  << const_mem << ", already exceeds Context's limit: " << mem_limit;
            abort();
        }

        // distribute remaining memory to nodes requesting maximum RAM amount

        if (max_mem_nodes.size()) {
            size_t remaining_mem = mem_limit - const_mem;
            remaining_mem /= max_mem_nodes.size();

            LOG1 << "StageBuilder: distribute remaining worker memory "
                 << remaining_mem << " to "
                 << max_mem_nodes.size() << " DIANodes";

            for (DIABase* target : max_mem_nodes) {
                target->set_mem_use(remaining_mem);
            }
        }

        // execute push data

        common::StatsTimerStart timer;
        node_->RunPushData();
        node_->RemoveAllChildren();
        timer.Stop();

        sLOG << "FINISH (PUSHDATA) stage" << *node_ << "targets" << TargetsString()
             << "took" << timer << "ms";

        logger_ << "class" << "StageBuilder" << "event" << "pushdata-done"
                << "targets" << target_ids << "elapsed" << timer;
    }

    bool operator < (const Stage& s) const { return node_ < s.node_; }

    //! shared pointer to node
    DIABasePtr node_;

    //! reference to ContextLogger via node.
    common::JsonLogger& logger_ { node_->logger_ };

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

    mem::deque<DIABasePtr> bfs_stack(
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
                // If parent was not executed push it to the BFS queue and
                // continue upwards. if state is EXECUTED, then we only need to
                // PushData(), which is already indicated by stages.insert().
                if (p->state() == DIAState::NEW)
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
    const Stage& s, mm_set<Stage>& stages, mem::vector<Stage>& result) {
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

static void TopoSortStages(mm_set<Stage>& stages, mem::vector<Stage>& result) {
    // iterate over all stages and visit nodes in DFS search
    for (const Stage& s : stages) {
        if (s.topo_seen_) continue;
        TopoSortVisit(s, stages, result);
    }
}

void DIABase::RunScope() {
    static const bool debug = Stage::debug;

    LOG << "DIABase::Execute() this=" << *this;

    if (!CanExecute())
        die("DIA node " << *this << " cannot be executed.");

    mm_set<Stage> stages {
        mem::Allocator<Stage>(mem_manager())
    };
    FindStages(shared_from_this(), stages);

    mem::vector<Stage> toporder {
        mem::Allocator<Stage>(mem_manager())
    };
    TopoSortStages(stages, toporder);

    LOG << "Topological order";
    for (auto top = toporder.rbegin(); top != toporder.rend(); ++top) {
        LOG << "  " << *top->node_;
    }

    assert(toporder.front().node_.get() == this);

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
            if (s.node_.get() != this)
                s.PushData();
        }
        else if (s.node_->state() == DIAState::EXECUTED) {
            if (s.node_.get() != this)
                s.PushData();
        }

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

} // namespace api
} // namespace thrill

/******************************************************************************/
