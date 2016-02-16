/*******************************************************************************
 * thrill/api/dia_base.hpp
 *
 * Untyped super class of DIANode. Used to build the execution graph.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DIA_BASE_HEADER
#define THRILL_API_DIA_BASE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/stats.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * Possible states a DIABase can be in.
 */
enum class DIAState {
    //! The DIABase has not been computed yet.
    NEW,
    //! The DIABase has been calculated but not explicitly cached.  Data might
    //! be available or has to be recalculated when needed
    EXECUTED,
    //! The DIABase is manually disposed by the user, needs to be recomputed
    //! when accessed.
    DISPOSED
};

/*!
 * The DIABase is the untyped super class of DIANode. DIABases are used to build
 * the execution graph, which is used to execute the computation.
 *
 * Each DIABase knows it's parents. Parents are node which have to computed
 * previously. Not all DIABases have children (ActionNodes do not), hence,
 * children are first introduced in DIANode.
 */
class DIABase
{
public:
    /*!
     * The constructor for a DIABase. Sets the parents for this node, but does
     * not register it has a child, since this must be done with a callback.
     */
    DIABase(Context& ctx,
            const std::vector<std::shared_ptr<DIABase> >& parents,
            StatsNode* stats_node)
        : context_(ctx), parents_(parents),
          execution_timer_(
              ctx.stats().CreateTimer(
                  "DIABase::execution", stats_node->label())),
          lifetime_(
              ctx.stats().CreateTimer(
                  "DIABase::lifetime", stats_node->label(), true)),
          stats_node_(stats_node) { }

    //! non-copyable: delete copy-constructor
    DIABase(const DIABase&) = delete;
    //! non-copyable: delete assignment operator
    DIABase& operator = (const DIABase&) = delete;
    //! move-constructor: default
    DIABase(DIABase&&) = default;
    //! move-assignment operator: default
    DIABase& operator = (DIABase&&) = default;

    //! Virtual destructor for a DIABase.
    virtual ~DIABase() {
        // Remove child pointer from parent If a parent loses all its childs its
        // reference count should be zero and he should be removed
        LOG0 << "~DIABase(): " << *this;

        // de-register at parents (if still hooked there)
        for (const std::shared_ptr<DIABase>& p : parents_)
            p->RemoveChild(this);

        STOP_TIMER(lifetime_)
    }

    //! Virtual method to determine whether a node can be Executed() = run such
    //! that it self-contains its data. This is currently true for all Nodes
    //! except Collapse().
    virtual bool CanExecute() { return true; }

    //! Virtual execution method. Triggers actual computation in sub-classes.
    virtual void Execute() = 0;

    //! Virtual method for pushing data. Triggers actual pushing in sub-classes.
    virtual void PushData(bool consume) = 0;

    //! Virtual clear method. Triggers actual disposing in sub-classes.
    virtual void Dispose() = 0;

    //! Virtual method for preparing start of data.
    virtual void StartPreOp(size_t /* id */) { }

    //! Virtual method for preparing end of data.
    virtual void StopPreOp(size_t /* id */) { }

    //! Performing push operation. Notifies children and calls actual push
    //! method. Then cleans up the DIA graph by freeing parent references of
    //! children.
    virtual void RunPushData(bool consume) = 0;

    //! Virtual method for removing a child.
    virtual void RemoveChild(DIABase* node) = 0;

    //! Virtual method for removing all childs. Triggers actual removing in
    //! sub-classes.
    virtual void RemoveAllChildren() = 0;

    //! return unique id() of DIANode subclass as stored by StatsNode
    const size_t & id() const {
        assert(stats_node_);
        return stats_node_->id();
    }

    //! return label() of DIANode subclass as stored by StatsNode
    const char * label() const {
        assert(stats_node_);
        return stats_node_->label();
    }

    //! make ostream-able.
    friend std::ostream& operator << (std::ostream& os, const DIABase& d) {
        return os << d.label() << '.' << d.id();
    }

    //! return type() of DIANode subclass as stored by StatsNode
    const DIANodeType & type() const {
        assert(stats_node_);
        return stats_node_->type();
    }

    //! Virtual SetConsume flag which is called by the user via .Keep() or
    //! .Consume() to set consumption.
    virtual void SetConsume(bool consume) {
        consume_on_push_data_ = consume;
    }

    //! Returns the parents of this DIABase.
    const std::vector<std::shared_ptr<DIABase> > & parents() {
        return parents_;
    }

    //! Returns the children of this DIABase.
    virtual std::vector<DIABase*> children() const = 0;

    //! Execute Scope.
    void RunScope();

    //! Returns the api::Context of this DIABase.
    Context & context() {
        return context_;
    }

    //! Return the Context's memory manager
    mem::Manager & mem_manager() {
        return context_.mem_manager();
    }

    DIAState state() const {
        return state_;
    }

    DIAState set_state(DIAState state) {
        return state_ = state;
    }

    bool consume_on_push_data() const {
        return consume_on_push_data_;
    }

    void AddStats(const std::string& msg) const {
        stats_node_->AddStatsMsg(msg, LogType::INFO);
    }

    // Why are these stupid functions here?
    // Because we do not want to include the stats.hpp into every
    // single node class
    inline void StartExecutionTimer() {
        START_TIMER(execution_timer_);
    }

    inline void StopExecutionTimer() {
        STOP_TIMER(execution_timer_);
        if (execution_timer_)
            stats_node_->AddStatsMsg(
                std::to_string(execution_timer_->Milliseconds()) + "ms",
                LogType::EXECUTION);
    }

    inline void WriteStreamStats(const data::StreamPtr& c) {
        if (common::g_enable_stats) {
            assert(!c->rx_lifetime_.running());
            assert(!c->tx_lifetime_.running());
            assert(!c->rx_timespan_.running());
            assert(!c->tx_timespan_.running());
            stats_node_->AddStatsMsg(
                "stream " + std::to_string(c->id()) + "; " +
                "incoming_bytes " + std::to_string(c->incoming_bytes_.value()) + "; " +
                "incoming_blocks " + std::to_string(c->incoming_blocks_.value()) + "; " +
                "outgoing_bytes " + std::to_string(c->outgoing_bytes_.value()) + "; " +
                "outgoing_blocks " + std::to_string(c->outgoing_blocks_.value()) + "; " +
                "rx_lifetime (us) " + std::to_string(c->rx_lifetime_.Microseconds()) + "; " +
                "tx_lifetime (us) " + std::to_string(c->tx_lifetime_.Microseconds()) + "; " +
                "rx_timespan (us) " + std::to_string(c->rx_timespan_.Microseconds()) + "; " +
                "tx_timespan (us) " + std::to_string(c->tx_timespan_.Microseconds()),
                LogType::NETWORK);
        }
    }

protected:
    //! State of the DIANode. State is NEW on creation.
    DIAState state_ = DIAState::NEW;

    //! Returns the state of this DIANode as a string. Used by ToString.
    std::string state_string() {
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

    //! Context, which can give iterators to data.
    Context& context_;

    //! Parents of this DIABase.
    std::vector<std::shared_ptr<DIABase> > parents_;

    //! Timer that tracks execution of this node
    common::TimerPtr execution_timer_;

    //! Timer that tracks the lifetime of this object
    common::TimerPtr lifetime_;

    //! Timer that tracks the lifetime of this object
    api::StatsNode* stats_node_;

    //! General consumption flag: set to true by default.
    bool consume_on_push_data_ = true;
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DIA_BASE_HEADER

/******************************************************************************/
