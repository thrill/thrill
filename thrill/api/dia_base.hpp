/*******************************************************************************
 * thrill/api/dia_base.hpp
 *
 * Untyped super class of DIANode. Used to build the execution graph.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
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
    //! The DIABase is disposed by the user, needs to be recomputed when
    //! accessed.
    DISPOSED
};

/*!
 * The DIABase is the untyped super class of DIANode. DIABases are used to build
 * the execution graph, which is used to execute the computation.

 * Each DIABase knows it's parents and children. Parents are node which have to
 * computed previously, children are nodes which have this node as a parent.

 * Additionally, a DIABase has a reference to the data::Manager, which can give
 * iterators to actual data.
 */
class DIABase
{
public:
    /*!
     * The constructor for a DIABase. Sets the data::Manager and the
     * associated \ref data::File 'result_file'.
     *
     * Sets the parents for this node and adds this node as a child for
     * each parent.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     *
     * \param parents Reference to parents of this node, which have to be computed previously
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
          stats_node_(stats_node) {

        for (auto parent : parents_) {
            parent->add_child(this);
        }
    }

    //! non-copyable: delete copy-constructor
    DIABase(const DIABase&) = delete;
    //! non-copyable: delete assignment operator
    DIABase& operator = (const DIABase&) = delete;

    //! Virtual destructor for a DIABase.
    virtual ~DIABase() {
        // Remove child pointer from parent
        // If a parent loses all its childs
        // its reference count should be zero and he
        // should be removed
        // parent.remove_child(this);
        STOP_TIMER(lifetime_)
    }

    //! Virtual execution method. Triggers actual computation in sub-classes.
    virtual void Execute() = 0;

    //! Virtual method for pushing data. Triggers actual pushing in sub-classes.
    virtual void PushData() = 0;

    //! Virtual clear method. Triggers actual disposing in sub-classes.
    virtual void Dispose() = 0;

    //! Virtual method for removing all childs. Triggers actual removing in sub-classes.
    virtual void UnregisterChilds() = 0;

    //! return label of DIANode subclass as stored by StatsNode
    const char * label() const {
        assert(stats_node_);
        return stats_node_->label();
    }

    const DIANodeType & type() const {
        assert(stats_node_);
        return stats_node_->type();
    }

    //! Returns the children of this DIABase.
    const std::vector<DIABase*> & children() {
        return children_;
    }

    //! Returns the parents of this DIABase.
    const std::vector<std::shared_ptr<DIABase> > & parents() {
        return parents_;
    }

    //! Returns the api::Context of this DIABase.
    Context & context() {
        return context_;
    }

    //! Return the Context's memory manager
    mem::Manager& mem_manager() {
        return context_.mem_manager();
    }

    //! Adds a child to the vector of children. This method is called in the constructor.
    void add_child(DIABase* child) {
        children_.push_back(child);
    }

    //! Returns the unique ID of this DIABase.
    size_t id() const {
        assert(stats_node_);
        return stats_node_->id();
    }

    DIAState state() const {
        return state_;
    }

    DIAState set_state(DIAState state) {
        return state_ = state;
    }

    // Why are these stupid functions here?
    // Because we do not want to include the stats.hpp into every
    // single node class
    inline void StartExecutionTimer() {
        START_TIMER(execution_timer_);
    }

    inline void StopExecutionTimer() {
        STOP_TIMER(execution_timer_);
        if (execution_timer_) stats_node_->AddStatsMsg(std::to_string(execution_timer_->Milliseconds()) + "ms", LogType::EXECUTION);
    }

    inline void WriteChannelStats(const data::ChannelPtr& c) {
        if (common::g_enable_stats) {
            assert(!c->rx_lifetime_.running());
            assert(!c->tx_lifetime_.running());
            assert(!c->rx_timespan_.running());
            assert(!c->tx_timespan_.running());
            stats_node_->AddStatsMsg(
                "channel " + std::to_string(c->id()) + "; " +
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
    std::string state_string_() {
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

    //! Children and parents of this DIABase.
    std::vector<DIABase*> children_;
    std::vector<std::shared_ptr<DIABase> > parents_;

    //! Timer that tracks execution of this node
    common::TimerPtr execution_timer_;

    //! Timer that tracks the lifetime of this object
    common::TimerPtr lifetime_;

    //! Timer that tracks the lifetime of this object
    api::StatsNode* stats_node_;
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DIA_BASE_HEADER

/******************************************************************************/
