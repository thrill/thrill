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
class DIABase : public std::enable_shared_from_this<DIABase>
{
public:
    using DIABasePtr = std::shared_ptr<DIABase>;

    /*!
     * The constructor for a DIABase. Sets the parents for this node, but does
     * not register it has a child, since this must be done with a callback.
     */
    DIABase(Context& ctx,
            const std::vector<DIABasePtr>& parents,
            StatsNode* stats_node)
        : context_(ctx), parents_(parents),
          /*
          execution_timer_(
              ctx.stats().CreateTimer(
                  "DIABase::execution", stats_node->label())),
          lifetime_(
              ctx.stats().CreateTimer(
                  "DIABase::lifetime", stats_node->label(), true)),
          */
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
        for (const DIABasePtr& p : parents_)
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
    virtual void RunPushData() = 0;

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
    friend std::ostream& operator << (std::ostream& os, const DIABase& d);

    //! return type() of DIANode subclass as stored by StatsNode
    const DIANodeType & type() const {
        assert(stats_node_);
        return stats_node_->type();
    }

    //! Virtual SetConsume flag which is called by the user via .Keep() or
    //! .Consume() to set consumption.
    virtual void IncConsumeCounter(size_t counter) {
        consume_counter_ += counter;
    }

    //! Returns consume_counter_
    size_t consume_counter() const { return consume_counter_; }

    //! Returns the parents of this DIABase.
    const std::vector<DIABasePtr> & parents() {
        return parents_;
    }

    //! Remove a parent
    void RemoveParent(DIABase* p) {
        parents_.erase(
            std::remove_if(
                parents_.begin(), parents_.end(),
                [p](const DIABasePtr& parent) { return parent.get() == p; }),
            parents_.end());
    }

    //! Returns the children of this DIABase.
    virtual std::vector<DIABase*> children() const = 0;

    //! Execute Scope and parents such that this (Action)Node is Executed.
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

    void AddStats(const std::string& msg) const {
        stats_node_->AddStatsMsg(msg, LogType::INFO);
    }

protected:
    //! State of the DIANode. State is NEW on creation.
    DIAState state_ = DIAState::NEW;

    //! Returns the state of this DIANode as a string. Used by ToString.
    const char * state_string();

    //! Context, which can give iterators to data.
    Context& context_;

    //! Parents of this DIABase.
    std::vector<DIABasePtr> parents_;

    //! Timer that tracks execution of this node
    common::TimerPtr execution_timer_;

    //! Timer that tracks the lifetime of this object
    common::TimerPtr lifetime_;

    //! Timer that tracks the lifetime of this object
    api::StatsNode* stats_node_;

    //! Consumption counter: when it reaches zero, PushData() is called with
    //! consume = true
    size_t consume_counter_ = 1;

    //! Never full consume
    static const size_t never_consume_ = size_t(-1);

public:
    /**************************************************************************/
    // NodeLogger

    class NodeLogger
    {
    public:
        explicit NodeLogger(DIABase& dia_base)
            : dia_base_(dia_base) { }

        //! create new JsonLine instance which will be written to this logger.
        template <typename Type>
        common::JsonLine operator << (Type const& t) {
            common::JsonLine line = dia_base_.context().logger_.line();
            line << "type" << "node"
                 << "node_id" << dia_base_.id()
                 << "node_label" << dia_base_.label()
                 << t;
            return line;
        }

    private:
        DIABase& dia_base_;
    };

    NodeLogger logger_ { *this };
};

using DIABasePtr = std::shared_ptr<DIABase>;

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DIA_BASE_HEADER

/******************************************************************************/
