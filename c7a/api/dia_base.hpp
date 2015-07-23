/*******************************************************************************
 * c7a/api/dia_base.hpp
 *
 * Untyped super class of DIANode. Used to build the execution graph.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_DIA_BASE_HEADER
#define C7A_API_DIA_BASE_HEADER

#include <c7a/api/context.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/data/manager.hpp>

#include <string>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * Possible states a DIABase can be in.
 * TODO(ch): turn this enum into an "enum class" within DIABase. These are
 * c7a-global identifiers atm.
 */
enum kState {
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
            const std::vector<std::shared_ptr<DIABase> >& parents, std::string stats_tag)
        : context_(ctx), parents_(parents),
          result_file_(ctx.data_manager().GetFile()),
          execution_timer_(ctx.stats().CreateTimer("DIABase::execution", stats_tag)),
          lifetime_(ctx.stats().CreateTimer("DIABase::lifetime", stats_tag, true)) {
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
        // parent->remove_child(this);
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

    //! Virtual ToString method. Returns the type of node in sub-classes.
    virtual std::string ToString() = 0;

    //! Returns the children of this DIABase.
    //! \return A vector of all children
    const std::vector<DIABase*> & children() {
        return children_;
    }

    //! Returns the parents of this DIABase.
    //! \return A vector of all parents
    const std::vector<std::shared_ptr<DIABase> > & parents() {
        return parents_;
    }

    //! Returns the data::Manager of this DIABase.
    //! \return The data::Manager of this DIABase.
    Context & context() {
        return context_;
    }

    //! Adds a child to the vector of children. This method is called in the constructor.
    //! \param child The child to add.
    void add_child(DIABase* child) {
        children_.push_back(child);
    }

    //! Returns the unique ID of this DIABase.
    //! \return The unique ID of this DIABase.
    data::File result_file() {
        return result_file_;
    }

    kState state() const {
        return state_;
    }

    kState set_state(kState state) {
        return state_ = state;
    }

protected:
    //! State of the DIANode. State is NEW on creation.
    kState state_ = NEW;

    //!Returns the state of this DIANode as a string. Used by ToString.
    std::string state_string_() {
        switch (state_) {
        case NEW:
            return "NEW";
        case EXECUTED:
            return "EXECUTED";
        case DISPOSED:
            return "DISPOSED";
        default:
            return "UNDEFINED";
        }
    }

    //Why are these stupid functions here?
    //Because we do not want to include the stats.hpp into every
    //single node class
    inline void StartExecutionTimer() {
        START_TIMER(execution_timer_)
    }
    inline void StopExecutionTimer() {
        STOP_TIMER(execution_timer_)
    }

    //! Context, which can give iterators to data.
    Context& context_;

    //! Children and parents of this DIABase.
    std::vector<DIABase*> children_;
    std::vector<std::shared_ptr<DIABase> > parents_;

    //! Unique ID of this DIABase. Used by the data::Manager.
    data::File result_file_;

    //! Timer that tracks execution of this node
    common::TimerPtr execution_timer_;

    //! Timer that tracks the lifetime of this object
    common::TimerPtr lifetime_;
};

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_DIA_BASE_HEADER

/******************************************************************************/
