/*******************************************************************************
 * c7a/api/dia_base.hpp
 *
 * Untyped super class of DIANode. Used to build the execution graph.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_DIA_BASE_HEADER
#define C7A_API_DIA_BASE_HEADER

#include <vector>
#include <string>
#include "context.hpp"
#include "types.hpp"
#include "../data/data_manager.hpp"

namespace c7a {
/*!
 * Possible states a DIABase can be in.
 */
enum kState {
    NEW,        /*!< The DIABase has not been computed yet. */
    CALCULATED, /*!< The DIABase has been calculated but not explicitly cached.
                  Data might be available or has to be recalculated when needed */
    CACHED,     /*!< The DIABase is cached and it's data can be accessed */
    DISPOSED    /*!< The DIABase is disposed by the user, needs to be recomputed when accessed */
};

//! \addtogroup api Interface
//! \{

/*!
 * The DIABase is the untyped super class of DIANode. DIABases are used to build
 * the execution graph, which is used to execute the computation.

 * Each DIABase knows it's parents and children. Parents are node which have to
 * computed previously, children are nodes which have this node as a parent.

 * Additionally, a DIABase has a reference to the DataManager, which can give
 * iterators to actual data.
 */
class DIABase
{
public:
    /*!
     * The constructor for a DIABase. Sets the DataManager and the
     * associated DIAId.
     *
     * Sets the parents for this node and adds this node as a child for
     * each parent.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     *
     * \param parents Reference to parents of this node, which have to be computed previously
     */
    DIABase(Context& ctx, const DIABaseVector& parents)
        : context_(ctx), parents_(parents)
    {
        for (auto parent : parents_) {
            parent->add_child(this);
        }
        data_id_ = context_.get_data_manager().AllocateDIA();
    }

    //!Virtual destructor for a DIABase.
    virtual ~DIABase() { }

    //!Virtual execution method. Triggers actual computation in sub-classes.
    virtual void execute() = 0;

    //!Virtual ToString method. Returns the type of node in sub-classes.
    virtual std::string ToString() = 0;

    //! Returns the childs of this DIABase.
    //! \return A vector of all childs
    const DIABaseVector & get_childs()
    {
        return childs_;
    }

    //! Returns the parents of this DIABase.
    //! \return A vector of all parents
    const DIABaseVector & get_parents()
    {
        return parents_;
    }

    //! Returns the DataManager of this DIABase.
    //! \return The DataManager of this DIABase.
    Context & get_context()
    {
        return context_;
    }

    //! Adds a child to the vector of childs. This method is called in the constructor.
    //! \param child The child to add.
    void add_child(DIABasePtr child)
    {
        childs_.push_back(child);
    }

    //! Returns the unique ID of this DIABase.
    //! \return The unique ID of this DIABase.
    data::DIAId get_data_id()
    {
        return data_id_;
    }

    kState state()
    {
        return state_;
    }

    kState set_state(kState state)
    {
        return state_ = state;
    }

protected:
    //! State of the DIANode. State is NEW on creation.
    kState state_ = NEW;

    //!Returns the state of this DIANode as a string. Used by ToString.
    std::string state_string_()
    {
        switch (state_) {
        case NEW:
            return "NEW";
        case CALCULATED:
            return "CALCULATED";
        case CACHED:
            return "CACHED";
        case DISPOSED:
            return "DISPOSED";
        default:
            return "UNDEFINED";
        }
    }

    //! DataManager, which can give iterators to data.
    Context& context_;
    //! Unique ID of this DIABase. Used by the DataManager.
    data::DIAId data_id_;
    //! Childs and parents of this DIABase.
    DIABaseVector childs_, parents_;
};

//! \}
} // namespace c7a

#endif // !C7A_API_DIA_BASE_HEADER

/******************************************************************************/
