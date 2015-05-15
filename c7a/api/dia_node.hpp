/*******************************************************************************
 * c7a/api/dia_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_DIA_NODE_HEADER
#define C7A_API_DIA_NODE_HEADER

#include <string>
#include <vector>

#include "dia_base.hpp"
#include "context.hpp"
#include "../data/data_manager.hpp"

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode is a typed node representing and operation in c7a. It is the super class for
 * all operation nodes and stores the state of the operation. The type of a DIANode is the type,
 * in which the DIA is after the last global barrier in the operation (between MainOp and PostOp).
 *
 * \tparam T Type of the DIA between MainOp and PostOp
 */
template <typename T>
class DIANode : public DIABase
{
public:
    /*!
     * Default constructor for a DIANode.
     */
    DIANode() { }

    /*!
     * Constructor for a DIANode, which sets references to the DataManager and parent nodes. Calls the constructor of DIABase
     * with the same parameters.
     *
     * \param data_manager Reference to DataManager, which gives iterators to data
     *
     * \param parents Reference to parents of this node, which have to be computed previously
     */
    DIANode(Context& ctx, const DIABaseVector& parents)
        : DIABase(ctx, parents)
    { }

    //! Virtual destructor for a DIANode.
    virtual ~DIANode() { }

    //! ToString-method. Returns DIANode and it's state as a string.
    std::string ToString() override
    {
        std::string str;
        str = std::string("[DIANode/State:") + state_string_() + "]";
        return str;
    }
};

//! \}

} // namespace c7a

#endif // !C7A_API_DIA_NODE_HEADER

/******************************************************************************/
