/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_DOP_NODE_HEADER
#define C7A_API_DOP_NODE_HEADER

#include "dia_node.hpp"
#include "context.hpp"

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * A DOpNode is a typed node representing and distributed operations in c7a. 
 * It is the super class for all distributed operation nodes.
 *
 * \tparam T Type of the corresponding DIANode 
 */
template <typename T>
class DOpNode : public DIANode<T>
{
public:
    /*!
     * Constructor for a DOpNode, which sets references to the DataManager and parent nodes.
     * Calls the constructor of DIANode with the same parameters.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     *
     * \param parents Reference to parents of this node, which have to be computed previously
     */
    DOpNode(Context& ctx,
            const DIABaseVector& parents)
        : DIANode<T>(ctx, parents) { }

    //! Virtual destructor for a DIANode.
    virtual ~DOpNode() { }

    //! ToString-method. Returns DOpNode as a string.
    std::string ToString() override
    {
        // Create string
        std::string str
            = std::string("[DOpNode]");
        return str;
    }
};

} // namespace c7a

#endif // !C7A_API_DOP_NODE_HEADER

/******************************************************************************/
