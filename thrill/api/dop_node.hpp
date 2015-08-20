/*******************************************************************************
 * thrill/api/dop_node.hpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DOP_NODE_HEADER
#define THRILL_API_DOP_NODE_HEADER

#include <thrill/api/dia_node.hpp>

#include <string>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DOpNode is a typed node representing and distributed operations in c7a.  It
 * is the super class for all distributed operation nodes.
 *
 * \tparam ValueType Type of the items in the DIA.
 */
template <typename ValueType>
class DOpNode : public DIANode<ValueType>
{
public:
    /*!
     * Constructor for a DOpNode, which sets references to the
     * parent nodes.  Calls the constructor of DIANode with the same parameters.
     *
     * \param ctx Reference to Context, which holds references to data and
     * network.
     *
     * \param parents Reference to parents of this node, which have to be
     * computed previously
     */
    DOpNode(Context& ctx,
            const std::vector<std::shared_ptr<DIABase> >& parents,
            const std::string& stats_tag,
            StatsNode* stats_node)
        : DIANode<ValueType>(ctx, parents, stats_tag, stats_node) { }
};

//! \}

} // namespace api
} // namespace c7a

#endif // !THRILL_API_DOP_NODE_HEADER

/******************************************************************************/
