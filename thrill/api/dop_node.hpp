/*******************************************************************************
 * thrill/api/dop_node.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DOP_NODE_HEADER
#define THRILL_API_DOP_NODE_HEADER

#include <thrill/api/dia_node.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DOpNode is a typed node representing and distributed operations in Thrill.
 * It is the super class for all distributed operation nodes.
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
     * \param label static label of DOp implementation
     *
     * \param parent_ids parent DIA ids
     *
     * \param parents Reference to parents of this node, which have to be
     * computed previously
     */
    DOpNode(Context& ctx, const char* label,
            const std::initializer_list<size_t>& parent_ids,
            const std::initializer_list<DIABasePtr>& parents)
        : DIANode<ValueType>(ctx, label, parent_ids, parents) { }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DOP_NODE_HEADER

/******************************************************************************/
