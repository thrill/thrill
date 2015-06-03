/*******************************************************************************
 * c7a/api/action_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_ACTION_NODE_HEADER
#define C7A_API_ACTION_NODE_HEADER

#include "dia_node.hpp"
#include <c7a/core/stage_builder.hpp>

namespace c7a {

template <typename T>
class ActionNode : public DIANode<T>
{
public:
    ActionNode(Context& ctx,
               const DIABaseVector& parents)
        : DIANode<T>(ctx, parents) { }

    virtual ~ActionNode() { }
};

} // namespace c7a

#endif // !C7A_API_ACTION_NODE_HEADER

/******************************************************************************/
