/*******************************************************************************
 * c7a/api/action_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_ACTION_NODE_HEADER
#define C7A_API_ACTION_NODE_HEADER

#include <c7a/api/dia_node.hpp>
#include <c7a/core/stage_builder.hpp>

namespace c7a {
namespace api {

class ActionNode : public DIABase
{
public:
    ActionNode(Context& ctx,
               const std::vector<std::shared_ptr<DIABase> >& parents)
        : DIABase(ctx, parents)
    { }

    virtual ~ActionNode() { }
};

} // namespace api
} // namespace c7a

#endif // !C7A_API_ACTION_NODE_HEADER

/******************************************************************************/
