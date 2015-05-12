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

namespace c7a {

template <typename T>
class ActionNode : public DIANode<T>
{
public:
    ActionNode(data::DataManager& data_manager,
               const DIABaseVector& parents)
        : DIANode<T>(data_manager, parents) { }
    virtual ~ActionNode() { }
};

} // namespace c7a

#endif // !C7A_API_ACTION_NODE_HEADER

/******************************************************************************/
