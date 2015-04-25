/*******************************************************************************
 * c7a/api/action_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_ACTION_NODE_HEADER
#define C7A_API_ACTION_NODE_HEADER

template <typename T>
class ActionNode : public DIANode<T> {
    ActionNode() {}
    virtual ~ActionNode() {}
};

#endif // !C7A_API_ACTION_NODE_HEADER

/******************************************************************************/
