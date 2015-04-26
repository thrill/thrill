/*******************************************************************************
 * c7a/api/action_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_ACTION_NODE_HEADER
#define C7A_API_ACTION_NODE_HEADER

namespace c7a {

template <typename T>
class ActionNode : public DIANode<T> {
public:
    ActionNode(data::DataManager &data_manager, 
            const std::vector<DIABase*>& parents)
        : DIANode<T>(data_manager, parents) {}
    virtual ~ActionNode() {}
};

} // namespace c7a

#endif // !C7A_API_ACTION_NODE_HEADER

/******************************************************************************/
