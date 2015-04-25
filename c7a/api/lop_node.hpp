/*******************************************************************************
 * c7a/api/lop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_LOP_NODE_HEADER
#define C7A_API_LOP_NODE_HEADER

#include "dia_node.hpp"

template <typename T, typename FuncType>
class LOpNode : public DIANode<T> {
public: 
    LOpNode(std::vector<DIABase> parents, FuncType func) : DIANode<T>(parents), DIANode<T>::my_func_(func) {};
    virtual ~LOpNode() {}

private: 
    FuncType my_func_;
};

#endif // !C7A_API_LOP_NODE_HEADER

/******************************************************************************/
