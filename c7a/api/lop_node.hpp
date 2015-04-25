/*******************************************************************************
 * c7a/api/lop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_LOP_NODE_HEADER
#define C7A_API_LOP_NODE_HEADER

#include "dia_node.hpp"

enum kType {
    FLATMAP,
    MAP
};

template <typename T, typename FuncType>
class LOpNode : public DIANode<T> {
public: 
    LOpNode(std::vector<DIABase> parents, kType op_type, FuncType func) : DIANode<T>::parents_(parents), op_type_(op_type), DIANode<T>::my_func_(func) {};

private: 
    kType op_type_;
    FuncType my_func_;
};

#endif // !C7A_API_LOP_NODE_HEADER

/******************************************************************************/
