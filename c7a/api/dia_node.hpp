/*******************************************************************************
 * c7a/api/dia_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/
#ifndef C7A_API_DIA_NODE_HEADER
#define C7A_API_DIA_NODE_HEADER

#include <string>
#include <vector>

#include "dia_base.hpp"


enum kState {
    NEW,
    CALCULATED,
    CACHED,
    DISPOSED
};

template <typename T>
class DIANode : public DIABase {

public:
    DIANode() {}

    DIANode(std::vector<DIABase> parents) : DIABase(parents) {}

    virtual ~DIANode() {}

    std::string toString() {
        std::string str;
        str = std::string("[DIANode/State:/Type:") + typeid(T).name() + "]";
        return str;
    }


protected:
    kState state_;
    //T my_func_;
};

#endif // !C7A_API_DIA_NODE_HEADER
/******************************************************************************/
