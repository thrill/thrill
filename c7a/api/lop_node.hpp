/*******************************************************************************
 * c7a/api/lop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_LOP_NODE_HEADER
#define C7A_API_LOP_NODE_HEADER

#include "dia_node.hpp"

namespace c7a {

template <typename T, typename LOpFunction>
class LOpNode : public DIANode<T> {
public:

    LOpNode(data::DataManager &data_manager, const std::vector<DIABase*>& parents, LOpFunction lop_function) : DIANode<T>(data_manager, parents), lop_function_(lop_function) {};
    virtual ~LOpNode() {}

    void execute() {};

    std::string ToString() override {
        using key_t = typename FunctionTraits<LOpFunction>::result_type;
        std::string str = std::string("[LOpNode/Type=[") + typeid(T).name() + "]";
        return str;
    }

private:
    LOpFunction lop_function_;
};

} // namespace c7a

#endif // !C7A_API_LOP_NODE_HEADER

/******************************************************************************/
