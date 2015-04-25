/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_REDUCE_NODE_HEADER
#define C7A_API_REDUCE_NODE_HEADER

#include "dop_node.hpp"

namespace c7a {

template <typename T, typename L, typename KeyExtractor, typename ReduceFunction>
class ReduceNode : public DOpNode<T> {
//! Hash elements of the current DIA onto buckets and reduce each bucket to a single value.
public: 
    ReduceNode(const std::vector<DIABase*>& parents,
               L lambda,
               KeyExtractor key_extractor,
               ReduceFunction reduce_function)
        : DOpNode<T>(parents),
        local_lambda_(lambda),
        key_extractor_(key_extractor),
        reduce_function_(reduce_function) { };

    std::string ToString() override {
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::string str = std::string("[ReduceNode/Type=[") + typeid(T).name() + "]/KeyType=[" + typeid(key_t).name() + "]";
        return str;
    }

    void execute() override {
        PreOp(T());
        MainOp();
    }

    auto get_pre_op() {
        return [=](T t) {
            return PreOp(t);
        };
    }

    auto get_post_op() {
        return [=](T t) {
            return PostOp(t);
        };
    }

private: 
    L local_lambda_;
    KeyExtractor key_extractor_;
    ReduceFunction reduce_function_;
    
    auto PreOp(T t) {
        local_lambda_(t);
        std::cout << "PreOp" << std::endl;
        return t;
    }

    auto MainOp() {
        std::cout << "MainOp" << std::endl;
    }

    auto PostOp(T t) {
        std::cout << "PostOp" << std::endl;
        return t;
    }
};

} // namespace c7a

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
