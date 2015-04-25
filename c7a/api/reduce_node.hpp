/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_REDUCE_NODE_HEADER
#define C7A_API_REDUCE_NODE_HEADER

#include "dop_node.hpp"
#include "../common/logger.hpp"

namespace c7a {

template <typename T, typename KeyExtractor, typename ReduceFunction>
class ReduceNode : public DOpNode<T> {
//! Hash elements of the current DIA onto buckets and reduce each bucket to a single value.
public: 
    ReduceNode(data::DataManager &data_manager, 
               const std::vector<DIABase*>& parents,
               KeyExtractor key_extractor,
               ReduceFunction reduce_function)
        : DOpNode<T>(data_manager, parents),
        key_extractor_(key_extractor),
        reduce_function_(reduce_function) {};

    std::string ToString() override
    {
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::string str = std::string("[ReduceNode/Type=[") + typeid(T).name() + "]/KeyType=[" + typeid(key_t).name() + "]";
        return str;
    }


private: 
    KeyExtractor key_extractor_;
    ReduceFunction reduce_function_;


    void ReducePreOp() {
        SpacingLogger(true) << "I'm doing reduce pre op";
    };
    
    void ReduceMainOp() {
        SpacingLogger(true) << "I'm doing reduce main op";
    };
    
    void ReducePostOp() {
        SpacingLogger(true) << "I'm doing reduce post up";
    };
};

} // namespace c7a

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
