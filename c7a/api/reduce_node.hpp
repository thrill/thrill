/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_REDUCE_NODE_HEADER
#define C7A_API_REDUCE_NODE_HEADER



template <typename T, typename KeyExtractor, typename ReduceFunction>
class ReduceNode : public DIANode<T> {
//! Hash elements of the current DIA onto buckets and reduce each bucket to a single value.
   public: 
    ReduceNode(std::vector<DIABase> parents, KeyExtractor key_extractor, ReduceFunction reduce_function) : DIANode<T>::parents_(parents), key_extractor_(key_extractor), reduce_function_(reduce_function) {};

    void execute() {};

    std::string toString() {
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::string str = std::string("[ReduceNode/Type=[") + typeid(T).name() + "]/KeyType=[" + typeid(key_t).name() + "]";
        return str;
    }


private: 
    KeyExtractor key_extractor_;
    ReduceFunction reduce_function_;
};

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
