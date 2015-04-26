/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_REDUCE_NODE_HEADER
#define C7A_API_REDUCE_NODE_HEADER

#include <unordered_map>
#include "dop_node.hpp"
#include "../common/logger.hpp"

namespace c7a {

template <typename T, typename L, typename KeyExtractor, typename ReduceFunction>
class ReduceNode : public DOpNode<T> {
//! Hash elements of the current DIA onto buckets and reduce each bucket to a single value.
public: 
    ReduceNode(data::DataManager &data_manager, 
               const std::vector<DIABase*>& parents,
               L lambda,
               KeyExtractor key_extractor,
               ReduceFunction reduce_function)
        : DOpNode<T>(data_manager, parents),
        local_lambda_(lambda),
        key_extractor_(key_extractor),
        reduce_function_(reduce_function) {};

    std::string ToString() override {
        // Create string
        std::string str 
            = std::string("[ReduceNode]");
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

        // //get data from data manager
        // // data::BlockIterator<T> it = (this->data_manager_).template GetLocalBlocks<T>(this->data_id_);
        // std::vector<std::string> local_block = {"I", "I", "hate", "code", "non-compiling", "code"};
        // std::vector<std::string>::iterator begin = local_block.begin();
        // std::vector<std::string>::iterator end = local_block.end();
        
        // //run local reduce
        // using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        // std::unordered_map<key_t, T> hash;

        // SpacingLogger(true) << "######################";
        // SpacingLogger(true) << "INPUT";
        // SpacingLogger(true) << "######################";

        // // loop over input        
        // for (auto it = begin; it != end; ++it){
        //     key_t key = key_extractor_(*it);
        //     auto elem = hash.find(key);            
        //     SpacingLogger(true) << *it;

        //     // is there already an element with same key?
        //     if(elem != hash.end()) {
        //         auto new_elem = reduce_function_(*it, elem->second);
        //         hash.at(key) = new_elem;
        //     }
        //     else {
        //         hash.insert(std::make_pair(key, *it));
        //     }
        // }


        // // just for testing
        // SpacingLogger(true) << "######################";
        // SpacingLogger(true) << "OUTPUT";
        // SpacingLogger(true) << "######################";
        // for (auto it = hash.begin(); it != hash.end(); ++it){
        //     SpacingLogger(true) << it->second;
        // }
        // SpacingLogger(true);



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
