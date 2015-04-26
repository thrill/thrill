/*******************************************************************************
 * c7a/api/dop_node.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeStackog and more.
 ******************************************************************************/

#ifndef C7A_API_REDUCE_NODE_HEADER
#define C7A_API_REDUCE_NODE_HEADER

#include <unordered_map>
#include <functional>
#include "dop_node.hpp"
#include "function_stack.hpp"
#include "../common/logger.hpp"

namespace c7a {

template <typename T, typename Stack, typename KeyExtractor, typename ReduceFunction>
class ReduceNode : public DOpNode<T> {
//! Hash elements of the current DIA onto buckets and reduce each bucket to a single value.
public: 
    ReduceNode(data::DataManager &data_manager, 
               const std::vector<DIABase*>& parents,
               Stack& stack,
               KeyExtractor key_extractor,
               ReduceFunction reduce_function)
        : DOpNode<T>(data_manager, parents),
        local_stack_(stack),
        key_extractor_(key_extractor),
        reduce_function_(reduce_function) {
            // This new DIA Allocate is needed to send data from Pre to Main
            // TODO: use network iterate later
            post_data_id_ = (this->data_manager_).AllocateDIA();

        };

    std::string ToString() override {
        // Create string
        std::string str 
            = std::string("[ReduceNode]");
        return str;
    }

    void execute() override {
        PreOp();
        MainOp();
        PostOp();
    }

    auto ProduceStack() {
        using reduce_t 
            = typename FunctionTraits<ReduceFunction>::result_type;


        auto id_fn = [=](reduce_t t, std::function<void(reduce_t)> emit_func) {
            return emit_func(t);
        };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }


private: 
    Stack local_stack_;
    KeyExtractor key_extractor_;
    ReduceFunction reduce_function_;
    data::DIAId post_data_id_;

    void PreOp() {
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        using reduce_arg_t = typename FunctionTraits<ReduceFunction>::template arg<0>;
        std::cout << "PreOp" << std::endl;

        data::DIAId pid = this->get_parents()[0]->get_data_id();
        // //get data from data manager
        data::BlockIterator<T> it = (this->data_manager_).template GetLocalBlocks<T>(pid);
        
        // //run local reduce
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::unordered_map<key_t, T> reduce_data;

        std::vector<reduce_arg_t> elements;
        auto save_fn = [&elements](reduce_arg_t input) {
                elements.push_back(input);
            };
        auto lop_chain = local_stack_.push(save_fn).emit();

        // loop over input        
        while (it.HasNext()){
            lop_chain(it.Next());
        }

        for (auto item : elements) { 
            key_t key = key_extractor_(item);
            auto elem = reduce_data.find(key);            
            // SpacingLogger(true) << item;

            // is there already an element with same key?
            if(elem != reduce_data.end()) {
                auto new_elem = reduce_function_(item, elem->second);
                reduce_data.at(key) = new_elem;
            }
            else {
                reduce_data.insert(std::make_pair(key, item));
            }
        }

        //TODO get number of worker by net-group or something similar
        int number_worker = 1;
        //TODO use network emitter in future
        std::vector<data::BlockEmitter<T>> emit_array;
        data::BlockEmitter<T> emit = (this->data_manager_).template GetLocalEmitter<T>(this->data_id_);
        for (auto it = reduce_data.begin(); it != reduce_data.end(); ++it){
            std::hash<T> t_hash;
            auto hashed = t_hash(it->second) % number_worker;
            // TODO When emitting and the real network emitter is there, hashed is needed to emit 
            emit(it->second);
        }
    }


    auto MainOp() {
    }

    void PostOp() {
        std::cout << "PostOp" << std::endl;
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::unordered_map<key_t, T> reduce_data;

        data::BlockIterator<T> it = (this->data_manager_).template GetLocalBlocks<T>(this->data_id_);

        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::unordered_map<key_t, T> global_data;

        while(it.HasNext()) {
            auto item = it.Next();
            key_t key = key_extractor_(item);
            auto elem = reduce_data.find(key);            

            // is there already an element with same key?
            if(elem != reduce_data.end()) {
                auto new_elem = reduce_function_(item, elem->second);
                reduce_data.at(key) = new_elem;
            }
            else {
                reduce_data.insert(std::make_pair(key, item));
            }
        }

        data::BlockEmitter<T> emit = (this->data_manager_).template GetLocalEmitter<T>(post_data_id_);
        for (auto it = reduce_data.begin(); it != reduce_data.end(); ++it){
            emit(it->second);
        }
    }
};

} // namespace c7a

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
