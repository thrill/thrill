/*******************************************************************************
 * c7a/api/reduce_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_REDUCE_NODE_HEADER
#define C7A_API_REDUCE_NODE_HEADER

#include <unordered_map>
#include <functional>
#include "dop_node.hpp"
#include "context.hpp"
#include "function_stack.hpp"
#include "../common/logger.hpp"

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Reduce operation. Reduce groups the elements in a DIA by their
 * key and reduces every key bucket to a single element each. The ReduceNode stores the
 * key_extractor and the reduce_function UDFs. The chainable LOps ahead of the Reduce operation
 * are stored in the Stack. The ReduceNode has the type T, which is the result type of the
 * reduce_function.
 *
 * \tparam T Output type of the Reduce operation
 * \tparam Stack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function
 */
template <typename T, typename Stack, typename KeyExtractor, typename ReduceFunction>
class ReduceNode : public DOpNode<T>
{
public:
    /*!
     * Constructor for a ReduceNode. Sets the DataManager, parents, stack, key_extractor and reduce_function.
     *
     * \param data_manager Reference to the DataManager, which gives iterators for data
     * \param parents Vector of parents. Has size 1, as a reduce node only has a single parent
     * \param stack Function stack with all lambdas between the parent and this node
     * \param key_extractor Key extractor function
     * \param reduce_function Reduce function
     */
    ReduceNode(Context& ctx,
               const DIABaseVector& parents,
               Stack& stack,
               KeyExtractor key_extractor,
               ReduceFunction reduce_function)
        : DOpNode<T>(ctx, parents),
          local_stack_(stack),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function)
    { }

    /*!
     * Returns "[ReduceNode]" as a string.
     * \return "[ReduceNode]"
     */
    std::string ToString() override
    {
        // Create string
        std::string str
            = std::string("[ReduceNode] Id: ") + std::to_string(DIABase::data_id_);
        return str;
    }

    /*!
     * Actually executes the reduce operation. Uses the member functions PreOp, MainOp and PostOp.
     */
    void execute() override
    {
        PreOp();
        MainOp();
        PostOp();
    }

    /*!
     * TODO: I have no idea...
     */
    auto ProduceStack() {
        using reduce_t
                  = typename FunctionTraits<ReduceFunction>::result_type;

        auto id_fn = [ = ](reduce_t t, std::function<void(reduce_t)> emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

private:
    //! Local stack
    Stack local_stack_;
    //!Key extractor function
    KeyExtractor key_extractor_;
    //!Reduce function
    ReduceFunction reduce_function_;

    //! Locally hash elements of the current DIA onto buckets and reduce each bucket to a single value,
    //! afterwards send data to another worker given by the shuffle algorithm.
    void PreOp()
    {
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        using reduce_arg_t = typename FunctionTraits<ReduceFunction>::template arg<0>;
        std::cout << "PreOp" << std::endl;

        data::DIAId pid = this->get_parents()[0]->get_data_id();
        // //get data from data manager
        data::BlockIterator<T> it = (this->context_).get_data_manager().template GetLocalBlocks<T>(pid);

        // //run local reduce
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::unordered_map<key_t, T> reduce_data;

        std::vector<reduce_arg_t> elements;
        auto save_fn = [&elements](reduce_arg_t input) {
                           elements.push_back(input);
                       };
        auto lop_chain = local_stack_.push(save_fn).emit();

        // loop over input
        while (it.HasNext()) {
            lop_chain(it.Next());
        }

        for (auto item : elements) {
            key_t key = key_extractor_(item);
            auto elem = reduce_data.find(key);
            SpacingLogger(true) << item;

            // is there already an element with same key?
            if (elem != reduce_data.end()) {
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
        std::vector<data::BlockEmitter<T> > emit_array;
        data::BlockEmitter<T> emit = (this->context_).get_data_manager().template GetLocalEmitter<T>(this->data_id_);
        for (auto it = reduce_data.begin(); it != reduce_data.end(); ++it) {
            std::hash<T> t_hash;
            auto hashed = t_hash(it->second) % number_worker;
            // TODO When emitting and the real network emitter is there, hashed is needed to emit
            emit(it->second);
        }
    }

    //!Recieve elements from other workers.
    auto MainOp() { }

    //! Hash recieved elements onto buckets and reduce each bucket to a single value.
    void PostOp()
    {
        std::cout << "PostOp" << std::endl;
        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::unordered_map<key_t, T> reduce_data;

        data::BlockIterator<T> it = (this->context_).get_data_manager().template GetLocalBlocks<T>(this->data_id_);

        using key_t = typename FunctionTraits<KeyExtractor>::result_type;
        std::unordered_map<key_t, T> global_data;

        while (it.HasNext()) {
            auto item = it.Next();
            key_t key = key_extractor_(item);
            auto elem = reduce_data.find(key);
            SpacingLogger(true) << item;

            // is there already an element with same key?
            if (elem != reduce_data.end()) {
                auto new_elem = reduce_function_(item, elem->second);
                reduce_data.at(key) = new_elem;
            }
            else {
                reduce_data.insert(std::make_pair(key, item));
            }
        }

        data::BlockEmitter<T> emit = (this->context_).get_data_manager().template GetLocalEmitter<T>(DIABase::data_id_);
        for (auto it = reduce_data.begin(); it != reduce_data.end(); ++it) {
            emit(it->second);
        }
    }
};

//! \}

} // namespace c7a

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
