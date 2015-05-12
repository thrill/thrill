/*******************************************************************************
 * c7a/api/reduce_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
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
#include "../core/hash_table.hpp"

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
     * \param ctx Reference to Context, which holds references to data and network.
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
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
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
        using reduce_arg_t = typename FunctionTraits<ReduceFunction>::template arg<0>;
        std::cout << "PreOp" << std::endl;

        data::DIAId pid = this->get_parents()[0]->get_data_id();
        // //get data from data manager
        data::BlockIterator<T> it = (this->context_).get_data_manager().template GetLocalBlocks<T>(pid);
        // //run local reduce

        //std::unordered_map<key_t, T> reduce_data;

        //TODO get number of worker by net-group or something similar
        //TODO make a static getter for this
        int number_worker = 1;

        data::BlockEmitter<T> emit = (this->context_).get_data_manager().template GetLocalEmitter<T>(this->data_id_);

        c7a::core::HashTable<KeyExtractor, ReduceFunction> reduce_data(number_worker, key_extractor_, reduce_function_, emit);

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
            reduce_data.Insert(item);
        }

        reduce_data.Flush();
    }

    //!Recieve elements from other workers.
    auto MainOp() { }

    //! Hash recieved elements onto buckets and reduce each bucket to a single value.
    void PostOp()
    {
        std::cout << "TODO: PostOp, when we have communication" << std::endl;
    }
};

//! \}

} // namespace c7a

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
