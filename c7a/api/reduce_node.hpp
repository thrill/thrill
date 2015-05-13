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
#include "../core/hash_table.hpp"

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Reduce operation. Reduce groups the elements in a DIA by their
 * key and reduces every key bucket to a single element each. The ReduceNode stores the
 * key_extractor and the reduce_function UDFs. The chainable LOps ahead of the Reduce operation
 * are stored in the Stack. The ReduceNode has the type Output, which is the result type of the
 * reduce_function.
 *
 * \tparam Input Input type of the Reduce operation
 * \tparam Output Output type of the Reduce operation
 * \tparam Stack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function
 */
template <typename Input, typename Output, typename Stack, typename KeyExtractor, typename ReduceFunction>
class ReduceNode : public DOpNode<Output>
{
    using reduce_arg_t = typename FunctionTraits<ReduceFunction>::template arg<0>;

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
               DIANode<Input>* parent,
               Stack& stack,
               KeyExtractor key_extractor,
               ReduceFunction reduce_function)
        : DOpNode<Output>(ctx, { parent }),
          local_stack_(stack),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          elements_()
    { 
        // Hook PreOp
        auto pre_op_fn = [=](reduce_arg_t input) {
                    PreOp(input);
                };
        auto lop_chain = local_stack_.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    virtual ~ReduceNode() { }

    /*!
     * Actually executes the reduce operation. Uses the member functions PreOp, MainOp and PostOp.
     */
    void execute() override
    {
        MainOp();
        // get data from data manager
        data::BlockIterator<Output> it = (this->context_).get_data_manager().template GetLocalBlocks<Output>(this->data_id_);
        // loop over input
        while (it.HasNext()) {
            auto item = it.Next();
            for (auto func : DIANode<Output>::callbacks_) {
                func(item);
            }
        }
    }

    /*!
     * Produces a function stack, which only contains the PostOp function.
     * \return PostOp function stack
     */
    auto ProduceStack() {
        // Hook PostOp
        auto post_op_fn = [ = ](Output elem, std::function<void(Output)> emit_func) {
                     return PostOp(elem, emit_func);
                 };

        FunctionStack<> stack;
        return stack.push(post_op_fn);
    }

    /*!
     * Returns "[ReduceNode]" as a string.
     * \return "[ReduceNode]"
     */
    std::string ToString() override
    {
        // Create string
        std::string str
            = std::string("[ReduceNode] Id: ") + std::to_string(this->data_id_);
        return str;
    }

private:
    //! Local stack
    Stack local_stack_;
    //!Key extractor function
    KeyExtractor key_extractor_;
    //!Reduce function
    ReduceFunction reduce_function_;
    //! Local storage
    std::vector<reduce_arg_t> elements_;

    //! Locally hash elements of the current DIA onto buckets and reduce each bucket to a single value,
    //! afterwards send data to another worker given by the shuffle algorithm.
    void PreOp(reduce_arg_t input)
    {
        SpacingLogger(true) << "PreOp: " << input;
        elements_.push_back(input);
    }

    //!Recieve elements from other workers.
    auto MainOp() { 
        data::BlockEmitter<Output> emit = (this->context_).get_data_manager().template GetLocalEmitter<Output>(this->data_id_);

        reduce_arg_t reduced = reduce_arg_t();
        for (reduce_arg_t elem : elements_) {
            reduced += elem;
        }
        
        SpacingLogger(true) << "MainOp: " << reduced;
        emit(reduced);
    }

    //! Hash recieved elements onto buckets and reduce each bucket to a single value.
    void PostOp(Output input, std::function<void(Output)> emit_func)
    {
        SpacingLogger(true) << "PostOp: " << input;
        emit_func(input);
    }
};

//! \}

} // namespace c7a

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
