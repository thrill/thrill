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

#include <c7a/api/dop_node.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/core/reduce_post_table.hpp>

#include <unordered_map>
#include <functional>
#include <string>
#include <vector>

namespace c7a {
//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Reduce operation. Reduce groups the elements in a
 * DIA by their key and reduces every key bucket to a single element each. The
 * ReduceNode stores the key_extractor and the reduce_function UDFs. The
 * chainable LOps ahead of the Reduce operation are stored in the Stack. The
 * ReduceNode has the type Output, which is the result type of the
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
    static const bool debug = true;

    typedef DOpNode<Output> Super;

    using reduce_arg_t = typename FunctionTraits<ReduceFunction>::template arg<0>;

    using Super::context_;
    using Super::data_id_;

public:
    /*!
     * Constructor for a ReduceNode. Sets the DataManager, parent, stack,
     * key_extractor and reduce_function.
     *
     * \param ctx Reference to Context, which holds references to data and network.
     * \param parent Parent DIANode.
     * \param stack Function chain with all lambdas between the parent and this node
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
          channel_id_(ctx.get_data_manager().AllocateNetworkChannel()),
          emitters_(ctx.get_data_manager().template GetNetworkEmitters<Output>(channel_id_)),
          reduce_pre_table_(ctx.number_worker(), key_extractor, reduce_function_, emitters_)
    {
        // Hook PreOp
        auto pre_op_fn = [=](reduce_arg_t input) {
                             PreOp(input);
                         };
        auto lop_chain = local_stack_.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    //! Virtual destructor for a ReduceNode.
    virtual ~ReduceNode() { }

    /*!
     * Actually executes the reduce operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void execute() override {
        MainOp();
    }

    /*!
     * Produces a function stack, which only contains the PostOp function.
     * \return PostOp function stack
     */
    auto ProduceStack() {
        // Hook PostOp
        auto post_op_fn = [=](Output elem, std::function<void(Output)> emit_func) {
                              return PostOp(elem, emit_func);
                          };

        FunctionStack<> stack;
        return stack.push(post_op_fn);
    }

    /*!
     * Returns "[ReduceNode]" and its id as a string.
     * \return "[ReduceNode]"
     */
    std::string ToString() override {
        return "[ReduceNode] Id: " + data_id_.ToString();
    }

private:
    //! Local stack
    Stack local_stack_;
    //!Key extractor function
    KeyExtractor key_extractor_;
    //!Reduce function
    ReduceFunction reduce_function_;

    data::ChannelId channel_id_;

    std::vector<data::BlockEmitter<Output> > emitters_;

    core::ReducePreTable<KeyExtractor, ReduceFunction, data::BlockEmitter<Output> > reduce_pre_table_;

    //! Locally hash elements of the current DIA onto buckets and reduce each
    //! bucket to a single value, afterwards send data to another worker given
    //! by the shuffle algorithm.
    void PreOp(reduce_arg_t input) {
        reduce_pre_table_.Insert(std::move(input));
    }

    //!Receive elements from other workers.
    auto MainOp() {
        LOG << ToString() << " running main op";
        //Flush hash table before the postOp
        reduce_pre_table_.Flush();
        reduce_pre_table_.CloseEmitter();

        using ReduceTable
                  = core::ReducePostTable<KeyExtractor,
                                          ReduceFunction,
                                          std::function<void(Output)> >;

        ReduceTable table(key_extractor_, reduce_function_, DIANode<Output>::callbacks());

        auto it = context_.get_data_manager().template GetIterator<Output>(channel_id_);

        sLOG << "reading data from" << channel_id_ << "to push into post table which flushes to" << data_id_;
        do {
            std::cout << "waiting...";
            it.WaitForMore();
            std::cout << " ... done" << std::endl;
            while (it.HasNext()) {
                table.Insert(it.Next());
            }
        } while (!it.IsClosed());

        table.Flush();
    }

    //! Hash recieved elements onto buckets and reduce each bucket to a single value.
    void PostOp(Output input, std::function<void(Output)> emit_func) {
        emit_func(input);
    }
};

//! \}

} // namespace c7a

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
