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
#include <c7a/api/context.hpp>
#include "function_stack.hpp"
#include "../common/logger.hpp"
#include "../core/hash_table.hpp"
#include "../data/data_manager.hpp"

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
    static const bool debug = true;

    typedef DOpNode<Output> Super;

    using reduce_arg_t = typename FunctionTraits<ReduceFunction>::template arg<0>;

    using Super::context_;
    using Super::data_id_;

public:
    /*!
     * Constructor for a ReduceNode. Sets the DataManager, parent, stack, key_extractor and reduce_function.
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
          elements_()
    {
        // Hook PreOp
        auto pre_op_fn = [ = ](reduce_arg_t input) {
                             PreOp(input);
                         };

        data::ChannelId preop_net_id_ = AllocateNetworkChannel(); 
        auto lop_chain = local_stack_.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    //! Virtual destructor for a ReduceNode.
    virtual ~ReduceNode() { }

    /*!
     * Actually executes the reduce operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void execute() override
    {
        //Flush Table to send elements over NETWORK TODO: does position of the flush operation make sense?
        reduce_pre_table_.Flush();   

        MainOp();
        // get data from data manager
        data::BlockIterator<Output> it = context_->get_data_manager().template GetLocalBlocks<Output>(data_id_);
        // loop over input
        while (it.HasNext()) {
            const Output& item = it.Next();
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
     * Returns "[ReduceNode]" and its id as a string.
     * \return "[ReduceNode]"
     */
    std::string ToString() override
    {
        return std::string("[ReduceNode id=") + std::to_string(data_id_) + "]";
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
    //!ChannelId to emit preop elements
    data::ChannelId preop_net_id_;
    //!Hashtable to do the local reduce
    //TODO HASHTABLE NEEDS whatever auto data::GetNetworkEmitters(preop_net_id_) returns as input param or such
    core::HashTable reduce_pre_table_(context_->number_worker, 
                                  key_extractor_, 
                                  reduce_function_, 
                                  context_->get_data_manager().GetNetworkEmitters(preop_net_id_));

    //! Locally hash elements of the current DIA onto buckets and reduce each bucket to a single value,
    //! afterwards send data to another worker given by the shuffle algorithm.
    void PreOp(reduce_arg_t input)
    {
        LOG << "PreOp: " << input;
        reduce_pre_table_.Insert(input);
    }

    //!Recieve elements from other workers.
    auto MainOp() {
        auto net_iterator = context_->get_data_manager().GetRemoteBlocks(preop_net_id_);

        //TODO: THIS MUST BE THE SPECIAL POST HASH TABLE
        //TODO: TELL HASHTABLE NOT TO FLUSH
        core::HashTable reduce_post_table_(1, 
                                           key_extractor_, 
                                           reduce_function_, 
                                           context_->get_data_manager().GetLocalEmitter<Output>(data_id_));

        while (net_iterator.HasNext()) {
            reduce_post_table_.Insert(net_iterator.Next());
        }


        // TODO: Call Callbakc in postop flush
        reduce_post_table_.Flush();

        LOG << "MainOp: " << reduced;
    }

    //! Hash recieved elements onto buckets and reduce each bucket to a single value.
    void PostOp(Output input, std::function<void(Output)> emit_func)
    {
        LOG << "PostOp: " << input;
        emit_func(input);
    }
};

//! \}

} // namespace c7a

#endif // !C7A_API_REDUCE_NODE_HEADER

/******************************************************************************/
