/*******************************************************************************
 * c7a/api/reduce_to_index_node.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_REDUCE_TO_INDEX_NODE_HEADER
#define C7A_API_REDUCE_TO_INDEX_NODE_HEADER

#include <c7a/api/dop_node.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/core/reduce_post_table.hpp>
#include <c7a/data/emitter.hpp>

#include <unordered_map>
#include <functional>
#include <string>
#include <vector>
#include <type_traits>

namespace c7a {
namespace api {
//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a ReduceToIndex operation. ReduceToIndex groups the elements in a
 * DIA by their key and reduces every key bucket to a single element each. The
 * ReduceToIndexNode stores the key_extractor and the reduce_function UDFs. The
 * chainable LOps ahead of the Reduce operation are stored in the Stack. The
 * ReduceToIndexNode has the type Output, which is the result type of the
 * reduce_function. The key type is an unsigned integer and the output DIA will have element
 * with key K at index K.
 *
 * \tparam Input Input type of the Reduce operation
 * \tparam Output Output type of the Reduce operation
 * \tparam Stack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function
 */
template <typename Input, typename Output, typename Stack,
          typename KeyExtractor, typename ReduceFunction>
class ReduceToIndexNode : public DOpNode<Output>
{
    static const bool debug = false;

    using Super = DOpNode<Output>;

    using ReduceArg = typename FunctionTraits<ReduceFunction>::template arg<0>;

    using Key = typename FunctionTraits<KeyExtractor>::result_type;

    static_assert(std::is_same<Key, size_t>::value, "Key must be an unsigned integer");

    using Value = typename FunctionTraits<ReduceFunction>::result_type;

    typedef std::pair<Key, Value> KeyValuePair;

    using Super::context_;
    using Super::data_id_;

public:
    using PreHashTable = typename c7a::core::ReducePreTable<KeyExtractor, ReduceFunction,
                                                            data::Emitter<KeyValuePair> >;

    /*!
     * Constructor for a ReduceToIndexNode. Sets the DataManager, parent, stack,
     * key_extractor and reduce_function.
     *
     * \param ctx Reference to Context, which holds references to data and
     * network.
     * \param parent Parent DIANode.
     * \param stack Function chain with all lambdas between the parent and this
     * node
     * \param key_extractor Key extractor function
     * \param reduce_function Reduce function
     */
    ReduceToIndexNode(Context& ctx,
                      DIANode<Input>* parent,
                      Stack& stack,
                      KeyExtractor key_extractor,
                      ReduceFunction reduce_function,
                      size_t max_index)
        : DOpNode<Output>(ctx, { parent }),
          local_stack_(stack),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          channel_id_(ctx.get_data_manager().AllocateNetworkChannel()),
          emitters_(ctx.get_data_manager().
                    template GetNetworkEmitters<KeyValuePair>(channel_id_)),
          reduce_pre_table_(ctx.number_worker(), key_extractor,
                            reduce_function_, emitters_,
                            [=](int key, PreHashTable* ht) {
                                size_t global_index = key * ht->NumBuckets() / max_index;
                                size_t partition_id = key * ht->NumPartitions() / max_index;
                                size_t partition_offset = global_index - (partition_id * ht->NumBucketsPerPartition());
                                return typename PreHashTable::hash_result(partition_id, partition_offset, global_index);
                            }),
          max_index_(max_index)
    {
        // Hook PreOp
        auto pre_op_fn = [=](ReduceArg input) {
                             PreOp(input);
                         };
        auto lop_chain = local_stack_.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    //! Virtual destructor for a ReduceToIndexNode.
    virtual ~ReduceToIndexNode() { }

    /*!
     * Actually executes the reduce to index operation. Uses the member functions PreOp,
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
        auto post_op_fn = [=](Output elem, auto emit_func) {
                              return this->PostOp(elem, emit_func);
                          };

        FunctionStack<> stack;
        return stack.push(post_op_fn);
    }

    /*!
     * Returns "[ReduceToIndexNode]" and its id as a string.
     * \return "[ReduceToIndexNode]"
     */
    std::string ToString() override {
        return "[ReduceToIndexNode] Id: " + data_id_.ToString();
    }

private:
    //! Local stack
    Stack local_stack_;
    //!Key extractor function
    KeyExtractor key_extractor_;
    //!Reduce function
    ReduceFunction reduce_function_;

    data::ChannelId channel_id_;

    std::vector<data::Emitter<KeyValuePair> > emitters_;

    core::ReducePreTable<KeyExtractor, ReduceFunction, data::Emitter<KeyValuePair> >
    reduce_pre_table_;

    size_t max_index_;

    //! Locally hash elements of the current DIA onto buckets and reduce each
    //! bucket to a single value, afterwards send data to another worker given
    //! by the shuffle algorithm.
    void PreOp(ReduceArg input) {
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
                                          std::function<void(Output)>,
                                          true>;

        ReduceTable table(key_extractor_, reduce_function_,
                          DIANode<Output>::callbacks(),
                          [=](Key key, ReduceTable* ht) {
                              return key * ht->NumBuckets() / max_index_;
                          },
                          max_index_);

        auto it = context_.get_data_manager().
                  template GetIterator<KeyValuePair>(channel_id_);

        sLOG << "reading data from" << channel_id_ << "to push into post table which flushes to" << data_id_;
        do {
            it.WaitForMore();
            while (it.HasNext()) {
                table.Insert(std::move(it.Next()));
            }
        } while (!it.IsFinished());

        table.Flush();
    }

    //! Hash recieved elements onto buckets and reduce each bucket to a single value.
    template <typename Emitter>
    void PostOp(Output input, Emitter emit_func) {
        emit_func(input);
    }
};

//! \}

template <typename T, typename Stack>
template <typename KeyExtractor, typename ReduceFunction>
auto DIARef<T, Stack>::ReduceToIndex(const KeyExtractor &key_extractor,
                                     const ReduceFunction &reduce_function,
                                     size_t max_index) {

    using DOpResult
              = typename FunctionTraits<ReduceFunction>::result_type;
    using ReduceResultNode
              = ReduceToIndexNode<T, DOpResult, decltype(local_stack_),
                                  KeyExtractor, ReduceFunction>;

    auto shared_node
        = std::make_shared<ReduceResultNode>(node_->get_context(),
                                             node_.get(),
                                             local_stack_,
                                             key_extractor,
                                             reduce_function,
                                             max_index);

    auto reduce_stack = shared_node->ProduceStack();

    return DIARef<DOpResult, decltype(reduce_stack)>
               (std::move(shared_node), reduce_stack);
}
}

} // namespace api

#endif // !C7A_API_REDUCE_TO_INDEX_NODE_HEADER

/******************************************************************************/
