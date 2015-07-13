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
 * ReduceToIndexNode has the type ValueType, which is the result type of the
 * reduce_function. The key type is an unsigned integer and the output DIA will have element
 * with key K at index K.
 *
 * \tparam ParentType Input type of the Reduce operation
 * \tparam ValueType Output type of the Reduce operation
 * \tparam ParentStack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function
 */
template <typename ValueType, typename ParentStack,
          typename KeyExtractor, typename ReduceFunction>
class ReduceToIndexNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;

    using ReduceArg =
              typename common::FunctionTraits<ReduceFunction>::template arg<0>;

    using Key =
              typename common::FunctionTraits<KeyExtractor>::result_type;

    static_assert(std::is_same<Key, size_t>::value, "Key must be an unsigned integer");

    using Value = typename common::FunctionTraits<ReduceFunction>::result_type;

    typedef std::pair<Key, Value> KeyValuePair;

    using ParentInput = typename ParentStack::Input;

    using Super::context_;
    using Super::data_id_;

public:
    using PreHashTable = typename c7a::core::ReducePreTable<
              KeyExtractor, ReduceFunction, data::Emitter<KeyValuePair> >;

    /*!
     * Constructor for a ReduceToIndexNode. Sets the DataManager, parent, stack,
     * key_extractor and reduce_function.
     *
     * \param ctx Reference to Context, which holds references to data and
     * network.
     * \param parent Parent DIANode.
     * \param parent_stack Function chain with all lambdas between the parent
     * and this node
     * \param key_extractor Key extractor function
     * \param reduce_function Reduce function
     * \param max_index maximum index returned by reduce_function.
     */
    ReduceToIndexNode(Context& ctx,
                      std::shared_ptr<DIANode<ParentInput> > parent,
                      ParentStack& parent_stack,
                      KeyExtractor key_extractor,
                      ReduceFunction reduce_function,
                      size_t max_index)
        : DOpNode<ValueType>(ctx, { parent }),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          channel_id_(ctx.data_manager().AllocateNetworkChannel()),
          emitters_(ctx.data_manager().
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
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent_stack.push(pre_op_fn).emit();
        parent->RegisterChild(lop_chain);
    }

    //! Virtual destructor for a ReduceToIndexNode.
    virtual ~ReduceToIndexNode() { }

    /*!
     * Actually executes the reduce to index operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() override {
        MainOp();
    }

    /*!
     * Produces a function stack, which only contains the PostOp function.
     * \return PostOp function stack
     */
    auto ProduceStack() {
        // Hook PostOp
        auto post_op_fn = [=](ValueType elem, auto emit_func) {
                              return this->PostOp(elem, emit_func);
                          };

        return MakeFunctionStack<ValueType>(post_op_fn);
    }

    /*!
     * Returns "[ReduceToIndexNode]" and its id as a string.
     * \return "[ReduceToIndexNode]"
     */
    std::string ToString() override {
        return "[ReduceToIndexNode] Id: " + data_id_.ToString();
    }

private:
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
                                          std::function<void(ValueType)>,
                                          true>;

        ReduceTable table(key_extractor_, reduce_function_,
                          DIANode<ValueType>::callbacks(),
                          [=](Key key, ReduceTable* ht) {
                              return key * ht->NumBuckets() / max_index_;
                          },
                          max_index_);

        auto it = context_.data_manager().
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
    void PostOp(ValueType input, Emitter emit_func) {
        emit_func(input);
    }
};

//! \}

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction>
auto DIARef<ValueType, Stack>::ReduceToIndex(const KeyExtractor &key_extractor,
                                             const ReduceFunction &reduce_function,
                                             size_t max_index) {

    using DOpResult
              = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(
        std::is_same<
            typename common::FunctionTraits<ReduceFunction>::template arg<0>,
            ValueType>::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<ReduceFunction>::template arg<1>,
            ValueType>::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            DOpResult,
            ValueType>::value,
        "ReduceFunction has the wrong output type");

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>::template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<KeyExtractor>::result_type,
            size_t>::value,
        "The key has to be an unsigned long int (aka. size_t).");

    using ReduceResultNode
              = ReduceToIndexNode<DOpResult, Stack,
                                  KeyExtractor, ReduceFunction>;

    auto shared_node
        = std::make_shared<ReduceResultNode>(node_->context(),
                                             node_,
                                             stack_,
                                             key_extractor,
                                             reduce_function,
                                             max_index);

    auto reduce_stack = shared_node->ProduceStack();

    return DIARef<DOpResult, decltype(reduce_stack)>
               (std::move(shared_node), reduce_stack);
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_REDUCE_TO_INDEX_NODE_HEADER

/******************************************************************************/
