/*******************************************************************************
 * thrill/api/reduce_to_index.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_REDUCE_TO_INDEX_HEADER
#define THRILL_API_REDUCE_TO_INDEX_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>
#include <thrill/core/reduce_post_table.hpp>
#include <thrill/core/reduce_pre_table.hpp>

#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace thrill {
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
template <typename ValueType, typename ParentDIARef,
          typename KeyExtractor, typename ReduceFunction,
          bool RobustKey, bool SendPair>
class ReduceToIndexNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;

    static_assert(std::is_same<Key, size_t>::value,
                  "Key must be an unsigned integer");

    using Value = typename common::FunctionTraits<ReduceFunction>::result_type;

    using KeyValuePair = std::pair<Key, Value>;

    using Super::context_;

public:
    using Emitter = data::DynBlockWriter;
    using PreHashTable = typename core::ReducePreTable<
              Key, Value,
              KeyExtractor, ReduceFunction, RobustKey, core::PreReduceByIndex>;

    /*!
     * Constructor for a ReduceToIndexNode. Sets the parent, stack,
     * key_extractor and reduce_function.
     *
     * \param parent Parent DIARef.
     * \param key_extractor Key extractor function
     * \param reduce_function Reduce function
     * \param result_size size of the resulting DIA, range of index returned by reduce_function.
     * \param neutral_element Item value with which to start the reduction in
     * each array cell.
     */
    ReduceToIndexNode(const ParentDIARef& parent,
                      KeyExtractor key_extractor,
                      ReduceFunction reduce_function,
                      size_t result_size,
                      Value neutral_element,
                      StatsNode* stats_node)
        : DOpNode<ValueType>(parent.ctx(), { parent.node() }, stats_node),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          channel_(parent.ctx().GetNewChannel()),
          emitters_(channel_->OpenWriters()),
          reduce_pre_table_(parent.ctx().num_workers(), key_extractor,
                            reduce_function_, emitters_, 1024 * 1024 * 128 * 5, 0.001, 0.5,
                            core::PreReduceByIndex(result_size)),
          result_size_(result_size),
          neutral_element_(neutral_element)
    {
        // Hook PreOp
        auto pre_op_fn = [=](ValueType input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    //! Virtual destructor for a ReduceToIndexNode.
    virtual ~ReduceToIndexNode() { }

    /*!
     * Actually executes the reduce to index operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {
        // TODO(tb@ms): this is not what should happen: every thing is reduced again:

        using ReduceTable
                  = core::ReducePostTable<ValueType, Key, Value,
                                          KeyExtractor,
                                          ReduceFunction,
                                          SendPair,
                                          false,
                                          core::PostReduceFlushToIndex<Value>,
                                          core::PostReduceByIndex,
                                          std::equal_to<Key>,
                                          16*1024>;

        size_t local_begin, local_end;

        std::tie(local_begin, local_end) = common::CalculateLocalRange(result_size_, context_);

        std::vector<std::function<void(const ValueType&)> > cbs;
        DIANode<ValueType>::callback_functions(cbs);

        ReduceTable table(context_, key_extractor_, reduce_function_, cbs,
                          core::PostReduceByIndex(),
                          core::PostReduceFlushToIndex<Value>(),
                          local_begin,
                          local_end,
                          neutral_element_,
                          1024 * 1024 * 128 * 5,
                          0.001,
                          0.5,
                          64);

        if (RobustKey) {
            // we actually want to wire up callbacks in the ctor and NOT use this blocking method
            auto reader = channel_->OpenConcatReader(consume);
            sLOG << "reading data from" << channel_->id() << "to push into post table which flushes to" << this->id();
            while (reader.HasNext()) {
                table.Insert(reader.template Next<Value>());
            }
            table.Flush();
        }
        else {
            // we actually want to wire up callbacks in the ctor and NOT use this blocking method
            auto reader = channel_->OpenConcatReader(consume);
            sLOG << "reading data from" << channel_->id() << "to push into post table which flushes to" << this->id();
            while (reader.HasNext()) {
                table.Insert(reader.template Next<KeyValuePair>());
            }
            table.Flush();
        }
    }

    void Dispose() final { }

    /*!
     * Produces a function stack, which only contains the PostOp function.
     * \return PostOp function stack
     */
    auto ProduceStack() {
        return FunctionStack<ValueType>();
        /*
       // Hook PostOp
       auto post_op_fn = [=](ValueType elem, auto emit_func) {
                             return this->PostOp(elem, emit_func);
                         };

                         return MakeFunctionStack<ValueType>(post_op_fn);*/
    }

private:
    //! Key extractor function
    KeyExtractor key_extractor_;
    //! Reduce function
    ReduceFunction reduce_function_;

    data::ChannelPtr channel_;

    std::vector<data::Channel::Writer> emitters_;

    PreHashTable reduce_pre_table_;

    size_t result_size_;

    Value neutral_element_;

    //! Locally hash elements of the current DIA onto buckets and reduce each
    //! bucket to a single value, afterwards send data to another worker given
    //! by the shuffle algorithm.
    void PreOp(ValueType input) {
        reduce_pre_table_.Insert(std::move(input));
    }

    //! Receive elements from other workers.
    auto MainOp() {
        LOG << this->label() << " running main op";
        // Flush hash table before the postOp
        reduce_pre_table_.Flush();
        reduce_pre_table_.CloseEmitter();
        channel_->Close();
        this->WriteChannelStats(channel_);
    }

    //! Hash recieved elements onto buckets and reduce each bucket to a single value.
    template <typename Emitter>
    void PostOp(ValueType input, Emitter emit_func) {
        emit_func(input);
    }
};

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction>
auto DIARef<ValueType, Stack>::ReduceToIndexByKey(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function,
    size_t size,
    ValueType neutral_element) const {
    assert(IsValid());

    using DOpResult
              = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<0>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<1>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            DOpResult,
            ValueType>::value,
        "ReduceFunction has the wrong output type");

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>::
                                template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<KeyExtractor>::result_type,
            size_t>::value,
        "The key has to be an unsigned long int (aka. size_t).");

    using ReduceResultNode
              = ReduceToIndexNode<DOpResult, DIARef,
                                  KeyExtractor, ReduceFunction,
                                  false, false>;

    StatsNode* stats_node = AddChildStatsNode("ReduceToIndexByKey", DIANodeType::DOP);
    auto shared_node
        = std::make_shared<ReduceResultNode>(*this,
                                             key_extractor,
                                             reduce_function,
                                             size,
                                             neutral_element,
                                             stats_node);

    auto reduce_stack = shared_node->ProduceStack();

    return DIARef<DOpResult, decltype(reduce_stack)>(
        shared_node,
        reduce_stack,
        { stats_node });
}

template <typename ValueType, typename Stack>
template <typename ReduceFunction>
auto DIARef<ValueType, Stack>::ReducePairToIndex(
    const ReduceFunction &reduce_function,
    size_t size,
    typename common::FunctionTraits<ReduceFunction>::result_type
    neutral_element) const {
    assert(IsValid());

    using DOpResult
              = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(common::is_pair<ValueType>::value,
                  "ValueType is not a pair");

    static_assert(
        std::is_convertible<
            typename ValueType::second_type,
            typename common::FunctionTraits<ReduceFunction>::template arg<0>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            typename ValueType::second_type,
            typename common::FunctionTraits<ReduceFunction>::template arg<1>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            DOpResult,
            typename ValueType::second_type>::value,
        "ReduceFunction has the wrong output type");

    static_assert(
        std::is_same<
            typename ValueType::first_type,
            size_t>::value,
        "The key has to be an unsigned long int (aka. size_t).");

    using Key = typename ValueType::first_type;

    using ReduceResultNode
              = ReduceToIndexNode<ValueType, DIARef,
                                  std::function<Key(Key)>,
                                  ReduceFunction, false, true>;

    StatsNode* stats_node = AddChildStatsNode("ReduceToPairIndex", DIANodeType::DOP);
    auto shared_node
        = std::make_shared<ReduceResultNode>(*this,
                                             [](Key key) {
                                                 // This function should not be
                                                 // called, it is only here to
                                                 // give the key type to the
                                                 // hashtables.
                                                 assert(1 == 0);
                                                 key = key;
                                                 return Key();
                                             },
                                             reduce_function,
                                             size,
                                             neutral_element,
                                             stats_node);

    auto reduce_stack = shared_node->ProduceStack();

    return DIARef<ValueType, decltype(reduce_stack)>(
        shared_node,
        reduce_stack,
        { stats_node });
}

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction>
auto DIARef<ValueType, Stack>::ReduceToIndex(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function,
    size_t size,
    ValueType neutral_element) const {
    assert(IsValid());

    using DOpResult
              = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<0>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<1>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            DOpResult,
            ValueType>::value,
        "ReduceFunction has the wrong output type");

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>::
                                template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<KeyExtractor>::result_type,
            size_t>::value,
        "The key has to be an unsigned long int (aka. size_t).");

    using ReduceResultNode
              = ReduceToIndexNode<DOpResult, DIARef,
                                  KeyExtractor, ReduceFunction,
                                  true, false>;

    StatsNode* stats_node = AddChildStatsNode("ReduceToIndex", DIANodeType::DOP);
    auto shared_node
        = std::make_shared<ReduceResultNode>(*this,
                                             key_extractor,
                                             reduce_function,
                                             size,
                                             neutral_element,
                                             stats_node);

    auto reduce_stack = shared_node->ProduceStack();

    return DIARef<DOpResult, decltype(reduce_stack)>(
        shared_node,
        reduce_stack,
        { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_REDUCE_TO_INDEX_HEADER

/******************************************************************************/
