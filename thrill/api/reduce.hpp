/*******************************************************************************
 * thrill/api/reduce.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_REDUCE_HEADER
#define THRILL_API_REDUCE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_post_table.hpp>
#include <thrill/core/reduce_pre_table.hpp>

#include <functional>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A DIANode which performs a Reduce operation. Reduce groups the elements in a
 * DIA by their key and reduces every key bucket to a single element each. The
 * ReduceNode stores the key_extractor and the reduce_function UDFs. The
 * chainable LOps ahead of the Reduce operation are stored in the Stack. The
 * ReduceNode has the type ValueType, which is the result type of the
 * reduce_function.
 *
 * \tparam ValueType Output type of the Reduce operation
 * \tparam Stack Function stack, which contains the chained lambdas between the
 *  last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function.
 * \tparam RobustKey Whether to reuse the key once extracted in during pre reduce
 * (false) or let the post reduce extract the key again (true).
 */
template <typename ValueType, typename ParentDIARef,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey, const bool SendPair>
class ReduceNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;

    using Value = typename common::FunctionTraits<ReduceFunction>::result_type;

    using ReduceArg = typename common::FunctionTraits<ReduceFunction>
                      ::template arg<0>;

    using KeyValuePair = std::pair<Key, Value>;

    using Super::context_;

public:
    /*!
     * Constructor for a ReduceNode. Sets the parent, stack,
     * key_extractor and reduce_function.
     *
     * \param parent Parent DIARef.
     * and this node
     * \param key_extractor Key extractor function
     * \param reduce_function Reduce function
     */
    ReduceNode(const ParentDIARef& parent,
               const KeyExtractor& key_extractor,
               const ReduceFunction& reduce_function,
               StatsNode* stats_node)
        : DOpNode<ValueType>(parent.ctx(), { parent.node() }, stats_node),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          stream_(parent.ctx().GetNewCatStream()),
          emitters_(stream_->OpenWriters()),
          reduce_pre_table_(
              parent.ctx().num_workers(), key_extractor,
              reduce_function_, emitters_, 1024 * 1024 * 128 * 8, 0.9, 0.6),
          reduce_post_table_(
              context_, key_extractor_, reduce_function_,
              [this](const ValueType& item) { return this->PushItem(item); },
              core::PostReduceByHashKey<Key>(),
              core::PostReduceFlushToDefault<Key,
                                             ReduceFunction>(reduce_function),
              0, 0, Value(), 1024 * 1024 * 128 * 8, 0.9, 0.6, 0.01)
    {
        // Hook PreOp: Locally hash elements of the current DIA onto buckets and
        // reduce each bucket to a single value, afterwards send data to another
        // worker given by the shuffle algorithm.

        auto pre_op_fn = [=](const ValueType& input) {
                             reduce_pre_table_.Insert(input);
                         };
        // close the function stack with our pre op and register it at
        // parent node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
        stream_->OnClose([this]() {
                             this->WriteStreamStats(this->stream_);
                         });
    }

    /*!
     * Actually executes the reduce operation.
     */
    void Execute() final {
        LOG << this->label() << " running main op";
        // Flush hash table before the postOp
        reduce_pre_table_.Flush();
        reduce_pre_table_.CloseEmitter();
        stream_->Close();
    }

    void PushData(bool consume) final {

        if (reduced) {
            reduce_post_table_.Flush(consume);
            return;
        }

        if (RobustKey) {
            auto reader = stream_->OpenCatReader(consume);
            sLOG << "reading data from" << stream_->id()
                 << "to push into post table which flushes to" << this->id();
            while (reader.HasNext()) {
                reduce_post_table_.Insert(reader.template Next<Value>());
            }
        }
        else {
            auto reader = stream_->OpenCatReader(consume);
            sLOG << "reading data from" << stream_->id()
                 << "to push into post table which flushes to" << this->id();
            while (reader.HasNext()) {
                reduce_post_table_.Insert(reader.template Next<KeyValuePair>());
            }
        }

        reduced = true;
        reduce_post_table_.Flush(consume);
    }

    void Dispose() final { }

    FunctionStack<ValueType> ProduceStack() {
        return FunctionStack<ValueType>();
    }

private:
    //! Key extractor function
    KeyExtractor key_extractor_;

    //! Reduce function
    ReduceFunction reduce_function_;

    data::CatStreamPtr stream_;

    std::vector<data::CatStream::Writer> emitters_;

    core::ReducePreTable<
        Key, Value, KeyExtractor, ReduceFunction, RobustKey,
        core::PreReduceByHashKey<Key>, std::equal_to<Key>, 16*16> reduce_pre_table_;

    core::ReducePostTable<
        ValueType, Key, Value, KeyExtractor, ReduceFunction, SendPair,
        core::PostReduceFlushToDefault<Key, ReduceFunction>, core::PostReduceByHashKey<Key>,
        std::equal_to<Key>, 16*16> reduce_post_table_;

    bool reduced = false;
};

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction>
auto DIARef<ValueType, Stack>::ReduceBy(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function) const {
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
            typename std::decay<typename common::FunctionTraits<KeyExtractor>
                                ::template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    StatsNode* stats_node = AddChildStatsNode("ReduceBy", DIANodeType::DOP);
    using ReduceResultNode
              = ReduceNode<DOpResult, DIARef, KeyExtractor,
                           ReduceFunction, true, false>;
    auto shared_node
        = std::make_shared<ReduceResultNode>(*this,
                                             key_extractor,
                                             reduce_function,
                                             stats_node);

    auto reduce_stack = shared_node->ProduceStack();

    return DIARef<DOpResult, decltype(reduce_stack)>(
        shared_node,
        reduce_stack,
        { stats_node });
}

template <typename ValueType, typename Stack>
template <typename ReduceFunction>
auto DIARef<ValueType, Stack>::ReducePair(
    const ReduceFunction &reduce_function) const {
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

    using Key = typename ValueType::first_type;
    using Value = typename ValueType::second_type;

    StatsNode* stats_node = AddChildStatsNode("ReducePair", DIANodeType::DOP);
    using ReduceResultNode
              = ReduceNode<ValueType, DIARef, std::function<Key(Value)>,
                           ReduceFunction, false, true>;
    auto shared_node
        = std::make_shared<ReduceResultNode>(*this,
                                             [](Value value) {
                                                 // This function should not be
                                                 // called, it is only here to
                                                 // give the key type to the
                                                 // hashtables.
                                                 assert(1 == 0);
                                                 value = value;
                                                 return Key();
                                             },
                                             reduce_function,
                                             stats_node);

    auto reduce_stack = shared_node->ProduceStack();

    return DIARef<ValueType, decltype(reduce_stack)>(
        shared_node,
        reduce_stack,
        { stats_node });
}

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction>
auto DIARef<ValueType, Stack>::ReduceByKey(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function) const {
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

    StatsNode* stats_node = AddChildStatsNode("ReduceByKey", DIANodeType::DOP);
    using ReduceResultNode
              = ReduceNode<DOpResult, DIARef, KeyExtractor,
                           ReduceFunction, false, false>;
    auto shared_node
        = std::make_shared<ReduceResultNode>(*this,
                                             key_extractor,
                                             reduce_function,
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

#endif // !THRILL_API_REDUCE_HEADER

/******************************************************************************/
