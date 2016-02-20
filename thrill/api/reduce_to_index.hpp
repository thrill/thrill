/*******************************************************************************
 * thrill/api/reduce_to_index.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_REDUCE_TO_INDEX_HEADER
#define THRILL_API_REDUCE_TO_INDEX_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/api/reduce_config.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_by_index_post_stage.hpp>
#include <thrill/core/reduce_pre_stage.hpp>

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
 * A DIANode which performs a ReduceToIndex operation. ReduceToIndex groups the
 * elements in a DIA by their key and reduces every key bucket to a single
 * element each. The ReduceToIndexNode stores the key_extractor and the
 * reduce_function UDFs. The chainable LOps ahead of the Reduce operation are
 * stored in the Stack. The ReduceToIndexNode has the type ValueType, which is
 * the result type of the reduce_function. The key type is an unsigned integer
 * and the output DIA will have element with key K at index K.
 *
 * \tparam ParentType Input type of the Reduce operation
 * \tparam ValueType Output type of the Reduce operation
 * \tparam ParentStack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function
 */
template <typename ValueType, typename ParentDIA,
          typename KeyExtractor, typename ReduceFunction,
          typename ReduceConfig,
          bool RobustKey, bool SendPair>
class ReduceToIndexNode final : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;

    static_assert(std::is_same<Key, size_t>::value,
                  "Key must be an unsigned integer");

    using Value = typename common::FunctionTraits<ReduceFunction>::result_type;

    using KeyValuePair = std::pair<Key, Value>;

    using Super::context_;

private:
    //! Emitter for PostStage to push elements to next DIA object.
    class Emitter
    {
    public:
        explicit Emitter(ReduceToIndexNode* node) : node_(node) { }
        void operator () (const ValueType& item) const
        { return node_->PushItem(item); }

    private:
        ReduceToIndexNode* node_;
    };

public:
    /*!
     * Constructor for a ReduceToIndexNode. Sets the parent, stack,
     * key_extractor and reduce_function.
     */
    ReduceToIndexNode(const ParentDIA& parent,
                      const char* label,
                      const KeyExtractor& key_extractor,
                      const ReduceFunction& reduce_function,
                      size_t result_size,
                      const Value& neutral_element,
                      const ReduceConfig& config)
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() }),
          stream_(parent.ctx().GetNewCatStream()),
          emitters_(stream_->OpenWriters()),
          result_size_(result_size),

          pre_stage_(context_,
                     context_.num_workers(),
                     key_extractor, reduce_function, emitters_,
                     core::ReduceByIndex<Key>(0, result_size),
                     config.pre_table),

          post_stage_(
              context_, key_extractor, reduce_function,
              Emitter(this),
              core::ReduceByIndex<Key>(
                  // parameterize with resulting key range on this worker
                  pre_stage_.key_range(context_.my_rank())),
              neutral_element, config.post_table)
    {
        // Hook PreOp: Locally hash elements of the current DIA onto buckets and
        // reduce each bucket to a single value, afterwards send data to another
        // worker given by the shuffle algorithm.
        auto pre_op_fn = [this](const ValueType& input) {
                             return pre_stage_.Insert(input);
                         };

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void StartPreOp(size_t /* id */) final {
        pre_stage_.Initialize();
    }

    void StopPreOp(size_t /* id */) final {
        LOG << *this << " running main op";
        // Flush hash table before the postOp
        pre_stage_.FlushAll();
        pre_stage_.CloseAll();
        stream_->Close();
    }

    void Execute() final { }

    void PushData(bool consume) final {

        if (reduced_) {
            post_stage_.PushData(consume);
            return;
        }

        post_stage_.Initialize();

        if (RobustKey) {
            // we actually want to wire up callbacks in the ctor and NOT use
            // this blocking method
            auto reader = stream_->OpenCatReader(consume);
            sLOG << "reading data from" << stream_->id()
                 << "to push into post table which flushes to" << this->id();
            while (reader.HasNext()) {
                post_stage_.Insert(reader.template Next<Value>());
            }
        }
        else {
            // we actually want to wire up callbacks in the ctor and NOT use
            // this blocking method
            auto reader = stream_->OpenCatReader(consume);
            sLOG << "reading data from" << stream_->id()
                 << "to push into post table which flushes to" << this->id();
            while (reader.HasNext()) {
                post_stage_.Insert(reader.template Next<KeyValuePair>());
            }
        }

        reduced_ = true;
        post_stage_.PushData(consume);
    }

    void Dispose() final { }

private:
    data::CatStreamPtr stream_;

    std::vector<data::CatStream::Writer> emitters_;

    size_t result_size_;

    core::ReducePreProbingStage<
        ValueType, Key, Value, KeyExtractor, ReduceFunction, RobustKey,
        core::ReduceByIndex<Key>,
        decltype(ReduceConfig::pre_table),
        std::equal_to<Key> > pre_stage_;

    core::ReduceByIndexPostProbingStage<
        ValueType, Key, Value, KeyExtractor, ReduceFunction, Emitter, SendPair,
        core::ReduceByIndex<Key>,
        decltype(ReduceConfig::post_table),
        std::equal_to<Key> > post_stage_;

    bool reduced_ = false;
};

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction, typename ReduceConfig>
auto DIA<ValueType, Stack>::ReduceToIndexByKey(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function,
    size_t size,
    const ValueType &neutral_element,
    const ReduceConfig &reduce_config) const {
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

    using ReduceNode
              = ReduceToIndexNode<DOpResult, DIA,
                                  KeyExtractor, ReduceFunction, ReduceConfig,
                                  false, false>;

    auto shared_node
        = std::make_shared<ReduceNode>(
        *this, "ReduceToIndexByKey", key_extractor, reduce_function,
        size, neutral_element, reduce_config);

    return DIA<DOpResult>(shared_node);
}

template <typename ValueType, typename Stack>
template <typename ReduceFunction, typename ReduceConfig>
auto DIA<ValueType, Stack>::ReducePairToIndex(
    const ReduceFunction &reduce_function,
    size_t size,
    const typename common::FunctionTraits<ReduceFunction>::result_type &
    neutral_element,
    const ReduceConfig &reduce_config) const {
    assert(IsValid());

    using DOpResult
              = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(common::is_std_pair<ValueType>::value,
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

    using ReduceNode
              = ReduceToIndexNode<ValueType, DIA,
                                  std::function<Key(Key)>,
                                  ReduceFunction, ReduceConfig, false, true>;

    auto shared_node = std::make_shared<ReduceNode>(
        *this, "ReduceToPairIndex",
        [](const Key& /* key */) {
            // This function should not be called, it is only here to give the
            // key type to the hashtables.
            assert(1 == 0);
            return Key();
        },
        reduce_function, size, neutral_element, reduce_config);

    return DIA<ValueType>(shared_node);
}

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction, typename ReduceConfig>
auto DIA<ValueType, Stack>::ReduceToIndex(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function,
    size_t size,
    const ValueType &neutral_element,
    const ReduceConfig &reduce_config) const {
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

    using ReduceNode
              = ReduceToIndexNode<DOpResult, DIA,
                                  KeyExtractor, ReduceFunction, ReduceConfig,
                                  true, false>;

    auto shared_node = std::make_shared<ReduceNode>(
        *this, "ReduceToIndex", key_extractor, reduce_function,
        size, neutral_element, reduce_config);

    return DIA<DOpResult>(shared_node);
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_REDUCE_TO_INDEX_HEADER

/******************************************************************************/
