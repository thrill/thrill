/*******************************************************************************
 * thrill/api/reduce.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_REDUCE_HEADER
#define THRILL_API_REDUCE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/meta.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/core/reduce_by_hash_post_stage.hpp>
#include <thrill/core/reduce_pre_stage.hpp>

#include <functional>
#include <string>
#include <thread>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

class DefaultReduceConfig : public core::DefaultReduceConfig
{ };

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
template <typename ValueType, typename ParentDIA,
          typename KeyExtractor, typename ReduceFunction,
          typename ReduceConfig,
          const bool RobustKey, const bool SendPair>
class ReduceNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using Value = typename common::FunctionTraits<ReduceFunction>::result_type;
    using KeyValuePair = std::pair<Key, Value>;

    using Output = typename common::If<RobustKey, Value, KeyValuePair>::type;

    static constexpr bool use_mix_stream_ = ReduceConfig::use_mix_stream_;
    static constexpr bool use_post_thread_ = ReduceConfig::use_post_thread_;

private:
    //! Emitter for PostStage to push elements to next DIA object.
    class Emitter
    {
    public:
        explicit Emitter(ReduceNode* node) : node_(node) { }
        void operator () (const ValueType& item) const
        { return node_->PushItem(item); }

    private:
        ReduceNode* node_;
    };

public:
    /*!
     * Constructor for a ReduceNode. Sets the parent, stack, key_extractor and
     * reduce_function.
     */
    ReduceNode(const ParentDIA& parent,
               const char* label,
               const KeyExtractor& key_extractor,
               const ReduceFunction& reduce_function,
               const ReduceConfig& config)
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() }),
          mix_stream_(use_mix_stream_ ?
                      parent.ctx().GetNewMixStream() : nullptr),
          cat_stream_(use_mix_stream_ ?
                      nullptr : parent.ctx().GetNewCatStream()),
          emitters_(use_mix_stream_ ?
                    mix_stream_->GetWriters() : cat_stream_->GetWriters()),
          pre_stage_(
              context_, parent.ctx().num_workers(),
              key_extractor, reduce_function, emitters_, config),
          post_stage_(
              context_, key_extractor, reduce_function,
              Emitter(this), config)
    {
        // Hook PreOp: Locally hash elements of the current DIA onto buckets and
        // reduce each bucket to a single value, afterwards send data to another
        // worker given by the shuffle algorithm.
        auto pre_op_fn = [this](const ValueType& input) {
                             return pre_stage_.Insert(input);
                         };
        // close the function stack with our pre op and register it at
        // parent node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    DIAMemUse PreOpMemUse() final {
        // request maximum RAM limit, the value is calculated by StageBuilder,
        // and set as DIABase::mem_limit_.
        return DIAMemUse::Max();
    }

    void StartPreOp(size_t /* id */) final {
        LOG << *this << " running StartPreOp";
        if (!use_post_thread_) {
            // use pre_stage without extra thread
            pre_stage_.Initialize(DIABase::mem_limit_);
        }
        else {
            pre_stage_.Initialize(DIABase::mem_limit_ / 2);
            post_stage_.Initialize(DIABase::mem_limit_ / 2);

            // start additional thread to receive from the channel
            thread_ = common::CreateThread([this] { ProcessChannel(); });
        }
    }

    void StopPreOp(size_t /* id */) final {
        LOG << *this << " running StopPreOp";
        // Flush hash table before the postOp
        pre_stage_.FlushAll();
        pre_stage_.CloseAll();
        // waiting for the additional thread to finish the reduce
        if (use_post_thread_) thread_.join();
        use_mix_stream_ ? mix_stream_->Close() : cat_stream_->Close();
    }

    void Execute() final { }

    DIAMemUse PushDataMemUse() final {
        return DIAMemUse::Max();
    }

    void PushData(bool consume) final {

        if (!use_post_thread_ && !reduced_) {
            // not final reduced, and no additional thread, perform post reduce
            post_stage_.Initialize(DIABase::mem_limit_);
            ProcessChannel();

            reduced_ = true;
        }
        post_stage_.PushData(consume);
    }

    //! process the inbound data in the post reduce stage
    void ProcessChannel() {
        if (use_mix_stream_)
        {
            auto reader = mix_stream_->GetMixReader(/* consume */ true);
            sLOG << "reading data from" << mix_stream_->id()
                 << "to push into post stage which flushes to" << this->id();
            while (reader.HasNext()) {
                post_stage_.Insert(reader.template Next<Output>());
            }
        }
        else
        {
            auto reader = cat_stream_->GetCatReader(/* consume */ true);
            sLOG << "reading data from" << cat_stream_->id()
                 << "to push into post stage which flushes to" << this->id();
            while (reader.HasNext()) {
                post_stage_.Insert(reader.template Next<Output>());
            }
        }
    }

    void Dispose() final {
        post_stage_.Dispose();
    }

private:
    // pointers for both Mix and CatStream. only one is used, the other costs
    // only a null pointer.
    data::MixStreamPtr mix_stream_;
    data::CatStreamPtr cat_stream_;

    std::vector<data::Stream::Writer> emitters_;

    //! handle to additional thread for post stage
    std::thread thread_;

    core::ReducePreStage<
        ValueType, Key, Value, KeyExtractor, ReduceFunction, RobustKey,
        ReduceConfig> pre_stage_;

    core::ReduceByHashPostStage<
        ValueType, Key, Value, KeyExtractor, ReduceFunction, Emitter, SendPair,
        ReduceConfig> post_stage_;

    bool reduced_ = false;
};

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction, typename ReduceConfig>
auto DIA<ValueType, Stack>::ReduceBy(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function,
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
            typename std::decay<typename common::FunctionTraits<KeyExtractor>
                                ::template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    using ReduceNode
              = api::ReduceNode<DOpResult, DIA, KeyExtractor, ReduceFunction,
                                ReduceConfig, true, false>;
    auto shared_node = std::make_shared<ReduceNode>(
        *this, "ReduceBy", key_extractor, reduce_function, reduce_config);

    return DIA<DOpResult>(shared_node);
}

template <typename ValueType, typename Stack>
template <typename ReduceFunction, typename ReduceConfig>
auto DIA<ValueType, Stack>::ReducePair(
    const ReduceFunction &reduce_function,
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

    using Key = typename ValueType::first_type;
    using Value = typename ValueType::second_type;

    using ReduceNode
              = api::ReduceNode<ValueType, DIA,
                                std::function<Key(Value)>, ReduceFunction,
                                ReduceConfig, false, true>;
    auto shared_node = std::make_shared<ReduceNode>(
        *this, "ReducePair", [](Value value) {
            // This function should not be
            // called, it is only here to
            // give the key type to the
            // hashtables.
            assert(1 == 0);
            value = value;
            return Key();
        },
        reduce_function, reduce_config);

    return DIA<ValueType>(shared_node);
}

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction, typename ReduceConfig>
auto DIA<ValueType, Stack>::ReduceByKey(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function,
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

    using ReduceNode
              = api::ReduceNode<DOpResult, DIA, KeyExtractor,
                                ReduceFunction, ReduceConfig, false, false>;
    auto shared_node = std::make_shared<ReduceNode>(
        *this, "ReduceByKey", key_extractor, reduce_function, reduce_config);

    return DIA<DOpResult>(shared_node);
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_REDUCE_HEADER

/******************************************************************************/
