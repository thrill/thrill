/*******************************************************************************
 * thrill/api/sample.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SAMPLE_HEADER
#define THRILL_API_SAMPLE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType, typename ParentDIA>
class SampleNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    SampleNode(const ParentDIA& parent, size_t sample_size)
        : Super(parent.ctx(), "Sample", { parent.id() }, { parent.node() }),
          sample_size_(sample_size)
    {
        samples_.reserve(sample_size);

        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    DIAMemUse PreOpMemUse() final {
        return sample_size_ * sizeof(ValueType);
    }

    void PreOp(const ValueType& input) {
        if (samples_.size() < sample_size_) {
            samples_.emplace_back(input);
        }
        else {
            samples_[rng_() % samples_.size()] = input;
        }
    }

    void Execute() final {

        size_t global_size = context_.net.AllReduce(samples_.size());

        // not enough items to discard some, done.
        if (global_size < sample_size_) return;

        size_t local_rank = context_.net.ExPrefixSum(samples_.size());

        // synchronize global random generator
        size_t seed = context_.my_rank() == 0 ? rng_() : 0;
        seed = context_.net.Broadcast(seed);
        rng_.seed(static_cast<unsigned>(seed));

        // globally select random samples among samples_
        typename std::vector<ValueType>::iterator it = samples_.begin();

        for (size_t i = 0; i < sample_size_; ++i) {
            size_t r = rng_() % sample_size_;
            if (r < local_rank || r >= local_rank + samples_.size()) continue;

            // swap selected item to front.
            using std::swap;
            swap(*it, samples_[r - local_rank]);
            ++it;
        }

        samples_.erase(it, samples_.end());

        sLOG << "SampleNode::Execute"
             << "global_size" << global_size
             << "local_rank" << local_rank
             << "AllReduce" << context_.net.AllReduce(samples_.size());

        assert(sample_size_ == context_.net.AllReduce(samples_.size()));
    }

    void PushData(bool consume) final {
        for (const ValueType& v : samples_) {
            this->PushItem(v);
        }
        if (consume)
            std::vector<ValueType>().swap(samples_);
    }

    void Dispose() final {
        std::vector<ValueType>().swap(samples_);
    }

private:
    size_t sample_size_;

    //! local samples
    std::vector<ValueType> samples_;

    //! Random generator for eviction
    std::default_random_engine rng_ { std::random_device { } () };
};

template <typename ValueType, typename Stack>
auto DIA<ValueType, Stack>::Sample(size_t sample_size) const {
    assert(IsValid());

    using SampleNode
              = api::SampleNode<ValueType, DIA>;

    auto shared_node
        = std::make_shared<SampleNode>(*this, sample_size);

    return DIA<ValueType>(shared_node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SAMPLE_HEADER

/******************************************************************************/
