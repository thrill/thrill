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
template <typename ValueType>
class SampleNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    template <typename ParentDIA>
    SampleNode(const ParentDIA& parent, size_t sample_size)
        : Super(parent.ctx(), "Sample", { parent.id() }, { parent.node() }),
          sample_size_(sample_size), count_(0)
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

    // This implements J. Vitter's Algorithm R for reservoir sampling
    void PreOp(const ValueType& input) {
        ++count_;
        if (samples_.size() < sample_size_) {
            samples_.push_back(input);
        }
        else {
            size_t pos = rng_() % count_;
            if (pos < sample_size_) {
                samples_[pos] = input;
            }
        }
    }

    void Execute() final {

        size_t local_size = samples_.size();

        size_t local_rank = local_size;
        size_t global_size = context_.net.ExPrefixSumTotal(local_rank);

        // not enough items to discard some, done.
        if (global_size < sample_size_) return;

        // synchronize global random generator
        size_t seed = context_.my_rank() == 0 ? rng_() : 0;
        seed = context_.net.Broadcast(seed);
        rng_.seed(static_cast<unsigned>(seed));

        // globally select random samples among samples_
        typename std::vector<ValueType>::iterator it = samples_.begin();

        // XXX THIS DOES NOT PRODUCE A RANDOM SAMPLE! SAMPLING THE LOCAL SAMPLES
        // ISN'T A CORRECT SAMPLING ALGORITHM! THIS NEEDS TO USE HYPERGEOMETRIC
        // DEVIATES TO DETERMINE BOUNDARIES (ALSO DEPENDANT ON count_)
        for (size_t i = 0; i < sample_size_; ++i) {
            size_t r = rng_() % global_size;
            if (r < local_rank || r >= local_rank + local_size) continue;

            // swap selected item to front. WTF NO THIS IS WRONG
            using std::swap;
            if (it < samples_.end()) {
                swap(*it, samples_[r - local_rank]);
            }
            else {
                it = samples_.insert(it, samples_[r - local_rank]);
            }
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
    //! Size of the sample
    size_t sample_size_;

    //! Number of values seen so far
    size_t count_;

    //! local samples
    std::vector<ValueType> samples_;

    //! Random generator for eviction
    std::default_random_engine rng_ { std::random_device { } () };
};

template <typename ValueType, typename Stack>
auto DIA<ValueType, Stack>::Sample(size_t sample_size) const {
    assert(IsValid());

    using SampleNode = api::SampleNode<ValueType>;

    auto node = tlx::make_counting<SampleNode>(
        *this, sample_size);

    return DIA<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SAMPLE_HEADER

/******************************************************************************/
