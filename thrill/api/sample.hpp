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
#include <thrill/common/hypergeometric_distribution.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/sampling.hpp>
#include <thrill/common/reservoir_sampling.hpp>

#include <tlx/math.hpp>

#include <algorithm>
#include <cassert>
#include <vector>

namespace thrill {
namespace api {

/*!
 * A DIANode which performs sampling *without* replacement.
 *
 * The implementation is an adaptation of Algorithm P from Sanders, Lamm,
 * Hübschle-Schneider, Schrade, Dachsbacher, ACM TOMS 2017: "Efficient Random
 * Sampling - Parallel, Vectorized, Cache-Efficient, and Online".  The
 * modification is in how samples are assigned to workers.  Instead of doing
 * log(num_workers) splits to assign samples to ranges of workers, do
 * O(log(input_size)) splits to assign samples to input ranges.  Workers only
 * compute the ranges which overlap their local input range, and then add up the
 * ranges that are fully contained in their local input range.  This ensures
 * consistency while requiring only a single prefix-sum and two scalar
 * broadcasts.
 *
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
          local_size_(0), sample_size_(sample_size), local_samples_(0),
          hyp_(42 /* dummy seed */),
          sampler_(sample_size, samples_, rng_),
          parent_stack_empty_(ParentDIA::stack_empty)
    {
        auto presample_fn = [this](const ValueType& input) {
            sampler_.add(input);
        };
        auto lop_chain = parent.stack().push(presample_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void Execute() final {
        local_timer_.Start();
        local_size_ = sampler_.count();
        sLOG << "SampleNode::Execute() processing" << local_size_
             << "elements of which" << samples_.size()
             << "were presampled, global sample size =" << sample_size_;

        if (context_.num_workers() == 1) {
            sample_size_ = std::min(sample_size_, local_size_);
            local_samples_ = sample_size_;
            sLOG << "SampleNode::Execute (alone) => all"
                 << local_samples_ << "samples";
            return;
        }

        // Compute number of input elements left of self and total input size
        size_t local_rank = local_size_;
        size_t global_size = context_.net.ExPrefixSumTotal(local_rank);

        if (global_size <= sample_size_) {
            // Requested sample is larger than the number of elements,
            // return everything
            assert(samples_.size() == local_size_);
            local_samples_ = local_size_;
            sLOG << "SampleNode::Execute (underfull)"
                 << local_samples_ << "of" << sample_size_ << "samples";
            return;
        }

        // Determine and broadcast seed
        size_t seed = 0;
        if (context_.my_rank() == 0) {
            seed = std::random_device{}();
        }
        local_timer_.Stop(), comm_timer_.Start();
        seed = context_.net.Broadcast(seed);
        comm_timer_.Stop(), local_timer_.Start();

        // Calculate number of local samples by recursively splitting the range
        // considered in half and assigning samples there
        local_samples_ = calc_local_samples(
            local_rank, local_rank + local_size_,
            0, global_size, sample_size_, seed);

        assert(local_samples_ <= local_size_);
        assert(local_samples_ <= samples_.size());

        local_timer_.Stop();
        sLOG << "SampleNode::Execute"
             << local_samples_ << "of" << sample_size_ << "samples"
             << "(got" << local_size_ << "=>"
             << samples_.size() << "elements),"
             << "communication time:" << comm_timer_.Microseconds() / 1000.0;
    }

    void PushData(bool consume) final {
        // don't start global timer in pushdata!
        common::StatsTimerStart push_timer;

        sLOGC(local_samples_ > samples_.size())
            << "WTF ERROR CAN'T DRAW" << local_samples_ << "FROM"
            << samples_.size() << "PRESAMPLES";

        // Most likely, we'll need to draw the requested number of samples from
        // the presample that we computed in the PreOp
        if (local_samples_ < samples_.size()) {
            sLOG << "Drawing" << local_samples_ << "samples locally from"
                 << samples_.size() << "pre-samples";
            std::vector<ValueType> subsample;
            common::Sampling<> subsampler(rng_);
            subsampler(samples_.begin(), samples_.end(),
                       local_samples_, subsample);
            samples_.swap(subsample);
            LOGC(samples_.size() != local_samples_)
                << "ERROR: SAMPLE SIZE IS WRONG";
        }
        push_timer.Stop(); // don't measure PushItem
        local_timer_ += push_timer;

        for (const ValueType& v : samples_) {
            this->PushItem(v);
        }
        if (consume)
            std::vector<ValueType>().swap(samples_);

        sLOG << "SampleNode::PushData finished; total local time excl PushData:"
             << local_timer_.Microseconds() / 1000.0
             << "ms, communication:" << comm_timer_.Microseconds() / 1000.0
             << "ms =" << comm_timer_.Microseconds() * 100.0 /
            (comm_timer_.Microseconds() + local_timer_.Microseconds())
             << "%";
    }

    void Dispose() final {
        std::vector<ValueType>().swap(samples_);
    }

private:
    size_t hash_combine(size_t seed, size_t v) {
        // technically v needs to be hashed...
        return seed ^ (v + 0x9e3779b9 + (seed << 6) + (seed >> 2));
    }

    // ranges are exclusive, like iterator begin / end
    size_t calc_local_samples(size_t my_begin, size_t my_end,
                              size_t range_begin, size_t range_end,
                              size_t sample_size, size_t seed) {
        // handle empty ranges and case without any samples
        if (range_begin >= range_end) return 0;
        if (my_begin >= my_end) return 0;
        if (sample_size == 0) return 0;

        // is the range contained in my part? then all samples are mine
        if (my_begin <= range_begin && range_end <= my_end) {
            LOG << "my range [" << my_begin << ", " << my_end
                << ") is contained in the currently considered range ["
                << range_begin << ", " << range_end << ") and thus gets all "
                << sample_size << " samples";
            return sample_size;
        }

        // does my range overlap the global range?
        if ((range_begin <= my_begin && my_begin <  range_end) ||
            (range_begin <  my_end   && my_end   <= range_end)) {

            // seed the distribution so that all PEs generate the same values in
            // the same subtrees, but different values in different subtrees
            size_t new_seed = hash_combine(hash_combine(seed, range_begin),
                                           range_end);
            hyp_.seed(new_seed);

            const size_t left_size = (range_end - range_begin) / 2,
                right_size = (range_end - range_begin) - left_size;
            const size_t left_samples = hyp_(left_size, right_size, sample_size);

            LOG << "my range [" << my_begin << ", " << my_end
                << ") overlaps the currently considered range ["
                << range_begin << ", " << range_end << "), splitting: "
                << "left range [" << range_begin << ", " << range_begin + left_size
                << ") gets " << left_samples << " samples, right range ["
                << range_begin + left_size << ", " << range_end << ") the remaining "
                << sample_size - left_samples << " for a total of " << sample_size
                << " samples";

            const size_t
                left_result = calc_local_samples(
                    my_begin, my_end, range_begin, range_begin + left_size,
                    left_samples, seed),
                right_result = calc_local_samples(
                    my_begin, my_end, range_begin + left_size, range_end,
                    sample_size - left_samples, seed);
            return left_result + right_result;
        }

        // no overlap
        return 0;
    }


    //! local input size, number of samples to draw globally, and locally
    size_t local_size_, sample_size_, local_samples_;
    //! local samples
    std::vector<ValueType> samples_;
    //! Hypergeometric distribution to calculate local sample sizes
    common::hypergeometric hyp_;
    //! Random generator for reservoir sampler
    std::mt19937 rng_ { std::random_device { } () };
    //! Reservoir sampler for pre-op
    common::ReservoirSamplingFast<ValueType, decltype(rng_)> sampler_;
    //! Timers for local work and communication
    common::StatsTimerStopped local_timer_, comm_timer_;
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;

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
