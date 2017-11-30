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
 * \ingroup api_layer
 */
template <typename ValueType>
class SampleNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;
    static constexpr bool verbose = false;

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
        auto save_fn = [this](const ValueType& input) {
            ++local_size_;
            sampler_.add(input);
        };
        auto lop_chain = parent.stack().push(save_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void Execute() final {
        local_timer_.Start();
        sLOG << "SampleNode::Execute() processing" << local_size_
             << "elements of which" << samples_.size()
             << "were presampled, global sample size =" << sample_size_;

        const size_t my_rank = context_.my_rank();
        const size_t num_workers = context_.num_workers();

        if (num_workers == 1) {
            sample_size_ = std::min(sample_size_, local_size_);
            local_samples_ = sample_size_;
            sLOG << "SampleNode::Execute (alone) => all"
                 << local_samples_ << "samples";
            return;
        }

        // + 1 for the seed to avoid an extra broadcast
        std::vector<size_t> input_sizes(num_workers + 1);
        input_sizes[my_rank] = local_size_;
        if (context_.my_rank() == 0) { // set the seed
            input_sizes[num_workers] = std::random_device{}();
        }
        // there's no allgather... fake it with a vector allreduce
        local_timer_.Stop(), comm_timer_.Start();
        input_sizes = context_.net.AllReduce(
            input_sizes, common::ComponentSum<decltype(input_sizes)>());
        comm_timer_.Stop(), local_timer_.Start();

        // Extract the seed
        const size_t seed = input_sizes[num_workers];

        // Build size tree (complete binary tree in level order, so that the
        // children of node i are at positions 2i+1 and 2i+2)
        const size_t num_leaves = tlx::round_up_to_power_of_two(num_workers);
        const size_t height = tlx::integer_log2_ceil(num_workers);
        std::vector<size_t> tree(2 * num_leaves - 1);
        // Fill leaf level with input sizes
        for (size_t i = 0; i < num_workers; ++i) {
            tree[num_leaves + i - 1] = input_sizes[i];
        }
        for (size_t level = 0; level < height; ++level) {
            const size_t min = num_leaves / (1ULL << (level + 1)) - 1;
            for (size_t i = min; i <= 2 * min; ++i) {
                tree[i] = tree[2 * i + 1] + tree[2 * i + 2];
            }
        }

        LOGC(verbose) << "Input sizes: " << input_sizes;
        LOGC(verbose) << "Tree: " << tree;

        if (/* total input size = */ tree[0] < sample_size_) {
            // more samples requested than there are elements
            sample_size_ = tree[0];
            local_samples_ = local_size_;
            sLOG << "SampleNode::Execute (underfull)"
                 << local_samples_ << "of" << sample_size_ << "samples";
            return;
        }

        // total = #samples to originate from current subtree,
        // index = current index in tree array
        size_t total = sample_size_, index = 0;
        for (size_t level = 0; level < height; ++level) {
            // get hypergeometric deviate. the sequence of calls is exactly the
            // same up to two workers' LCA in the binary tree above all workers,
            // i.e. until they no longer depend on each other
            hyp_.seed(seed + index);
            size_t left = hyp_(tree[2 * index + 1], tree[2 * index + 2], total);
            // descend into correct branch
            const bool go_right = my_rank & (1ULL << (height - level - 1));

            sLOGC(verbose)
                << "Level" << level << "distributing" << total << "samples,"
                << "tree index:" << index
                << "left subtree size:" << tree[2 * index + 1]
                << "right:" << tree[2 * index + 2]
                << "=> samples from left:" << left
                << "going" << (go_right ? "right" : "left");

            if (go_right) {
                index = 2 * index + 2;
                total = total - left;
            } else {
                index = 2 * index + 1;
                total = left;
            }
            if (total == 0) break;
        }

        local_samples_ = total;
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
        local_timer_.Start();

        sLOGC(local_samples_ > samples_.size())
            << "WTF ERROR CAN'T DRAW" << local_samples_ << "FROM"
            << samples_.size() << "PRESAMPLES";

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
        local_timer_.Stop(); // don't measure PushItem

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
