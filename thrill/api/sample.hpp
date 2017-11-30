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
          sample_size_(sample_size), hyp_(42 /* dummy seed */),
          parent_stack_empty_(ParentDIA::stack_empty)
    {
        auto save_fn = [this](const ValueType& input) {
            writer_.Put(input);
        };
        auto lop_chain = parent.stack().push(save_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!parent_stack_empty_) {
            LOGC(common::g_debug_push_file)
                << "Sample rejected File from parent "
                << "due to non-empty function stack.";
            return false;
        }
        assert(file_.num_items() == 0);
        file_ = file.Copy();
        return true;
    }

    void StopPreOp(size_t /* id */) final {
        // Push local elements to children
        writer_.Close();
    }

    void Execute() final {
        local_timer_.Start();
        const size_t local_size = file_.num_items();
        sLOG << "SampleNode::Execute() processing" << local_size
             << "elements, sample size =" << sample_size_;

        const size_t my_rank = context_.my_rank();
        const size_t num_workers = context_.num_workers();

        if (num_workers == 1) {
            sample_size_ = std::min(sample_size_, local_size);
            local_samples_ = sample_size_;
            sLOG << "SampleNode::Execute (alone) => all"
                 << local_samples_ << "samples";
            return;
        }

        // + 1 for the seed to avoid an extra broadcast
        std::vector<size_t> input_sizes(num_workers + 1);
        input_sizes[my_rank] = local_size;
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
            local_samples_ = local_size;
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
        }

        local_samples_ = total;
        assert(local_size <= local_samples_);

        local_timer_.Stop();
        sLOG << "SampleNode::Execute"
             << local_samples_ << "of" << sample_size_ << "samples"
             << "communication time:" << comm_timer_.Microseconds() / 1000.0;
    }

    void PushData(bool consume) final {
        local_timer_.Start();
        samples_.clear();

        const size_t local_size = file_.num_items();
        if (local_samples_ == local_size) {
            LOG << "Sample: returning all local items";
            auto reader = file_.GetReader(consume);
            local_timer_.Stop(); // don't measure PushItem
            while (reader.HasNext()) {
                this->PushItem(reader.template Next<ValueType>());
            }
        } else {
            sLOG << "Drawing" << local_samples_ << "samples locally from"
                 << local_size << "input elements";
            common::ReservoirSamplingFast<ValueType> sampler(
                local_samples_, samples_, rng_);

            auto reader = file_.GetReader(consume);
            while (reader.HasNext()) {
                sampler.add(reader.template Next<ValueType>());
            }
            local_timer_.Stop(); // don't measure PushItem

            for (const ValueType& v : sampler.samples()) {
                this->PushItem(v);
            }
        }
        sLOG << "SampleNode::PushData finished; total local time excl PushData:"
             << local_timer_.Microseconds() / 1000.0
             << "ms, communication:" << comm_timer_.Microseconds() / 1000.0
             << "ms =" << comm_timer_.Microseconds() * 100.0 /
            (comm_timer_.Microseconds() + local_timer_.Microseconds())
             << "%";
    }

    void Dispose() final {
        file_.Clear();
    }

private:
    //! Local data file
    data::File file_ { context_.GetFile(this) };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ { file_.GetWriter() };
    //! number of samples to draw globally
    size_t sample_size_, local_samples_;
    //! local samples
    std::vector<ValueType> samples_;
    //! Hypergeometric distribution to calculate local sample sizes
    common::hypergeometric hyp_;
    //! Random generator for reservoir sampler
    std::default_random_engine rng_ { std::random_device { } () };
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
