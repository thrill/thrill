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
        LOG << "SampleNode::Execute() processing";
        const size_t local_size = file_.num_items();
        sLOG << "local_size" << local_size;

        const size_t my_rank = context_.my_rank();
        const size_t num_workers = context_.num_workers();

        if (num_workers == 1) {
            sample_size_ = std::min(sample_size_, local_size);
            local_samples_ = sample_size_;
            sLOG << "SampleNode::Execute (alone)"
                 << local_samples_ << "of" << sample_size_ << "samples";

            return;
        }

        std::vector<size_t> input_sizes(num_workers);
        input_sizes[my_rank] = local_size;
        // there's no group::all_gather... do it totally naively
        for (size_t i = 0; i < num_workers; ++i) {
            input_sizes[i] = context_.net.Broadcast(input_sizes[i], i);
        }

        // Determine and broadcast seed
        size_t seed = 0;
        if (context_.my_rank() == 0) {
            seed = std::random_device{}();
        }
        seed = context_.net.Broadcast(seed, 0);
        // Seed hypergeometric distribution with the same value on each PE
        hyp_.seed(seed);

        // Build size tree
        const size_t num_leaves = tlx::round_up_to_power_of_two(num_workers);
        std::vector<size_t> tree(2 * num_leaves - 1);
        for (size_t i = 0; i < num_workers; ++i) {
            tree[num_leaves + i - 1] = input_sizes[i];
        }
        const size_t height = tlx::integer_log2_ceil(num_workers);
        for (size_t i = 0; i < height; ++i) {
            const size_t min = num_leaves / (1ULL << (i + 1)) - 1;
            for (size_t j = min; j <= 2 * min; ++j) {
                tree[j] = tree[2 * j + 1] + tree[2 * j + 2];
            }
        }

        LOG << "Input sizes: " << input_sizes;
        LOG << "Tree: " << tree;

        if (tree[0] < sample_size_) {
            // more samples requested than there are elements
            sample_size_ = tree[0];
            local_samples_ = local_size;
            sLOG << "SampleNode::Execute (underfull)"
                 << local_samples_ << "of" << sample_size_ << "samples";
            return;
        }

        size_t total = sample_size_, base = 1;
        for (size_t i = 0; i < height; ++i) {
            // get hypergeometric deviate. the sequence of calls is exactly the
            // same up to two workers' LCA in the binary tree above all workers,
            // i.e. until they no longer depend on each other
            hyp_.seed(seed + base);
            size_t left = hyp_(tree[base], tree[base + 1], total);
            // descend into correct branch
            const bool go_right = my_rank & (1ULL << (height - i - 1));

            sLOG << "Level" << i << "distributing" << total << "samples,"
                 << "base index:" << base
                 << "left size:" << tree[base] << "right:" << tree[base + 1]
                 << "result for left:" << left
                 << "going" << (go_right ? "right" : "left");

            if (go_right) {
                base = 2 * base + 3;
                total = total - left;
            } else {
                base = 2 * base + 1;
                total = left;
            }
        }

        local_samples_ = total;

        if (local_samples_ > local_size) {
            sLOG1 << "Sample error: need" << local_samples_ << ">"
                  << local_size << "(=available) elements from this worker";
        }

        sLOG << "SampleNode::Execute"
             << local_samples_ << "of" << sample_size_ << "samples";
    }

    void PushData(bool consume) final {
        samples_.clear();

        const size_t local_size = file_.num_items();
        if (local_samples_ == local_size) {
            LOG << "Sample: returning all local items";
            auto reader = file_.GetReader(consume);
            while (reader.HasNext()) {
                this->PushItem(reader.template Next<ValueType>());
            }
            return;
        }

        sLOG << "Drawing" << local_samples_ << "locally from" << local_size;
        common::ReservoirSamplingFast<ValueType> sampler(
            local_samples_, samples_, rng_);

        auto reader = file_.GetReader(consume);
        while (reader.HasNext()) {
            sampler.add(reader.template Next<ValueType>());
        }

        for (const ValueType& v : sampler.samples()) {
            this->PushItem(v);
        }
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
    //! Random generator for eviction
    std::default_random_engine rng_ { std::random_device { } () };
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
