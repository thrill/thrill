/*******************************************************************************
 * thrill/api/bernoulli_sample.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016-2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_BERNOULLI_SAMPLE_HEADER
#define THRILL_API_BERNOULLI_SAMPLE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/data/serialization.hpp>

#include <random>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class BernoulliSampleNode final : public DOpNode<ValueType>
{
    static const bool debug = false;

    using SkipDistValueType = int;

    using Super = DOpNode<ValueType>;
    using Super::context_;
public:
    template <typename ParentDIA>
    BernoulliSampleNode(const ParentDIA& parent, double p)
        : Super(parent.ctx(), "BernoulliSample", { parent.id() }, {parent.node() }),
          engine_(std::random_device { } ()),
          skip_dist_(p),
          p_(p)
    {
        assert(p >= 0.0 && p <= 1.0);

        skip_remaining_ = skip_dist_(engine_);
        LOG << "Skip value initialised with " << skip_remaining_ << ", p=" << p;

        // Hook PreOp(s)
        auto pre_op_fn = [this](const ValueType& input) {
            PreOp(input);
        };

        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    DIAMemUse PreOpMemUse() final {
        return samples_.size() * sizeof(ValueType);
    }

    void PreOp (const ValueType& item) {
        // use geometric distribution and skip values
        if (skip_remaining_ == 0) {
            // sample element
            LOG << "sampled item " << item;
            samples_.push_back(item);
            skip_remaining_ = skip_dist_(engine_);
        }
        else {
            --skip_remaining_;
        }
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        LOG << "Sampling file of size " << file.num_items() << " with p=" << p_;

        if (p_ == 0.0) { return true; }

        auto reader = file.GetKeepReader();
        const size_t file_size = file.num_items();
        size_t pos = 0;

        if (p_ == 1.0) {
            // degenerate case, sample entire file
            LOG << "Degenerate: p = 1";
            samples_.reserve(file_size);
            for (; pos < file_size; ++pos) {
                assert(reader.HasNext());
                samples_.push_back(reader.Next<ValueType>());
            }
            return true;
        }

        // Use reader.Skip if its data has a constant size
        if (data::Serialization<decltype(reader), ValueType>::is_fixed_size) {
            LOG << "Sampler using FAST path (reader.Skip)";
            // fetch a Block to get typecode_verify flag
            reader.HasNext();
            const size_t bytes_per_item =
                (reader.typecode_verify() ? sizeof(size_t) : 0)
                + data::Serialization<decltype(reader), ValueType>::fixed_size;

            pos = skip_remaining_;
            while (pos < file_size) {
                reader.Skip(skip_remaining_, skip_remaining_ * bytes_per_item);
                assert(reader.HasNext());
                samples_.push_back(reader.Next<ValueType>());
                skip_remaining_ = skip_dist_(engine_);
                pos += skip_remaining_ + 1;  // +1 because we just read one item
            }

        } else {
            LOG << "Sampler using SLOW path (advance reader one-by-one)";
            assert(skip_remaining_ >= 0);
            // Skip items at the beginning of the file
            for( ; pos < static_cast<size_t>(skip_remaining_) &&
                     pos < file_size; ++pos) {
                reader.Next<ValueType>();
            }
            while (pos < file_size) {
                assert(reader.HasNext());
                samples_.push_back(reader.Next<ValueType>());
                pos++;
                auto next = pos + skip_dist_(engine_);
                if (next >= file_size) {
                    LOG << "Aborting: next=" << next << " pos=" << pos
                        << " file_size=" << file_size;
                    pos = next; // ensure we skip_remaining_ correctly
                    break;
                }
                for (; pos < next; ++pos) {
                    assert(reader.HasNext());
                    reader.Next<ValueType>();
                }
            }
            skip_remaining_ = pos - file_size;
        }
        return true;
    }

    void Execute() final {
        LOG << "Sampled " << samples_.size() << " elements!";
        // nothing to do
    }

    void PushData(bool consume) final {
        for (const ValueType& v : samples_) {
            this->PushItem(v);
        }
        if (consume) {
            std::vector<ValueType>().swap(samples_);
        }
    }

    void Dispose() final {
        std::vector<ValueType>().swap(samples_);
    }

private:
    std::vector<ValueType> samples_;
    // Random generator
    std::default_random_engine engine_;
    // Skip Value Distribution
    std::geometric_distribution<SkipDistValueType> skip_dist_;
    // Sampling rate
    const double p_;
    // Remaining skip value
    SkipDistValueType skip_remaining_ = -1;
};

template <typename ValueType, typename Stack>
auto DIA<ValueType, Stack>::BernoulliSample(const double p) const {
    assert(IsValid());

    using BernoulliSampleNode = api::BernoulliSampleNode<ValueType>;

    auto node = common::MakeCounting<BernoulliSampleNode>(
        *this, p);

    return DIA<ValueType>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_BERNOULLI_SAMPLE_HEADER

/******************************************************************************/
