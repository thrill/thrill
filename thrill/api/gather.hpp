/*******************************************************************************
 * thrill/api/gather.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GATHER_HEADER
#define THRILL_API_GATHER_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>

#include <iostream>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class GatherNode final : public ActionResultNode<std::vector<ValueType> >
{
public:
    using Super = ActionResultNode<std::vector<ValueType> >;
    using Super::context_;

    template <typename ParentDIA>
    GatherNode(const ParentDIA& parent, const char* label,
               size_t target_id,
               std::vector<ValueType>* out_vector)
        : Super(parent.ctx(), label,
                { parent.id() }, { parent.node() }),
          target_id_(target_id),
          out_vector_(out_vector) {
        assert(target_id_ < context_.num_workers());

        auto pre_op_fn = [this](const ValueType& input) {
                             emitters_[target_id_].Put(input);
                         };

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    void StartPreOp(size_t /* parent_index */) final {
        emitters_ = stream_->GetWriters();

        // close all but the target
        for (size_t i = 0; i < emitters_.size(); i++) {
            if (i == target_id_) continue;
            emitters_[i].Close();
        }
    }

    void StopPreOp(size_t /* parent_index */) final {
        emitters_[target_id_].Close();
    }

    void Execute() final {
        auto reader = stream_->GetCatReader(true /* consume */);

        while (reader.HasNext()) {
            out_vector_->push_back(reader.template Next<ValueType>());
        }
    }

    const std::vector<ValueType>& result() const final {
        return *out_vector_;
    }

private:
    //! target worker id, which collects vector, all other workers do not get
    //! the data.
    size_t target_id_;
    //! Vector pointer to write elements to.
    std::vector<ValueType>* out_vector_;

    data::CatStreamPtr stream_ { context_.GetNewCatStream(this) };
    data::CatStream::Writers emitters_;
};

template <typename ValueType, typename Stack>
std::vector<ValueType>
DIA<ValueType, Stack>::Gather(size_t target_id) const {
    assert(IsValid());

    using GatherNode = api::GatherNode<ValueType>;

    std::vector<ValueType> output;

    auto node = tlx::make_counting<GatherNode>(
        *this, "Gather", target_id, &output);

    node->RunScope();

    return output;
}

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::Gather(
    size_t target_id, std::vector<ValueType>* out_vector) const {
    assert(IsValid());

    using GatherNode = api::GatherNode<ValueType>;

    auto node = tlx::make_counting<GatherNode>(
        *this, "Gather", target_id, out_vector);

    node->RunScope();
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GATHER_HEADER

/******************************************************************************/
