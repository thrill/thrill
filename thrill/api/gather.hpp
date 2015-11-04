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
#include <thrill/core/stage_builder.hpp>

#include <iostream>
#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ParentDIA>
class GatherNode final : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;

    //! input and output type is the parent's output value type.
    using ValueType = typename ParentDIA::ValueType;

    GatherNode(const ParentDIA& parent,
               size_t target_id,
               std::vector<ValueType>* out_vector,
               StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, stats_node),
          target_id_(target_id),
          out_vector_(out_vector),
          stream_(parent.ctx().GetNewCatStream()),
          emitters_(stream_->OpenWriters())
    {
        assert(target_id_ < context_.num_workers());

        auto pre_op_function =
            [=](const ValueType& input) {
                emitters_[target_id_](input);
            };

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_function).emit();
        parent.node()->RegisterChild(lop_chain, this->type());

        // close all but the target
        for (size_t i = 0; i < emitters_.size(); i++) {
            if (i == target_id_) continue;
            emitters_[i].Close();
        }
    }

    void StopPreOp(size_t /* id */) final {
        emitters_[target_id_].Close();
    }

    void Execute() final {
        auto reader = stream_->OpenCatReader(true /* consume */);

        while (reader.HasNext()) {
            out_vector_->push_back(reader.template Next<ValueType>());
        }

        this->WriteStreamStats(stream_);
    }

    void Dispose() final { }

private:
    //! target worker id, which collects vector, all other workers do not get
    //! the data.
    size_t target_id_;
    //! Vector pointer to write elements to.
    std::vector<ValueType>* out_vector_;

    data::CatStreamPtr stream_;
    std::vector<data::CatStream::Writer> emitters_;
};

template <typename ValueType, typename Stack>
std::vector<ValueType>
DIA<ValueType, Stack>::Gather(size_t target_id) const {
    assert(IsValid());

    using GatherNode = api::GatherNode<DIA>;

    std::vector<ValueType> output;

    StatsNode* stats_node = AddChildStatsNode("Gather", DIANodeType::ACTION);
    auto shared_node =
        std::make_shared<GatherNode>(*this, target_id, &output, stats_node);

    core::StageBuilder().RunScope(shared_node.get());

    return std::move(output);
}

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::Gather(
    size_t target_id, std::vector<ValueType>* out_vector) const {
    assert(IsValid());

    using GatherNode = api::GatherNode<DIA>;

    StatsNode* stats_node = AddChildStatsNode("Gather", DIANodeType::ACTION);
    auto shared_node =
        std::make_shared<GatherNode>(*this, target_id, out_vector, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::Print(const std::string& name, std::ostream& os) const {
    assert(IsValid());

    using GatherNode = api::GatherNode<DIA>;

    std::vector<ValueType> output;

    StatsNode* stats_node = AddChildStatsNode("Print", DIANodeType::ACTION);
    auto shared_node =
        std::make_shared<GatherNode>(*this, 0, &output, stats_node);

    core::StageBuilder().RunScope(shared_node.get());

    if (shared_node->context().my_rank() == 0)
    {
        os << name
           << " --- Begin DIA.Print() --- size=" << output.size() << '\n';
        for (size_t i = 0; i < output.size(); ++i) {
            os << name << '[' << i << "]: " << output[i] << '\n';
        }
        os << name
           << " --- End DIA.Print() --- size=" << output.size() << std::endl;
    }
}

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::Print(const std::string& name) const {
    return Print(name, std::cout);
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GATHER_HEADER

/******************************************************************************/
