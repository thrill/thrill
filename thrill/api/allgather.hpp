/*******************************************************************************
 * thrill/api/allgather.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_ALLGATHER_HEADER
#define THRILL_API_ALLGATHER_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/stage_builder.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ParentDIA>
class AllGatherNode final : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;

    //! input and output type is the parent's output value type.
    using ValueType = typename ParentDIA::ValueType;

    AllGatherNode(const ParentDIA& parent,
                  std::vector<ValueType>* out_vector,
                  StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, stats_node),
          out_vector_(out_vector),
          stream_(parent.ctx().GetNewCatStream()),
          emitters_(stream_->OpenWriters())
    {
        auto pre_op_function = [=](const ValueType& input) {
                                   PreOp(input);
                               };

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_function).emit();
        parent.node()->AddChild(this, lop_chain);
    }

    void PreOp(const ValueType& element) {
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i].Put(element);
        }
    }

    void StopPreOp(size_t /* id */) final {
        // data has been pushed during pre-op -> close emitters
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i].Close();
        }
    }

    //! Closes the output file
    void Execute() final {

        bool consume = false;
        auto reader = stream_->OpenCatReader(consume);

        while (reader.HasNext()) {
            out_vector_->push_back(reader.template Next<ValueType>());
        }

        this->WriteStreamStats(stream_);
    }

    void Dispose() final { }

private:
    //! Vector pointer to write elements to.
    std::vector<ValueType>* out_vector_;

    data::CatStreamPtr stream_;
    std::vector<data::CatStream::Writer> emitters_;

    static const bool debug = false;
};

template <typename ValueType, typename Stack>
std::vector<ValueType> DIA<ValueType, Stack>::AllGather()  const {
    assert(IsValid());

    using AllGatherNode = api::AllGatherNode<DIA>;

    std::vector<ValueType> output;

    StatsNode* stats_node = AddChildStatsNode("AllGather", DIANodeType::ACTION);
    auto shared_node =
        std::make_shared<AllGatherNode>(*this, &output, stats_node);

    core::StageBuilder().RunScope(shared_node.get());

    return output;
}

template <typename ValueType, typename Stack>
void DIA<ValueType, Stack>::AllGather(
    std::vector<ValueType>* out_vector)  const {
    assert(IsValid());

    using AllGatherNode = api::AllGatherNode<DIA>;

    StatsNode* stats_node = AddChildStatsNode("AllGather", DIANodeType::ACTION);
    auto shared_node =
        std::make_shared<AllGatherNode>(*this, out_vector, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_ALLGATHER_HEADER

/******************************************************************************/
