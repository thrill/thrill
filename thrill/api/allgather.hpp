/*******************************************************************************
 * thrill/api/allgather.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
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

template <typename ValueType, typename ParentDIARef>
class AllGatherNode : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;

    AllGatherNode(const ParentDIARef& parent,
                  std::vector<ValueType>* out_vector,
                  StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, stats_node),
          out_vector_(out_vector),
          channel_(parent.ctx().GetNewChannel()),
          emitters_(channel_->OpenWriters())
    {
        auto pre_op_function = [=](const ValueType& input) {
                                   PreOp(input);
                               };

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_function).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    void PreOp(const ValueType& element) {
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i](element);
        }
    }

    //! Closes the output file
    void Execute() final {
        // data has been pushed during pre-op -> close emitters
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i].Close();
        }

        auto reader = channel_->OpenReader();

        while (reader.HasNext()) {
            out_vector_->push_back(reader.template Next<ValueType>());
        }

        this->WriteChannelStats(channel_);
    }

    void Dispose() final { }

private:
    //! Vector pointer to write elements to.
    std::vector<ValueType>* out_vector_;

    data::ChannelPtr channel_;
    std::vector<data::Channel::Writer> emitters_;

    static const bool debug = false;
};

template <typename ValueType, typename Stack>
std::vector<ValueType> DIARef<ValueType, Stack>::AllGather()  const {
    assert(IsValid());

    using AllGatherNode = api::AllGatherNode<ValueType, DIARef>;

    std::vector<ValueType> output;

    StatsNode* stats_node = AddChildStatsNode("AllGather", DIANodeType::ACTION);
    auto shared_node =
        std::make_shared<AllGatherNode>(*this, &output, stats_node);

    core::StageBuilder().RunScope(shared_node.get());

    return std::move(output);
}

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::AllGather(
    std::vector<ValueType>* out_vector)  const {
    assert(IsValid());

    using AllGatherNode = api::AllGatherNode<ValueType, DIARef>;

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
