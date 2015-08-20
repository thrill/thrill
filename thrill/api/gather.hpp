/*******************************************************************************
 * thrill/api/gather.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GATHER_HEADER
#define THRILL_API_GATHER_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/core/stage_builder.hpp>

#include <string>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class GatherNode : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;
    using Super::result_file_;

    GatherNode(const ParentDIARef& parent,
               size_t target_id,
               std::vector<ValueType>* out_vector,
               StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, "Gather", stats_node),
          target_id_(target_id),
          out_vector_(out_vector),
          channel_(parent.ctx().GetNewChannel()),
          emitters_(channel_->OpenWriters())
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
    }

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

    std::string ToString() final {
        return "[GatherNode] Id: " + result_file_.ToString();
    }

private:
    //! target worker id, which collects vector, all other workers do not get
    //! the data.
    size_t target_id_;
    //! Vector pointer to write elements to.
    std::vector<ValueType>* out_vector_;

    data::ChannelPtr channel_;
    std::vector<data::BlockWriter> emitters_;
};

/*!
 * Gather is an Action, which collects all data of the DIA into a vector at the
 * given worker. This should only be done if the received data can fit into RAM
 * of the one worker.
 */
template <typename ValueType, typename Stack>
std::vector<ValueType>
DIARef<ValueType, Stack>::Gather(size_t target_id) const {

    using GatherNode = api::GatherNode<ValueType, DIARef>;

    std::vector<ValueType> output;

    StatsNode* stats_node = AddChildStatsNode("Gather", NodeType::ACTION);
    auto shared_node =
        std::make_shared<GatherNode>(*this, target_id, &output, stats_node);

    core::StageBuilder().RunScope(shared_node.get());

    return std::move(output);
}

/*!
 * Gather is an Action, which collects all data of the DIA into a vector at the
 * given worker. This should only be done if the received data can fit into RAM
 * of the one worker.
 */
template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::Gather(
    size_t target_id, std::vector<ValueType>* out_vector)  const {

    using GatherNode = api::GatherNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("Gather", NodeType::ACTION);
    auto shared_node =
        std::make_shared<GatherNode>(*this, target_id, out_vector, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace c7a

#endif // !THRILL_API_GATHER_HEADER

/******************************************************************************/
