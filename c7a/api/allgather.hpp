/*******************************************************************************
 * c7a/api/allgather.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_ALLGATHER_HEADER
#define C7A_API_ALLGATHER_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/data/manager.hpp>

#include <string>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class AllGatherNode : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;
    using Super::result_file_;

    AllGatherNode(const ParentDIARef& parent,
                  std::vector<ValueType>* out_vector,
                  StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() }, "AllGather", stats_node),
          out_vector_(out_vector),
          channel_(parent.ctx().data_manager().GetNewChannel()),
          emitters_(channel_->OpenWriters())
    {
        auto pre_op_function = [=](ValueType input) {
                                   PreOp(input);
                               };

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_function).emit();
        parent.node()->RegisterChild(lop_chain);
    }

    void PreOp(ValueType element) {
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i](element);
        }
    }

    //! Closes the output file
    void Execute() override {
        this->StartExecutionTimer();
        //data has been pushed during pre-op -> close emitters
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i].Close();
        }

        auto reader = channel_->OpenReader();

        while (reader.HasNext()) {
            out_vector_->push_back(reader.template Next<ValueType>());
        }
        this->StopExecutionTimer();
    }

    void Dispose() override { }

    /*!
     * Returns "[AllGatherNode]" and its id as a string.
     * \return "[AllGatherNode]"
     */
    std::string ToString() override {
        return "[AllGatherNode] Id: " + result_file_.ToString();
    }

private:
    //! Vector pointer to write elements to.
    std::vector<ValueType>* out_vector_;

    data::ChannelPtr channel_;
    std::vector<data::BlockWriter> emitters_;

    static const bool debug = false;
};

template <typename ValueType, typename Stack>
std::vector<ValueType> DIARef<ValueType, Stack>::AllGather()  const {

    using AllGatherResultNode = AllGatherNode<ValueType, DIARef>;

    std::vector<ValueType> output;


    StatsNode* stats_node = AddChildStatsNode("AllGather", "Action");
    auto shared_node =
        std::make_shared<AllGatherResultNode>(*this, &output, stats_node);

    core::StageBuilder().RunScope(shared_node.get());

    return std::move(output);
}

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::AllGather(
    std::vector<ValueType>* out_vector)  const {

    using AllGatherResultNode = AllGatherNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("AllGather", "Action");
    auto shared_node =
        std::make_shared<AllGatherResultNode>(*this, out_vector, stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_ALLGATHER_HEADER

/******************************************************************************/
