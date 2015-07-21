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
#include <c7a/api/dia_node.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/data/manager.hpp>
#include <c7a/net/collective_communication.hpp>

#include <string>
#include <vector>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentStack>
class AllGatherNode : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;
    using Super::result_file_;

    using ParentInput = typename ParentStack::Input;

    AllGatherNode(Context& ctx,
                  const std::shared_ptr<DIANode<ParentInput> >& parent,
                  const ParentStack& parent_stack,
                  std::vector<ValueType>* out_vector
                  )
        : ActionNode(ctx, { parent }, "AllGather"),
          out_vector_(out_vector),
          channel_(ctx.data_manager().GetNewChannel()),
          emitters_(channel_->OpenWriters()),
          parent_(parent)
    {
        auto pre_op_function = [=](ValueType input) {
                                   PreOp(input);
                               };

        // close the function stack with our pre op and register it at parent
        // node for output
        lop_chain_ = parent_stack.push(pre_op_function).emit();
        parent_->RegisterChild(lop_chain_);
    }

    void PreOp(ValueType element) {
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i](element);
        }
    }

    virtual ~AllGatherNode() { 
        parent_->UnregisterChild(lop_chain_);
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

    data::ChannelSPtr channel_;
    std::vector<data::BlockWriter> emitters_;

    std::shared_ptr<DIANode<ParentInput>> parent_; 
    common::delegate<void(ParentInput)> lop_chain_; 

    static const bool debug = false;
};

template <typename ValueType, typename Stack>
std::vector<ValueType> DIARef<ValueType, Stack>::AllGather()  const {

    using AllGatherResultNode = AllGatherNode<ValueType, Stack>;

    std::vector<ValueType> output;

    auto shared_node =
        std::make_shared<AllGatherResultNode>(
            node_->context(), node_, stack_, &output);

    core::StageBuilder().RunScope(shared_node.get());

    return std::move(output);
}

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::AllGather(
    std::vector<ValueType>* out_vector)  const {

    using AllGatherResultNode = AllGatherNode<ValueType, Stack>;

    auto shared_node =
        std::make_shared<AllGatherResultNode>(
            node_->context(), node_, stack_, out_vector);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_ALLGATHER_HEADER

/******************************************************************************/
