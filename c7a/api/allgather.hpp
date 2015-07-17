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

#include <c7a/common/future.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/data/manager.hpp>
#include <c7a/api/action_node.hpp>
#include <c7a/api/dia_node.hpp>
#include <c7a/api/function_stack.hpp>

#include <string>
#include <vector>

namespace c7a {
namespace api {

template <typename ValueType, typename ParentStack>
class AllGatherNode : public ActionNode
{
public:
    using Super = ActionNode;
    using Super::context_;
    using Super::data_id_;

    using ParentInput = typename ParentStack::Input;

    AllGatherNode(Context& ctx,
                  std::shared_ptr<DIANode<ParentInput> > parent,
                  const ParentStack& parent_stack,
                  std::vector<ValueType>* out_vector
                  )
        : ActionNode(ctx, { parent }),
          out_vector_(out_vector),
          channel_used_(ctx.data_manager().AllocateChannelId())
    {
        emitters_ = context_.data_manager().
                    template GetNetworkEmitters<ValueType>(channel_used_);

        auto pre_op_function = [ = ](ValueType input) {
                                   PreOp(input);
                               };

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent_stack.push(pre_op_function).emit();
        parent->RegisterChild(lop_chain);
    }

    void PreOp(ValueType element) {
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i](element);
        }
    }

    virtual ~AllGatherNode() { }

    //! Closes the output file
    void Execute() override {
        //data has been pushed during pre-op -> close emitters
        for (size_t i = 0; i < emitters_.size(); i++) {
            emitters_[i].Close();
        }

        auto it = context_.data_manager().template GetIterator<ValueType>(channel_used_);

        do {
            it.WaitForMore();
            while (it.HasNext()) {
                out_vector_->push_back(it.Next());
            }
        } while (!it.IsFinished());
    }

    /*!
     * Returns "[AllGatherNode]" and its id as a string.
     * \return "[AllGatherNode]"
     */
    std::string ToString() override {
        return "[AllGatherNode] Id: " + data_id_.ToString();
    }

private:
    std::vector<ValueType>* out_vector_;

    data::ChannelId channel_used_;

    static const bool debug = false;

    std::vector<data::Emitter> emitters_;
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::AllGather(
    std::vector<ValueType>* out_vector)  const {

    using AllGatherResultNode = AllGatherNode<ValueType, Stack>;

    auto shared_node =
        std::make_shared<AllGatherResultNode>(node_->context(),
                                              node_,
                                              stack_,
                                              out_vector);

    core::StageBuilder().RunScope(shared_node.get());
}
} // namespace api
} // namespace c7a

#endif // !C7A_API_ALLGATHER_HEADER

/******************************************************************************/
