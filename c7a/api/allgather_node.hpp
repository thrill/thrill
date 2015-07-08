/*******************************************************************************
 * c7a/api/allgather_node.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_ALLGATHER_NODE_HEADER
#define C7A_API_ALLGATHER_NODE_HEADER

#include <c7a/common/future.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/data/manager.hpp>
#include <string>
#include "action_node.hpp"
#include "dia_node.hpp"
#include "function_stack.hpp"

namespace c7a {
namespace api {

template <typename Input, typename Output, typename Stack>
class AllGatherNode : public ActionNode<Input>
{
public:
    using Super = ActionNode<Input>;
    using Super::context_;
    using Super::data_id_;

    AllGatherNode(Context& ctx,
                  DIANode<Input>* parent, //TODO(??) don't we need to pass shared ptrs for the ref counting?
                  Stack& stack,
                  std::vector<Output>* out_vector
                  )
        : ActionNode<Input>(ctx, { parent }),
          local_stack_(stack),
          out_vector_(out_vector),
          channel_used_(ctx.get_data_manager().AllocateNetworkChannel())
    {
        emitters_ = context_.
                    get_data_manager().template GetNetworkEmitters<Output>(channel_used_);

        auto pre_op_function = [=](Output input) {
                                   PreOp(input);
                               };
        auto lop_chain = local_stack_.push(pre_op_function).emit();
        parent->RegisterChild(lop_chain);
    }

    void PreOp(Output element) {
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

        auto it = context_.get_data_manager().template GetIterator<Output>(channel_used_);

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
    //! Local stack
    Stack local_stack_;

    std::vector<Output>* out_vector_;

    data::ChannelId channel_used_;

    static const bool debug = false;

    std::vector<data::Emitter<Output> > emitters_;
};

template <typename NodeType, typename CurrentType, typename Stack>
template <typename Out>
void DIARef<NodeType, CurrentType, Stack>::AllGather(std::vector<Out>* out_vector) {

    using AllGatherResultNode = AllGatherNode<NodeType, Out, decltype(local_stack_)>;

    auto shared_node =
        std::make_shared<AllGatherResultNode>(node_->get_context(),
                                              node_.get(),
                                              local_stack_,
                                              out_vector);

    core::StageBuilder().RunScope(shared_node.get());
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_ALLGATHER_NODE_HEADER

/******************************************************************************/
