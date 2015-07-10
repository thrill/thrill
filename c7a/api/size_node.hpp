/*******************************************************************************
 * c7a/api/size_node.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SIZE_NODE_HEADER
#define C7A_API_SIZE_NODE_HEADER

#include "action_node.hpp"
#include "function_stack.hpp"
#include "dia.hpp"
#include <c7a/net/group.hpp>
#include <c7a/net/collective_communication.hpp>
#include <c7a/net/flow_control_channel.hpp>
#include <c7a/net/flow_control_manager.hpp>

namespace c7a {
namespace api {

template <typename ValueType, typename ParentStack>
class SizeNode : public ActionNode
{
    static const bool debug = false;

    using Super = ActionNode;
    using Super::context_;
    using Super::data_id_;
    using SumArg0 = ValueType;

    using ParentInput = typename ParentStack::Input;

public:
    SizeNode(Context& ctx,
             std::shared_ptr<DIANode<ParentInput>> parent,
             ParentStack& parent_stack)
        : ActionNode(ctx, { parent })
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](ValueType input) {
                             PreOp(input);
                         };

        auto lop_chain = parent_stack.push(pre_op_fn).emit();
        parent->RegisterChild(lop_chain);
    }

    virtual ~SizeNode() { }

    //! Executes the size operation.
    void Execute() override {
        MainOp();
    }

    /*!
     * Returns result of global size.
     * \return result
     */
    auto result() {
        return global_size;
    }

    /*!
     * Returns "[SizeNode]" as a string.
     * \return "[SizeNode]"
     */
    std::string ToString() override {
        return "[SizeNode] Id:" + data_id_.ToString();
    }

private:
    // Local size to be used.
    size_t local_size = 0;
    // Global size resulting from all reduce.
    size_t global_size = 0;

    void PreOp(ValueType input) { }

    void MainOp() {
        // get the number of elements that are stored on this worker
        data::Manager& manager = context_.get_data_manager();
        local_size = manager.GetNumElements(manager.AllocateDIA());

        LOG << "MainOp processing";
        net::FlowControlChannel& channel = context_.get_flow_control_channel();

        // process the reduce
        global_size = channel.AllReduce(local_size, [](size_t in1, size_t in2) {
                                            return in1 + in2;
                                        });
    }

    void PostOp() { }
};

template <typename ValueType, typename Stack>
auto DIARef<ValueType, Stack>::Size() {
    using SizeResultNode
              = SizeNode<ValueType, Stack>;

    auto shared_node
        = std::make_shared<SizeResultNode>(node_->get_context(),
                                           node_,
                                           local_stack_);

    core::StageBuilder().RunScope(shared_node.get());
    return shared_node.get()->result();
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_SIZE_NODE_HEADER

/******************************************************************************/
