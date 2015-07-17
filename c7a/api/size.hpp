/*******************************************************************************
 * c7a/api/size.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SIZE_HEADER
#define C7A_API_SIZE_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/api/dia.hpp>
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
    using Super::parents;
    using Super::result_file_;

    using ParentInput = typename ParentStack::Input;

public:
    SizeNode(Context& ctx,
             const std::shared_ptr<DIANode<ParentInput> >& parent,
             const ParentStack& parent_stack)
        : ActionNode(ctx, { parent })
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const ValueType&) { ++local_size_; };

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
        return global_size_;
    }

    /*!
     * Returns "[SizeNode]" as a string.
     * \return "[SizeNode]"
     */
    std::string ToString() override {
        return "[SizeNode] Id:" + result_file_.ToString();
    }

private:
    // Local size to be used.
    size_t local_size_ = 0;
    // Global size resulting from all reduce.
    size_t global_size_ = 0;

    void PreOp() { }

    void MainOp() {
        // get the number of elements that are stored on this worker
        LOG1 << "MainOp processing, sum: " << local_size_;
        net::FlowControlChannel& channel = context_.flow_control_channel();

        // process the reduce, default argument is SumOp.
        global_size_ = channel.AllReduce(local_size_);
    }

    void PostOp() { }
};

template <typename ValueType, typename Stack>
size_t DIARef<ValueType, Stack>::Size() const {

    using SizeResultNode = SizeNode<ValueType, Stack>;

    auto shared_node
        = std::make_shared<SizeResultNode>(node_->context(), node_, stack_);

    core::StageBuilder().RunScope(shared_node.get());
    return shared_node.get()->result();
}

} // namespace api
} // namespace c7a

#endif // !C7A_API_SIZE_HEADER

/******************************************************************************/
