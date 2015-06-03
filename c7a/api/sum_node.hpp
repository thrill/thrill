/*******************************************************************************
 * c7a/api/sum_node.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_SUM_NODE_HEADER
#define C7A_API_SUM_NODE_HEADER

#include "action_node.hpp"
#include "function_stack.hpp"
#include <c7a/net/group.hpp>
#include <c7a/net/collective_communication.hpp>

namespace c7a {

template <typename Input, typename Output, typename Stack, typename SumFunction>
class SumNode : public ActionNode<Input>
{
    static const bool debug = false;

    using sum_arg_0_t = typename FunctionTraits<SumFunction>::template arg<0>;

public:
    SumNode(Context& ctx,
            DIANode<Input>* parent,
            Stack& stack,
            SumFunction sum_function)
        : ActionNode<Input>(ctx, { parent }),
          stack_(stack),
          sum_function_(sum_function)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](Input input) {
                             PreOp(input);
                         };

        auto lop_chain = stack_.push(pre_op_fn).emit();

        parent->RegisterChild(lop_chain);
    }

    virtual ~SumNode() { }

    //! Executes the sum operation.
    void execute() override {
        MainOp();
    }

    /*!
     * Produces an 'empty' function stack, which only contains the identity emitter function.
     * \return Empty function stack
     */
    auto ProduceStack() {
        // Hook Identity
        auto id_fn = [=](Input t, std::function<void(Input)> emit_func) {
                         return emit_func(t);
                     };

        FunctionStack<> stack;
        return stack.push(id_fn);
    }

    /*!
     * Returns result of global sum.
     * \return result
     */
    auto result() {
        return local_sum;
    }

    /*!
     * Returns "[SumNode]" as a string.
     * \return "[SumNode]"
     */
    std::string ToString() override {
        return "[SumNode] Id:" + this->data_id_.ToString();
    }

private:
    //! Local stack.
    Stack stack_;
    //! The sum function which is applied to two elements.
    SumFunction sum_function_;
    // Local sum to be forwarded to other worker.
    Input local_sum = 0;

    void PreOp(sum_arg_0_t input) {
        LOG << "PreOp: " << input;
        local_sum = sum_function_(local_sum, input);
    }

    void MainOp() {
        LOG << "MainOp processing";
        net::Group& flow_group = (this->context_).get_flow_net_group();

        // process the reduce
        net::ReduceToRoot<Output, SumFunction>(flow_group, local_sum, sum_function_);

        // global barrier
        // TODO(ms): replace prefixsum (used as temporary global barrier)
        // with actual global barrier
        size_t sum = 0;
        net::PrefixSum(flow_group, sum);

        // broadcast to all other workers
        if ((this->context_).rank() == 0)
            net::Broadcast(flow_group, local_sum);
    }

    void PostOp() { }
};

} // namespace c7a

#endif // !C7A_API_SUM_NODE_HEADER

/******************************************************************************/
